#include <linux/module.h>
#include <linux/kernel.h>

#include <linux/init.h>
#include <linux/bio.h>
#include <linux/device-mapper.h>
#include <linux/log2.h>
#include <linux/delay.h>
#include <linux/kfifo.h>
#include <linux/ktime.h>
#include <linux/smp.h>
#include <asm/current.h>
#include <linux/sched.h>
#include <linux/spinlock.h>
#include <linux/nvme_ioctl.h>

#include "raizn.h"

// Main workqueue for RAIZN
struct workqueue_struct *raizn_wq;
struct workqueue_struct *raizn_gc_wq;

// Workqueue functions
static void raizn_handle_io_mt(struct work_struct *work);
static void raizn_gc(struct work_struct *work);

// Universal endio for raizn
static void raizn_endio(struct bio *bio);
static void raizn_rebuild_endio(struct bio *bio);
static int raizn_process_stripe_head(struct raizn_stripe_head *sh);
static struct raizn_sub_io *raizn_alloc_md(struct raizn_stripe_head *sh,
					   sector_t lzoneno,
					   struct raizn_dev *dev,
					   raizn_zone_type mdtype, void *data,
					   size_t len);
inline sector_t pba_to_pzone(struct raizn_ctx *ctx, sector_t lba);


enum nvme_zns_send_action {
	NVME_ZNS_ZSA_CLOSE		= 0x1,
	NVME_ZNS_ZSA_FINISH		= 0x2,
	NVME_ZNS_ZSA_OPEN		= 0x3,
	NVME_ZNS_ZSA_RESET		= 0x4,
	NVME_ZNS_ZSA_OFFLINE		= 0x5,
	NVME_ZNS_ZSA_SET_DESC_EXT	= 0x10,
	NVME_ZNS_ZSA_ZRWA_FLUSH		= 0x11,
};
#define NVME_ZNS_MGMT_SEND_ZSASO_SHIFT 9
#define	NVME_ZNS_MGMT_SEND_ZSASO_MASK 0x1
#define	NVME_ZNS_MGMT_SEND_SEL_SHIFT 8
#define	NVME_ZNS_MGMT_SEND_SEL_MASK	0x1
#define NVME_ZNS_MGMT_SEND_ZSA_SHIFT 0
#define NVME_ZNS_MGMT_SEND_ZSA_MASK 0xff

#define NVME_SET(value, name) \
        (((value) & NVME_##name##_MASK) << NVME_##name##_SHIFT)

// void open_zone(struct nvme_passthru_cmd *cmd, __u64 lba, __u32 ns_id, int select_all, __u32 timeout) 
struct nvme_passthru_cmd* reset_zone(struct nvme_passthru_cmd *cmd, __u64 lba, __u32 ns_id,  int select_all, __u32 timeout) 
{
    // struct nvme_passthru_cmd *cmd = NULL;
    // cmd = (struct nvme_passthru_cmd*)kmalloc(sizeof(struct nvme_passthru_cmd), GFP_KERNEL);

    cmd->opcode = 0x79;
    cmd->nsid = ns_id;
    cmd->cdw10 = lba & 0xFFFFFFFF;
    cmd->cdw11 = lba >> 32;
    cmd->cdw13 =  NVME_SET(0, ZNS_MGMT_SEND_ZSASO) |
                    NVME_SET(!!select_all, ZNS_MGMT_SEND_SEL) |
                    NVME_SET(NVME_ZNS_ZSA_RESET, ZNS_MGMT_SEND_ZSA);

    cmd->addr = 0;
    cmd->data_len = 0;
    cmd->timeout_ms = timeout;

    return cmd;
}

// void open_zone(struct nvme_passthru_cmd *cmd, __u64 lba, __u32 ns_id,  int select_all, __u32 timeout) 
struct nvme_passthru_cmd* finish_zone(struct nvme_passthru_cmd *cmd, __u64 lba, __u32 ns_id,  int select_all, __u32 timeout) 
{
    // struct nvme_passthru_cmd *cmd = NULL;
    // cmd = (struct nvme_passthru_cmd*)kmalloc(sizeof(struct nvme_passthru_cmd), GFP_KERNEL);

    cmd->opcode = 0x79;
    cmd->nsid = ns_id;
    cmd->cdw10 = lba & 0xFFFFFFFF;
    cmd->cdw11 = lba >> 32;
    cmd->cdw13 =  NVME_SET(0, ZNS_MGMT_SEND_ZSASO) |
                    NVME_SET(!!select_all, ZNS_MGMT_SEND_SEL) |
                    NVME_SET(NVME_ZNS_ZSA_FINISH, ZNS_MGMT_SEND_ZSA);

    cmd->addr = 0;
    cmd->data_len = 0;
    cmd->timeout_ms = timeout;

    return cmd;
}


static inline raizn_op_t raizn_op(struct bio *bio)
{
	if (bio) {
		switch (bio_op(bio)) {
		case REQ_OP_READ:
			return RAIZN_OP_READ;
		case REQ_OP_WRITE:
			return RAIZN_OP_WRITE;
		case REQ_OP_FLUSH:
			return RAIZN_OP_FLUSH;
		case REQ_OP_DISCARD:
			return RAIZN_OP_DISCARD;
		case REQ_OP_SECURE_ERASE:
			return RAIZN_OP_SECURE_ERASE;
		case REQ_OP_ZONE_OPEN:
			return RAIZN_OP_ZONE_OPEN;
		case REQ_OP_ZONE_CLOSE:
			return RAIZN_OP_ZONE_CLOSE;
		case REQ_OP_ZONE_FINISH:
			return RAIZN_OP_ZONE_FINISH;
		case REQ_OP_ZONE_APPEND:
			return RAIZN_OP_ZONE_APPEND;
		case REQ_OP_ZONE_RESET:
			return RAIZN_OP_ZONE_RESET_LOG;
		case REQ_OP_ZONE_RESET_ALL:
			return RAIZN_OP_ZONE_RESET_ALL;
		}
	}
	return RAIZN_OP_OTHER;
}

static void raizn_record_op(struct raizn_stripe_head *sh)
{
#ifdef PROFILING
	struct raizn_ctx *ctx = sh->ctx;
	if (sh->op == RAIZN_OP_GC) {
		atomic_inc(&ctx->counters.gc_count);
	} else {
		switch (bio_op(sh->orig_bio)) {
		case REQ_OP_READ:
			atomic_inc(&ctx->counters.reads);
			atomic64_add(bio_sectors(sh->orig_bio),
				     &ctx->counters.read_sectors);
			break;
		case REQ_OP_WRITE:
			atomic_inc(&ctx->counters.writes);
			atomic64_add(bio_sectors(sh->orig_bio),
				     &ctx->counters.write_sectors);
			break;
		case REQ_OP_ZONE_RESET:
			atomic_inc(&ctx->counters.zone_resets);
			break;
		case REQ_OP_FLUSH:
			atomic_inc(&ctx->counters.flushes);
			break;
		}
		if (sh->orig_bio->bi_opf & REQ_FUA) {
			atomic_inc(&ctx->counters.fua);
		}
		if (sh->orig_bio->bi_opf & REQ_PREFLUSH) {
			atomic_inc(&ctx->counters.preflush);
		}
	}
#else
	(void)sh;
#endif
}

// block unit (= minimal read/write unit of real device, 4096b or etc.) to sector unit (512b)
// inline sector_t block_to_sector_addr(sector_t block_addr)
// {
//     return (block_addr << DEV_BLOCKSHIFT) >> SECTOR_SHIFT;
// }

// inline sector_t sector_to_block_addr(sector_t sector_addr)
// {
//     return (sector_addr << SECTOR_SHIFT) >> DEV_BLOCKSHIFT;
// }

static inline struct raizn_dev *get_bio_dev(struct raizn_ctx *ctx,
					    struct bio *bio)
{
	int i;
	dev_t bd_dev = bio->bi_bdev->bd_dev;
	for (i = 0; i < ctx->params->array_width; i++) {
		if (ctx->devs[i].dev->bdev->bd_dev == bd_dev) {
			return &ctx->devs[i];
		}
	}
	return NULL;
}

int get_dev_idx(struct raizn_ctx *ctx, struct raizn_dev *dev)
{
	int i;
	for (i = 0; i < ctx->params->array_width; i++) {
		if (&ctx->devs[i] == dev) {
			return i;
		}
	}
	return -1;
}

// From the beginning of the logical zone, which number stripe is LBA in
static inline sector_t lba_to_stripe(struct raizn_ctx *ctx, sector_t lba)
{
#ifdef NON_POW_2_ZONE_SIZE
	return (lba % (ctx->params->lzone_size_sectors)) /
	       ctx->params->stripe_sectors;
#else
	return (lba & (ctx->params->lzone_size_sectors - 1)) /
	       ctx->params->stripe_sectors;
#endif
}
// From the beginning of the logical zone, which number stripe unit is LBA in
static inline sector_t lba_to_su(struct raizn_ctx *ctx, sector_t lba)
{
#ifdef NON_POW_2_ZONE_SIZE
	return (lba % (ctx->params->lzone_size_sectors)) /
	       ctx->params->su_sectors;
#else
	return (lba & (ctx->params->lzone_size_sectors - 1)) >>
	       ctx->params->su_shift;
#endif
}
// Which logical zone number is LBA in
static inline sector_t lba_to_lzone(struct raizn_ctx *ctx, sector_t lba)
{
#ifdef NON_POW_2_ZONE_SIZE
	return lba / ctx->params->lzone_size_sectors;
#else
	return lba >> ctx->params->lzone_shift;
#endif
}
// Which device (index of the device in the array) holds the parity for data written in the stripe containing LBA
// Assuming RAID5 scheme
static int lba_to_parity_dev_idx(struct raizn_ctx *ctx, sector_t lba)
{
	return (lba_to_stripe(ctx, lba) + lba_to_lzone(ctx, lba)) %
	       ctx->params->array_width;
}
// Same as above, but returns the actual device object
static struct raizn_dev *lba_to_parity_dev(struct raizn_ctx *ctx, sector_t lba)
{
	return &ctx->devs[lba_to_parity_dev_idx(ctx, lba)];
}
// Which device holds the data chunk associated with LBA
static struct raizn_dev *lba_to_dev(struct raizn_ctx *ctx, sector_t lba)
{
	sector_t su_position = lba_to_su(ctx, lba) % ctx->params->stripe_width;
#ifndef IGNORE_FULL_PARITY
	if (su_position >= lba_to_parity_dev_idx(ctx, lba)) {
		su_position += 1;
	}
#endif
	return &ctx->devs[su_position];
}
// What is the offset of LBA within the logical zone (in 512b sectors)
static inline sector_t lba_to_lzone_offset(struct raizn_ctx *ctx, sector_t lba)
{
#ifdef NON_POW_2_ZONE_SIZE
	return lba % (ctx->params->lzone_size_sectors);
#else
	return lba & (ctx->params->lzone_size_sectors - 1);
#endif
}
// What is the offset of LBA within the stripe (in 512b sectors)
static inline sector_t lba_to_stripe_offset(struct raizn_ctx *ctx, sector_t lba)
{
	return lba_to_lzone_offset(ctx, lba) % ctx->params->stripe_sectors;
}
// What is the offset of LBA within the stripe unit (in 512b sectors)
static inline sector_t lba_to_su_offset(struct raizn_ctx *ctx, sector_t lba)
{
	return lba_to_lzone_offset(ctx, lba) & (ctx->params->su_sectors - 1);
}
// Same as above, except in bytes instead of sectors
static inline sector_t bytes_to_stripe_offset(struct raizn_ctx *ctx,
					      uint64_t ptr)
{
	return (ptr & ((ctx->params->lzone_size_sectors << SECTOR_SHIFT) - 1)) %
	       (ctx->params->stripe_sectors << SECTOR_SHIFT);
}
// Return the starting LBA for the stripe containing lba (in sectors)
static inline sector_t lba_to_stripe_addr(struct raizn_ctx *ctx, sector_t lba)
{
#ifdef NON_POW_2_ZONE_SIZE
	return (lba_to_lzone(ctx, lba) * ctx->params->lzone_size_sectors) +
	       lba_to_stripe(ctx, lba) * ctx->params->stripe_sectors;
#else
    return (lba_to_lzone(ctx, lba) << ctx->params->lzone_shift) +
	       lba_to_stripe(ctx, lba) * ctx->params->stripe_sectors;
#endif
}

// Logical -> physical default mapping translation helpers
// Simple arithmetic translation from lba to pba,
// assumes all drives have the same zone cap and size
static inline sector_t lba_to_pba_default(struct raizn_ctx *ctx, sector_t lba)
{
	sector_t zone_idx = lba_to_lzone(ctx, lba);
#ifdef NON_POW_2_ZONE_SIZE
	sector_t zone_offset = lba % (ctx->params->lzone_size_sectors);
#else
	sector_t zone_offset = lba & (ctx->params->lzone_size_sectors - 1);
#endif
	sector_t offset = zone_offset & (ctx->params->su_sectors - 1);
	sector_t stripe_id = zone_offset / ctx->params->stripe_sectors;
#ifdef NON_POW_2_ZONE_SIZE
	return (zone_idx * ctx->devs[0].zones[0].len) +
#else
	return (zone_idx << ctx->devs[0].zone_shift) +
#endif
	       stripe_id * ctx->params->su_sectors + offset;
}

inline int get_bio_dev_idx(struct raizn_ctx *ctx,
					    struct bio *bio)
{
    int i;
    if (!bio->bi_bdev)
        return -1;
	dev_t bd_dev = bio->bi_bdev->bd_dev;
	for (i = 0; i < ctx->params->array_width; i++) {
		if (ctx->devs[i].dev->bdev->bd_dev == bio->bi_bdev->bd_dev) {
			return i;
		}
	}
	return -1;
}


inline int raizn_submit_bio(struct raizn_ctx *ctx, char *funcname, struct bio *bio, bool wait)
{
// #if 1
#ifdef DEBUG   
    sector_t dev_pba = bio->bi_iter.bi_sector;
    sector_t stripe_start_lba = (dev_pba >> ctx->params->su_shift) * ctx->params->stripe_sectors;
    struct raizn_zone *lzone = &ctx->zone_mgr.lzones[lba_to_lzone(ctx, stripe_start_lba)];
	char rw[10], devtype[10];
	if (bio==NULL) {
		printk("[raizn_submit_bio] bio is NULL!!");
		goto submit;
	}
	if (op_is_write(bio_op(bio)))
		strcpy(rw, "WRITE");
	else
		strcpy(rw, "READ");
	if (lba_to_parity_dev_idx(ctx, stripe_start_lba) == get_bio_dev(ctx, bio)->idx)
		strcpy(devtype, "PARITY");
	else
		strcpy(devtype, "DATA");

    // if ((lba_to_lzone(ctx, stripe_start_lba) == DEBUG_TARG_ZONE_1) // debug
    //     || (lba_to_lzone(ctx, stripe_start_lba) == DEBUG_TARG_ZONE_2)) // debug
    printk("(%d)[%d:%d] submit_bio from [%s] dev: %d(%s), rw: %s, pba: %lldKB, len: %dKB\n", 
        current->pid, lba_to_lzone(ctx, stripe_start_lba), lba_to_stripe(ctx, stripe_start_lba),
        funcname, get_bio_dev(ctx, bio)->idx, devtype, rw,
        dev_pba/2, bio->bi_iter.bi_size/1024);
#endif //DEBUG
#ifdef RECORD_SUBIO
    struct raizn_sub_io *subio = bio->bi_private;
    if (subio)
        subio->submit_time = ktime_get_ns();
#endif
submit:
	if(unlikely(wait)) {
		return submit_bio_wait(bio);
	}
	else {
		return submit_bio_noacct(bio);
	}
}

#ifdef SMALL_ZONE_AGGR
// aggr zone idx (column)
inline sector_t pba_to_aggr_zone(struct raizn_ctx *ctx, sector_t pba)
{
#ifdef NON_POW_2_ZONE_SIZE
	sector_t pzone_offset = pba % (ctx->devs[0].zones[0].len);
#else
	sector_t pzone_offset = pba & (ctx->devs[0].zones[0].len - 1);
#endif
    return (pzone_offset >> ctx->params->aggr_chunk_shift) & (ctx->params->num_zone_aggr - 1);
}

inline sector_t pba_to_aggr_addr(struct raizn_ctx *ctx, sector_t pba)
{
    // printk("[pba_to_aggr_addr] pba: %lld\n", pba);
    sector_t pzone_idx = pba_to_pzone(ctx, pba);

#ifdef NON_POW_2_ZONE_SIZE
	sector_t pzone_offset = pba % (ctx->devs[0].zones[0].len);
#else
	sector_t pzone_offset = pba & (ctx->devs[0].zones[0].len - 1);
#endif

    sector_t aggr_zone_idx = (pzone_offset / ctx->params->aggr_chunk_sector) & (ctx->params->num_zone_aggr - 1); // row num in 2-dimensional chunk array
	sector_t aggr_stripe_id = (pzone_offset / ctx->params->aggr_chunk_sector) >> ctx->params->aggr_zone_shift; // col num in 2-dimensional chunk array
	sector_t aggr_chunk_offset = pzone_offset & (ctx->params->aggr_chunk_sector - 1);
    // if (pzone_idx == 469) {
    //     printk("ctx->params->aggr_chunk_sector: %d, ctx->params->num_zone_aggr: %d\n",
    //         ctx->params->aggr_chunk_sector,
    //         ctx->params->num_zone_aggr
    //     );
    //     printk("pzone_offset: %lld, aggr_zone_idx: %d, aggr_stripe_id: %d, aggr_chunk_offset: %d, (aggr_stripe_id << ctx->params->aggr_chunk_shift: %lld\n",
    //         pzone_offset,
    //         aggr_zone_idx,
    //         aggr_stripe_id,
    //         aggr_chunk_offset,
    //         (aggr_stripe_id << ctx->params->aggr_chunk_shift)
    //         );
    //     printk("ret: %lld\n", (ctx->devs[0].zones[pzone_idx].start) + // pzone start
    //         aggr_zone_idx * ctx->devs[0].zones[0].phys_len + // aggr_zone start
    //         (aggr_stripe_id << ctx->params->aggr_chunk_shift) + 
    //         aggr_chunk_offset);
    // }
	return (pzone_idx * ctx->params->gap_zone_aggr * ctx->devs[0].zones[0].phys_len) + // pzone start
            aggr_zone_idx * ctx->devs[0].zones[0].phys_len + // aggr_zone start
            (aggr_stripe_id << ctx->params->aggr_chunk_shift) + 
            aggr_chunk_offset;
}

inline int raizn_submit_bio_aggr(struct raizn_ctx *ctx, char *funcname, struct bio *bio, struct raizn_dev *dev, bool wait)
{
    // printk("raizn_submit_bio_aggr");
// #if 1
#ifdef DEBUG   
    if (bio_op(bio) == REQ_OP_ZONE_CLOSE) {
    int targ_zone = 0;
    BUG_ON((dev==NULL));
    int i;
    char rw[10], devtype[10];
    sector_t dev_pba = bio->bi_iter.bi_sector;
    sector_t stripe_start_lba = (dev_pba / ctx->params->su_sectors) * ctx->params->stripe_sectors;
    // if (lba_to_lzone(ctx, stripe_start_lba) == targ_zone) {
    if (1) {
        struct raizn_zone *lzone = &ctx->zone_mgr.lzones[lba_to_lzone(ctx, stripe_start_lba)];
        if (bio==NULL) {
            printk("[raizn_submit_bio] bio is NULL!!");
            goto submit;
        }
        if (op_is_write(bio_op(bio)))
            strcpy(rw, "WRITE");
        else
            strcpy(rw, "READ");
        if (lba_to_parity_dev_idx(ctx, stripe_start_lba) == get_bio_dev(ctx, bio)->idx)
            strcpy(devtype, "PARITY");
        else
            strcpy(devtype, "DATA");

        // if ((lba_to_lzone(ctx, stripe_start_lba) == DEBUG_TARG_ZONE_1) // debug
        //     || (lba_to_lzone(ctx, stripe_start_lba) == DEBUG_TARG_ZONE_2)) // debug
        printk("(%d)[%d:%d] submit_bio from [%s] dev: %d(%s), rw: %s, pba: %lldKB, len: %dKB, op: %d\n", 
            current->pid, lba_to_lzone(ctx, stripe_start_lba), lba_to_stripe(ctx, stripe_start_lba),
            funcname, get_bio_dev(ctx, bio)->idx, devtype, rw,
            dev_pba/2, bio->bi_iter.bi_size/1024,  
            bio_op(bio));
        // printk("pba: %d, stripe_start_lba: %d\n", dev_pba, stripe_start_lba);
        // printk("round_up(bio->bi_iter.bi_sector + 1, AGGR_CHUNK_SECTOR): %lld, bio_end_sector(bio): %lld",
        //     round_up(bio->bi_iter.bi_sector + 1, AGGR_CHUNK_SECTOR), bio_end_sector(bio));
    }
	}
#endif

	struct bio *split;
    int ret;
	sector_t append_pba;
    if (bio_op(bio) == REQ_OP_ZONE_APPEND) {
		// append_pba = bio->bi_iter.bi_sector;
	    mutex_lock(&dev->lock);
        if ((dev->md_azone_wp + bio_sectors(bio)) > dev->zones[0].phys_len) {
            dev->md_azone_idx++;
            dev->md_azone_wp = bio_sectors(bio);
        }
        else
            dev->md_azone_wp += bio_sectors(bio);

        if (dev->md_azone_idx == ctx->params->num_zone_aggr) {
            struct raizn_zone *mdzone = &dev->zones[pba_to_pzone(ctx, bio->bi_iter.bi_sector)];
            atomic64_set(&mdzone->wp, mdzone->start + mdzone->capacity);
            dev->md_azone_idx = 0;
        }

	    mutex_unlock(&dev->lock);
		append_pba = bio->bi_iter.bi_sector + 
			(dev->md_azone_idx << ctx->params->aggr_chunk_shift);
        // printk("append_pba: %llu", append_pba);
		bio->bi_iter.bi_sector = 
			pba_to_aggr_addr(ctx, append_pba);
    }
	else {
		while (round_up(bio->bi_iter.bi_sector + 1, AGGR_CHUNK_SECTOR) <
			bio_end_sector(bio)) {
			sector_t su_boundary = round_up(bio->bi_iter.bi_sector + 1,
				AGGR_CHUNK_SECTOR);
			sector_t chunk_size = su_boundary - bio->bi_iter.bi_sector;

			split = bio_split(bio, chunk_size, GFP_NOIO, &dev->bioset);
			BUG_ON(split==NULL);
			bio_set_dev(split, dev->dev->bdev);
			split->bi_iter.bi_sector = 
				pba_to_aggr_addr(ctx, split->bi_iter.bi_sector);
// #if 1
#ifdef DEBUG   
		// if (lba_to_lzone(ctx, stripe_start_lba) == targ_zone) {
		if (bio_op(bio) == REQ_OP_ZONE_CLOSE) {
			// printk("dev[%d]: %p", dev->idx, dev);
			print_bio_info(ctx, split, "aggr 1");
		}
#endif
			bio_chain(split, bio);
			submit_bio_noacct(split);
    	}
    	bio->bi_iter.bi_sector =
        	pba_to_aggr_addr(ctx, bio->bi_iter.bi_sector);
	}
// #if 1
#ifdef DEBUG   
    // if (lba_to_lzone(ctx, stripe_start_lba) == targ_zone) {
    if (bio_op(bio) == REQ_OP_ZONE_CLOSE) {
        // printk("dev[%d]: %p", get_bio_dev_idx(ctx, bio), get_bio_dev(ctx, bio));
        print_bio_info(ctx, bio, "aggr 2");
    }
#endif
submit:
	if(unlikely(wait)) {
		return submit_bio_wait(bio);
	}
	else {
		return submit_bio_noacct(bio);
	}
}
#endif

static void raizn_queue_gc(struct raizn_dev *dev)
{
	queue_work(raizn_gc_wq, &dev->gc_flush_workers.work);
	// queue_work(raizn_wq, &dev->gc_flush_workers.work);
}

// Constructors and destructors for most data structures
static void raizn_workqueue_deinit(struct raizn_workqueue *wq)
{
	if (kfifo_initialized(&wq->work_fifo)) {
		kfifo_free(&wq->work_fifo);
	}
}

static int raizn_workqueue_init(struct raizn_ctx *ctx,
				struct raizn_workqueue *wq, int num_threads,
				void (*func)(struct work_struct *))
{
	wq->ctx = ctx;
	wq->num_threads = num_threads;
	if (kfifo_alloc(&wq->work_fifo, RAIZN_WQ_MAX_DEPTH, GFP_NOIO)) {
		return -ENOMEM;
	}
	spin_lock_init(&wq->rlock);
	spin_lock_init(&wq->wlock);
	INIT_WORK(&wq->work, func);
	return 0;
}

static void raizn_zone_stripe_buffers_deinit(struct raizn_zone *lzone)
{
	mutex_lock(&lzone->lock);
	if (lzone->stripe_buffers) {
		for (int i = 0; i < STRIPE_BUFFERS_PER_ZONE; ++i) {
			kvfree(lzone->stripe_buffers[i].data);
			lzone->stripe_buffers[i].data = NULL;
		}
		kvfree(lzone->stripe_buffers);
		lzone->stripe_buffers = NULL;
	}
	mutex_unlock(&lzone->lock);
}

static int raizn_zone_stripe_buffers_init(struct raizn_ctx *ctx,
					  struct raizn_zone *lzone)
{
	mutex_lock(&lzone->lock);
	if (lzone->stripe_buffers) {
		mutex_unlock(&lzone->lock);
		return 0;
	}
	lzone->stripe_buffers =
		kcalloc(STRIPE_BUFFERS_PER_ZONE,
			sizeof(struct raizn_stripe_buffer), GFP_NOIO);
	mutex_unlock(&lzone->lock);
	if (!lzone->stripe_buffers) {
		pr_err("Failed to allocate stripe buffers\n");
		return -1;
	}
	for (int i = 0; i < STRIPE_BUFFERS_PER_ZONE; ++i) {
		struct raizn_stripe_buffer *buf = &lzone->stripe_buffers[i];
		buf->data =
			vzalloc(ctx->params->stripe_sectors << SECTOR_SHIFT);
		if (!buf->data) {
			pr_err("Failed to allocate stripe buffer data\n");
			return -1;
		}
		mutex_init(&lzone->stripe_buffers[i].lock);
	}
	return 0;
}

static void raizn_rebuild_mgr_deinit(struct raizn_rebuild_mgr *buf)
{
	kfree(buf->open_zones);
	kfree(buf->incomplete_zones);
}

static int raizn_rebuild_mgr_init(struct raizn_ctx *ctx,
				  struct raizn_rebuild_mgr *mgr)
{
	mutex_init(&mgr->lock);
	mgr->incomplete_zones =
		kzalloc(BITS_TO_BYTES(ctx->params->num_zones), GFP_NOIO);
	mgr->open_zones =
		kzalloc(BITS_TO_BYTES(ctx->params->num_zones), GFP_NOIO);
	if (!mgr->incomplete_zones || !mgr->open_zones) {
		return -ENOMEM;
	}
	return 0;
}

static void raizn_zone_mgr_deinit(struct raizn_ctx *ctx)
{
	for (int zone_idx = 0; zone_idx < ctx->params->num_zones; ++zone_idx) {
		struct raizn_zone *zone = &ctx->zone_mgr.lzones[zone_idx];
		raizn_zone_stripe_buffers_deinit(zone);
		kfree(ctx->zone_mgr.lzones[zone_idx].persistence_bitmap);
	}
	kfree(ctx->zone_mgr.lzones);
	kfree(ctx->zone_mgr.gen_counts);
	raizn_rebuild_mgr_deinit(&ctx->zone_mgr.rebuild_mgr);
}

static int raizn_zone_mgr_init(struct raizn_ctx *ctx)
{
	int ret;
	ctx->zone_mgr.lzones = kcalloc(ctx->params->num_zones,
				       sizeof(struct raizn_zone), GFP_NOIO);
	ctx->zone_mgr.gen_counts = kcalloc(
		roundup(ctx->params->num_zones, RAIZN_GEN_COUNTERS_PER_PAGE) /
			RAIZN_GEN_COUNTERS_PER_PAGE,
		PAGE_SIZE, GFP_NOIO);
	if (!ctx->zone_mgr.lzones || !ctx->zone_mgr.gen_counts) {
		return -ENOMEM;
	}
	for (int zone_num = 0; zone_num < ctx->params->num_zones; ++zone_num) {
		struct raizn_zone *zone = &ctx->zone_mgr.lzones[zone_num];
		int stripe_units_per_zone =
			ctx->params->lzone_capacity_sectors >>
			ctx->params->su_shift;
#ifdef ATOMIC_WP
		atomic64_set(&zone->wp, ctx->params->lzone_size_sectors * zone_num);
		zone->start = atomic64_read(&zone->wp);
#else
		zone->wp = ctx->params->lzone_size_sectors * zone_num;
		zone->start = zone->wp;
#endif
		zone->capacity = ctx->params->lzone_capacity_sectors;
		zone->len = ctx->params->lzone_size_sectors;
		zone->persistence_bitmap = kzalloc(
			BITS_TO_BYTES(stripe_units_per_zone), GFP_KERNEL);
		atomic_set(&zone->cond, BLK_ZONE_COND_EMPTY);
		mutex_init(&zone->lock);
	}
	if ((ret = raizn_rebuild_mgr_init(ctx, &ctx->zone_mgr.rebuild_mgr))) {
		return ret;
	}
	return 0;
}

static int raizn_rebuild_next(struct raizn_ctx *ctx)
{
	struct raizn_rebuild_mgr *mgr = &ctx->zone_mgr.rebuild_mgr;
	int zoneno = -1;
	if (!bitmap_empty(mgr->open_zones, ctx->params->num_zones)) {
		zoneno =
			find_first_bit(mgr->open_zones, ctx->params->num_zones);
		clear_bit(zoneno, mgr->open_zones);
	} else if (!bitmap_empty(mgr->incomplete_zones,
				 ctx->params->num_zones)) {
		zoneno = find_first_bit(mgr->incomplete_zones,
					ctx->params->num_zones);
		clear_bit(zoneno, mgr->incomplete_zones);
	} else {
		ctx->zone_mgr.rebuild_mgr.end = ktime_get();
	}
	return zoneno;
}

static void raizn_rebuild_prepare(struct raizn_ctx *ctx, struct raizn_dev *dev)
{
	struct raizn_rebuild_mgr *rebuild_mgr = &ctx->zone_mgr.rebuild_mgr;
	if (rebuild_mgr->target_dev) { // Already a rebuild in progress
		return;
	}
	rebuild_mgr->target_dev = dev;
	for (int zoneno = 0; zoneno < ctx->params->num_zones; ++zoneno) {
		switch (atomic_read(&ctx->zone_mgr.lzones[zoneno].cond)) {
		case BLK_ZONE_COND_IMP_OPEN:
		case BLK_ZONE_COND_EXP_OPEN:
			printk("zone: %d, cond: %d", zoneno, atomic_read(&ctx->zone_mgr.lzones[zoneno].cond));
			set_bit(zoneno, rebuild_mgr->open_zones);
			break;
		case BLK_ZONE_COND_CLOSED:
		case BLK_ZONE_COND_FULL:
			printk("zone: %d, cond: %d", zoneno, atomic_read(&ctx->zone_mgr.lzones[zoneno].cond));
			set_bit(zoneno, rebuild_mgr->incomplete_zones);
			break;
		default:
			break;
		}
	}
}

static void raizn_stripe_head_free(struct raizn_stripe_head *sh)
{
#ifndef IGNORE_PARITY_BUF
	if ((sh->op == RAIZN_OP_WRITE) || (sh->op == RAIZN_OP_REBUILD_INGEST)) {
		kvfree(sh->parity_bufs);
	}
#else
	if (sh->op == RAIZN_OP_REBUILD_INGEST) {
		kvfree(sh->parity_bufs);
	}
#endif
	/* amazing note: the if statement below occurs 10% throughput loss.
		This function is called from every end of subio. Overhead of conditional statment is supected to large */
// 	if (sh->parity_bufs)
// 		kvfree(sh->parity_bufs);
// 	else
// #ifdef DEBUG
// 		printk("[WARN] raizn_stripe_head_free: parity_buf is NULL\n");
// #endif
	for (int i = 0; i < RAIZN_MAX_SUB_IOS; ++i) {
		if (sh->sub_ios[i]) {
			struct raizn_sub_io *subio = sh->sub_ios[i];
// #ifdef PP_NOWAIT
// 			if (subio->sub_io_type == RAIZN_SUBIO_MD)
// 				continue;
// #endif
			if (subio->defer_put) {
				// mutex_lock(&subio->lock);
				bio_put(subio->bio);
				// mutex_unlock(&subio->lock);
			}
			kvfree(subio->data);
			kvfree(subio);
		} else {
			break;
		}
	}
	kfree(sh);
}

static struct raizn_stripe_head *
raizn_stripe_head_alloc(struct raizn_ctx *ctx, struct bio *bio, raizn_op_t op)
{
	struct raizn_stripe_head *sh =
		kzalloc(sizeof(struct raizn_stripe_head), GFP_NOIO);
	if (!sh) {
		return NULL;
	}
	sh->ctx = ctx;
	sh->orig_bio = bio;
	if (bio) {
		sh->zone = &ctx->zone_mgr.lzones[lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector)];
		// printk("zone start: %lld", sh->zone->start);
	}
	atomic_set(&sh->refcount, 0);
	atomic_set(&sh->subio_idx, -1);
	sh->op = op;
	sh->sentinel.sh = sh;
	sh->sentinel.sub_io_type = RAIZN_SUBIO_OTHER;
	return sh;
}

static void raizn_stripe_head_hold_completion(struct raizn_stripe_head *sh)
{
	atomic_inc(&sh->refcount);
	sh->sentinel.bio = bio_alloc_bioset(GFP_NOIO, 0, &sh->ctx->bioset);
	sh->sentinel.bio->bi_end_io = raizn_endio;
	sh->sentinel.bio->bi_private = &sh->sentinel;
}

static void raizn_stripe_head_release_completion(struct raizn_stripe_head *sh)
{
	bio_endio(sh->sentinel.bio);
}

static struct raizn_sub_io *
raizn_stripe_head_alloc_subio(struct raizn_stripe_head *sh,
			      sub_io_type_t sub_io_type)
{
	struct raizn_sub_io *subio;
#ifdef PP_NOWAIT
	// printk("subiotype: %d\n", sub_io_type);
	if (sub_io_type != RAIZN_SUBIO_MD) {
		atomic_inc(&sh->refcount);
	}
#else
	atomic_inc(&sh->refcount);
	int subio_idx = atomic_inc_return(&sh->subio_idx);
	if (subio_idx >= RAIZN_MAX_SUB_IOS) {
		pr_err("Too many sub IOs generated, please increase RAIZN_MAX_SUB_IOS\n");
		return NULL;
	}
#endif
	subio = kzalloc(sizeof(struct raizn_sub_io), GFP_NOIO);
	BUG_ON(!subio);
#ifndef PP_NOWAIT
	sh->sub_ios[subio_idx] = subio;
#endif
	subio->sub_io_type = sub_io_type;
	subio->sh = sh;
	// mutex_init(&subio->lock);
	return subio;
}

static struct raizn_sub_io *
raizn_stripe_head_add_bio(struct raizn_stripe_head *sh, struct bio *bio,
			  sub_io_type_t sub_io_type)
{
	struct raizn_sub_io *subio =
		raizn_stripe_head_alloc_subio(sh, sub_io_type);
	subio->bio = bio;
	subio->bio->bi_end_io = raizn_endio;
	subio->bio->bi_private = subio;
	return subio;
}

static struct raizn_sub_io *
raizn_stripe_head_alloc_bio(struct raizn_stripe_head *sh,
			    struct bio_set *bioset, int bvecs,
			    sub_io_type_t sub_io_type)
{
	struct raizn_sub_io *subio =
		raizn_stripe_head_alloc_subio(sh, sub_io_type);
	subio->bio = bio_alloc_bioset(GFP_NOIO, bvecs, bioset);
	subio->bio->bi_end_io = raizn_endio;
	subio->bio->bi_private = subio;
	return subio;
}

int init_pzone_descriptor(struct blk_zone *zone, unsigned int idx, void *data)
{
	struct raizn_dev *dev = (struct raizn_dev *)data;
	struct raizn_zone *pzone = &dev->zones[idx];
	mutex_init(&pzone->lock);
	atomic_set(&pzone->cond, zone->cond);
#ifdef ATOMIC_WP
	atomic64_set(&pzone->wp, zone->wp);
	// printk("[DEBUG] pzone_wp: %lld, report_zone_wp: %lld\n",
	// 	atomic64_read(&pzone->wp), zone)
#else
	pzone->wp = zone->wp;
#endif
	pzone->start = zone->start;
	pzone->capacity = zone->capacity;
	pzone->len = zone->len;
	pzone->dev = dev;
	return 0;
}

#ifdef SMALL_ZONE_AGGR
static void reinit_aggr_zones(struct raizn_ctx *ctx, struct raizn_dev *dev)
{
	int i;
	dev->num_zones /= GAP_ZONE_AGGR;
	dev->md_azone_wp = 0;
	dev->md_azone_idx = 0;
	for (i=0; i<dev->num_zones; i++)
	{
		struct raizn_zone *pzone = &dev->zones[i];
		pzone->phys_len = pzone->len;
		pzone->phys_capacity = pzone->capacity;
		atomic64_set(&pzone->wp, atomic64_read(&pzone->wp) * NUM_ZONE_AGGR);
		pzone->start *= NUM_ZONE_AGGR;
		pzone->capacity *= NUM_ZONE_AGGR;
		pzone->len *= NUM_ZONE_AGGR;
	}
}
#endif

static int raizn_init_devs(struct raizn_ctx *ctx)
{
	int ret, zoneno;
	BUG_ON(!ctx);
	for (int dev_idx = 0; dev_idx < ctx->params->array_width; ++dev_idx) {
		struct raizn_dev *dev = &ctx->devs[dev_idx];
		dev->num_zones = blkdev_nr_zones(dev->dev->bdev->bd_disk);
		dev->zones = kcalloc(dev->num_zones, sizeof(struct raizn_zone),
				     GFP_NOIO);
		if (!dev->zones) {
			pr_err("ERROR: %s dev->zones mem allocation failed!\n", __func__);
			return -ENOMEM;
		}
		blkdev_report_zones(dev->dev->bdev, 0, dev->num_zones,
				    init_pzone_descriptor, dev);
#ifdef SMALL_ZONE_AGGR
		reinit_aggr_zones(ctx, dev);
#endif
		ret = bioset_init(&dev->bioset, RAIZN_BIO_POOL_SIZE, 0,
				  BIOSET_NEED_BVECS);
		mutex_init(&dev->lock);
		mutex_init(&dev->bioset_lock);
		dev->zone_shift = ilog2(dev->zones[0].len);
		dev->idx = dev_idx;
		spin_lock_init(&dev->free_wlock);
		spin_lock_init(&dev->free_rlock);
		if ((ret = kfifo_alloc(&dev->free_zone_fifo,
				       RAIZN_RESERVED_ZONES, GFP_NOIO))) {
			pr_err("ERROR: %s kfifo for free zone allocation failed!\n", __func__);
			return ret;
		}
	// printk("dev num zones: %d\n", dev->num_zones);
		if (dev->num_zones <= RAIZN_RESERVED_ZONES) {
			pr_err("ERROR: %s reserved zones are bigger than total zones!\n", __func__);
			return -EINVAL;
		}

		kfifo_reset(&dev->free_zone_fifo);
		for (zoneno = dev->num_zones - 1;
		     zoneno >= dev->num_zones - RAIZN_RESERVED_ZONES;
		     --zoneno) {
			// printk("zoneno: %d\n", zoneno);
			struct raizn_zone *z = &dev->zones[zoneno];
			if (!kfifo_in_spinlocked(&dev->free_zone_fifo, &z, 1,
						 &dev->free_wlock)) {
				pr_err("ERROR: %s kfifo for free zone insert failed!\n", __func__);
				return -EINVAL;
			}
		}
		// printk("RAIZN_ZONE_NUM_MD_TYPES: %d\n", RAIZN_ZONE_NUM_MD_TYPES);
		for (int mdtype = RAIZN_ZONE_MD_GENERAL;
		     mdtype < RAIZN_ZONE_NUM_MD_TYPES; ++mdtype) {
			mutex_init(&dev->md_group_lock[mdtype]);
			// printk("mdtype: %d, addr: %p\n", mdtype, &dev->md_zone[mdtype]);
			if (!kfifo_out_spinlocked(&dev->free_zone_fifo,
						  &dev->md_zone[mdtype], 1,
						  &dev->free_rlock)) {
				return -EINVAL;
			}
			dev->md_zone[mdtype]->zone_type = mdtype;
			pr_info("RAIZN writing mdtype %d to zone %lld (%lld)\n",
				mdtype,
				pba_to_pzone(ctx, dev->md_zone[mdtype]->start),
				dev->md_zone[mdtype]->start);
		}
		raizn_workqueue_init(ctx, &dev->gc_ingest_workers,
				     ctx->num_gc_workers, raizn_gc);
		dev->gc_ingest_workers.dev = dev;
		raizn_workqueue_init(ctx, &dev->gc_flush_workers,
				     ctx->num_gc_workers, raizn_gc);
		dev->gc_flush_workers.dev = dev;
		dev->sb.params = *ctx->params; // Shallow copy is fine
		dev->sb.idx = dev->idx;
	}
	return ret;
}

static int raizn_init_volume(struct raizn_ctx *ctx)
{
	// Validate the logical zone capacity against the array
	int dev_idx;
	BUG_ON(!ctx);
	ctx->params->lzone_size_sectors = 1;
	// Autoset logical zone capacity if necessary
	if (ctx->params->lzone_capacity_sectors == 0) {
		sector_t zone_cap;
		ctx->params->lzone_capacity_sectors =
			ctx->devs[0].zones[0].capacity *
			ctx->params->stripe_width;
		zone_cap = ctx->devs[0].zones[0].capacity;
		ctx->params->num_zones = ctx->devs[0].num_zones;
		for (dev_idx = 0; dev_idx < ctx->params->array_width;
		     ++dev_idx) {
			struct raizn_dev *dev = &ctx->devs[dev_idx];
			if (dev->zones[0].capacity != zone_cap ||
			    dev->num_zones != ctx->params->num_zones) {
				pr_err("Automatic zone capacity only supported for homogeneous arrays.");
				return -1;
			}
		}
	} else {
		pr_err("Adjustable zone capacity is not yet supported.");
		return -1;
	}
#ifdef NON_POW_2_ZONE_SIZE
	ctx->params->lzone_size_sectors = ctx->params->lzone_capacity_sectors;
#else
	// Calculate the smallest power of two that is enough to hold the entire lzone capacity
	while (ctx->params->lzone_size_sectors <
	       ctx->params->lzone_capacity_sectors) {
		ctx->params->lzone_size_sectors *= 2;
	}
#endif
	printk("lzone_size_sectors = %lld\n", ctx->params->lzone_size_sectors);
	printk("lzone_capacity_sectors = %lld\n", ctx->params->lzone_capacity_sectors);
	printk("total cap = %lld\n", ctx->params->lzone_capacity_sectors * ctx->devs[0].num_zones);

	ctx->params->lzone_shift = ilog2(ctx->params->lzone_size_sectors);
	// TODO: change for configurable zone size
	ctx->params->num_zones -= RAIZN_RESERVED_ZONES;
	return 0;
}

/* This function should be callable from any point in the code, and
	 gracefully deallocate any data structures that were allocated.
*/
static void deallocate_target(struct dm_target *ti)
{
	struct raizn_ctx *ctx = ti->private;

	if (!ctx) {
		return;
	}
	if (bioset_initialized(&ctx->bioset)) {
		bioset_exit(&ctx->bioset);
	}

	// deallocate ctx->devs
	if (ctx->devs) {
		for (int devno = 0; devno < ctx->params->array_width; ++devno) {
			struct raizn_dev *dev = &ctx->devs[devno];
			if (dev->dev) {
				dm_put_device(ti, dev->dev);
			}
			if (bioset_initialized(&dev->bioset)) {
				bioset_exit(&dev->bioset);
			}
			kvfree(dev->zones);
			if (kfifo_initialized(&dev->free_zone_fifo)) {
				kfifo_free(&dev->free_zone_fifo);
			}
			raizn_workqueue_deinit(&dev->gc_ingest_workers);
			raizn_workqueue_deinit(&dev->gc_flush_workers);
		}
		kfree(ctx->devs);
	}

	// deallocate ctx->zone_mgr
	raizn_zone_mgr_deinit(ctx);

	kfree(ctx->params);

#ifdef MULTI_FIFO
	int i;
	// for (i=0; i<ctx->num_cpus; i++) {
	for (i=0; i<min(ctx->num_cpus, ctx->num_io_workers); i++) {
		raizn_workqueue_deinit(&ctx->io_workers[i]);
	}
	kfree(ctx->io_workers);
#else
	raizn_workqueue_deinit(&ctx->io_workers);
#endif
	// deallocate ctx
	kfree(ctx);
}

#ifdef RECORD_PP_AMOUNT
void raizn_init_pp_counter(struct raizn_ctx *ctx)
{
	atomic64_set(&ctx->total_write_amount, 0);
	atomic64_set(&ctx->total_write_count, 0);
	atomic64_set(&ctx->pp_volatile, 0);
	atomic64_set(&ctx->pp_permanent, 0);
	atomic64_set(&ctx->gc_migrated, 0);
	atomic64_set(&ctx->gc_count, 0);
}
#endif

int raizn_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
#ifdef IGNORE_PARITY_BUF
	printk("[DEBUG] parity buf is ignored\n");
#endif
#if defined(SEPARATE_WQ)
	printk("SEPARATE_WQ\n");
#endif

#ifdef SMALL_ZONE_AGGR
	printk("Small zone device. Zones are aggregated\n");
#endif

#ifdef MULTI_FIFO
	printk("MULTI_FIFO enabled\n");
#else
	printk("SINGLE_FIFO enabled\n");
#endif	

#ifdef MULTI_PP_ZONE
	printk("Multiple PP zones (%d) enabled\n", RAIZN_ZONE_NUM_MD_TYPES - 1);
#endif
#ifdef SKIP_HDR
	printk("PP Header Skipping\n");
#endif

	int ret = -EINVAL;
	struct raizn_ctx *ctx;
	int idx;
	struct gendisk *disk = dm_disk(dm_table_get_md(ti->table));

	if (argc < NUM_TABLE_PARAMS + MIN_DEVS) {
		ret = -EINVAL;
		ti->error =
			"dm-raizn: Too few arguments <stripe unit (KiB)> <num io workers> <num gc workers> <logical zone capacity in KiB (0 for auto)> [drives]";
		goto err;
	}
	ctx = kzalloc(sizeof(struct raizn_ctx), GFP_NOIO);
	if (!ctx) {
		ti->error = "dm-raizn: Failed to allocate context";
		ret = -ENOMEM;
		goto err;
	}
	ti->private = ctx;
#ifdef DEBUG
	printk("!!num_cpu: %d\n", num_online_cpus());
#endif
	ctx->num_cpus = num_online_cpus();

	ctx->params = kzalloc(sizeof(struct raizn_params), GFP_NOIO);
	if (!ctx->params) {
		ti->error = "dm-raizn: Failed to allocate context params";
		ret = -ENOMEM;
		goto err;
	}
	ctx->params->array_width = argc - NUM_TABLE_PARAMS;
	ctx->params->stripe_width = ctx->params->array_width - NUM_PARITY_DEV;

#ifdef SMALL_ZONE_AGGR
	ctx->params->num_zone_aggr = NUM_ZONE_AGGR;
	ctx->params->gap_zone_aggr = GAP_ZONE_AGGR;
	ctx->params->aggr_chunk_sector = AGGR_CHUNK_SECTOR;
	ctx->params->aggr_zone_shift = ilog2(NUM_ZONE_AGGR);
	ctx->params->aggr_chunk_shift = ilog2(AGGR_CHUNK_SECTOR);
#endif

	// parse arguments
	ret = kstrtoull(argv[0], 0, &ctx->params->su_sectors);
	ctx->params->su_sectors *= 1024; // Convert from KiB to bytes
	if (ret || ctx->params->su_sectors < PAGE_SIZE ||
	    (ctx->params->su_sectors & (ctx->params->su_sectors - 1))) {
		ti->error =
			"dm-raizn: Invalid stripe unit size (must be a power of two and at least 4)";
		goto err;
	}
	ctx->params->su_sectors = ctx->params->su_sectors >>
				  SECTOR_SHIFT; // Convert from bytes to sectors
	ctx->params->stripe_sectors =
		ctx->params->su_sectors * ctx->params->stripe_width;
	ctx->params->su_shift = ilog2(ctx->params->su_sectors);
	ret = kstrtoint(argv[1], 0, &ctx->num_io_workers);
	if (ret) {
		ti->error = "dm-raizn: Invalid num of IO workers";
		goto err;
	}
#ifdef MULTI_FIFO
	int i;
	ctx->io_workers = kcalloc(ctx->num_cpus, sizeof(struct raizn_workqueue), GFP_NOIO);
	// for (i=0; i<ctx->num_cpus; i++) {
	for (i=0; i<min(ctx->num_cpus, ctx->num_io_workers); i++) {
		raizn_workqueue_init(ctx, &ctx->io_workers[i], min(ctx->num_cpus, ctx->num_io_workers),
			     raizn_handle_io_mt);
	}
#else
	raizn_workqueue_init(ctx, &ctx->io_workers, ctx->num_io_workers,
			     raizn_handle_io_mt);
#endif

	ret = kstrtoint(argv[2], 0, &ctx->num_gc_workers);
	if (ret) {
		ti->error = "dm-raizn: Invalid num of GC workers";
		goto err;
	}

	ret = kstrtoull(argv[3], 0, &ctx->params->lzone_capacity_sectors);
	ctx->params->lzone_capacity_sectors *= 1024; // Convert to bytes
	// Logical zone capacity must have an equal number of sectors per data device
	if (ret || ctx->params->lzone_capacity_sectors %
			   (ctx->params->stripe_width * SECTOR_SIZE)) {
		ti->error = "dm-raizn: Invalid logical zone capacity";
		goto err;
	}
	// Convert bytes to sectors
	ctx->params->lzone_capacity_sectors =
		ctx->params->lzone_capacity_sectors >> SECTOR_SHIFT;

	// Lookup devs and set up logical volume
	ctx->devs = kcalloc(ctx->params->array_width, sizeof(struct raizn_dev),
			    GFP_NOIO);
	if (!ctx->devs) {
		ti->error = "dm-raizn: Failed to allocate devices in context";
		ret = -ENOMEM;
		goto err;
	}
	for (idx = 0; idx < ctx->params->array_width; idx++) {
		ret = dm_get_device(ti, argv[NUM_TABLE_PARAMS + idx],
				    dm_table_get_mode(ti->table),
				    &ctx->devs[idx].dev);
		if (ret) {
			ti->error = "dm-raizn: Data device lookup failed";
			goto err;
		}
	}
#ifdef SAMSUNG_MODE
	// ctx->raw_bdev = blkdev_get_by_path(RAW_DEV_NAME, FMODE_READ|FMODE_WRITE|FMODE_EXCL, THIS_MODULE);
	ctx->raw_bdev = blkdev_get_by_path(RAW_DEV_NAME, FMODE_READ|FMODE_WRITE, THIS_MODULE);
	ret = PTR_ERR_OR_ZERO(ctx->raw_bdev);
	if (ret) {
		// 에러 처리
		printk("Error finding device %s\n", RAW_DEV_NAME);
		goto err;
	} else {
		// bdev 사용
	}
	ctx->params->div_capacity = get_capacity(ctx->devs[0].dev->bdev->bd_disk);
	printk("div_capacity: %llu", ctx->params->div_capacity);
#endif

	if (raizn_init_devs(ctx) != 0) {
		goto err;
	}
	bitmap_zero(ctx->dev_status, RAIZN_MAX_DEVS);
	raizn_init_volume(ctx);
	raizn_zone_mgr_init(ctx);
#ifdef RECORD_PP_AMOUNT
	raizn_init_pp_counter(ctx);
#endif

	bioset_init(&ctx->bioset, RAIZN_BIO_POOL_SIZE, 0, BIOSET_NEED_BVECS);
	set_capacity(dm_disk(dm_table_get_md(ti->table)),
		     ctx->params->num_zones *
			     ctx->params->lzone_capacity_sectors);

#ifdef SAMSUNG_MODE
	blk_queue_max_open_zones(disk->queue, SAMSUNG_MAX_OPEN_ZONE / (ctx->params->array_width * ctx->params->num_zone_aggr) - RAIZN_ZONE_NUM_MD_TYPES);
	blk_queue_max_active_zones(disk->queue, SAMSUNG_MAX_OPEN_ZONE / (ctx->params->array_width * ctx->params->num_zone_aggr) - RAIZN_ZONE_NUM_MD_TYPES);
#else
	struct request_queue *raw_dev_queue = bdev_get_queue(ctx->devs[0].dev->bdev);		
	blk_queue_max_open_zones(disk->queue, queue_max_open_zones(raw_dev_queue) - RAIZN_ZONE_NUM_MD_TYPES);
	blk_queue_max_active_zones(disk->queue, queue_max_active_zones(raw_dev_queue) - RAIZN_ZONE_NUM_MD_TYPES);
#endif		 

	raizn_wq = alloc_workqueue(WQ_NAME, WQ_UNBOUND,
	// raizn_wq = alloc_workqueue(WQ_NAME, WQ_UNBOUND | WQ_HIGHPRI,
	// raizn_wq = alloc_workqueue(WQ_NAME, WQ_HIGHPRI,
				   ctx->num_io_workers +  ctx->num_gc_workers);
				//    0);
	// raizn_gc_wq = alloc_workqueue(GC_WQ_NAME, WQ_UNBOUND, ctx->num_gc_workers);
	raizn_gc_wq = alloc_workqueue(GC_WQ_NAME, WQ_UNBOUND, 16);
	for (int dev_idx = 0; dev_idx < ctx->params->array_width; ++dev_idx) {
		struct raizn_dev *dev = &ctx->devs[dev_idx];
		struct bio *bio = bio_alloc_bioset(GFP_NOIO, 1, &dev->bioset);
		struct raizn_zone *mdzone;
		bio_set_op_attrs(bio, REQ_OP_WRITE, REQ_FUA);
		bio_set_dev(bio, dev->dev->bdev);
		if (bio_add_page(bio, virt_to_page(&dev->sb), sizeof(dev->sb),
				 0) != sizeof(dev->sb)) {
			ti->error = "Failed to write superblock";
			ret = -1;
			goto err;
		}
		mdzone = dev->md_zone[RAIZN_ZONE_MD_GENERAL];
#ifdef ATOMIC_WP
		atomic64_add(sizeof(dev->sb), &mdzone->wp);
#else
		mdzone->wp += sizeof(dev->sb);
#endif
		bio->bi_iter.bi_sector = mdzone->start;
#ifdef SMALL_ZONE_AGGR
		if (raizn_submit_bio_aggr(ctx, __func__, bio, dev, 1)) {
#else
		if (raizn_submit_bio(ctx, __func__, bio, 1)) {
#endif
			ti->error = "IO error when writing superblock";
			ret = -1;
			goto err;
		}
		bio_put(bio);
	}
	return 0;

err:
	pr_err("raizn_ctr error: %s\n", ti->error);
	return ret;
}

// DM callbacks
static void raizn_dtr(struct dm_target *ti)
{	
#ifdef RECORD_PP_AMOUNT
	// printk("★★★---pp_volatile: %llu\n", atomic64_read(&ctx->pp_volatile));
	// printk("★★★---pp_permanent: %llu\n", atomic64_read(&ctx->pp_permanent));
	// printk("★★★---gc_migrated: %llu\n", atomic64_read(&ctx->gc_migrated));
	// printk("★★★---gc_count: %llu\n", atomic64_read(&ctx->gc_count));
#endif

	deallocate_target(ti);
	if (raizn_wq) {
		destroy_workqueue(raizn_wq);
	}
	if (raizn_gc_wq) {
		destroy_workqueue(raizn_gc_wq);
	}
}

// Core RAIZN logic

// Returns 0 on success, nonzero on failure
static int raizn_zone_mgr_execute(struct raizn_stripe_head *sh)
{
	struct raizn_ctx *ctx = sh->ctx;
	struct raizn_zone *lzone = &ctx->zone_mgr.lzones[lba_to_lzone(
		ctx, sh->orig_bio->bi_iter.bi_sector)];
	int ret = 0;
	sector_t lzone_wp = 0;
	if (sh->op == RAIZN_OP_WRITE) {
		switch (atomic_read(&lzone->cond)) {
		case BLK_ZONE_COND_FULL:
		case BLK_ZONE_COND_READONLY:
		case BLK_ZONE_COND_OFFLINE:
			ret = -1; // Cannot write to a full or failed zone
			break;
		case BLK_ZONE_COND_EMPTY: // Init buffers for empty zone
			raizn_zone_stripe_buffers_init(ctx, lzone);
		case BLK_ZONE_COND_CLOSED: // Empty and closed transition to imp open
			atomic_set(&lzone->cond, BLK_ZONE_COND_IMP_OPEN);
		case BLK_ZONE_COND_IMP_OPEN:
		case BLK_ZONE_COND_EXP_OPEN:
		default:
#ifdef ATOMIC_WP
			lzone_wp = atomic64_read(&lzone->wp);
#else
			lzone_wp = lzone->wp;
#endif
			// Empty, closed, imp and exp open all perform check to see if zone is now full
			if (sh->status == RAIZN_IO_COMPLETED) {
#ifdef DEBUG
				printk("[DEBUG] %s tid: %d, wp: %lld, bi_sector: %lld, start: %lld, zone: %p\n", 
					__func__, current->pid,
					// atomic64_read(&lzone->wp), bio_sectors(sh->orig_bio),
					lzone->wp, bio_sectors(sh->orig_bio),
					lzone->start, lzone);
#endif
#ifdef ATOMIC_WP
				atomic64_add(bio_sectors(sh->orig_bio), &lzone->wp);
				lzone_wp += bio_sectors(sh->orig_bio);
#else
				lzone->wp += bio_sectors(sh->orig_bio);
				lzone_wp += bio_sectors(sh->orig_bio);
#endif
			} else if (lzone_wp >
				   sh->orig_bio->bi_iter.bi_sector) {
				pr_err("Cannot execute op %d to address %lld < wp %lld\n",
				       bio_op(sh->orig_bio),
				       sh->orig_bio->bi_iter.bi_sector,
				       lzone_wp);
				return -1;
			}
			if (lzone_wp >= lzone->start + lzone->capacity) {
				raizn_zone_stripe_buffers_deinit(lzone);
				atomic_set(&lzone->cond, BLK_ZONE_COND_FULL);
			}
		}
	}
	if (sh->op == RAIZN_OP_ZONE_RESET) {
		switch (atomic_read(&lzone->cond)) {
		case BLK_ZONE_COND_READONLY:
		case BLK_ZONE_COND_OFFLINE:
			ret = -1;
			break;
		case BLK_ZONE_COND_FULL:
		case BLK_ZONE_COND_IMP_OPEN:
		case BLK_ZONE_COND_EXP_OPEN:
		case BLK_ZONE_COND_EMPTY:
		case BLK_ZONE_COND_CLOSED:
		default:
			raizn_zone_stripe_buffers_deinit(
				lzone); // checks for null
			if (sh->status == RAIZN_IO_COMPLETED) {
				atomic_set(&lzone->cond, BLK_ZONE_COND_EMPTY);
#ifdef ATOMIC_WP
				atomic64_set(&lzone->wp, lzone->start);
#else
				lzone->wp = lzone->start;
#endif
			}
		}
	}
	if (sh->op == RAIZN_OP_ZONE_CLOSE) {
		switch (atomic_read(&lzone->cond)) {
		case BLK_ZONE_COND_READONLY:
		case BLK_ZONE_COND_OFFLINE:
			ret = -1;
			break;
		case BLK_ZONE_COND_FULL:
		case BLK_ZONE_COND_IMP_OPEN:
		case BLK_ZONE_COND_EXP_OPEN:
		case BLK_ZONE_COND_EMPTY:
		case BLK_ZONE_COND_CLOSED:
		default:
			// raizn_zone_stripe_buffers_deinit(lzone); // checks for null
			if (sh->status == RAIZN_IO_COMPLETED) {
				atomic_set(&lzone->cond, BLK_ZONE_COND_CLOSED);
			}
		}
	}
	if (sh->op == RAIZN_OP_ZONE_FINISH) {
		switch (atomic_read(&lzone->cond)) {
		case BLK_ZONE_COND_READONLY:
		case BLK_ZONE_COND_OFFLINE:
			ret = -1;
			break;
		case BLK_ZONE_COND_FULL:
		case BLK_ZONE_COND_IMP_OPEN:
		case BLK_ZONE_COND_EXP_OPEN:
		case BLK_ZONE_COND_EMPTY:
		case BLK_ZONE_COND_CLOSED:
		default:
			raizn_zone_stripe_buffers_deinit(lzone); // checks for null
			if (sh->status == RAIZN_IO_COMPLETED) {
				atomic_set(&lzone->cond, BLK_ZONE_COND_FULL);
				atomic64_set(&lzone->wp, lzone->start + lzone->capacity);
			}
		}
	}

	if (op_is_flush(sh->orig_bio->bi_opf)) {
		// Update persistence bitmap, TODO: this only works for writes now
		sector_t start = sh->orig_bio->bi_iter.bi_sector;
		sector_t len = bio_sectors(sh->orig_bio);
		int start_su = lba_to_su(ctx, start);
		int end_su = lba_to_su(ctx, start + len);
		if (start_su < end_su) {
			// Race condition if async reset, but that is not standard
			bitmap_set(lzone->persistence_bitmap, start_su,
				   end_su - start_su);
		}
	}
	return ret;
}

static void raizn_degraded_read_reconstruct(struct raizn_stripe_head *sh)
{
	struct raizn_ctx *ctx = sh->ctx;
	sector_t start_lba = sh->orig_bio->bi_iter.bi_sector;
	sector_t cur_lba = start_lba;
	// Iterate through clone, splitting stripe units that have to be reconstructed
	int failed_dev_idx = find_first_bit(ctx->dev_status, RAIZN_MAX_DEVS);
	while (cur_lba < bio_end_sector(sh->orig_bio)) {
		int parity_dev_idx = lba_to_parity_dev_idx(ctx, cur_lba);
		int failed_dev_su_idx = failed_dev_idx > parity_dev_idx ?
						failed_dev_idx - 1 :
						failed_dev_idx;
		sector_t cur_stripe_start_lba =
			lba_to_stripe_addr(ctx, cur_lba);
		sector_t cur_stripe_failed_su_start_lba =
			cur_stripe_start_lba +
			(failed_dev_su_idx * ctx->params->su_sectors);
		if (parity_dev_idx !=
			    failed_dev_idx // Ignore stripes where the failed device is the parity device
		    &&
		    !(cur_stripe_failed_su_start_lba + ctx->params->su_sectors <
		      start_lba) // Ignore stripes that end before the failed SU
		    &&
		    !(cur_stripe_failed_su_start_lba >
		      bio_end_sector(
			      sh->orig_bio)) // Ignore stripes that start after the failed SU
		) {
			sector_t cur_su_end_lba =
				min(bio_end_sector(sh->orig_bio),
				    cur_stripe_failed_su_start_lba +
					    ctx->params->su_sectors);
			sector_t start_offset = cur_lba - start_lba;
			sector_t len = cur_su_end_lba - cur_lba;
			struct bio *split,
				*clone = bio_clone_fast(sh->orig_bio, GFP_NOIO,
							&ctx->bioset);
			struct bio *temp =
				bio_alloc_bioset(GFP_NOIO, 1, &ctx->bioset);
			void *stripe_units[RAIZN_MAX_DEVS];
			struct raizn_sub_io *subio = sh->sub_ios[0];
			int xor_buf_idx = 0;
			sector_t added;
			BUG_ON(!clone);
			BUG_ON(!temp);
			bio_advance(clone, start_offset << SECTOR_SHIFT);
			if (len < bio_sectors(clone)) {
				split = bio_split(clone, len, GFP_NOIO,
						  &ctx->bioset);
			} else {
				split = clone;
				clone = NULL;
			}
			BUG_ON(!split);
			for (int subio_idx = 0; subio;
			     subio = sh->sub_ios[++subio_idx]) {
				if (subio->sub_io_type == RAIZN_SUBIO_REBUILD &&
				    lba_to_stripe(ctx,
						  subio->header.header.start) ==
					    lba_to_stripe(ctx, cur_lba) &&
				    subio->data) {
					stripe_units[xor_buf_idx++] =
						subio->data;
				}
			}
			if (xor_buf_idx > 1) {
				xor_blocks(xor_buf_idx, len << SECTOR_SHIFT,
					   stripe_units[0], stripe_units);
			}
			if ((added = bio_add_page(
				     temp, virt_to_page(stripe_units[0]),
				     len << SECTOR_SHIFT,
				     offset_in_page(stripe_units[0]))) !=
			    len << SECTOR_SHIFT) {
				sh->orig_bio->bi_status = BLK_STS_IOERR;
				pr_err("Added %lld bytes to temp bio, expected %lld\n",
				       added, len);
			}
			// Copy the data back
			bio_copy_data(split, temp);
			bio_put(split);
			bio_put(temp);
			if (clone) {
				bio_put(clone);
			}
		}
		cur_lba = cur_stripe_start_lba + ctx->params->stripe_sectors;
	}
}

// Physical addr (for each raw dev) to physical zone num
inline sector_t pba_to_pzone(struct raizn_ctx *ctx, sector_t lba)
{
#ifdef NON_POW_2_ZONE_SIZE
	return lba / ctx->devs[0].zones[0].len;
#else
	return lba >> ctx->devs[0].zone_shift;
#endif
}


void print_bio_info(struct raizn_ctx *ctx, struct bio *bio, char *funcname)
{
	if (bio==NULL) {
		printk("bio is NULL!!");
		return;
	}
    sector_t dev_lba = bio->bi_iter.bi_sector;
    sector_t stripe_start_lba = (dev_lba >> ctx->params->su_shift) * ctx->params->stripe_sectors;
	char rw[10], devtype[10];
    int dev_idx;
	if (op_is_write(bio_op(bio)))
		strcpy(rw, "WRITE");
	else
		strcpy(rw, "READ");
    // printk("0 %s", rw);
    if (bio->bi_bdev!=NULL) {
        // printk("1");
        if (lba_to_parity_dev_idx(ctx, stripe_start_lba) == get_bio_dev(ctx, bio)->idx)
            strcpy(devtype, "PARITY");
        else
            strcpy(devtype, "DATA");
        // printk("2");
        dev_idx = get_bio_dev(ctx, bio)->idx;
        // printk("3");
    }
    else {
        // printk("4");
        strcpy(devtype, "UNKNOWN");
        dev_idx = -1;
    }
    // printk("5");
	printk("(%d) [%s] err: %d, dev: %d(%s), rw: %s, lba: %lldKB, len: %dKB, zone: %d, stripe: %d\n", 
		current->pid, funcname, bio->bi_status, dev_idx, devtype, rw,
		dev_lba/2, bio->bi_iter.bi_size/1024, pba_to_pzone(ctx, dev_lba), (dev_lba >> ctx->params->su_shift));
}


static void raizn_endio(struct bio *bio)
{
	// Common endio handles marking subio as failed and deallocation of stripe header
	struct raizn_sub_io *subio = bio->bi_private;
	struct raizn_stripe_head *sh = subio->sh;
	bool defer_put = subio->defer_put;
#ifdef DEBUG
	printk("[DEBUG] %s 1 tid: %d, bi_sector: %lld, len: %lld, subio: %p, refcount: %d\n", 
		__func__, current->pid, bio->bi_iter.bi_sector,
		bio_sectors(bio), bio, atomic_read(&sh->refcount));
#endif
	if (bio->bi_status != BLK_STS_OK) {
		if (subio->zone) {
			sector_t zoneno;
			if (subio->zone->zone_type == RAIZN_ZONE_DATA) {
				zoneno = lba_to_lzone(sh->ctx,
						      subio->zone->start);
			} else {
#ifdef NON_POW_2_ZONE_SIZE
				zoneno = subio->zone->start /
					 subio->zone->dev->zones[0].len;
#else
				zoneno = subio->zone->start >>
					 subio->zone->dev->zone_shift;
#endif
			}
		}
	}
	if (subio->sub_io_type == RAIZN_SUBIO_MD) {
		atomic_dec(&subio->zone->refcount);
#ifdef PP_NOWAIT
		if (subio->defer_put) {
			// mutex_lock(&subio->lock);
			bio_put(subio->bio);
			// mutex_unlock(&subio->lock);
		}
		kvfree(subio->data);
		kvfree(subio);
		return;
#endif
	}
	if (sh->op == RAIZN_OP_REBUILD_INGEST ||
	    sh->op == RAIZN_OP_REBUILD_FLUSH) {
		print_bio_info(sh->ctx, bio, __func__);
		raizn_rebuild_endio(bio);
	} else {
		if (!defer_put) {			
			// mutex_lock(&subio->lock);
			bio_put(bio);
			// mutex_unlock(&subio->lock);
		}
		if (atomic_dec_and_test(&sh->refcount)) {
#ifdef DEBUG
	printk("[DEBUG] %s 1.5 tid: %d, bi_sector: %lld, len: %lld, subio: %p, refcount: %d, orig_bio: %p\n", 
		__func__, current->pid, bio->bi_iter.bi_sector,
		bio_sectors(bio), bio, atomic_read(&sh->refcount), sh->orig_bio);
#endif
			sh->status = RAIZN_IO_COMPLETED;
			if (sh->op == RAIZN_OP_WRITE ||
			    sh->op == RAIZN_OP_ZONE_RESET ||
			    sh->op == RAIZN_OP_ZONE_CLOSE ||
			    sh->op == RAIZN_OP_ZONE_FINISH ||
			    sh->op == RAIZN_OP_FLUSH) {
				raizn_zone_mgr_execute(sh);
			} else if (sh->op == RAIZN_OP_DEGRADED_READ) {
				raizn_degraded_read_reconstruct(sh);
			}
			if (sh->orig_bio) {
#ifdef DEBUG
	printk("[DEBUG] %s bio_endio call\n", __func__);
#endif
				bio_endio(sh->orig_bio);
			}
			if (sh->next) {
				raizn_process_stripe_head(sh->next);
			}
			raizn_stripe_head_free(sh);
		}
	}
#ifdef DEBUG
	printk("[DEBUG] %s 2 tid: %d, bi_sector: %lld, len: %lld, subio: %p, refcount: %d\n", 
		__func__, current->pid, bio->bi_iter.bi_sector,
		bio_sectors(bio), bio, atomic_read(&sh->refcount));
#endif
}

static int buffer_stripe_data(struct raizn_stripe_head *sh, sector_t start,
			      sector_t end)
{
	struct raizn_ctx *ctx = sh->ctx;
	struct raizn_zone *lzone =
		&ctx->zone_mgr.lzones[lba_to_lzone(ctx, start)];
	struct raizn_stripe_buffer *buf =
		&lzone->stripe_buffers[lba_to_stripe(ctx, start) &
				       STRIPE_BUFFERS_MASK];
	sector_t len = end - start;
	size_t bytes_copied = 0;
	struct bio_vec bv;
	struct bvec_iter iter;
	void *pos =
		buf->data + (lba_to_stripe_offset(ctx, start) << SECTOR_SHIFT);
	struct bio *clone =
		bio_clone_fast(sh->orig_bio, GFP_NOIO, &ctx->bioset);
	if (start - sh->orig_bio->bi_iter.bi_sector > 0) {
		bio_advance(clone, (start - sh->orig_bio->bi_iter.bi_sector)
					   << SECTOR_SHIFT);
	}
	mutex_lock(&buf->lock);
	bio_for_each_bvec (bv, clone, iter) {
		uint8_t *data = bvec_kmap_local(&bv);
		size_t copylen =
			min((size_t)bv.bv_len,
			    (size_t)(len << SECTOR_SHIFT) - bytes_copied);
		memcpy(pos, data, copylen);
		kunmap_local(data);
		pos += copylen;
		bytes_copied += copylen;
		if (bytes_copied >= len << SECTOR_SHIFT) {
			break;
		}
	}
	bio_put(clone);
	mutex_unlock(&buf->lock);
	return 0;
}

// dst must be allocated and sufficiently large
// srcoff is the offset within the stripe
// Contents of dst are not included in parity calculation
static size_t raizn_stripe_buffer_parity(struct raizn_ctx *ctx,
					 sector_t start_lba, void *dst)
{
	int i;
	void *stripe_units[RAIZN_MAX_DEVS];
	struct raizn_zone *lzone =
		&ctx->zone_mgr.lzones[lba_to_lzone(ctx, start_lba)];
	struct raizn_stripe_buffer *buf =
		&lzone->stripe_buffers[lba_to_stripe(ctx, start_lba) &
				       STRIPE_BUFFERS_MASK];
	for (i = 0; i < ctx->params->stripe_width; ++i) {
		stripe_units[i] = buf->data +
				  i * (ctx->params->su_sectors << SECTOR_SHIFT);
	}
	xor_blocks(ctx->params->stripe_width,
		   ctx->params->su_sectors << SECTOR_SHIFT, dst, stripe_units);
	return 0;
}

// dst must be allocated and sufficiently large (always a multiple of stripe unit size)
static int raizn_bio_parity(struct raizn_ctx *ctx, struct bio *src, void *dst)
{
	sector_t start_lba = src->bi_iter.bi_sector;
	uint64_t stripe_offset_bytes = lba_to_stripe_offset(ctx, start_lba)
				       << SECTOR_SHIFT;
	uint64_t su_bytes = (ctx->params->su_sectors << SECTOR_SHIFT);
	uint64_t stripe_bytes = (ctx->params->stripe_sectors << SECTOR_SHIFT);
	struct bvec_iter iter;
	struct bio_vec bv;
	bio_for_each_bvec (bv, src, iter) {
		uint8_t *data = bvec_kmap_local(&bv);
		uint8_t *data_end = data + bv.bv_len;
		uint8_t *data_itr = data;
		void *stripe_units[RAIZN_MAX_DEVS];
		size_t su_offset = stripe_offset_bytes & (su_bytes - 1);
		uint64_t su_remaining_bytes =
			su_offset > 0 ? su_bytes - su_offset : 0;
		// Finish the first partial stripe unit
		while (su_remaining_bytes > 0 && data_itr < data_end) {
			uint8_t *border =
				min(data_itr + su_remaining_bytes, data_end);
			size_t chunk_nbytes = border - data_itr;

			uint64_t pos_offset_bytes =
				(stripe_offset_bytes / stripe_bytes) *
					su_bytes +
				su_offset;
			stripe_units[0] = data_itr;
			stripe_units[1] = dst + pos_offset_bytes;
			xor_blocks(2, chunk_nbytes, dst + pos_offset_bytes,
				   stripe_units);
			data_itr += chunk_nbytes;
			stripe_offset_bytes += chunk_nbytes;
			su_offset = stripe_offset_bytes % su_bytes;
			su_remaining_bytes =
				su_offset > 0 ? su_bytes - su_offset : 0;
		}
		// data_itr is aligned on su boundary
		// Finish first partial stripe
		if (data_end >= data_itr + su_bytes &&
		    stripe_offset_bytes % stripe_bytes > 0) {
			size_t stripe_remaining_bytes =
				stripe_bytes -
				(stripe_offset_bytes % stripe_bytes);
			uint64_t pos_offset_bytes =
				(stripe_offset_bytes / stripe_bytes) * su_bytes;
			size_t num_su, i;
			uint8_t *border = data_itr + stripe_remaining_bytes;
			while (border > data_end)
				border -= su_bytes;
			num_su = (border - data_itr) / su_bytes;
			for (i = 0; i < num_su; i++)
				stripe_units[i] = data_itr + i * su_bytes;
			stripe_units[num_su] = dst + pos_offset_bytes;
			xor_blocks(num_su + 1, su_bytes, dst + pos_offset_bytes,
				   stripe_units);
			stripe_offset_bytes += num_su * su_bytes;
			data_itr += num_su * su_bytes;
		}
		// Step 3: Go stripe by stripe, XORing it into the buffer
		while (data_itr + stripe_bytes <= data_end) {
			uint64_t pos_offset_bytes =
				(stripe_offset_bytes / stripe_bytes) * su_bytes;
			int i;
			for (i = 0; i < ctx->params->stripe_width; i++) {
				stripe_units[i] = data_itr + i * su_bytes;
			}
			xor_blocks(ctx->params->stripe_width, su_bytes,
				   dst + pos_offset_bytes, stripe_units);
			data_itr += stripe_bytes;
			stripe_offset_bytes += stripe_bytes;
		}
		// Step 4: consume all of the remaining whole stripe units
		if (data_end >= data_itr + su_bytes) {
			size_t i;
			size_t num_su =
				min((size_t)((data_end - data_itr) / su_bytes),
				    (size_t)(ctx->params->array_width - 2));
			uint64_t pos_offset_bytes =
				(stripe_offset_bytes / stripe_bytes) * su_bytes;
			for (i = 0; i < num_su; i++)
				stripe_units[i] = data_itr + i * su_bytes;
			stripe_units[num_su] = dst + pos_offset_bytes;
			xor_blocks(num_su + 1, su_bytes, dst + pos_offset_bytes,
				   stripe_units);
			data_itr += num_su * su_bytes;
			stripe_offset_bytes += num_su * su_bytes;
		}
		// Step 5: go from the end of the last stripe unit border to the mid stripe border, XOR it into the buffer
		if (data_end - data_itr > 0) {
			uint64_t pos_offset_bytes =
				(stripe_offset_bytes / stripe_bytes) * su_bytes;
			size_t chunk_nbytes = data_end - data_itr;
			stripe_units[0] = data_itr;
			stripe_units[1] = dst + pos_offset_bytes;
			xor_blocks(2, chunk_nbytes, dst + pos_offset_bytes,
				   stripe_units);
			stripe_offset_bytes += chunk_nbytes;
		}
		kunmap_local(data);
	}
	return 0;
}

static void raizn_rebuild_read_next_stripe(struct raizn_stripe_head *sh)
{
	struct raizn_ctx *ctx = sh->ctx;
	struct raizn_dev *rebuild_dev = ctx->zone_mgr.rebuild_mgr.target_dev;
	raizn_stripe_head_hold_completion(sh);
	BUG_ON(ctx->params->stripe_sectors << SECTOR_SHIFT >
	       1 << KMALLOC_SHIFT_MAX);
	// Reuse parity bufs to hold the entire data for this IO
	sh->lba = ctx->zone_mgr.rebuild_mgr.rp;
	ctx->zone_mgr.rebuild_mgr.rp += ctx->params->stripe_sectors;
	sh->parity_bufs =
		kzalloc(ctx->params->stripe_sectors << SECTOR_SHIFT, GFP_NOIO);
	if (!sh->parity_bufs) {
		pr_err("Fatal error: failed to allocate rebuild buffer\n");
	}
	// Iterate and map each buffer to a device bio
	for (int bufno = 0; bufno < ctx->params->stripe_width; ++bufno) {
		void *bio_data =
			sh->parity_bufs +
			bufno * (ctx->params->su_sectors << SECTOR_SHIFT);
		struct raizn_dev *dev = bufno >= rebuild_dev->idx ?
						&ctx->devs[bufno + 1] :
						&ctx->devs[bufno];
		struct raizn_sub_io *subio = raizn_stripe_head_alloc_bio(
			sh, &dev->bioset, 1, RAIZN_SUBIO_REBUILD);
		bio_set_op_attrs(subio->bio, REQ_OP_READ, 0);
		bio_set_dev(subio->bio, dev->dev->bdev);
		if (bio_add_page(subio->bio, virt_to_page(bio_data),
				 ctx->params->su_sectors << SECTOR_SHIFT,
				 offset_in_page(bio_data)) !=
		    ctx->params->su_sectors << SECTOR_SHIFT) {
			pr_err("Fatal error: failed to add pages to rebuild read bio\n");
		}
		subio->bio->bi_iter.bi_sector =
			lba_to_pba_default(ctx, sh->lba);
#ifdef SMALL_ZONE_AGGR
		raizn_submit_bio_aggr(ctx, __func__, subio->bio, dev, 0);
#else
		raizn_submit_bio(ctx, __func__, subio->bio, 0);
#endif
	}
	raizn_stripe_head_release_completion(sh);
}

static void raizn_rebuild_endio(struct bio *bio)
{
	struct raizn_sub_io *subio = bio->bi_private;
	struct raizn_stripe_head *sh = subio->sh;
	sector_t lba = sh->lba;
	struct raizn_ctx *ctx = sh->ctx;
	struct raizn_dev *dev = ctx->zone_mgr.rebuild_mgr.target_dev;
	bio_put(bio);
	// printk("refcount: %d", sh->refcount);
	if (atomic_dec_and_test(&sh->refcount)) {
		if (sh->op == RAIZN_OP_REBUILD_INGEST) {
			// Queue a flush
			sh->op = RAIZN_OP_REBUILD_FLUSH;
			kfifo_in_spinlocked(
				&dev->gc_flush_workers.work_fifo, &sh, 1,
				&dev->gc_flush_workers.wlock);
			// while (kfifo_in_spinlocked(
			// 	       &dev->gc_flush_workers.work_fifo, &sh, 1,
			// 	       &dev->gc_flush_workers.wlock) < 1) {
			// }
			queue_work(raizn_gc_wq, &dev->gc_flush_workers.work);
			// queue_work(raizn_wq, &dev->gc_flush_workers.work);
		} else {
			struct raizn_zone *lzone =
				&ctx->zone_mgr.lzones[lba_to_lzone(ctx, lba)];
			raizn_stripe_head_free(sh);
#ifdef ATOMIC_WP
			if (lba + ctx->params->stripe_sectors >= atomic64_read(&lzone->wp)) {
#else
			if (lba + ctx->params->stripe_sectors >= lzone->wp) {
#endif
				sh = raizn_stripe_head_alloc(
					ctx, NULL, RAIZN_OP_REBUILD_INGEST);
				kfifo_in_spinlocked(
					&dev->gc_ingest_workers.work_fifo,
					&sh, 1,
					&dev->gc_ingest_workers.wlock);
				// while (kfifo_in_spinlocked(
				// 	       &dev->gc_ingest_workers.work_fifo,
				// 	       &sh, 1,
				// 	       &dev->gc_ingest_workers.wlock) <
				//        1) {
				// }
				queue_work(raizn_gc_wq,
				// queue_work(raizn_wq,
					   &dev->gc_ingest_workers.work);
			}
		}
	}
}

// The garbage collector handles garbage collection of device zones as well as rebuilding/reshaping
static void raizn_gc(struct work_struct *work)
{
	// printk("[DEBUG] raizn GC occured!!\n");
	struct raizn_workqueue *wq =
		container_of(work, struct raizn_workqueue, work);
	struct raizn_dev *dev = wq->dev;
	struct raizn_stripe_head *sh;
	struct raizn_zone *gczone;
	sector_t gczone_wp;
	int j, ret;
	while (kfifo_out_spinlocked(&wq->work_fifo, &sh, 1, &wq->rlock)) {
		struct raizn_ctx *ctx = sh->ctx;
		if (sh->op == RAIZN_OP_GC) {
			gczone = sh->zone;
#ifdef ATOMIC_WP
			gczone_wp = atomic64_read(&gczone->wp);
#else	
			gczone_wp = gczone->wp;
#endif
		}
		if (sh->op == RAIZN_OP_GC && gczone_wp > gczone->start) {
			// printk("[DEBUG] %s sh->op: %d, gczone->start: %lld, gczone->wp: %lld\n",
			// 	__func__, sh->op, gczone->start, gczone_wp);
			if (gczone_wp > gczone->start) {
				// profile_bio(sh);
				BUG_ON((gczone->start * dev->zones[0].len) <
				       ctx->params->num_zones);
#ifdef RECORD_PP_AMOUNT
		        atomic64_inc(&ctx->gc_count);
#endif
#ifdef SAMSUNG_MODE
		        // struct nvme_passthru_cmd *nvme_cmd = kzalloc(sizeof(struct nvme_passthru_cmd), GFP_KERNEL);
				struct block_device *nvme_bdev = ctx->raw_bdev;
				sector_t pzone_base_addr = dev->idx * ctx->params->div_capacity +
					(pba_to_pzone(ctx, gczone->start) * ctx->params->gap_zone_aggr * ctx->devs[0].zones[0].phys_len);
				for (j=0; j<ctx->params->num_zone_aggr; j++) {
					blkdev_zone_mgmt(nvme_bdev,
						REQ_OP_ZONE_FINISH,
						pzone_base_addr + j * ctx->devs[0].zones[0].phys_len,
						ctx->devs[0].zones[0].phys_len,
						GFP_NOIO);
			// 		sector_t cmd_addr = pzone_base_addr + j * ctx->devs[0].zones[0].phys_len;
			// 		finish_zone(nvme_cmd, sector_to_block_addr(cmd_addr), NS_NUM, 0, 0); // 3rd parameter is nsid of device e.g.) nvme0n2 --> 2
			// 		ret = nvme_submit_passthru_cmd_sync(nvme_bdev, nvme_cmd);
			// 		if (ret != 0) {
			// 			printk("[Fail]\tzrwa finish zone ret: %d GCpba: %llu\n", ret, (cmd_addr));
			// 		}
			// #ifdef DEBUG
			// 		else
			// 			printk("[Success]\tzrwa finish zone ret: %d pba: %llu\n", ret, (cmd_addr));
			// #endif
				}
				// kfree(nvme_cmd);
#else
				blkdev_zone_mgmt(dev->dev->bdev,
					REQ_OP_ZONE_FINISH,
					gczone->start,
#ifdef NON_POW_2_ZONE_SIZE
					dev->zones[0].len,
#else
					1 << dev->zone_shift,
#endif
					GFP_NOIO);
#endif
				if (gczone->zone_type ==
				    RAIZN_ZONE_MD_GENERAL) {
					size_t gencount_size =
						PAGE_SIZE *
						roundup(ctx->params->num_zones,
							RAIZN_GEN_COUNTERS_PER_PAGE) /
						RAIZN_GEN_COUNTERS_PER_PAGE;
					struct raizn_sub_io *gencount_io =
						raizn_alloc_md(
							sh, 0, gczone->dev,
							RAIZN_ZONE_MD_GENERAL,
							ctx->zone_mgr.gen_counts,
							gencount_size);
					struct raizn_sub_io *sb_io =
						raizn_alloc_md(
							sh, 0, gczone->dev,
							RAIZN_ZONE_MD_GENERAL,
							&gczone->dev->sb,
							PAGE_SIZE);
					bio_set_op_attrs(gencount_io->bio,
							 REQ_OP_ZONE_APPEND,
							 REQ_FUA);
					bio_set_op_attrs(sb_io->bio,
							 REQ_OP_ZONE_APPEND,
							 REQ_FUA);

#ifdef RECORD_PP_AMOUNT
		        	atomic64_add(gencount_size >> SECTOR_SHIFT, &ctx->pp_permanent);
		        	atomic64_add(PAGE_SIZE >> SECTOR_SHIFT, &ctx->pp_permanent);
		        	atomic64_add(gencount_size >> SECTOR_SHIFT, &ctx->gc_migrated);
		        	atomic64_add(PAGE_SIZE >> SECTOR_SHIFT, &ctx->gc_migrated);
#endif

#ifdef SMALL_ZONE_AGGR
					raizn_submit_bio_aggr(ctx, __func__, gencount_io->bio, gczone->dev, 0);
					raizn_submit_bio_aggr(ctx, __func__, sb_io->bio, gczone->dev, 0);
#else
					raizn_submit_bio(ctx, __func__, gencount_io->bio, 0);
					raizn_submit_bio(ctx, __func__, sb_io->bio, 0);
#endif
				} else if ((gczone->zone_type > RAIZN_ZONE_MD_GENERAL) &&
						(gczone->zone_type < RAIZN_ZONE_NUM_MD_TYPES)
					   ) {
					raizn_stripe_head_hold_completion(sh);
					for (int zoneno = 0;
					     zoneno < ctx->params->num_zones;
					     ++zoneno) {
						struct raizn_zone *lzone =
							&ctx->zone_mgr
								 .lzones[zoneno];
						int cond;
						size_t stripe_offset_bytes;
#ifdef ATOMIC_WP
						sector_t lzone_wp = atomic64_read(&lzone->wp);
#else
						sector_t lzone_wp = lzone->wp;
#endif
						mutex_lock(&lzone->lock);
						cond = atomic_read(
							&lzone->cond);
						stripe_offset_bytes =
							lba_to_stripe_offset(
								ctx, lzone_wp)
							<< SECTOR_SHIFT;
						if ((cond == BLK_ZONE_COND_IMP_OPEN ||
						     cond == BLK_ZONE_COND_EXP_OPEN ||
						     cond == BLK_ZONE_COND_CLOSED) &&
						    stripe_offset_bytes) {
							struct raizn_stripe_buffer *buf =
								&lzone->stripe_buffers
									 [lba_to_stripe(
										  ctx,
										  lzone_wp) &
									  STRIPE_BUFFERS_MASK];
							void *data = kmalloc(
								stripe_offset_bytes,
								GFP_NOIO);
							struct raizn_sub_io
								*sbuf_io;
							BUG_ON(!data);
							memcpy(data, buf->data,
							       stripe_offset_bytes);
							sbuf_io = raizn_alloc_md(
								sh, 0,
								gczone->dev,
								gczone->zone_type,
								data,
								stripe_offset_bytes);
							bio_set_op_attrs(
								sbuf_io->bio,
								REQ_OP_ZONE_APPEND,
								REQ_FUA);
							sbuf_io->data = data;

#ifdef RECORD_PP_AMOUNT
							atomic64_add(stripe_offset_bytes >> SECTOR_SHIFT, &ctx->pp_permanent);
							atomic64_add(stripe_offset_bytes >> SECTOR_SHIFT, &ctx->gc_migrated);
#endif

#ifdef SMALL_ZONE_AGGR
							raizn_submit_bio_aggr(ctx, __func__, sbuf_io->bio, gczone->dev, 0);
#else
							raizn_submit_bio(ctx, __func__, sbuf_io->bio, 0);
#endif
						}
						mutex_unlock(&lzone->lock);
					}
					raizn_stripe_head_release_completion(
						sh);
				} else {
					pr_err("FATAL: Cannot garbage collect zone %lld on dev %d of type %d\n",
					       gczone->start / dev->zones[0].len,
					       gczone->dev->idx,
					       gczone->zone_type);
				}
				int cnt = 0;
				while (atomic_read(&gczone->refcount) > 0) {
#ifdef DEBUG
					printk("## waiting in GC\n");
					msleep(1000);
#endif
					if (cnt > 10000) {// TODO: don't know why sometimes raizn stalls here. (samsung mode, zenfs 10m 4thr 16,16 comp flush)
						// printk("## Skip waiting in GC");
						break;
					}
					cnt++;
					udelay(1);
				}
#ifdef SAMSUNG_MODE
				nvme_bdev = ctx->raw_bdev;
				pzone_base_addr = dev->idx * ctx->params->div_capacity +
					(pba_to_pzone(ctx, gczone->start) * ctx->params->gap_zone_aggr * ctx->devs[0].zones[0].phys_len);
				for (j=0; j<ctx->params->num_zone_aggr; j++) {
					blkdev_zone_mgmt(nvme_bdev,
						REQ_OP_ZONE_RESET,
						pzone_base_addr + j * ctx->devs[0].zones[0].phys_len,
						ctx->devs[0].zones[0].phys_len,
						GFP_NOIO);
				}
#else
				blkdev_zone_mgmt(dev->dev->bdev,
						 REQ_OP_ZONE_RESET,
						 gczone->start,
#ifdef NON_POW_2_ZONE_SIZE
						 dev->zones[0].len,
#else
						 1 << dev->zone_shift,
#endif
						 GFP_NOIO);
#endif
#ifdef ATOMIC_WP
				atomic64_set(&gczone->wp, gczone->start);
#else
				gczone->wp = gczone->start;
#endif
				// gczone->zone_type = RAIZN_ZONE_DATA;
				atomic_set(&gczone->cond, BLK_ZONE_COND_EMPTY);
				ret = kfifo_in_spinlocked(&dev->free_zone_fifo,
						    &gczone, 1,
						    &dev->free_wlock);
				if (!ret) {
					pr_err("ERROR: %s kfifo insert failed!\n", __func__);
					BUG_ON(1);
				}
			}
		} else if (sh->op == RAIZN_OP_REBUILD_INGEST) {
			int next_zone;
			raizn_rebuild_prepare(ctx, dev);
			if ((next_zone = raizn_rebuild_next(ctx)) >= 0) {
				struct raizn_zone *cur_zone =
					&ctx->zone_mgr.lzones[next_zone];
				ctx->zone_mgr.rebuild_mgr.rp =
					next_zone *
					ctx->params->lzone_size_sectors;
#ifdef ATOMIC_WP
				atomic64_set(&ctx->zone_mgr.rebuild_mgr.wp, 
					lba_to_pba_default(
						ctx,
						ctx->zone_mgr.rebuild_mgr.rp));
#else
				ctx->zone_mgr.rebuild_mgr.wp =
					lba_to_pba_default(
						ctx,
						ctx->zone_mgr.rebuild_mgr.rp);
#endif
				printk("zone: %d, rp: %lld, wp: %lld",
					next_zone, ctx->zone_mgr.rebuild_mgr.rp, atomic64_read(&cur_zone->wp));
				while (ctx->zone_mgr.rebuild_mgr.rp <
#ifdef ATOMIC_WP
				       	atomic64_read(&cur_zone->wp)) {
#else
						cur_zone->wp) {
#endif
					struct raizn_stripe_head *next_stripe =
						raizn_stripe_head_alloc(
							ctx, NULL,
							RAIZN_OP_REBUILD_INGEST);
					raizn_rebuild_read_next_stripe(
						next_stripe);
				}
			}
			raizn_stripe_head_free(sh);
		} else if (sh->op == RAIZN_OP_REBUILD_FLUSH) {
			struct raizn_zone *zone =
				&dev->zones[lba_to_lzone(ctx, sh->lba)];
			struct raizn_sub_io *subio =
				raizn_stripe_head_alloc_bio(
					sh, &dev->bioset, 1,
					RAIZN_SUBIO_REBUILD);
			void *stripe_units[RAIZN_MAX_DEVS];
			char *dst;
			for (int i = 0; i < ctx->params->stripe_width; ++i) {
				stripe_units[i] = sh->parity_bufs +
						  i * (ctx->params->su_sectors
						       << SECTOR_SHIFT);
			}
			dst = stripe_units[0];
			BUG_ON(!dst);
			// XOR data
			xor_blocks(ctx->params->stripe_width,
				   ctx->params->su_sectors << SECTOR_SHIFT, dst,
				   stripe_units);
			// Submit write
			bio_set_op_attrs(subio->bio, REQ_OP_WRITE, REQ_FUA);
			bio_set_dev(subio->bio, dev->dev->bdev);
			if (bio_add_page(subio->bio, virt_to_page(dst),
					 ctx->params->su_sectors
						 << SECTOR_SHIFT,
					 offset_in_page(dst)) !=
			    ctx->params->su_sectors << SECTOR_SHIFT) {
				pr_err("Fatal error: failed to add pages to rebuild write bio\n");
			}
#ifdef ATOMIC_WP
			subio->bio->bi_iter.bi_sector = atomic64_read(&zone->wp);
#else
			subio->bio->bi_iter.bi_sector = zone->wp;
#endif
			// print_bio_info(ctx, subio->bio, __func__);
#ifdef SMALL_ZONE_AGGR
			raizn_submit_bio_aggr(ctx, __func__, subio->bio, dev, 0);
#else
			raizn_submit_bio(ctx, __func__, subio->bio, 0);
#endif
			// Update write pointer
#ifdef ATOMIC_WP
			atomic64_add(ctx->params->su_sectors, &zone->wp);
#else
			zone->wp += ctx->params->su_sectors;
#endif
		}
	}
}

// Returns the new zone PBA on success, -1 on failure
// This function invokes the garbage collector
// Caller is responsible for holding dev->lock
struct raizn_zone *raizn_swap_mdzone(struct raizn_stripe_head *sh,
				     struct raizn_dev *dev,
				     raizn_zone_type mdtype,
				     struct raizn_zone *old_md_zone)
{
	struct raizn_zone *new_md_zone;
	int foreground = 0, submitted = 0, ret, j;
	atomic_set(&old_md_zone->cond, BLK_ZONE_COND_FULL);
retry:
	if (!kfifo_out_spinlocked(&dev->free_zone_fifo, &new_md_zone, 1,
				  &dev->free_rlock)) {
		foreground = 1;
		pr_err("Fatal error, no metadata zones remain\n");
		new_md_zone = NULL;
		atomic_set(&old_md_zone->cond, BLK_ZONE_COND_FULL);
		if (!submitted) {
			struct raizn_stripe_head *gc_sh =
				raizn_stripe_head_alloc(sh->ctx, NULL, RAIZN_OP_GC);
			gc_sh->zone = old_md_zone;
			ret = kfifo_in_spinlocked(
				&gc_sh->zone->dev->gc_flush_workers.work_fifo, &gc_sh,
				1, &gc_sh->zone->dev->gc_flush_workers.wlock);
			if (!ret) {
				pr_err("ERROR: %s kfifo insert failed!\n", __func__);
				BUG_ON(1);
			}
			raizn_queue_gc(gc_sh->zone->dev);
		}
#ifdef DEBUG
		msleep(1000);
#else
		usleep_range(2, 5);
#endif
		goto retry;
	}
	dev->md_zone[mdtype] = new_md_zone;
	new_md_zone->zone_type = mdtype;
	// BUG_ON(new_md_zone->start >> dev->zone_shift <
	//        sh->ctx->params->num_zones);
	atomic_set(&old_md_zone->cond, BLK_ZONE_COND_FULL);
	// blkdev_zone_mgmt(dev->dev->bdev,
	// 	REQ_OP_ZONE_OPEN,
	// 	new_md_zone->start,
	// 	1 << dev->zone_shift,
	// 	GFP_NOIO);
	if (!foreground) {
		struct raizn_stripe_head *gc_sh =
			raizn_stripe_head_alloc(sh->ctx, NULL, RAIZN_OP_GC);
		gc_sh->zone = old_md_zone;
		ret = kfifo_in_spinlocked(
			&gc_sh->zone->dev->gc_flush_workers.work_fifo, &gc_sh,
			1, &gc_sh->zone->dev->gc_flush_workers.wlock);
		if (!ret) {
			pr_err("ERROR: %s kfifo insert failed!\n", __func__);
			BUG_ON(1);
		}
		raizn_queue_gc(gc_sh->zone->dev);
	}
	return new_md_zone;
}

// Returns the LBA that the metadata should be written at
// RAIZN uses zone appends, so the LBA will align to a zone start
static struct raizn_zone *raizn_md_lba(struct raizn_stripe_head *sh,
				       struct raizn_dev *dev,
				       raizn_zone_type mdtype,
				       sector_t md_sectors)
{
	struct raizn_zone *mdzone;
	mutex_lock(&dev->lock);
	// mutex_lock(&dev->md_group_lock[mdtype]);
	mdzone = dev->md_zone[mdtype];
#ifndef NON_POW_2_ZONE_SIZE
	BUG_ON(!mdzone);
	BUG_ON((mdzone->start >> dev->zone_shift) < sh->ctx->params->num_zones);
#endif
#ifdef ATOMIC_WP
	atomic64_add(md_sectors, &mdzone->wp);
#else
	mdzone->wp += md_sectors;
#endif
#ifdef ATOMIC_WP
	if (mdzone->start + mdzone->capacity < atomic64_read(&mdzone->wp)) {
#else
	if (mdzone->start + mdzone->capacity < mdzone->wp) {
#endif
		// printk("mdzone type: %d, start: %lld, cap: %lld, wp: %lld\n",
		// 	mdzone->zone_type, mdzone->start, mdzone->capacity, mdzone->wp);
		// mutex_lock(&dev->lock);
		mdzone = raizn_swap_mdzone(sh, dev, mdtype, mdzone);
		// mutex_unlock(&dev->lock);
		if (mdzone == NULL)
			return NULL;
	}
	atomic_inc(&mdzone->refcount);
	mutex_unlock(&dev->lock);
	// mutex_unlock(&dev->md_group_lock[mdtype]);
#ifndef NON_POW_2_ZONE_SIZE
	BUG_ON(mdzone->start >> dev->zone_shift < sh->ctx->params->num_zones);
#endif
	return mdzone;
}

static struct raizn_sub_io *raizn_alloc_md(struct raizn_stripe_head *sh,
					   sector_t lzoneno,
					   struct raizn_dev *dev,
					   raizn_zone_type mdtype, void *data,
					   size_t len)
{
	struct raizn_ctx *ctx = sh->ctx;
	struct raizn_sub_io *mdio = raizn_stripe_head_alloc_bio(
		sh, &dev->bioset, data ? 2 : 1, RAIZN_SUBIO_MD);
	struct bio *mdbio = mdio->bio;
	struct page *p;
	sector_t sectors;
#ifndef SKIP_HDR
	sectors =
		(round_up(len, PAGE_SIZE) + PAGE_SIZE) >>
			SECTOR_SHIFT; // TODO: does round_up round 0 to PAGE_SIZE?
#else
	if (unlikely((mdtype == RAIZN_ZONE_MD_GENERAL))) {
		// printk("RAIZN_ZONE_MD_GENERAL: %d\n", mdtype);
		sectors = (round_up(len, PAGE_SIZE) + PAGE_SIZE) >>	SECTOR_SHIFT; // TODO: does round_up round 0 to PAGE_SIZE?
	}
	else {
		sectors = (round_up(len, PAGE_SIZE)) >>	SECTOR_SHIFT; // TODO: does round_up round 0 to PAGE_SIZE?
	}
#endif
	// printk("len: %d, sectors: %d\n", len, sectors);
	struct raizn_zone *mdzone = raizn_md_lba(
		sh, dev, mdtype, sectors);

	BUG_ON(!mdzone);
	mdio->zone = mdzone;
	mdio->header.header.zone_generation =
		ctx->zone_mgr.gen_counts[lzoneno / RAIZN_GEN_COUNTERS_PER_PAGE]
			.zone_generation[lzoneno % RAIZN_GEN_COUNTERS_PER_PAGE];
	mdio->header.header.magic = RAIZN_MD_MAGIC;
	mdio->dbg = len;
	bio_set_op_attrs(mdbio, REQ_OP_ZONE_APPEND, 0);
	bio_set_dev(mdbio, dev->dev->bdev);
	mdbio->bi_iter.bi_sector = mdzone->start;
#ifdef SKIP_HDR
	if (unlikely((mdtype == RAIZN_ZONE_MD_GENERAL)))
#endif
	{
		p = is_vmalloc_addr(&mdio->header) ? vmalloc_to_page(&mdio->header) :
							virt_to_page(&mdio->header);
		if (bio_add_page(mdbio, p, PAGE_SIZE, offset_in_page(&mdio->header)) !=
			PAGE_SIZE) {
			pr_err("Failed to add md header page\n");
			bio_endio(mdbio);
			return NULL;
		}
	}
	if (data) {
		p = is_vmalloc_addr(data) ? vmalloc_to_page(data) :
					    virt_to_page(data);
		if (bio_add_page(mdbio, p, len, 0) != len) {
			pr_err("Failed to add md data page\n");
			bio_endio(mdbio);
			return NULL;
		}
	}

// #ifndef NON_POW_2_ZONE_SIZE
// 	BUG_ON((mdbio->bi_iter.bi_sector >> dev->zone_shift) <
// 	       ctx->params->num_zones);
// #endif
// 	BUG_ON(((round_up(len, PAGE_SIZE) + PAGE_SIZE) >> SECTOR_SHIFT) <
// 	       bio_sectors(mdbio));
	return mdio;
}

// Header must not be null, but data can be null
// Returns 0 on success, nonzero on failure
static int raizn_write_md(struct raizn_stripe_head *sh, sector_t lzoneno,
			  struct raizn_dev *dev, raizn_zone_type mdtype,
			  void *data, size_t len)
{
	struct raizn_sub_io *mdio =
		raizn_alloc_md(sh, lzoneno, dev, mdtype, data, len);
#ifndef SKIP_HDR
	if (!mdio) {
		pr_err("Fatal: Failed to write metadata\n");
		return -1;
	}
#else
	if (!mdio)
		return 0;
#endif
#ifdef RECORD_PP_AMOUNT
	atomic64_add((len + PAGE_SIZE) >> SECTOR_SHIFT, &sh->ctx->pp_permanent);
#endif
#ifdef TIMING
	uint64_t lba = mdio->bio->bi_iter.bi_sector;
	printk("pp %llu %d %d %d %lld %d\n", 
		ktime_get_ns(), smp_processor_id(), current->pid, get_dev_idx(sh->ctx, dev), lba, bio_sectors(mdio->bio));
#endif
#ifdef IMM_ENDIO_PP
#else
	// print_bio_info(sh->ctx, mdio->bio, __func__);
#ifdef SMALL_ZONE_AGGR
		raizn_submit_bio_aggr(sh->ctx, __func__, mdio->bio, dev, 0);
#else
		raizn_submit_bio(sh->ctx, __func__, mdio->bio, 0);
#endif
#endif
	return 0;
}

// Alloc bio starting at lba if it doesn't exist, otherwise add to existing bio
static struct bio *check_alloc_dev_bio(struct raizn_stripe_head *sh,
				       struct raizn_dev *dev, sector_t lba, sub_io_type_t sub_io_type)
{
	if (sh->bios[dev->idx] &&
	    sh->bios[dev->idx]->bi_vcnt >= RAIZN_MAX_BVECS) {
		sh->bios[dev->idx] = NULL;
	}
	if (!sh->bios[dev->idx]) {
		struct raizn_sub_io *subio = raizn_stripe_head_alloc_bio(
			sh, &dev->bioset, RAIZN_MAX_BVECS, sub_io_type);
		if (!subio) {
			pr_err("Failed to allocate subio\n");
		}
		sh->bios[dev->idx] = subio->bio;
		subio->dev = dev;
		subio->bio->bi_opf = sh->orig_bio->bi_opf;
		subio->bio->bi_iter.bi_sector =
			lba_to_pba_default(sh->ctx, lba);
		bio_set_dev(subio->bio, dev->dev->bdev);
		subio->zone = &dev->zones[lba_to_lzone(sh->ctx, lba)];
	}
	return sh->bios[dev->idx];
}


static int raizn_write_dummy(struct raizn_stripe_head *sh)
{
	udelay(5);
	bio_endio(sh->orig_bio);
	raizn_stripe_head_free(sh);
	return 0;
}


static int raizn_write(struct raizn_stripe_head *sh)
{
	struct raizn_ctx *ctx = sh->ctx;
	sector_t start_lba = sh->orig_bio->bi_iter.bi_sector;
	int start_stripe_id = lba_to_stripe(ctx, start_lba);
	#ifdef RECORD_PP_AMOUNT
	atomic64_add(bio_sectors(sh->orig_bio), &ctx->total_write_amount);
	atomic64_inc(&ctx->total_write_count);
#endif
	// End LBA of the first stripe in this IO
	sector_t leading_stripe_end_lba =
		min(lba_to_stripe_addr(ctx, start_lba) +
			    ctx->params->stripe_sectors,
		    bio_end_sector(sh->orig_bio));

	// Number of sectors in the leading partial stripe, 0 if the first stripe is full or the entire bio is a trailing stripe
	// A leading stripe starts in the middle of a stripe, and can potentially fill the remainder of the stripe
	// A trailing stripe starts at the beginning of a stripe and ends before the last LBA of the stripe
	// *If* the offset within the stripe is nonzero, we take the contents of the first stripe and treat it as a leading substripe
	sector_t leading_substripe_sectors =
		lba_to_stripe_offset(ctx, start_lba) > 0 ?
			leading_stripe_end_lba - start_lba :
			0;
	// Number of sectors in the trailing partial stripe, 0 if the last stripe is full or the entire bio is a leading stripe
	sector_t trailing_substripe_sectors =
		(bio_sectors(sh->orig_bio) - leading_substripe_sectors) %
		ctx->params->stripe_sectors;
#ifdef DEBUG
	printk("%s [1] sh: %p, pid: %d, refcount: %d\n", __func__, sh, current->pid, sh->refcount);
#endif
// #ifdef DEBUG
#if 1
	// printk("lss: %d, tss: %d\n", leading_substripe_sectors, trailing_substripe_sectors);
#endif
	// Maximum number of parity to write. This could be better, as it currently ignores cases where a subset of the final parity is known
	int parity_su = (bio_sectors(sh->orig_bio) - leading_substripe_sectors -
			 trailing_substripe_sectors) /
				ctx->params->stripe_sectors +
			(leading_substripe_sectors > 0 ? 1 : 0) +
			(trailing_substripe_sectors > 0 ? 1 : 0);
	struct bio *bio;
	struct bio_vec bv;
	struct bvec_iter iter;
	struct raizn_dev *dev;
	unsigned int op_flags =
		op_is_flush(bio_op(sh->orig_bio)) ?
			((sh->orig_bio->bi_opf & REQ_FUA) | REQ_PREFLUSH) :
			0;
#ifndef BITMAP_OFF
	// Tracks which devices have been written to by a subio, and which have to be explicitly flushed (if necessary)
	DECLARE_BITMAP(dev_bitmap, RAIZN_MAX_DEVS);
#endif
	raizn_stripe_head_hold_completion(sh);
#ifdef DEBUG
	printk("%s [2] sh: %p, pid: %d, refcount: %d\n", __func__, sh, current->pid, sh->refcount);
#endif
	BUG_ON(bio_sectors(sh->orig_bio) == 0);
	// Allocate buffer to hold all parity
#ifndef IGNORE_PARITY_BUF
	sh->parity_bufs =
		vzalloc(parity_su * (ctx->params->su_sectors << SECTOR_SHIFT));
	if (!sh->parity_bufs) {
		pr_err("Failed to allocate parity buffers\n");
		return DM_MAPIO_KILL;
	}
#else
	// udelay(10);
#endif
#ifndef BITMAP_OFF
	bitmap_zero(dev_bitmap, RAIZN_MAX_DEVS);
#endif
	// Split off any partial stripes
	// Handle leading stripe units
	if (leading_substripe_sectors) {
#ifndef IGNORE_PARITY_BUF
		// Copy stripe data if necessary
		buffer_stripe_data(sh, start_lba, leading_stripe_end_lba);
		// Always calculate full parity, but only use part of it
		raizn_stripe_buffer_parity(ctx, start_lba, sh->parity_bufs);
#endif
		if (lba_to_stripe_offset(ctx, leading_stripe_end_lba) != 0) {
#ifndef IGNORE_PART_PARITY
			size_t leading_substripe_start_offset_bytes =
				lba_to_su_offset(ctx, start_lba)
				<< SECTOR_SHIFT;
			size_t leading_substripe_parity_bytes =
				min(ctx->params->su_sectors,
				    leading_substripe_sectors)
				<< SECTOR_SHIFT;
			// Calculate and submit partial parity if the entire bio is a leading stripe
			raizn_write_md(
				sh,
				lba_to_lzone(ctx,
					     sh->orig_bio->bi_iter.bi_sector),
				lba_to_parity_dev(ctx, start_lba),
#ifdef MULTI_PP_ZONE
				RAIZN_ZONE_MD_PARITY_LOG_1 + 
					(lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector)
					 + lba_to_stripe(ctx, sh->orig_bio->bi_iter.bi_sector))
					 % (RAIZN_ZONE_NUM_MD_TYPES - 1),
#else
				RAIZN_ZONE_MD_PARITY_LOG,
#endif
				sh->parity_bufs +
					leading_substripe_start_offset_bytes,
				leading_substripe_parity_bytes);
#endif
		}
	}
#ifdef DEBUG
	printk("%s [3] sh: %p, pid: %d, refcount: %d\n", __func__, sh, current->pid, sh->refcount);
#endif
	if (bio_sectors(sh->orig_bio) >
	    leading_substripe_sectors + trailing_substripe_sectors) {
		if (leading_substripe_sectors) {
			bio = bio_clone_fast(sh->orig_bio, GFP_NOIO,
					     &ctx->bioset);
			BUG_ON(!bio);
			bio_advance(bio,
				    leading_substripe_sectors << SECTOR_SHIFT);
		} else {
			bio = sh->orig_bio;
		}
#ifndef IGNORE_FULL_PARITY
		raizn_bio_parity(ctx, bio, sh->parity_bufs);
#endif
		if (leading_substripe_sectors) {
			bio_put(bio);
		}
	}
	if (trailing_substripe_sectors) {
		sector_t trailing_substripe_start_lba =
			bio_end_sector(sh->orig_bio) -
			trailing_substripe_sectors;
		size_t trailing_substripe_parity_bytes =
			min(ctx->params->su_sectors, trailing_substripe_sectors)
			<< SECTOR_SHIFT;
#ifndef IGNORE_PARITY_BUF
		// Copy stripe data if necessary
		buffer_stripe_data(sh, trailing_substripe_start_lba,
				   bio_end_sector(sh->orig_bio));
		// Calculate partial parity
		// submit parity log, always starts at offset 0 in parity, may end before su_bytes
		//raizn_stripe_buffer_parity(ctx, trailing_substripe_start_lba, &sh->parity_bufs[parity_su - 1]);
		raizn_stripe_buffer_parity(
			ctx, trailing_substripe_start_lba,
			sh->parity_bufs +
				(parity_su - 1) * ctx->params->su_sectors);
#endif
#ifndef IGNORE_PART_PARITY
		raizn_write_md(
			sh, lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector),
			lba_to_parity_dev(ctx, trailing_substripe_start_lba),
#ifdef MULTI_PP_ZONE
			RAIZN_ZONE_MD_PARITY_LOG_1 + 
				(lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector)
				+ lba_to_stripe(ctx, sh->orig_bio->bi_iter.bi_sector))
				% (RAIZN_ZONE_NUM_MD_TYPES - 1),
#else
			RAIZN_ZONE_MD_PARITY_LOG,
#endif
			//sh->parity_bufs, trailing_substripe_parity_bytes);
			sh->parity_bufs +
				(parity_su - 1) * ctx->params->su_sectors,
			trailing_substripe_parity_bytes);
#endif
	}
#ifdef DEBUG
	printk("%s [4] sh: %p, pid: %d, refcount: %d\n", __func__, sh, current->pid, sh->refcount);
#endif
	// Go stripe by stripe, splitting the bio and adding parity
	// This handles data and parity for the *entire* bio, including leading and trailing substripes
	bio_for_each_bvec (bv, sh->orig_bio, iter) {
		size_t data_pos = 0;
		while (data_pos < bv.bv_len) {
			sector_t lba =
				iter.bi_sector + (data_pos >> SECTOR_SHIFT);
			int stripe_id = lba_to_stripe(ctx, lba);
			size_t su_remaining_bytes =
				(round_up(lba + 1, ctx->params->su_sectors) -
				 lba)
				<< SECTOR_SHIFT;
			size_t su_bytes = ctx->params->su_sectors
					  << SECTOR_SHIFT;
			size_t chunk_bytes =
				min(su_remaining_bytes, bv.bv_len - data_pos);
			sector_t chunk_end_lba =
				lba + (chunk_bytes >> SECTOR_SHIFT);
			dev = lba_to_dev(ctx, lba);
			bio = check_alloc_dev_bio(sh, dev, lba, RAIZN_SUBIO_DATA);
			BUG_ON(!bio);
			BUG_ON(chunk_bytes == 0);
			bio->bi_opf |= op_flags;
			if (bio_add_page(bio, bv.bv_page, chunk_bytes,
					 bv.bv_offset + data_pos) <
			    chunk_bytes) {
				pr_err("Failed to add pages\n");
				goto submit;
			}
#ifndef BITMAP_OFF
			set_bit(dev->idx, dev_bitmap);
#endif
#ifndef IGNORE_FULL_PARITY
			// if (lba_to_lzone(ctx, lba) != lba_to_lzone(ctx, chunk_end_lba))
			// 	printk("lba_to_lzone(ctx, lba) != lba_to_lzone(ctx, chunk_end_lba, %d, %d\n", lba_to_lzone(ctx, lba), lba_to_lzone(ctx, chunk_end_lba));
			// If we write the last sector of a stripe unit, add parity
			if ( stripe_id < lba_to_stripe(ctx, chunk_end_lba) ||
				(lba_to_lzone(ctx, lba) != lba_to_lzone(ctx, chunk_end_lba)) // the last chunk	
			) {
				dev = lba_to_parity_dev(ctx, lba);
				bio = check_alloc_dev_bio(
					sh, dev,
					lba_to_stripe_addr(sh->ctx, lba), RAIZN_SUBIO_PARITY);
				if (bio_add_page(bio,
						 vmalloc_to_page(
							 sh->parity_bufs +
							 (su_bytes *
							  (stripe_id -
							   start_stripe_id))),
						 su_bytes, 0) < su_bytes) {
					pr_err("Failed to add parity pages\n");
					goto submit;
				}
#ifndef BITMAP_OFF
				set_bit(dev->idx, dev_bitmap);
#endif
			}
#endif
			data_pos += chunk_bytes;
		}
	}
#ifdef DEBUG
	printk("%s [5] sh: %p, pid: %d, refcount: %d\n", __func__, sh, current->pid, sh->refcount);
#endif
submit:
	for (int subio_idx = 0; subio_idx <= atomic_read(&sh->subio_idx);
	     ++subio_idx) {
		struct raizn_sub_io *subio = sh->sub_ios[subio_idx];
		struct raizn_zone *zone = subio->zone;
		if ((subio->sub_io_type == RAIZN_SUBIO_DATA) ||
			(subio->sub_io_type == RAIZN_SUBIO_PARITY))  {
			int bio_len = bio_sectors(subio->bio); 

			
			// if (atomic64_read(&subio->zone->wp) < 0) {
			// 	printk("[DEBUG] wp: %lld, bi_sector: %lld\n", 
			// 		subio->zone->wp, subio->bio->bi_iter.bi_sector);
			// 	return -1;
			// }
			// printk("[DEBUG]1 %s tid: %d, wp: %lld, bi_sector: %lld, start: %lld, subio: %p, zone: %p\n", 
			// 	__func__, current->pid,
			// 	atomic64_read(&subio->zone->wp), subio->bio->bi_iter.bi_sector,
			// 	subio->zone->start, subio->bio, subio->zone);
#ifdef DEBUG
			printk("[DEBUG]1 %s tid: %d, wp: %lld, bi_sector: %lld, start: %lld, subio: %p, zone: %p\n", 
				__func__, current->pid,
				// atomic64_read(&subio->zone->wp), subio->bio->bi_iter.bi_sector,
				subio->zone->wp, subio->bio->bi_iter.bi_sector,
				subio->zone->start, subio->bio, subio->zone);
#endif
#ifndef IGNORE_ORDERING
			int count = 0;
#ifdef ATOMIC_WP
			while (atomic64_read(&subio->zone->wp) !=
#else
			while (subio->zone->wp !=
#endif
				       subio->bio->bi_iter.bi_sector) {
#ifdef DEBUG
// #if 1
				printk("[DEBUG]2 %s tid: %d, wp: %lld, bi_sector: %lld, start: %lld, subio: %p, zone: %p\n", 
					__func__, current->pid,
					// atomic64_read(&subio->zone->wp), subio->bio->bi_iter.bi_sector,
					subio->zone->wp, subio->bio->bi_iter.bi_sector,
					subio->zone->start, subio->bio, subio->zone);
				// mdelay(1000);
				msleep(1000);
#else
				// if (count%1000000==0)
				// 	printk("[DEBUG]2 %s tid: %d, wp: %lld, bi_sector: %lld, start: %lld, subio: %p, zone: %p\n", 
				// 		__func__, current->pid,
				// 		atomic64_read(&subio->zone->wp), subio->bio->bi_iter.bi_sector,
				// 		// subio->zone->wp, subio->bio->bi_iter.bi_sector,
				// 		subio->zone->start, subio->bio, subio->zone);
				// count++;
// #ifdef MULTI_FIFO
#if 1
				udelay(2);
#else
				usleep_range(10, 20);
#endif
#endif
				// usleep(2);
			}
#endif
			// mutex_lock(&subio->lock);
#ifdef TIMING
			if (op_is_write(bio_op(subio->bio))) {
				uint64_t lba = subio->bio->bi_iter.bi_sector;
				if (subio->sub_io_type == RAIZN_SUBIO_DATA)
					printk("data %llu %d %d %d %lld %d\n", 
						ktime_get_ns(), smp_processor_id(), current->pid, get_dev_idx(sh->ctx, subio->dev), lba, bio_sectors(subio->bio));
				else if (subio->sub_io_type == RAIZN_SUBIO_PARITY)
					printk("fp %llu %d %d %d %lld %d\n", 
						ktime_get_ns(), smp_processor_id(), current->pid, get_dev_idx(sh->ctx, subio->dev), lba, bio_sectors(subio->bio));
			}
#endif
			#if 0
			// #ifdef DEBUG   
			sector_t dev_lba = subio->bio->bi_iter.bi_sector;
			sector_t stripe_start_lba = (dev_lba >> ctx->params->su_shift) * ctx->params->stripe_sectors;
			char rw[10], devtype[10];
			if (subio->bio==NULL) {
				printk("bio is NULL!!");
				goto submit;
			}
			if (op_is_write(bio_op(subio->bio)))
				strcpy(rw, "WRITE");
			else
				strcpy(rw, "READ");
			if (lba_to_parity_dev_idx(ctx, stripe_start_lba) == get_bio_dev(ctx, subio->bio)->idx)
				strcpy(devtype, "PARITY");
			else
				strcpy(devtype, "DATA");

			printk("(%d) submit_bio dev: %d(%s), rw: %s, lba: %lldKB, len: %dKB, zone: %d, stripe: %d\n", 
				current->pid, get_bio_dev(ctx, subio->bio)->idx, devtype, rw,
				dev_lba/2, subio->bio->bi_iter.bi_size/1024, dev_lba >> ctx->devs[0].zone_shift, (dev_lba >> ctx->params->su_shift));
			#endif
#ifdef SMALL_ZONE_AGGR
			raizn_submit_bio_aggr(ctx, __func__, subio->bio, subio->dev, 0);
#else
			raizn_submit_bio(ctx, __func__, subio->bio, 0);
#endif
			/* There was a bug when "raizn_endio" processed between 
			 submit_bio_noacct(subio->bio) & zone->wp += bio_sectors(subio->bio)
			 "raizn_endio" frees the bio, bio_sectors(subio->bio) becomes to zero.
			 Thus WP isn't increased and the next write request will wait forever in udelay loop.
			 bio_sectors(subio->bio) is stored before the bio is submitted. 
			 After submit, the stored length is added to the zone's WP
			*/

#ifdef DEBUG
			printk("[DEBUG]3 %s tid: %d, wp: %lld, bi_sector: %lld, add: %lld, start: %lld, subio: %p, zone: %p\n", 
				__func__, current->pid,
				// atomic64_read(&subio->zone->wp), subio->bio->bi_iter.bi_sector,
				subio->zone->wp, subio->bio->bi_iter.bi_sector,
				bio_len, subio->zone->start, subio->bio, subio->zone);
#endif
#ifdef ATOMIC_WP
			atomic64_add(bio_len, &zone->wp);
#else
			zone->wp += bio_len;
#endif
			// mutex_unlock(&subio->lock);
			
#ifdef DEBUG
			printk("[DEBUG]4 %s tid: %d, wp: %lld, bi_sector: %lld, add: %lld, start: %lld, subio: %p\n", 
				__func__, current->pid,
				// atomic64_read(&subio->zone->wp), subio->bio->bi_iter.bi_sector,
				subio->zone->wp, subio->bio->bi_iter.bi_sector,
				bio_len, subio->zone->start, subio->bio);
#endif
		}
#ifdef IMM_ENDIO_PP
		else if (subio->sub_io_type == RAIZN_SUBIO_MD) {
			bio_endio(subio->bio);
		}
#endif
	}
#ifdef DEBUG
	printk("%s [6] sh: %p, pid: %d, refcount: %d\n", __func__, sh, current->pid, sh->refcount);
#endif
	/*if (op_is_flush(bio_op(sh->orig_bio))) {
		for_each_clear_bit(dev_idx, dev_bitmap, RAIZN_MAX_DEVS) {
			dev = &ctx->devs[dev_idx];
			if (dev_idx < ctx->params->array_width) {
				// submit flush subio
				struct raizn_sub_io *subio = raizn_stripe_head_alloc_bio(sh, &dev->bioset, 0, RAIZN_SUBIO_DATA);
				bio_set_op_attrs(subio->bio, REQ_OP_FLUSH, REQ_PREFLUSH);
				submit_bio_noacct(subio->bio);
			}
		}
	}*/
	raizn_stripe_head_release_completion(sh);
#ifdef DEBUG
	printk("%s [7] sh: %p, pid: %d, refcount: %d\n", __func__, sh, current->pid, sh->refcount);
#endif
#ifdef DEBUG
	printk("@@end %s %d\n", __func__, current->pid);
#endif
	return DM_MAPIO_SUBMITTED;
}

// Must only be called if the entire bio is handled by the read_simple path
// *Must* be called if the stripe head is of type RAIZN_OP_READ
// Bypasses the normal endio handling using bio_chain
static inline int raizn_read_simple(struct raizn_stripe_head *sh)
{
	struct raizn_ctx *ctx = sh->ctx;
	struct bio *split,
		*clone = bio_clone_fast(sh->orig_bio, GFP_NOIO, &ctx->bioset);
	atomic_set(&sh->refcount, 1);
	clone->bi_private = &sh->sentinel;
	clone->bi_end_io = raizn_endio;
	while (round_up(clone->bi_iter.bi_sector + 1, ctx->params->su_sectors) <
	       bio_end_sector(sh->orig_bio)) {
		sector_t su_boundary = round_up(clone->bi_iter.bi_sector + 1,
						ctx->params->su_sectors);
		sector_t chunk_size = su_boundary - clone->bi_iter.bi_sector;
		struct raizn_dev *dev =
			lba_to_dev(ctx, clone->bi_iter.bi_sector);
		split = bio_split(clone, chunk_size, GFP_NOIO, &dev->bioset);
		bio_set_dev(split, dev->dev->bdev);
		split->bi_iter.bi_sector =
			lba_to_pba_default(ctx, split->bi_iter.bi_sector);
		bio_chain(split, clone);
#ifdef SMALL_ZONE_AGGR
		raizn_submit_bio_aggr(ctx, __func__, split, dev, 0);
#else
		raizn_submit_bio(ctx, __func__, split, 0);
#endif
	}
	// dev = lba_to_dev(ctx, clone->bi_iter.bi_sector);
	// bio_set_dev(clone, dev->dev->bdev);
#ifdef SMALL_ZONE_AGGR
	struct raizn_dev *dev =
		lba_to_dev(ctx, clone->bi_iter.bi_sector);
#endif
	bio_set_dev(clone,
		lba_to_dev(ctx, clone->bi_iter.bi_sector)->dev->bdev);
	clone->bi_iter.bi_sector =
		lba_to_pba_default(ctx, clone->bi_iter.bi_sector);
#ifdef SMALL_ZONE_AGGR
	raizn_submit_bio_aggr(ctx, __func__, clone, dev, 0);
#else
	raizn_submit_bio(ctx, __func__, clone, 0);
#endif
	return DM_MAPIO_SUBMITTED;
}

static int raizn_read(struct raizn_stripe_head *sh)
{
	struct raizn_ctx *ctx = sh->ctx;
	sector_t start_lba = sh->orig_bio->bi_iter.bi_sector;
	// Determine if the read involves rebuilding a missing stripe unit
	if (bitmap_empty(ctx->dev_status, RAIZN_MAX_DEVS)) {
		return raizn_read_simple(sh);
	} else {
		int failed_dev_idx =
			find_first_bit(ctx->dev_status, RAIZN_MAX_DEVS);
		raizn_stripe_head_hold_completion(sh);
		for (sector_t stripe_lba = lba_to_stripe_addr(ctx, start_lba);
		     stripe_lba < bio_end_sector(sh->orig_bio);
		     stripe_lba += ctx->params->stripe_sectors) {
			int parity_dev_idx =
				lba_to_parity_dev_idx(ctx, stripe_lba);
			int failed_dev_su_idx =
				failed_dev_idx > parity_dev_idx ?
					failed_dev_idx - 1 :
					failed_dev_idx;
			sector_t start_su_idx = lba_to_su(ctx, start_lba) %
						ctx->params->stripe_width;
			//sector_t cur_stripe_start_lba = max(start_lba, stripe_lba);
			sector_t cur_stripe_end_lba =
				min(stripe_lba + ctx->params->stripe_sectors,
				    bio_end_sector(sh->orig_bio));
			sector_t end_su_idx =
				lba_to_su(ctx, cur_stripe_end_lba - 1) %
				ctx->params->stripe_width;
			int su_touched = max(
				(sector_t)1,
				end_su_idx -
					start_su_idx); // Cover edge case where only 1 stripe unit is involved in the IO
			bool stripe_degraded = false;
			struct bio *stripe_bio,
				*temp = bio_clone_fast(sh->orig_bio, GFP_NOIO,
						       &ctx->bioset);
			BUG_ON(!temp);
			if (temp->bi_iter.bi_sector < stripe_lba) {
				bio_advance(temp,
					    stripe_lba -
						    temp->bi_iter.bi_sector);
			}
			if (bio_end_sector(temp) > cur_stripe_end_lba) {
				stripe_bio = bio_split(
					temp,
					cur_stripe_end_lba -
						temp->bi_iter.bi_sector,
					GFP_NOIO, &ctx->bioset);
				bio_put(temp);
			} else {
				stripe_bio = temp;
			}
			stripe_bio->bi_private = NULL;
			stripe_bio->bi_end_io = NULL;
			BUG_ON(ctx->params->stripe_sectors == 0);
			// If the failed device is the parity device, the read can operate normally for this stripe
			// Or if the read starts on a stripe unit after the failed device, the read can operate normally for this stripe
			// Or if the read ends on a stripe unit before the failed device, the read can operate normally for this stripe
			stripe_degraded =
				parity_dev_idx != failed_dev_idx &&
				!(stripe_lba < start_lba &&
				  start_su_idx > failed_dev_su_idx) &&
				!((stripe_lba + ctx->params->stripe_sectors) >=
					  bio_end_sector(sh->orig_bio) &&
				  end_su_idx < failed_dev_su_idx);
			if (stripe_degraded) {
				sector_t failed_dev_su_start_lba =
					stripe_lba +
					failed_dev_su_idx *
						ctx->params->su_sectors;
				sector_t failed_dev_su_end_lba =
					failed_dev_su_start_lba +
					ctx->params->su_sectors;
				sector_t stripe_data_start_lba =
					max(stripe_lba, start_lba);
				sector_t stripe_data_end_lba =
					min(stripe_lba +
						    ctx->params->stripe_sectors,
					    bio_end_sector(sh->orig_bio));
				sector_t missing_su_start_offset = 0;
				sector_t missing_su_end_offset = 0;
				sh->op = RAIZN_OP_DEGRADED_READ;
				if (stripe_data_start_lba >
				    failed_dev_su_start_lba) {
					// If the stripe data starts in the middle of the failed dev SU
					missing_su_start_offset =
						stripe_data_start_lba -
						failed_dev_su_start_lba;
				}
				if (stripe_data_end_lba <
				    failed_dev_su_end_lba) {
					// If the stripe data ends in the middle of the failed dev SU
					missing_su_end_offset =
						failed_dev_su_end_lba -
						stripe_data_end_lba;
				}
				// Make sure each stripe unit in this stripe is read from missing_su_start_offset to missing_su_end_offset
				for (int su_idx = 0;
				     su_idx < ctx->params->stripe_width;
				     ++su_idx) {
					sector_t su_start_lba =
						stripe_lba +
						(su_idx *
						 ctx->params
							 ->su_sectors); // Theoretical
					sector_t su_data_required_start_lba =
						su_start_lba +
						missing_su_start_offset;
					sector_t su_data_required_end_lba =
						su_start_lba +
						ctx->params->su_sectors -
						missing_su_end_offset;
					sector_t num_sectors =
						su_data_required_end_lba -
						su_data_required_start_lba;
					struct raizn_dev *cur_dev =
						lba_to_dev(ctx, su_start_lba);
					struct raizn_sub_io *subio;
					if (cur_dev->idx == failed_dev_idx) {
						cur_dev =
							&ctx->devs[parity_dev_idx];
					}
					subio = raizn_stripe_head_alloc_bio(
						sh, &cur_dev->bioset, 1,
						RAIZN_SUBIO_REBUILD);
					BUG_ON(!subio);
					BUG_ON(!subio->bio);
					BUG_ON(!num_sectors);
					bio_set_op_attrs(subio->bio,
							 REQ_OP_READ, 0);
					bio_set_dev(subio->bio,
						    cur_dev->dev->bdev);
					subio->data = kmalloc(
						num_sectors << SECTOR_SHIFT,
						GFP_NOIO);
					BUG_ON(!subio->data);
					if (bio_add_page(
						    subio->bio,
						    virt_to_page(subio->data),
						    num_sectors << SECTOR_SHIFT,
						    offset_in_page(
							    subio->data)) !=
					    num_sectors << SECTOR_SHIFT) {
						pr_err("Failed to add extra pages for degraded read\n");
					}
					subio->bio->bi_iter
						.bi_sector = lba_to_pba_default(
						ctx,
						su_data_required_start_lba);
					subio->header.header.start =
						su_data_required_start_lba;
					subio->header.header.end =
						su_data_required_start_lba +
						bio_sectors(subio->bio);
					//ctx->counters.read_overhead += subio->header.size; // TODO add this back in
#ifdef SMALL_ZONE_AGGR
					raizn_submit_bio_aggr(ctx, __func__, subio->bio, cur_dev, 0);
#else					
					raizn_submit_bio(ctx, __func__, subio->bio, 0);
#endif		
				}
			}
			// Read the necessary stripe units normally
			for (; su_touched > 0; --su_touched) {
				struct raizn_dev *cur_dev = lba_to_dev(
					ctx, stripe_bio->bi_iter.bi_sector);
				sector_t su_end_lba = roundup(
					stripe_bio->bi_iter.bi_sector + 1,
					ctx->params->su_sectors);
				struct raizn_sub_io *su_subio;
				if (cur_dev->idx == failed_dev_idx) {
					if (bio_end_sector(stripe_bio) <=
					    su_end_lba) {
						break;
					}
					bio_advance(stripe_bio,
						    su_end_lba -
							    stripe_bio->bi_iter
								    .bi_sector);
					continue;
				}
				// Split the bio and read the failed stripe unit
				if (su_end_lba < bio_end_sector(stripe_bio)) {
					su_subio = raizn_stripe_head_add_bio(
						sh,
						bio_split(
							stripe_bio,
							su_end_lba -
								stripe_bio
									->bi_iter
									.bi_sector,
							GFP_NOIO,
							&cur_dev->bioset),
						RAIZN_SUBIO_REBUILD);
				} else {
					su_subio = raizn_stripe_head_add_bio(
						sh, stripe_bio,
						RAIZN_SUBIO_REBUILD);
					su_subio->defer_put = true;
				}
				bio_set_dev(su_subio->bio, cur_dev->dev->bdev);
				su_subio->bio->bi_iter
					.bi_sector = lba_to_pba_default(
					ctx, su_subio->bio->bi_iter.bi_sector);
#ifdef SMALL_ZONE_AGGR
				raizn_submit_bio_aggr(ctx, __func__, su_subio->bio, cur_dev, 0);
#else				
				raizn_submit_bio(ctx, __func__, su_subio->bio, 0);
#endif		
			}
		}
		raizn_stripe_head_release_completion(sh);
	}
	return DM_MAPIO_SUBMITTED;
}

static int raizn_flush(struct raizn_stripe_head *sh)
{
	struct raizn_ctx *ctx = sh->ctx;
	int dev_idx;
	atomic_set(&sh->refcount, ctx->params->array_width);
	BUG_ON(bio_sectors(sh->orig_bio) !=
	       sh->orig_bio->bi_iter.bi_size >> SECTOR_SHIFT);
	for (dev_idx = 0; dev_idx < ctx->params->array_width; ++dev_idx) {
		struct raizn_dev *dev = &ctx->devs[dev_idx];
		struct bio *clone =
			bio_clone_fast(sh->orig_bio, GFP_NOIO, &dev->bioset);
		clone->bi_iter.bi_sector = lba_to_pba_default(
			ctx, sh->orig_bio->bi_iter.bi_sector);
		clone->bi_iter.bi_size =
			bio_sectors(sh->orig_bio) / ctx->params->stripe_width;
		clone->bi_private = &sh->sentinel;
		clone->bi_end_io = raizn_endio;
		bio_set_dev(clone, dev->dev->bdev);
#ifdef SMALL_ZONE_AGGR
		raizn_submit_bio_aggr(ctx, __func__, clone, dev, 0);
#else
		raizn_submit_bio(ctx, __func__, clone, 0);
#endif
	}
	return DM_MAPIO_SUBMITTED;
}

static int raizn_zone_open(struct raizn_stripe_head *sh)
{
	raizn_zone_mgr_execute(sh);
	return DM_MAPIO_SUBMITTED;
}

static int raizn_zone_close(struct raizn_stripe_head *sh)
{
	struct raizn_ctx *ctx = sh->ctx;
	unsigned int flags;
	int zoneno = lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector);
	// printk("[raizn_zone_reset_bottom] %d", zoneno);
    struct raizn_zone *lzone = &ctx->zone_mgr.lzones[zoneno];
	raizn_stripe_head_hold_completion(sh);
	for (int devno = 0; devno < ctx->params->array_width; ++devno) {
		struct raizn_dev *dev = &ctx->devs[devno];
		struct raizn_zone *pzone = &dev->zones[zoneno];
		struct raizn_sub_io *subio = raizn_stripe_head_alloc_bio(
			sh, &dev->bioset, 1, RAIZN_SUBIO_DATA);
		subio->bio->bi_iter.bi_sector = lba_to_pba_default(
			ctx, sh->orig_bio->bi_iter.bi_sector);
		bio_set_op_attrs(subio->bio, REQ_OP_ZONE_CLOSE, 0);
		bio_set_dev(subio->bio, dev->dev->bdev);
#ifdef SMALL_ZONE_AGGR
		subio->bio->bi_iter.bi_size = (ctx->params->num_zone_aggr << ctx->params->aggr_chunk_shift) << SECTOR_SHIFT;
		raizn_submit_bio_aggr(ctx, __func__, subio->bio, dev, 0);
#else
		raizn_submit_bio(ctx, __func__, subio->bio, 0);
#endif
	}
	raizn_stripe_head_release_completion(sh);
	return DM_MAPIO_SUBMITTED;
}

static int raizn_zone_finish(struct raizn_stripe_head *sh)
{
	struct raizn_ctx *ctx = sh->ctx;
	unsigned int flags;
	int zoneno = lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector);
	// printk("[raizn_zone_reset_bottom] %d", zoneno);
    struct raizn_zone *lzone = &ctx->zone_mgr.lzones[zoneno];
	raizn_stripe_head_hold_completion(sh);
	for (int devno = 0; devno < ctx->params->array_width; ++devno) {
		struct raizn_dev *dev = &ctx->devs[devno];
		struct raizn_zone *pzone = &dev->zones[zoneno];
		struct raizn_sub_io *subio = raizn_stripe_head_alloc_bio(
			sh, &dev->bioset, 1, RAIZN_SUBIO_DATA);
		subio->bio->bi_iter.bi_sector = lba_to_pba_default(
			ctx, sh->orig_bio->bi_iter.bi_sector);
		bio_set_op_attrs(subio->bio, REQ_OP_ZONE_FINISH, 0);
		bio_set_dev(subio->bio, dev->dev->bdev);
#ifdef SMALL_ZONE_AGGR
		subio->bio->bi_iter.bi_size = (ctx->params->num_zone_aggr << ctx->params->aggr_chunk_shift) << SECTOR_SHIFT;
		raizn_submit_bio_aggr(ctx, __func__, subio->bio, dev, 0);
#else
		raizn_submit_bio(ctx, __func__, subio->bio, 0);
#endif
#ifdef ATOMIC_WP
		atomic64_set(&pzone->wp, pzone->start + pzone->capacity);
#else
		pzone->pzone_wp = pzone->start + pzone->capacity;
#endif
	}
	raizn_stripe_head_release_completion(sh);
	return DM_MAPIO_SUBMITTED;
}

static int raizn_zone_append(struct raizn_stripe_head *sh)
{
	return DM_MAPIO_SUBMITTED;
}

static int raizn_zone_reset_bottom(struct raizn_stripe_head *sh)
{
	struct raizn_ctx *ctx = sh->ctx;
	int zoneno = lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector);
	raizn_stripe_head_hold_completion(sh);
	ctx->zone_mgr.gen_counts[zoneno / RAIZN_GEN_COUNTERS_PER_PAGE]
		.zone_generation[zoneno % RAIZN_GEN_COUNTERS_PER_PAGE] += 1;
	for (int devno = 0; devno < ctx->params->array_width; ++devno) {
		struct raizn_dev *dev = &ctx->devs[devno];
		struct raizn_zone *pzone = &dev->zones[zoneno];
		struct raizn_sub_io *subio = raizn_stripe_head_alloc_bio(
			sh, &dev->bioset, 1, RAIZN_SUBIO_DATA);
		subio->bio->bi_iter.bi_sector = lba_to_pba_default(
			ctx, sh->orig_bio->bi_iter.bi_sector);
		bio_set_op_attrs(subio->bio, REQ_OP_ZONE_RESET, 0);
		bio_set_dev(subio->bio, dev->dev->bdev);
#ifdef SMALL_ZONE_AGGR
		subio->bio->bi_iter.bi_size = (ctx->params->num_zone_aggr << ctx->params->aggr_chunk_shift) << SECTOR_SHIFT;
	// printk("[raizn_zone_reset_bottom] bi_size: %lld, end_sector: %lld", subio->bio->bi_iter.bi_size, bio_end_sector(subio->bio));
		raizn_submit_bio_aggr(ctx, __func__, subio->bio, dev, 0);
#else
		raizn_submit_bio(ctx, __func__, subio->bio, 0);
#endif
#ifdef ATOMIC_WP
		atomic64_set(&pzone->wp, pzone->start);
#else
		pzone->wp = pzone->start;
#endif
	}
	raizn_stripe_head_release_completion(sh);
	return DM_MAPIO_SUBMITTED;
}

static int raizn_zone_reset_top(struct raizn_stripe_head *sh)
{
	struct raizn_ctx *ctx = sh->ctx;
	struct raizn_dev *dev =
		lba_to_dev(ctx, sh->orig_bio->bi_iter.bi_sector);
	struct raizn_dev *parity_dev =
		lba_to_parity_dev(ctx, sh->orig_bio->bi_iter.bi_sector);
	int zoneno = lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector);
	struct raizn_stripe_head *log_sh =
		raizn_stripe_head_alloc(ctx, NULL, RAIZN_OP_ZONE_RESET_LOG);
	struct raizn_sub_io *devlog =
		raizn_alloc_md(sh, zoneno, dev, RAIZN_ZONE_MD_GENERAL, NULL, 0);
	struct raizn_sub_io *pdevlog = raizn_alloc_md(
		sh, zoneno, parity_dev, RAIZN_ZONE_MD_GENERAL, NULL, 0);
	raizn_stripe_head_hold_completion(log_sh);
	sh->op = RAIZN_OP_ZONE_RESET;
	log_sh->next = sh; // Defer the original stripe head
	BUG_ON(!devlog || !pdevlog);
	bio_set_op_attrs(devlog->bio, REQ_OP_ZONE_APPEND, REQ_FUA);
	bio_set_op_attrs(pdevlog->bio, REQ_OP_ZONE_APPEND, REQ_FUA);
	devlog->header.header.logtype = RAIZN_MD_RESET_LOG;
	pdevlog->header.header.logtype = RAIZN_MD_RESET_LOG;
	devlog->header.header.start = sh->orig_bio->bi_iter.bi_sector;
	pdevlog->header.header.start = sh->orig_bio->bi_iter.bi_sector;
	devlog->header.header.end =
		devlog->header.header.start + ctx->params->lzone_size_sectors;
	pdevlog->header.header.end =
		pdevlog->header.header.start + ctx->params->lzone_size_sectors;
#ifdef SMALL_ZONE_AGGR
	raizn_submit_bio_aggr(ctx, __func__, devlog->bio, dev, 0);
	raizn_submit_bio_aggr(ctx, __func__, pdevlog->bio, dev, 0);
#else
	raizn_submit_bio(ctx, __func__, devlog->bio, 0);
	raizn_submit_bio(ctx, __func__, pdevlog->bio, 0);
#endif	
	raizn_stripe_head_release_completion(log_sh);
	return DM_MAPIO_SUBMITTED;
}

static int raizn_zone_reset_all(struct raizn_stripe_head *sh)
{
	return DM_MAPIO_SUBMITTED;
}

static void raizn_handle_io_mt(struct work_struct *work)
{
	struct raizn_workqueue *wq =
		container_of(work, struct raizn_workqueue, work);
	struct raizn_stripe_head *sh;
#ifdef DEBUG
// #if 1
	printk("### work thread pid: %d, core: %d\n", current->pid, smp_processor_id());
#endif
#if defined(BATCH_WQ)
	while (kfifo_out_spinlocked(&wq->work_fifo, &sh, 1, &wq->rlock)) {
		// udelay(1000);
#ifdef IMM_ENDIO
		raizn_write_dummy(sh);
#else
		raizn_write(sh);
#endif
	}
#elif defined(SEPARATE_WQ)
	int first_zone = -1;
	int ret;
	while (1) {
		printk("before lock pid: %d\n", current->pid);
		spin_lock(&wq->rlock);
		printk("after lock pid :%d\n", current->pid);
		ret = kfifo_peek(&wq->work_fifo, &sh);
		printk("ret: %d\n", ret);
		// printk("[DEBUG]1 %s tid: %d, bi_sector: %lld, start: %lld, subio: %p, zone: %p\n", 
		// 	__func__, current->pid,
		// 	sh->orig_bio->bi_iter.bi_sector,
		// 	sh->zone->start, sh->orig_bio, sh->zone);
		if (!kfifo_peek(&wq->work_fifo, &sh)) {
			printk("no work left\n");
			spin_unlock(&wq->rlock);
			return;
		}
		if (first_zone == -1) {
			first_zone = lba_to_lzone(sh->ctx, sh->orig_bio->bi_iter.bi_sector);
			printk("first_zone: %d\n", first_zone);
		}
		else {
			if (first_zone != lba_to_lzone(sh->ctx, sh->orig_bio->bi_iter.bi_sector)) {
				printk("zone diff! first:%d, new:%d\n", first_zone, lba_to_lzone(sh->ctx, sh->orig_bio->bi_iter.bi_sector));
				spin_unlock(&wq->rlock);
				return;
			}
		}
		printk("zone same~ first:%d, new:%d\n", first_zone, lba_to_lzone(sh->ctx, sh->orig_bio->bi_iter.bi_sector));
		kfifo_out(&wq->work_fifo, &sh, 1);
		spin_unlock(&wq->rlock);
		BUG_ON(bio_op(sh->orig_bio) != REQ_OP_WRITE);
#ifdef IMM_ENDIO
		raizn_write_dummy(sh);
#else
		raizn_write(sh);
#endif
	}
#else
	kfifo_out_spinlocked(&wq->work_fifo, &sh, 1, &wq->rlock);
#ifdef IMM_ENDIO
	raizn_write_dummy(sh);
#else
	raizn_write(sh);
#endif
#endif
}

static int raizn_process_stripe_head(struct raizn_stripe_head *sh)
{
	struct raizn_ctx *ctx = sh->ctx;
	int ret;
	switch (sh->op) {
	case RAIZN_OP_READ:
		return raizn_read(sh);
	case RAIZN_OP_WRITE:
		// Validate the write can be serviced
		if (raizn_zone_mgr_execute(sh) != 0) {
			pr_err("Failed to validate write\n");
			return DM_MAPIO_KILL;
		}
		if (ctx->num_io_workers > 1) {
#ifdef MULTI_FIFO
			int fifo_idx;
			fifo_idx = lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector) %
				min(ctx->num_cpus, ctx->num_io_workers);
			ret = kfifo_in_spinlocked(&ctx->io_workers[fifo_idx].work_fifo, &sh, 1,
					    &ctx->io_workers[fifo_idx].wlock);
			if (!ret) {
				pr_err("ERROR: %s kfifo insert failed!\n", __func__);
				BUG_ON(1);
			}
			queue_work(raizn_wq, &ctx->io_workers[fifo_idx].work);
#else
			// Push it onto the fifo
			// udelay(1000);
			ret = kfifo_in_spinlocked(&ctx->io_workers.work_fifo, &sh, 1,
					    &ctx->io_workers.wlock);
			if (!ret) {
				pr_err("ERROR: %s kfifo insert failed!\n", __func__);
				BUG_ON(1);
			}
			queue_work(raizn_wq, &ctx->io_workers.work);
#endif
			return DM_MAPIO_SUBMITTED;
		} else {
			return raizn_write(sh);
		}
	case RAIZN_OP_FLUSH:
		return raizn_flush(sh);
	case RAIZN_OP_DISCARD:
		pr_err("RAIZN_OP_DISCARD is not supported.\n");
		return DM_MAPIO_KILL;
	case RAIZN_OP_SECURE_ERASE:
		pr_err("RAIZN_OP_SECURE_ERASE is not supported.\n");
		return DM_MAPIO_KILL;
	case RAIZN_OP_WRITE_ZEROES:
		pr_err("RAIZN_OP_WRITE_ZEROES is not supported.\n");
		return DM_MAPIO_KILL;
	case RAIZN_OP_ZONE_OPEN:
		return raizn_zone_open(sh);
	case RAIZN_OP_ZONE_CLOSE:
		return raizn_zone_close(sh);
	case RAIZN_OP_ZONE_FINISH:
		return raizn_zone_finish(sh);
	case RAIZN_OP_ZONE_APPEND:
		return raizn_zone_append(sh);
	case RAIZN_OP_ZONE_RESET_LOG:
		return raizn_zone_reset_top(sh);
	case RAIZN_OP_ZONE_RESET:
		return raizn_zone_reset_bottom(sh);
	case RAIZN_OP_ZONE_RESET_ALL:
		return raizn_zone_reset_all(sh);
	default:
		pr_err("This stripe unit should not be handled by process_stripe_head\n");
		return DM_MAPIO_KILL;
	}
	return DM_MAPIO_KILL;
}

static int raizn_map(struct dm_target *ti, struct bio *bio)
{
	struct raizn_ctx *ctx = (struct raizn_ctx *)ti->private;
	struct raizn_stripe_head *sh =
		raizn_stripe_head_alloc(ctx, bio, raizn_op(bio));
	// profile_bio(sh);
// #ifdef DEBUG
#if 1
	// printk("[DEBUG] %s: orig_bio [%d] pid: %d, lba: %lld, length: %d\n",
	// 	__func__, op_is_write(bio_op(bio)), current->pid, bio->bi_iter.bi_sector, bio_sectors(bio));
#endif
	return raizn_process_stripe_head(sh);
}

static void raizn_status(struct dm_target *ti, status_type_t type,
			 unsigned int status_flags, char *result,
			 unsigned int maxlen)
{
	struct raizn_ctx *ctx = ti->private;
	if (ctx->zone_mgr.rebuild_mgr.end) {
		pr_info("Rebuild took %lld ns\n",
			ktime_to_ns(
				ktime_sub(ctx->zone_mgr.rebuild_mgr.end,
					  ctx->zone_mgr.rebuild_mgr.start)));
	}
#ifdef PROFILING
	pr_info("write sectors = %lld\n",
		atomic64_read(&ctx->counters.write_sectors));
	pr_info("read sectors = %lld\n",
		atomic64_read(&ctx->counters.read_sectors));
	pr_info("writes = %d\n", atomic_read(&ctx->counters.writes));
	pr_info("reads = %d\n", atomic_read(&ctx->counters.reads));
	pr_info("zone_resets = %d\n", atomic_read(&ctx->counters.zone_resets));
	pr_info("flushes = %d\n", atomic_read(&ctx->counters.flushes));
	pr_info("preflush = %d\n", atomic_read(&ctx->counters.preflush));
	pr_info("fua = %d\n", atomic_read(&ctx->counters.fua));
	pr_info("gc_count = %d\n", atomic_read(&ctx->counters.gc_count));
#endif
}

static int raizn_iterate_devices(struct dm_target *ti,
				 iterate_devices_callout_fn fn, void *data)
{
	struct raizn_ctx *ctx = ti->private;
	int i, ret = 0;
	if (!ctx || !ctx->devs) {
		return -1;
	}

	for (i = 0; i < ctx->params->array_width; i++) {
		struct raizn_dev *dev = &ctx->devs[i];
		ret = fn(ti, dev->dev, 0, dev->num_zones * dev->zones[0].len,
			 data);
		if (ret) {
			break;
		}
	}

	// Why does dm keep trying to add more sectors to the device???
	set_capacity(dm_disk(dm_table_get_md(ti->table)),
		     ctx->params->num_zones * ctx->params->lzone_size_sectors);
	return ret;
}

static void raizn_io_hints(struct dm_target *ti, struct queue_limits *limits)
{
	struct raizn_ctx *ctx = (struct raizn_ctx *)ti->private;
	limits->chunk_sectors = ctx->params->lzone_size_sectors;
	blk_limits_io_min(limits, ctx->params->su_sectors << SECTOR_SHIFT);
	blk_limits_io_opt(limits, ctx->params->stripe_sectors << SECTOR_SHIFT);
	limits->zoned = BLK_ZONED_HM;
}

static void raizn_suspend(struct dm_target *ti)
{
}

static void raizn_resume(struct dm_target *ti)
{
}

static int raizn_report_zones(struct dm_target *ti,
			      struct dm_report_zones_args *args,
			      unsigned int nr_zones)
{
	struct raizn_ctx *ctx = ti->private;
#ifdef NON_POW_2_ZONE_SIZE
	int zoneno = args->next_sector / ctx->params->lzone_size_sectors;
#else
	int zoneno = args->next_sector >> ctx->params->lzone_shift;
#endif
	struct raizn_zone *zone = &ctx->zone_mgr.lzones[zoneno];
	struct blk_zone report;
	if (!nr_zones || zoneno > ctx->params->num_zones) {
		return args->zone_idx;
	}
	mutex_lock(&zone->lock);
	report.start = zone->start;
	report.len = ctx->params->lzone_size_sectors;
#ifdef ATOMIC_WP
	report.wp = atomic64_read(&zone->wp);
#else
	report.wp = zone->wp;
#endif
	report.type = BLK_ZONE_TYPE_SEQWRITE_REQ;
	report.cond = (__u8)atomic_read(&zone->cond);
	report.non_seq = 0;
	report.reset = 0;
	report.capacity = ctx->params->lzone_capacity_sectors;
	mutex_unlock(&zone->lock);
	args->start = report.start;
	args->next_sector += ctx->params->lzone_size_sectors;
	return args->orig_cb(&report, args->zone_idx++, args->orig_data);
}

// More investigation is necessary to see what this function is actually used for in f2fs etc.
static int raizn_prepare_ioctl(struct dm_target *ti, struct block_device **bdev)
{
	struct raizn_ctx *ctx = ti->private;
	*bdev = ctx->devs[0].dev->bdev;
	return 0;
}

static int raizn_command(struct raizn_ctx *ctx, int argc, char **argv,
			 char *result, unsigned maxlen)
{
	static const char errmsg[] = "Error: Invalid command\n";
	if (argc >= 2 && !strcmp(argv[0], RAIZN_DEV_TOGGLE_CMD)) {
		int dev_idx, ret;
		static const char successmsg[] =
			"Success: Set device %d to %s\n";
		ret = kstrtoint(argv[1], 0, &dev_idx);
		if (!ret && dev_idx < ctx->params->array_width) {
			bool old_status =
				test_and_change_bit(dev_idx, ctx->dev_status);
			if (strlen("DISABLED") + strlen(successmsg) < maxlen) {
				sprintf(result, successmsg, dev_idx,
					old_status ? "ACTIVE" : "DISABLED");
			}
		}
	} else if (argc >= 2 && !strcmp(argv[0], RAIZN_DEV_REBUILD_CMD)) {
		int dev_idx, ret, j;
		static const char successmsg[] =
			"Success: Resetting and rebuilding device %d\n";
		ret = kstrtoint(argv[1], 0, &dev_idx);
		if (!ret && strlen(successmsg) < maxlen) {
			struct raizn_dev *dev = &ctx->devs[dev_idx];
			struct raizn_stripe_head *sh = raizn_stripe_head_alloc(
				ctx, NULL, RAIZN_OP_REBUILD_INGEST);
			set_bit(dev_idx, ctx->dev_status);
			sprintf(result, successmsg, dev_idx);
			// 1. Reset all zones
			for (int zoneno = 0; zoneno < dev->num_zones;
			     ++zoneno) {
#ifdef SAMSUNG_MODE
				struct block_device *nvme_bdev = ctx->raw_bdev;
				sector_t pzone_base_addr = dev_idx * ctx->params->div_capacity +
				 	(zoneno * ctx->params->gap_zone_aggr * ctx->devs[0].zones[0].phys_len);
				for (j=0; j<ctx->params->num_zone_aggr; j++) {
					blkdev_zone_mgmt(nvme_bdev,
						REQ_OP_ZONE_RESET,
						pzone_base_addr + j * ctx->devs[0].zones[0].phys_len,
						ctx->devs[0].zones[0].phys_len,
						GFP_NOIO);
				}
#else
				blkdev_zone_mgmt(dev->dev->bdev,
						 REQ_OP_ZONE_RESET,
#ifdef NON_POW_2_ZONE_SIZE
						 zoneno * dev->zones[0].len,
						 dev->zones[0].len,
#else
						zoneno << dev->zone_shift,
						1 << dev->zone_shift,
#endif
						 GFP_NOIO);
#endif
			}
			// 2. Reset all physical zone descriptors for this device
			blkdev_report_zones(dev->dev->bdev, 0, dev->num_zones,
					    init_pzone_descriptor, dev);
			// 3. Schedule rebuild
			ctx->zone_mgr.rebuild_mgr.start = ktime_get();
			ret = kfifo_in_spinlocked(&dev->gc_ingest_workers.work_fifo,
					    &sh, 1,
					    &dev->gc_ingest_workers.wlock);
			if (!ret) {
				pr_err("ERROR: %s kfifo insert failed!\n", __func__);
				return -1;
			}
			queue_work(raizn_gc_wq, &dev->gc_ingest_workers.work);
			// queue_work(raizn_wq, &dev->gc_ingest_workers.work);
		}
	}
	else if (argc == 1 && !strcmp(argv[0], RAIZN_DEV_STAT_CMD)) {
#ifdef RECORD_PP_AMOUNT
		printk("★★★---total_write_count: %llu\n", atomic64_read(&ctx->total_write_count));
		printk("★★★---total_write_amount: %llu(KB)\n", atomic64_read(&ctx->total_write_amount)/2);
		printk("★★★---pp_volatile: %llu(KB)\n", atomic64_read(&ctx->pp_volatile)/2);
		printk("★★★---pp_permanent: %llu(KB)\n", atomic64_read(&ctx->pp_permanent)/2);
		printk("★★★---gc_count: %llu\n", atomic64_read(&ctx->gc_count));
		printk("★★★---gc_migrated: %llu(KB)\n", atomic64_read(&ctx->gc_migrated)/2);
#endif
	}
	else if (argc == 1 && !strcmp(argv[0], RAIZN_DEV_STAT_RESET_CMD)) {
#ifdef RECORD_PP_AMOUNT
		printk("★★★---dev_stat reset\n");
		raizn_init_pp_counter(ctx);
#endif
	}
	else if (strlen(errmsg) < maxlen) {
		strcpy(result, errmsg);
	}
	return 1;
}

#ifdef RAIZN_TEST
void raizn_test_parse_command(int argc, char **argv)
{
	// Command structure
	// <function_name> [args...]
	if (!strcmp(argv[0], "lba_to_stripe")) {
	} else if (!strcmp(argv[0], "lba_to_su")) {
	} else if (!strcmp(argv[0], "lba_to_lzone")) {
	} else if (!strcmp(argv[0], "lba_to_parity_dev_idx")) {
	} else if (!strcmp(argv[0], "lba_to_parity_dev")) {
	} else if (!strcmp(argv[0], "lba_to_dev")) {
	} else if (!strcmp(argv[0], "lba_to_lzone_offset")) {
	} else if (!strcmp(argv[0], "lba_to_stripe_offset")) {
	} else if (!strcmp(argv[0], "bytes_to_stripe_offset")) {
	} else if (!strcmp(argv[0], "lba_to_stripe_addr")) {
	} else if (!strcmp(argv[0], "lba_to_pba_default")) {
	} else if (!strcmp(argv[0], "validate_parity")) {
	}
}

static int raizn_message(struct dm_target *ti, unsigned argc, char **argv,
			 char *result, unsigned maxlen)
{
	struct raizn_ctx *ctx = ti->private;
	int idx;
	raizn_command(ctx, argc, argv, result, maxlen);
	pr_info("Received message, output buffer maxlen=%d\n", maxlen);
	for (idx = 0; idx < argc; ++idx) {
		pr_info("argv[%d] = %s\n", idx, argv[idx]);
	}
	return 1;
}
#else
static int raizn_message(struct dm_target *ti, unsigned argc, char **argv,
			 char *result, unsigned maxlen)
{
	return raizn_command(ctx, argc, argv, result, maxlen);
}
#endif

// Module
static struct target_type raizn = {
	.name = "raizn",
	.version = { 1, 0, 0 },
	.module = THIS_MODULE,
	.ctr = raizn_ctr,
	.dtr = raizn_dtr,
	.map = raizn_map,
	.io_hints = raizn_io_hints,
	.status = raizn_status,
	.prepare_ioctl = raizn_prepare_ioctl,
	.report_zones = raizn_report_zones,
	.postsuspend = raizn_suspend,
	.resume = raizn_resume,
	.features = DM_TARGET_ZONED_HM,
	.iterate_devices = raizn_iterate_devices,
	.message = raizn_message,
};

static int init_raizn(void)
{
	return dm_register_target(&raizn);
}

static void cleanup_raizn(void)
{
	dm_unregister_target(&raizn);
}
module_init(init_raizn);
module_exit(cleanup_raizn);
MODULE_LICENSE("GPL");
