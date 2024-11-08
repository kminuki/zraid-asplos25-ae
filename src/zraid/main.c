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
#include <linux/nvme_ioctl.h>
#include <linux/blkdev.h>

#include "raizn.h"
#include "pp.h"
#include "util.h"
#include "zrwa.h"

// Main workqueue for RAIZN
struct workqueue_struct *raizn_wq;
struct workqueue_struct *raizn_gc_wq;
struct workqueue_struct *raizn_manage_wq;


// Workqueue functions
static void raizn_handle_io_mt(struct work_struct *work);
static void raizn_gc(struct work_struct *work);
static void raizn_zone_manage(struct work_struct *work);
static int raizn_zone_finish(struct raizn_stripe_head *sh);

// Universal endio for raizn
void raizn_endio(struct bio *bio);
static void raizn_rebuild_endio(struct bio *bio);
static int raizn_process_stripe_head(struct raizn_stripe_head *sh);
static struct raizn_sub_io *raizn_alloc_md(struct raizn_stripe_head *sh,
					   sector_t lzoneno,
					   struct raizn_dev *dev,
					   raizn_zone_type mdtype,
					   sub_io_type_t subio_type,
					   void *data,
					   size_t len);

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


void print_buf(char *buf)
{
	int i;
	for (i = 0; i < 8; i++) {
		printk("%d", !!(((*buf) << i) & 0x80));
	}
	printk("\n");
}


static void raizn_queue_gc(struct raizn_dev *dev)
{
	queue_work(raizn_gc_wq, &dev->gc_flush_workers.work);
	// queue_work(raizn_wq, &dev->gc_flush_workers.work);
}

void raizn_queue_manage(struct raizn_ctx *ctx, int fifo_idx)
{
#ifdef MULTI_FIFO
	queue_work(raizn_manage_wq, &ctx->zone_manage_workers[fifo_idx].work);
#else
	queue_work(raizn_manage_wq, &ctx->zone_manage_workers.work);
#endif
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

// caller should hold lzone->lock
static void raizn_zone_stripe_buffers_deinit(struct raizn_zone *lzone)
{
	if (lzone->stripe_buffers) {
		for (int i = 0; i < STRIPE_BUFFERS_PER_ZONE; ++i) {
			kvfree(lzone->stripe_buffers[i].data);
			lzone->stripe_buffers[i].data = NULL;
		}
		kvfree(lzone->stripe_buffers);
		lzone->stripe_buffers = NULL;
	}
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
		kfree(ctx->zone_mgr.lzones[zone_idx].stripe_prog_bitmap);
	}
	kfree(ctx->zone_mgr.lzones);
	kfree(ctx->zone_mgr.gen_counts);
	raizn_rebuild_mgr_deinit(&ctx->zone_mgr.rebuild_mgr);
}

static int raizn_zone_mgr_init(struct raizn_ctx *ctx)
{
	int ret;
	printk("[%s] ctx->params->num_zones: %d\n", __func__, ctx->params->num_zones);
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
		atomic64_set(&zone->lzone_wp, ctx->params->lzone_size_sectors * zone_num);
		zone->start = atomic64_read(&zone->lzone_wp);
		zone->capacity = ctx->params->lzone_capacity_sectors;
		zone->len = ctx->params->lzone_size_sectors;
		zone->persistence_bitmap = kzalloc(
			BITS_TO_BYTES(stripe_units_per_zone), GFP_KERNEL);
		zone->stripe_prog_bitmap = kzalloc(ctx->params->stripe_prog_bitmap_size_bytes, GFP_KERNEL);
		// zone->persistence_bitmap = vzalloc(
		// 	BITS_TO_BYTES(stripe_units_per_zone));
		// zone->stripe_prog_bitmap = vzalloc(ctx->params->stripe_prog_bitmap_size_bytes);
		if (!zone->stripe_prog_bitmap) {
			printk("Failed to allocate stripe_prog_bitmap");
			return -ENOMEM;
		}
		zone->last_complete_stripe = -1;
		zone->waiting_str_num = -1;
		zone->waiting_str_num2= -1;
		zone->waiting_data_lba= -1;
		zone->waiting_pp_lba= -1;
		atomic_set(&zone->wp_entry_idx, 0);
		atomic_set(&zone->wait_count, 0);
		atomic_set(&zone->wait_count2, 0);
		atomic_set(&zone->wait_count_data, 0);
		atomic_set(&zone->wait_count_pp, 0);
    	// print_bitmap(ctx, zone);
		atomic_set(&zone->cond, BLK_ZONE_COND_EMPTY);
		mutex_init(&zone->lock);
		spin_lock_init(&zone->prog_bitmap_lock);
		spin_lock_init(&zone->last_comp_str_lock);
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
			set_bit(zoneno, rebuild_mgr->open_zones);
			break;
		case BLK_ZONE_COND_CLOSED:
		case BLK_ZONE_COND_FULL:
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
	kvfree(sh->parity_bufs);
	// kfree(sh->parity_bufs);
	// if ((sh->op == RAIZN_OP_WRITE) || (sh->op == RAIZN_OP_REBUILD_INGEST)) {
	// }
#else
	if (sh->op == RAIZN_OP_REBUILD_INGEST) {
		kvfree(sh->parity_bufs);
		// kfree(sh->parity_bufs);
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
	if (sh->orig_bio) {
		for (int i = 0; i < RAIZN_MAX_SUB_IOS; ++i) {
			if (sh->sub_ios[i]) {
				struct raizn_sub_io *subio = sh->sub_ios[i];
				if (subio->defer_put) {
					bio_put(subio->bio);
				}
				kvfree(subio->data);
				kvfree(subio);
			} else {
				break;
			}
		}
	}
	kfree(sh);
}

struct raizn_stripe_head *
raizn_stripe_head_alloc(struct raizn_ctx *ctx, struct bio *bio, raizn_op_t op)
{
	struct raizn_stripe_head *sh;
	sh = kzalloc(sizeof(struct raizn_stripe_head), GFP_NOIO);
	// if (op !=RAIZN_OP_GC)
	// 	sh = kzalloc(sizeof(struct raizn_stripe_head), GFP_NOIO);
	// else
	// 	sh = kzalloc(sizeof(struct raizn_stripe_head), GFP_ATOMIC);

	if (!sh) {
		return NULL;
	}
	sh->ctx = ctx;
	sh->orig_bio = bio;
	if (bio) {
		sh->zone = &ctx->zone_mgr.lzones[lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector)];
		// printk("zone start: %llu", sh->zone->start);
	}
	atomic_set(&sh->refcount, 0);
	atomic_set(&sh->subio_idx, -1);
	sh->op = op;
	sh->sentinel.sh = sh;
	sh->sentinel.sub_io_type = RAIZN_SUBIO_OTHER;
	sh->zf_submitted = false;
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
	int subio_idx = atomic_inc_return(&sh->subio_idx);
	atomic_inc(&sh->refcount);
	if (subio_idx >= RAIZN_MAX_SUB_IOS) {
		pr_err("Too many sub IOs generated, please increase RAIZN_MAX_SUB_IOS\n");
		return NULL;
	}
	subio = kzalloc(sizeof(struct raizn_sub_io), GFP_NOIO);
	BUG_ON(!subio);
	sh->sub_ios[subio_idx] = subio;
	subio->sub_io_type = sub_io_type;
	subio->sh = sh;
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

struct raizn_sub_io *
raizn_stripe_head_alloc_bio(struct raizn_stripe_head *sh,
			    struct bio_set *bioset, int bvecs,
			    sub_io_type_t sub_io_type, void *data)
{
	struct raizn_sub_io *subio;
#if (defined NOFLUSH) || (defined PP_OUTPLACE)
	if (1)
#else
	if (sub_io_type != RAIZN_SUBIO_WP_LOG)
#endif
	{
		subio =
			raizn_stripe_head_alloc_subio(sh, sub_io_type);
		subio->bio = bio_alloc_bioset(GFP_NOIO, bvecs, bioset);
	}
	else {
		subio = data;
		subio->bio = bio_alloc_bioset(GFP_ATOMIC, bvecs, bioset);
		BUG_ON(subio->bio == NULL);
		subio->sub_io_type = RAIZN_SUBIO_WP_LOG;
		atomic_inc(&sh->refcount);
	}
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
	pzone->pzone_wp = zone->wp;
	pzone->start = zone->start;
	pzone->capacity = zone->capacity;
	pzone->len = zone->len;
	pzone->dev = dev;
	spin_lock_init(&pzone->pzone_wp_lock);
	// printk("[DEBUG] [%s] zone start: %x, capacity: %x, len: %x\n", 
	// 	__func__, zone->start, zone->capacity, zone->len);
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
		pzone->pzone_wp *= NUM_ZONE_AGGR;
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
#ifdef DEBUG
	printk("[DEBUG] [%s] num_zones: %d\n", __func__, dev->num_zones);
#endif
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
#if (defined NOFLUSH) || (defined PP_OUTPLACE)
		mutex_init(&dev->lock);
#else
		spin_lock_init(&dev->lock);
#endif
		mutex_init(&dev->bioset_lock);
		dev->zone_shift = ilog2(dev->zones[0].len);
#ifdef DEBUG
	printk("[DEBUG] [%s] len: %llu\n", __func__, dev->zones[0].len);
	printk("[DEBUG] [%s] zone_shift: %llu\n", __func__, dev->zone_shift);
#endif
		dev->idx = dev_idx;
		spin_lock_init(&dev->free_wlock);
		spin_lock_init(&dev->free_rlock);
		if ((ret = kfifo_alloc(&dev->free_zone_fifo,
				       RAIZN_RESERVED_ZONES, GFP_NOIO))) {
			pr_err("ERROR: %s kfifo for free zone allocation failed!\n", __func__);
			return ret;
		}
		kfifo_reset(&dev->free_zone_fifo);
		for (zoneno = dev->num_zones - 1;
		     zoneno >= dev->num_zones - RAIZN_RESERVED_ZONES;
		     --zoneno) {
			struct raizn_zone *z = &dev->zones[zoneno];
			if (!kfifo_in_spinlocked(&dev->free_zone_fifo, &z, 1,
						 &dev->free_wlock)) {
				pr_err("ERROR: %s kfifo for free zone insert failed!\n", __func__);
				return -EINVAL;
			}
		}
		for (int mdtype = RAIZN_ZONE_MD_GENERAL;
		     mdtype < RAIZN_ZONE_NUM_MD_TYPES; ++mdtype) {
			if (!kfifo_out_spinlocked(&dev->free_zone_fifo,
						  &dev->md_zone[mdtype], 1,
						  &dev->free_rlock)) {
				return -EINVAL;
			}
			dev->md_zone[mdtype]->zone_type = mdtype;
			atomic64_set(&dev->md_zone[mdtype]->mdzone_wp, 0);
			pr_info("RAIZN writing mdtype %d to zone %llu (%llu)\n",
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
	printk("lzone_capacity_sectors(512 sector): %d\n", ctx->params->lzone_capacity_sectors);
		zone_cap = ctx->devs[0].zones[0].capacity;
	printk("zone_cap(512 sector): %d\n", zone_cap);
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
	printk("lzone_size_sectors = %llu\n", ctx->params->lzone_size_sectors);
	ctx->params->lzone_shift = ilog2(ctx->params->lzone_size_sectors);
	// TODO: change for configurable zone size
	ctx->params->num_zones -= RAIZN_RESERVED_ZONES;
	printk("num_zones: %d, RAIZN_RESERVED_ZONES: %d\n", ctx->params->num_zones, RAIZN_RESERVED_ZONES);
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
	for (i=0; i<min(ctx->num_cpus, ctx->num_manage_workers); i++) {
		raizn_workqueue_deinit(&ctx->zone_manage_workers[i]);
	}
	kfree(ctx->zone_manage_workers);
#else
	raizn_workqueue_deinit(&ctx->io_workers);
	raizn_workqueue_deinit(&ctx->zone_manage_workers);
#endif
	// deallocate ctx
	kfree(ctx);
}

void raizn_init_stat_counter(struct raizn_ctx *ctx)
{
	memset(&ctx->subio_counters, 0, sizeof(ctx->subio_counters));
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
#ifdef SMALL_ZONE_AGGR
	printk("Small zone device. Zones are aggregated\n");
#endif

#ifdef IGNORE_PARITY_BUF
	printk("[DEBUG] parity buf is ignored\n");
#endif
#ifdef PP_INPLACE
	printk("PP_INPLACE logging\n");
#endif
#ifdef PP_OUTPLACE
	printk("PP_OUTPLACE logging\n");
#endif
#ifdef DUMMY_HDR
	printk("Dummy header(4k) is added\n");
#endif
#ifdef PERF_MODE
	pr_warn("PERF_MODE is on. Silent bug(infinite loop) can happen!\n");
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
	ctx->params->stripe_shift = ilog2(ctx->params->stripe_sectors);
	ret = kstrtoint(argv[1], 0, &ctx->num_io_workers);
	if (ret) {
		ti->error = "dm-raizn: Invalid num of IO workers";
		goto err;
	}
	ctx->num_manage_workers = ctx->num_io_workers;
	// ctx->num_manage_workers = 32;
#ifdef MULTI_FIFO
	int i;
	ctx->io_workers = kcalloc(ctx->num_cpus, sizeof(struct raizn_workqueue), GFP_NOIO);
	// for (i=0; i<ctx->num_cpus; i++) {
	for (i=0; i<min(ctx->num_cpus, ctx->num_io_workers); i++) {
		raizn_workqueue_init(ctx, &ctx->io_workers[i], min(ctx->num_cpus, ctx->num_io_workers),
			     raizn_handle_io_mt);
		ctx->io_workers[i].idx = i;
	}
	ctx->zone_manage_workers = kcalloc(ctx->num_cpus, sizeof(struct raizn_workqueue), GFP_NOIO);
	for (i=0; i<min(ctx->num_cpus, ctx->num_manage_workers); i++) {
		raizn_workqueue_init(ctx, &ctx->zone_manage_workers[i], min(ctx->num_cpus, ctx->num_manage_workers),
			     raizn_zone_manage);
		ctx->zone_manage_workers[i].idx = i;
	}
#else
	raizn_workqueue_init(ctx, &ctx->io_workers, ctx->num_io_workers,
			     raizn_handle_io_mt);
	raizn_workqueue_init(ctx, &ctx->zone_manage_workers, ctx->num_manage_workers, 
				raizn_zone_manage);
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

	ctx->params->chunks_in_zrwa = ZRWASZ / ctx->params->su_sectors;
	// TODO: bitmap size equal to ZRWASZ makes conflict (can't find reason)
	ctx->params->stripes_in_stripe_prog_bitmap = ctx->params->chunks_in_zrwa * 2;
	ctx->params->stripe_prog_bitmap_size_bytes = 
		BITS_TO_BYTES(sector_to_block_addr(
			ctx->params->stripes_in_stripe_prog_bitmap * ctx->params->su_sectors * ctx->params->stripe_width));
	printk("stripes_in_stripe_prog_bitmap: %d, stripe_prog_bitmap_size_bytes: %d",\
		ctx->params->stripes_in_stripe_prog_bitmap, ctx->params->stripe_prog_bitmap_size_bytes);

#ifdef SAMSUNG_MODE
	ctx->params->max_io_len = (ZRWASZ * ctx->params->stripe_width / 2 - ctx->params->stripe_sectors);
#else
	ctx->params->max_io_len = (ZRWASZ * ctx->params->stripe_width / 2);
#endif
	if ((s64)ctx->params->max_io_len < 0) {
		printk("ZRWASZ is too small to setup ZRAID\n");
		goto err;
	}
	// ctx->params->max_io_len = (ZRWASZ * ctx->params->stripe_width / 4);

	// Lookup devs and set up logical volume
	ctx->devs = kcalloc(ctx->params->array_width, sizeof(struct raizn_dev),
			    GFP_NOIO);
	if (!ctx->devs) {
		ti->error = "dm-raizn: Failed to allocate devices in context";
		ret = -ENOMEM;
		goto err;
	}
	for (idx = 0; idx < ctx->params->array_width; idx++) {
		printk("dev: %s\n", argv[NUM_TABLE_PARAMS + idx]);
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
	raizn_init_stat_counter(ctx);
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

	// TODO: don't restict in here. bio must be splitted inside ZRAID
	ti->max_io_len = ZRWASZ * ctx->params->stripe_width / 2;
	// ti->max_io_len = ZRWASZ * ctx->params->stripe_width / 4;

	// printk("max_hw_sectors: %d, num_zones: %d\n", ctx->params->max_io_len, ctx->params->num_zones);
	raizn_wq = alloc_workqueue(WQ_NAME, WQ_UNBOUND,
	// raizn_wq = alloc_workqueue(WQ_NAME, WQ_UNBOUND | WQ_HIGHPRI,
	// raizn_wq = alloc_workqueue(WQ_NAME, WQ_HIGHPRI,
				   ctx->num_io_workers +  ctx->num_gc_workers);
				//    0);
	raizn_gc_wq = alloc_workqueue(GC_WQ_NAME, WQ_UNBOUND, ctx->num_gc_workers);
	// raizn_manage_wq = alloc_workqueue(MANAGE_WQ_NAME, WQ_UNBOUND, 256);
	// raizn_manage_wq = alloc_workqueue(MANAGE_WQ_NAME, WQ_HIGHPRI | WQ_UNBOUND, 8);
	raizn_manage_wq = alloc_workqueue(MANAGE_WQ_NAME, WQ_HIGHPRI | WQ_UNBOUND, ctx->num_manage_workers);
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
		mdzone->pzone_wp += sizeof(dev->sb);
		bio->bi_iter.bi_sector = mdzone->start;
		// bio->bi_private = NULL;
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
	pr_err("idx: %d\n", idx);
	return ret;
}

// DM callbacks
static void raizn_dtr(struct dm_target *ti)
{	
#ifdef RECORD_PP_AMOUNT
	struct raizn_ctx *ctx = ti->private;
	// printk("★★★---pp_volatile: %llu\n", atomic64_read(&ctx->pp_volatile));
	// printk("★★★---pp_permanent: %llu\n", atomic64_read(&ctx->pp_permanent));
	// printk("★★★---gc_migrated: %llu\n", atomic64_read(&ctx->gc_migrated));
	// printk("★★★---gc_count: %llu\n", atomic64_read(&ctx->gc_count));
#endif
#ifdef RECORD_SUBIO
	// raizn_print_subio_counter(ti->private);
#endif
#ifdef RECORD_ZFLUSH
	raizn_print_zf_counter(ti->private);
#endif

	deallocate_target(ti);
	if (raizn_wq) {
		destroy_workqueue(raizn_wq);
	}
	if (raizn_gc_wq) {
		destroy_workqueue(raizn_gc_wq);
	}
	if (raizn_manage_wq) {
		destroy_workqueue(raizn_manage_wq);
	}
}

// Core RAIZN logic

bool check_prog_bitmap_empty(struct raizn_ctx *ctx, struct raizn_zone *lzone)
{
    unsigned long *bitmap = lzone->stripe_prog_bitmap;
	int i;
    for(i=0; i<BITS_TO_LONGS(ctx->params->stripe_prog_bitmap_size_bytes * 8); i++)
	{
		if (*(bitmap+i) != 0) {
            return false;
        }
	}
	return true;
}

// open every pzones in the lzone associated with the given lba
// caller should hold lzone->lock
static void raizn_open_zone_zrwa(struct raizn_ctx *ctx, struct raizn_zone *lzone)
{
	// already opend by other thread
	int lzone_cond = atomic_read(&lzone->cond);
	if ((lzone_cond == BLK_ZONE_COND_IMP_OPEN) 
		|| (lzone_cond == BLK_ZONE_COND_EXP_OPEN)) {
		printk("zrwa zone already opened, lzone start: %llu\n", lzone->start);
		return;
	}

	// TODO: don't know why, but raizn_reset_prog_bitmap_zone() doesn't work.
	// it passes check_prog_bitmap_empty() test below, but somehow makes runtime fail.
	// temporarily use reallocation
	// kfree(lzone->stripe_prog_bitmap);
	// lzone->stripe_prog_bitmap = kzalloc(ctx->params->stripe_prog_bitmap_size_bytes, GFP_KERNEL);
	// if (!lzone->stripe_prog_bitmap) {
	// 	printk("Failed to allocate stripe_prog_bitmap");
	// 	BUG_ON(1);
	// }
	if(!check_prog_bitmap_empty(ctx, lzone)) 
		BUG_ON(1);

	struct nvme_passthru_cmd *nvme_open_zone = kzalloc(sizeof(struct nvme_passthru_cmd), GFP_KERNEL);
	struct block_device *nvme_bdev;
	sector_t pba;
	int i, j, ret, zone_idx;
	zone_idx = lba_to_lzone(ctx, lzone->start);
#ifdef DEBUG
	BUG_ON(zone_idx >= ctx->params->num_zones);
#endif
	for (i=0; i<ctx->params->array_width; i++) {
#ifdef SAMSUNG_MODE
		nvme_bdev = ctx->raw_bdev;
		sector_t pzone_base_addr = i * ctx->params->div_capacity +
			(zone_idx * ctx->params->gap_zone_aggr * ctx->devs[0].zones[0].phys_len);
#else
		nvme_bdev = ctx->devs[i].dev->bdev;
		pba = ctx->devs[i].zones[zone_idx].start;
		pba = sector_to_block_addr(pba); // nvme cmd should have block unit addr
#endif
#ifdef DEBUG
		printk("dev idx: %d, start_lba: %llu\n", i, pba);
#endif

#ifdef SAMSUNG_MODE
		for (j=0; j<ctx->params->num_zone_aggr; j++) {
			pba = pzone_base_addr + j * ctx->devs[0].zones[0].phys_len;
			open_zone(nvme_open_zone, sector_to_block_addr(pba), NS_NUM, 1, 0, 0); // 3rd parameter is nsid of device e.g.) nvme0n2 --> 2
			ret = nvme_submit_passthru_cmd_sync(nvme_bdev, nvme_open_zone);
			if (ret != 0) {
				printk("[Failed]\tzrwa open zone: %d, idx: %d, pba: %llu\n", ret, j, (pba));
			}
#ifdef DEBUG
// #if 1
			else {
				printk("[success]\tzrwa open zone, idx: %d, pba: %llu\n", j, (pba));
			}
#endif
		}
#else
		open_zone(nvme_open_zone, pba, NS_NUM, 1, 0, 0); // 3rd parameter is nsid of device e.g.) nvme0n2 --> 2
		ret = nvme_submit_passthru_cmd_sync(nvme_bdev, nvme_open_zone);
		if (ret != 0) {
			printk("[Failed]\tzrwa open zone: %d, lzone start: %llu\n", ret, pba);
		}
#ifdef DEBUG
		else {
			printk("[success]\tzrwa open zone, idx: %d, lba: %llu\n", i, pba);
		}
#endif
#endif // SAMSUNG
	}
	// if (lzone_cond == BLK_ZONE_COND_EMPTY) {
	// 	for (i=0; i<ctx->params->array_width; i++) {
	// 		struct raizn_zone *pzone = &ctx->devs[i].zones[lba_to_dev_idx(ctx, lzone->start)];
	// 		pzone->pzone_wp = pzone->start;
	// 	}
	// }
	kfree(nvme_open_zone);
	atomic_set(&lzone->cond, BLK_ZONE_COND_IMP_OPEN);
}

// caller should hold lzone->lock
static int raizn_zone_stripe_buffers_init(struct raizn_ctx *ctx,
					  struct raizn_zone *lzone)
{
	if (lzone->stripe_buffers) {
		return 0;
	}
	lzone->stripe_buffers =
		kcalloc(STRIPE_BUFFERS_PER_ZONE,
			sizeof(struct raizn_stripe_buffer), GFP_NOIO);
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



static int raizn_open_zone(struct raizn_ctx *ctx, struct raizn_zone *lzone)
{
	mutex_lock(&lzone->lock);
	raizn_zone_stripe_buffers_init(ctx, lzone);
#if 1
// #ifdef PP_INPLACE
	raizn_open_zone_zrwa(ctx, lzone);
#endif

	mutex_unlock(&lzone->lock);
	return 0;
}


static void raizn_request_zone_finish(struct raizn_ctx *ctx, struct raizn_zone *lzone) 
{
	if ( (atomic_read(&lzone->cond) == BLK_ZONE_COND_FULL) ) {
		printk("zrwa zone already fulled, lzone start: %llu\n", lzone->start);
		return;
	}
	struct raizn_stripe_head *fn_sh =
		raizn_stripe_head_alloc(ctx, NULL, RAIZN_OP_ZONE_FINISH);
	fn_sh->zone = lzone;

	int fifo_idx, ret;
#ifdef MULTI_FIFO
	fifo_idx = lba_to_lzone(ctx, lzone->start) %
		min(ctx->num_cpus, ctx->num_io_workers);
	ret = kfifo_in_spinlocked(
		&ctx->zone_manage_workers[fifo_idx].work_fifo, &fn_sh,
		1, &ctx->zone_manage_workers[fifo_idx].wlock);
#else
	ret = kfifo_in_spinlocked(&ctx->zone_manage_workers.work_fifo, &fn_sh, 1,
				&ctx->zone_manage_workers.wlock);
	fifo_idx = 0;
#endif
	if (!ret) {
		pr_err("ERROR: %s kfifo insert failed!\n", __func__);
		return;
	}
	raizn_queue_manage(ctx, fifo_idx);

	// TODO: if some point needs accurate BLK_ZONE_COND_FULL condition, setting should be moved to raizn_do_finish_zone()
	atomic_set(&lzone->cond, BLK_ZONE_COND_FULL);
}

// finish every pzones in the lzone associated with the given lba
static void raizn_do_finish_zone(struct raizn_ctx *ctx, struct raizn_zone *lzone)
{
	struct nvme_passthru_cmd *nvme_cmd = kzalloc(sizeof(struct nvme_passthru_cmd), GFP_KERNEL);
	struct block_device *nvme_bdev;
	int i, j, ret;
	for (i=0; i<ctx->params->array_width; i++) {
#ifdef SAMSUNG_MODE
		nvme_bdev = ctx->raw_bdev;
		sector_t pzone_base_addr = i * ctx->params->div_capacity +
			(lba_to_lzone(ctx, lzone->start) * ctx->params->gap_zone_aggr * ctx->devs[0].zones[0].phys_len);
		for (j=0; j<ctx->params->num_zone_aggr; j++) {
			blkdev_zone_mgmt(nvme_bdev,
				REQ_OP_ZONE_FINISH,
				pzone_base_addr + j * ctx->devs[0].zones[0].phys_len,
				ctx->devs[0].zones[0].phys_len,
				GFP_NOIO);
		}
#else
		nvme_bdev = ctx->devs[i].dev->bdev;
		blkdev_zone_mgmt(nvme_bdev,
			REQ_OP_ZONE_FINISH,
			ctx->devs[i].zones[lba_to_lzone(ctx, lzone->start)].start,
#ifdef NON_POW_2_ZONE_SIZE
			ctx->devs[i].zones[0].len,
#else
			1 << ctx->devs[i].zone_shift,
#endif
			GFP_NOIO);
#endif
		continue;

		sector_t dev_start_lba, flush_lba;
		dev_start_lba = ctx->devs[i].zones[lba_to_lzone(ctx, lzone->start)].start;
		dev_start_lba = sector_to_block_addr(dev_start_lba); // nvme cmd should have block unit addr
		flush_lba = dev_start_lba + ctx->devs[i].zones[lba_to_lzone(ctx, lzone->start)].capacity - 1;
		flush_lba = sector_to_block_addr(flush_lba); // nvme cmd should have block unit addr
#ifdef DEBUG
		printk("zone idx: %d, finish_cmd_lba: %llu, flush_cmd_lba: %llu\n", i, dev_start_lba, flush_lba);
#endif
		finish_zone(nvme_cmd, dev_start_lba, NS_NUM, 0, 0); // 3rd parameter is nsid of device e.g.) nvme0n2 --> 2
		ret = nvme_submit_passthru_cmd_sync(nvme_bdev, nvme_cmd);
		if (ret != 0) {
			printk("[Failed]\tzrwa finish zone: %d\n", ret);
		}
#ifdef DEBUG
		else {
			printk("[success]\tzrwa finish zone, idx: %d, lba: %llu\n", i, lzone->start);
		}
#endif
	}
	kfree(nvme_cmd);
}


static int raizn_finish_zone(struct raizn_ctx *ctx, struct raizn_zone *lzone)
{
	// printk("zone[%d] fininshed!", lba_to_lzone(ctx, lzone->start));
	mutex_lock(&lzone->lock);
	raizn_zone_stripe_buffers_deinit(lzone);
#if 1
// #ifdef PP_INPLACE
	raizn_request_zone_finish(ctx, lzone);

	// struct bio *bio = bio_alloc_bioset(GFP_NOIO, 1, &dev->bioset);
	// bio_set_op_attrs(bio, REQ_OP_WRITE, REQ_FUA);
	// struct raizn_stripe_head *sh =
	// 	raizn_stripe_head_alloc(ctx, bio, RAIZN_OP_ZONE_FINISH);
	// raizn_zone_finish(sh);
	// atomic_set(&lzone->cond, BLK_ZONE_COND_FULL);
#else
	atomic_set(&lzone->cond, BLK_ZONE_COND_FULL);
#endif
	mutex_unlock(&lzone->lock);
	return 0;
}

void raizn_reset_lzone_structures(struct raizn_ctx *ctx, struct raizn_zone *lzone)
{
	lzone->last_complete_stripe = -1;
	lzone->waiting_str_num = -1;
	lzone->waiting_str_num2= -1;
	lzone->waiting_data_lba= -1;
	lzone->waiting_pp_lba= -1;
	atomic_set(&lzone->wait_count, 0);
	atomic_set(&lzone->wait_count2, 0);
	atomic_set(&lzone->wait_count_data, 0);
	atomic_set(&lzone->wait_count_pp, 0);
	raizn_reset_prog_bitmap_zone(ctx, lzone);
}

// Returns 0 on success, nonzero on failure
static int raizn_zone_mgr_execute(struct raizn_stripe_head *sh)
{
	struct raizn_ctx *ctx = sh->ctx;
#ifdef DEBUG
	BUG_ON(lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector) >= ctx->params->num_zones);
	BUG_ON(lba_to_stripe(ctx, sh->orig_bio->bi_iter.bi_sector) >= 
		(ctx->params->lzone_size_sectors / ctx->params->stripe_sectors));
#endif
	int lzone_num = lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector);
	struct raizn_zone *lzone = &ctx->zone_mgr.lzones[lzone_num];
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
			raizn_open_zone(ctx, lzone);
		case BLK_ZONE_COND_CLOSED: // Empty and closed transition to imp open
#if 1
// #ifdef PP_INPLACE
#else
			atomic_set(&lzone->cond, BLK_ZONE_COND_IMP_OPEN);
#endif
		case BLK_ZONE_COND_IMP_OPEN:
		case BLK_ZONE_COND_EXP_OPEN:
		default:
			lzone_wp = atomic64_read(&lzone->lzone_wp);
			// Empty, closed, imp and exp open all perform check to see if zone is now full
			if (sh->status == RAIZN_IO_COMPLETED) {
#ifdef DEBUG
#endif
				// printk("[DEBUG] %s tid: %d, wp: %llu, bi_sector: %llu, start: %llu, zone: %p\n", 
				// 	__func__, current->pid,
				// 	atomic64_read(&lzone->lzone_wp), bio_sectors(sh->orig_bio),
				// 	// lzone->lzone_wp, bio_sectors(sh->orig_bio),
				// 	lzone->start, lzone);
				atomic64_add(bio_sectors(sh->orig_bio), &lzone->lzone_wp);
				lzone_wp += bio_sectors(sh->orig_bio);
			} else if (lzone_wp >
				   sh->orig_bio->bi_iter.bi_sector) {
				pr_err("Cannot execute op %d to address %llu < wp %llu, zone addr: %p\n",
				       bio_op(sh->orig_bio),
				       sh->orig_bio->bi_iter.bi_sector,
				       lzone_wp,
					   lzone);
				return -1;
			}
			if (lzone_wp >= lzone->start + lzone->capacity) {
				raizn_finish_zone(ctx, lzone);
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
			raizn_zone_stripe_buffers_deinit(lzone); // checks for null
			raizn_reset_lzone_structures(ctx, lzone);
			if (sh->status == RAIZN_IO_COMPLETED) {
				atomic_set(&lzone->cond, BLK_ZONE_COND_EMPTY);
				atomic64_set(&lzone->lzone_wp, lzone->start);
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
			raizn_reset_lzone_structures(ctx, lzone);
			if (sh->status == RAIZN_IO_COMPLETED) {
				atomic_set(&lzone->cond, BLK_ZONE_COND_FULL);
				atomic64_set(&lzone->lzone_wp, lzone->start + lzone->capacity);
			}
		}
	}


	// if (op_is_flush(sh->orig_bio->bi_opf)) {
	// 	// Update persistence bitmap, TODO: this only works for writes now
	// 	sector_t start = sh->orig_bio->bi_iter.bi_sector;
	// 	sector_t len = bio_sectors(sh->orig_bio);
	// 	int start_su = lba_to_su(ctx, start);
	// 	int end_su = lba_to_su(ctx, start + len);
	// 	if (start_su < end_su) {
	// 		// Race condition if async reset, but that is not standard
	// 		bitmap_set(lzone->persistence_bitmap, start_su,
	// 			   end_su - start_su);
	// 	}
	// }
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
				pr_err("Added %llu bytes to temp bio, expected %llu\n",
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

void raizn_endio(struct bio *bio)
{
	// Common endio handles marking subio as failed and deallocation of stripe header
	struct raizn_sub_io *subio = bio->bi_private;
	struct raizn_stripe_head *sh = subio->sh;
	bool defer_put = subio->defer_put;
	// printk("(%d)raizn_endio", current->pid);

#ifdef DEBUG
// #if 1
	if (bio->bi_status) {
		// print_bio_info(sh->ctx, bio, __func__);
		// pr_err("error: %d", bio->bi_status);
		// pr_err("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
		// mdelay(1);
		// BUG_ON(1);
		if ((bio_op(bio) != REQ_OP_WRITE) && (bio_op(bio) != REQ_OP_READ) && (bio_op(bio) != REQ_OP_ZONE_APPEND)) {
			pr_err("[raizn_endio] error: %d, op: %d\n", bio->bi_status, bio_op(bio));
			print_bio_info(sh->ctx, bio, __func__);
		}
	}
	if ((sh->orig_bio)) {
		if ((sh->op == RAIZN_OP_ZONE_FINISH) || (bio_op(bio) == REQ_OP_ZONE_FINISH))
		// if ((lba_to_lzone(sh->ctx, sh->orig_bio->bi_iter.bi_sector) == DEBUG_TARG_ZONE_1) // debug
		// 	||(lba_to_lzone(sh->ctx, sh->orig_bio->bi_iter.bi_sector) == DEBUG_TARG_ZONE_2)) // debug
	//     print_bio_info(sh->ctx, bio, __func__);
	    // print_bio_info(sh->ctx, sh->oriGg_bio, __func__);
		printk("bio complete[%d:%d] lba: %lluKB(%dKB) ~ %lluKB(%dKB), dev: %d, pba: %lluKB, ref: %d, sh->op: %d, bio_op : %d, is_flush: %d\n", 
			lba_to_lzone(sh->ctx, sh->orig_bio->bi_iter.bi_sector), lba_to_stripe(sh->ctx, sh->orig_bio->bi_iter.bi_sector),
			sh->orig_bio->bi_iter.bi_sector/2, lba_to_stripe_offset(sh->ctx, sh->orig_bio->bi_iter.bi_sector)/2,  // absolute LBA,  stripe offset
			bio_end_sector(sh->orig_bio)/2, lba_to_stripe_offset(sh->ctx, bio_end_sector(sh->orig_bio))/2, // absolute LBA,  stripe offset
			get_bio_dev_idx(sh->ctx, bio), bio->bi_iter.bi_sector/2,
			atomic_read(&sh->refcount), sh->op, bio_op(bio), op_is_flush(sh->orig_bio->bi_opf));
			// atomic_read(&sh->refcount), op_is_write(bio_op(bio)));
	}
	else{
		// printk("[raizn_endio] no orig_bio!!");
	    print_bio_info(sh->ctx, bio, __func__);
	}
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
	// for GC zone cleaning
	if (subio->sub_io_type == RAIZN_SUBIO_PP_OUTPLACE) {
		atomic_dec(&subio->zone->refcount);
	}
#ifdef RECORD_SUBIO
	// if ((sh->op == RAIZN_OP_WRITE) && (subio->sub_io_type != RAIZN_SUBIO_OTHER))
	// 	raizn_record_subio(sh, subio);
#endif
	if (sh->op == RAIZN_OP_REBUILD_INGEST ||
	    sh->op == RAIZN_OP_REBUILD_FLUSH) {
		raizn_rebuild_endio(bio);
	} else {
		if (!defer_put) {
			bio_put(bio);
		}
		if (atomic_dec_and_test(&sh->refcount)) {
			bool bio_extended = 0; // for FLUSH
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
				sector_t start_lba = sh->orig_bio->bi_iter.bi_sector;
				sector_t end_lba = bio_end_sector(sh->orig_bio);
#ifdef DEBUG
// #if 1
				if (sh->op != RAIZN_OP_READ) {
					print_bio_info(sh->ctx, sh->orig_bio, __func__);
					printk("flush: %d, wp_log: %d, op_is_flush: %d\n", 
						sh->op == RAIZN_OP_FLUSH,
						sh->op == RAIZN_OP_WP_LOG,
						op_is_flush(sh->orig_bio->bi_opf));
				}
				// printk("sh->sub_ios: %p", sh->sub_ios);
				// printk("subio_idx: %d, subio_1: %p, subio_2: %p", 
				// 	atomic_read(&sh->subio_idx), sh->sub_ios[0], sh->sub_ios[1]);
#endif

#if (defined NOFLUSH) || (defined PP_OUTPLACE)
				bio_endio(sh->orig_bio);
#else
				if ((sh->op == RAIZN_OP_WP_LOG) || // WP_LOG complete
					((sh->op != RAIZN_OP_FLUSH) && !op_is_flush(sh->orig_bio->bi_opf))) {// Normal read/write
					bio_endio(sh->orig_bio);
					/* 
						TODO: flush req should not endio here. instead insert the end_lba and the 'sh' to a list.
						Then ZRWA flusher checks the list every time when increase WP.
						If WP is bigger than the end_lba, eventually the flush orig_bio of the inserted sh is endio'ed
					*/
				}
				else { // flush-related I/Os, must write WP LOG
					printk("FLUSH");
					print_bio_info(sh->ctx, sh->orig_bio, __func__);
					if (raizn_write_wp_log(sh, end_lba)) // sh->op changed to RAIZN_OP_WP_LOG in raizn_write_wp_log()
					{
						bio_extended = 1;
					}
					else
						bio_endio(sh->orig_bio);
				}
				
#endif
				// printk("orig_bio complete start_lba: %lluKB, end_lba: %lluKB", start_lba/2, end_lba/2);
				// printk("orig_bio complete start_lba: %llu, end_lba: %llu", start_lba, end_lba);
// #ifdef PP_INPLACE
#if (defined NOFLUSH) || (defined PP_OUTPLACE)
				if (sh->op == RAIZN_OP_WRITE)
#else
				if ((sh->op == RAIZN_OP_WRITE) ||
					((sh->op == RAIZN_OP_WP_LOG) && bio_extended))
				// if ((sh->op == RAIZN_OP_WRITE) && !op_is_flush(sh->orig_bio->bi_opf) || // Normal write
				// 	((sh->op == RAIZN_OP_WP_LOG) && !bio_extended)) // WP_LOG complete 
				// TODO: In principle, pp_manage must be called after the WP logs are completed. (To prevent the following writes)
				// But, RAIZN doesn't follow this sementics.
#endif
				{
					// no need for MD zones		
					if (lba_to_lzone(sh->ctx, start_lba) < sh->ctx->params->num_zones) {
						raizn_pp_manage(sh->ctx, start_lba, end_lba);
					}
#ifdef DEBUG
// #if 1
					else 
						printk("++++++++lzone exceed! no pp_manage, start: %llu, zone_idx: %d, num_zones: %d, bool: %d",
							start_lba, lba_to_lzone(sh->ctx, start_lba), sh->ctx->params->num_zones,
							lba_to_lzone(sh->ctx, start_lba) < sh->ctx->params->num_zones);
#endif							
				}
							
				if (lba_to_stripe(sh->ctx, start_lba) != lba_to_stripe(sh->ctx, end_lba)) // even if orig_bio is on the end boundary of a single stripe, end_lba is calculated as next stripe.
				{
					// TODO
					// reset_stripe_buf(sh->ctx, start_lba);
				}
			}
			if (!bio_extended) {
				if (sh->next) {
					// printk("sh->op: %d", sh->op);
					// BUG_ON(!((sh->op == RAIZN_OP_ZONE_RESET) || (sh->op == RAIZN_OP_ZONE_RESET_LOG)));
					raizn_process_stripe_head(sh->next);
				}
				raizn_stripe_head_free(sh);
			}
		}
	}
}

// add bio data into lzone->stripe_buffer
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
		// printk("--stripe_buffer[%d]--\n", i);
		// print_buf(stripe_units[i]);
		// printk("\n");
		
	}

	xor_blocks(ctx->params->stripe_width,
		   ctx->params->su_sectors << SECTOR_SHIFT, dst, stripe_units);
	// printk("--parity buf--\n");
	// print_buf(dst);
	// printk("\n");
	return 0;
}

// xor bio with pre-calculated part parity
// xor bio
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
		kzalloc(ctx->params->stripe_sectors << SECTOR_SHIFT, GFP_KERNEL);
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
			sh, &dev->bioset, 1, RAIZN_SUBIO_REBUILD, NULL);
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
			if (lba + ctx->params->stripe_sectors >= atomic64_read(&lzone->lzone_wp)) {
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

static void raizn_zone_manage(struct work_struct *work)
{
	struct raizn_workqueue *wq = container_of(work, struct raizn_workqueue, work);
	struct raizn_dev *dev = wq->dev;
	struct raizn_stripe_head *sh;
	while (kfifo_out_spinlocked(&wq->work_fifo, &sh, 1, &wq->rlock)) {
		if (sh->op == RAIZN_OP_ZONE_FINISH) {
			raizn_do_finish_zone(sh->ctx, sh->zone);
		}
		else if (sh->op == RAIZN_OP_ZONE_ZRWA_FLUSH) {
			raizn_do_zrwa_flush(sh, sh->start_lba, sh->end_lba);
		}
		raizn_stripe_head_free(sh);
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
	int ret, j;
	unsigned int flags;
	while (kfifo_out_spinlocked(&wq->work_fifo, &sh, 1, &wq->rlock)) {
		struct raizn_ctx *ctx = sh->ctx;
		struct raizn_zone *gczone = sh->zone;
		sector_t gczone_wp = gczone->pzone_wp;
		// printk("[DEBUG] %s sh->op: %d, gczone->start: %llu, gczone->wp: %llu\n",
		// 	__func__, sh->op, gczone->start, gczone_wp);

		if (sh->op == RAIZN_OP_GC && gczone_wp > gczone->start) {
			if (gczone_wp > gczone->start) {
				// profile_bio(sh);
				BUG_ON((gczone->start * dev->zones[0].len) <
				       ctx->params->num_zones);
#ifdef RECORD_PP_AMOUNT
		        atomic64_inc(&ctx->gc_count);
#endif
#ifdef SAMSUNG_MODE
				struct block_device *nvme_bdev = ctx->raw_bdev;
				sector_t pzone_base_addr = dev->idx * ctx->params->div_capacity +
					(pba_to_pzone(ctx, gczone->start) * ctx->params->gap_zone_aggr * ctx->devs[0].zones[0].phys_len);
				for (j=0; j<ctx->params->num_zone_aggr; j++) {
					blkdev_zone_mgmt(nvme_bdev,
						REQ_OP_ZONE_FINISH,
						pzone_base_addr + j * ctx->devs[0].zones[0].phys_len,
						ctx->devs[0].zones[0].phys_len,
						GFP_NOIO);
				}
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
							RAIZN_SUBIO_OTHER,
							ctx->zone_mgr.gen_counts,
							gencount_size);
					struct raizn_sub_io *sb_io =
						raizn_alloc_md(
							sh, 0, gczone->dev,
							RAIZN_ZONE_MD_GENERAL,
							RAIZN_SUBIO_OTHER,
							&gczone->dev->sb,
							PAGE_SIZE);
					bio_set_op_attrs(gencount_io->bio,
							 REQ_OP_ZONE_APPEND,
							 REQ_FUA);
					bio_set_op_attrs(sb_io->bio,
							 REQ_OP_ZONE_APPEND,
							 REQ_FUA);
#ifdef PP_INPLACE					
					// TODO: add sbuf_io as below MD_PARITY_LOG. PP_INPLACE uses SB zone as PP zone.
#endif

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
				} 
#ifdef PP_OUTPLACE
				else if (gczone->zone_type ==
					   RAIZN_ZONE_MD_PARITY_LOG) {
					raizn_stripe_head_hold_completion(sh);
					for (int zoneno = 0;
					     zoneno < ctx->params->num_zones;
					     ++zoneno) {
						struct raizn_zone *lzone =
							&ctx->zone_mgr
								 .lzones[zoneno];
						int cond;
						size_t stripe_offset_bytes;
						sector_t lzone_wp = atomic64_read(&lzone->lzone_wp);
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
								RAIZN_ZONE_MD_PARITY_LOG,
								RAIZN_SUBIO_PP_OUTPLACE,
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
				} 
#endif
				else {
					pr_err("FATAL: Cannot garbage collect zone %llu on dev %d of type %d\n",
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
				gczone->pzone_wp = gczone->start;
				gczone->zone_type = RAIZN_ZONE_DATA;
				atomic_set(&gczone->cond, BLK_ZONE_COND_EMPTY);
				ret = kfifo_in_spinlocked(&dev->free_zone_fifo,
						    &gczone, 1,
						    &dev->free_wlock);
				if (!ret) {
					pr_err("ERROR: %s kfifo insert failed!\n", __func__);
					return;
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
				while (ctx->zone_mgr.rebuild_mgr.rp <
				       	cur_zone->pzone_wp) {
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
					RAIZN_SUBIO_REBUILD, NULL);
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
			subio->bio->bi_iter.bi_sector = zone->pzone_wp;
#ifdef SMALL_ZONE_AGGR
			raizn_submit_bio_aggr(ctx, __func__, subio->bio, dev, 0);
#else			
			raizn_submit_bio(ctx, __func__, subio->bio, 0);
#endif			
			// Update write pointer
#ifdef ATOMIC_WP
			spin_lock_irqsave(&zone->pzone_wp_lock, flags);
			zone->pzone_wp += ctx->params->su_sectors;
			spin_unlock_irqrestore(&zone->pzone_wp_lock, flags);
#else
			zone->pzone_wp += ctx->params->su_sectors;
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
			submitted = 1;
		}
#ifdef DEBUG
		printk("raizn_swap_mdzone waiting");
		msleep(1000);
		// mdelay(1000);
#else
		printk("raizn_swap_mdzone waiting");
		usleep_range(10, 20);
		// usleep_range(2, 5);
		// udelay(2);
#endif
		goto retry;
	}
	// BUG_ON(mdtype >= RAIZN_ZONE_NUM_MD_TYPES);
	dev->md_zone[mdtype] = new_md_zone;
	new_md_zone->zone_type = mdtype;
	atomic64_set(&new_md_zone->mdzone_wp, 0);
	// BUG_ON(new_md_zone->zone_type == RAIZN_ZONE_DATA);
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
	unsigned int flags;
#if (defined NOFLUSH) || (defined PP_OUTPLACE)
	mutex_lock(&dev->lock);
#else
	spin_lock_irqsave(&dev->lock, flags);
#endif
	mdzone = dev->md_zone[mdtype];
	if (mdzone->capacity < atomic64_add_return(md_sectors, &mdzone->mdzone_wp)) {
		// printk("mdzone start: %llu, cap: %llu, wp: %llu, md_sectors: %d\n", mdzone->start, mdzone->capacity, atomic64_read(&mdzone->mdzone_wp), md_sectors);
		mdzone->pzone_wp = mdzone->start + atomic64_read(&mdzone->mdzone_wp);
		mdzone = raizn_swap_mdzone(sh, dev, mdtype, mdzone);
		if (mdzone == NULL)
			return NULL;
	}
	atomic_inc(&mdzone->refcount);
#if (defined NOFLUSH) || (defined PP_OUTPLACE)
	mutex_unlock(&dev->lock);
#else
	spin_unlock_irqrestore(&dev->lock, flags);
#endif
	return mdzone;
}

static struct raizn_sub_io *raizn_alloc_md(struct raizn_stripe_head *sh,
					   sector_t lzoneno,
					   struct raizn_dev *dev,
					   raizn_zone_type mdtype, 
					   sub_io_type_t subio_type,
					   void *data,
					   size_t len)
{
	struct raizn_ctx *ctx = sh->ctx;
	struct raizn_sub_io *mdio = raizn_stripe_head_alloc_bio(
		sh, &dev->bioset, data ? 2 : 1, subio_type, data);
	
	struct bio *mdbio = mdio->bio;
	struct page *p;
	sector_t sectors;
#if defined (DUMMY_HDR) && defined (PP_OUTPLACE)
	sectors =
		(round_up(len, PAGE_SIZE) + PAGE_SIZE) >>
			SECTOR_SHIFT; // TODO: does round_up round 0 to PAGE_SIZE?
#else
	if ((mdtype == RAIZN_ZONE_MD_GENERAL)) {
		// printk("RAIZN_ZONE_MD_GENERAL: %d\n", mdtype);
		sectors = (round_up(len, PAGE_SIZE) + PAGE_SIZE) >>	SECTOR_SHIFT; // TODO: does round_up round 0 to PAGE_SIZE?
	}
	else {
		sectors = (round_up(len, PAGE_SIZE)) >>	SECTOR_SHIFT; // TODO: does round_up round 0 to PAGE_SIZE?
	}
#endif
	struct raizn_zone *mdzone = raizn_md_lba(
		sh, dev, mdtype, sectors); // TODO: does round_up round 0 to PAGE_SIZE?
	BUG_ON(!mdzone);
	mdio->zone = mdzone;

	if (subio_type == RAIZN_SUBIO_WP_LOG) {
		// mdio->header.data
		// TODO: fill header.data with WPs of all opened zones
		// maybe linked list tracking for opened zones is needed for optimization (minizing the iteration over all zones overhead)
	}
	mdio->header.header.zone_generation =
		ctx->zone_mgr.gen_counts[lzoneno / RAIZN_GEN_COUNTERS_PER_PAGE]
			.zone_generation[lzoneno % RAIZN_GEN_COUNTERS_PER_PAGE];
	mdio->header.header.magic = RAIZN_MD_MAGIC;
	mdio->dbg = len;
	bio_set_op_attrs(mdbio, REQ_OP_ZONE_APPEND, 0);
	bio_set_dev(mdbio, dev->dev->bdev);
	mdbio->bi_iter.bi_sector = mdzone->start;
#if !(defined (DUMMY_HDR) && defined (PP_OUTPLACE))
	if ((mdtype == RAIZN_ZONE_MD_GENERAL))
#endif
	{
		p = is_vmalloc_addr(&mdio->header) ? vmalloc_to_page(&mdio->header) :
							virt_to_page(&mdio->header);
		if (bio_add_page(mdbio, p, PAGE_SIZE, offset_in_page(&mdio->header)) !=
			PAGE_SIZE) {
			pr_err("Failed to add md header page\n");
			bio_endio(mdbio);
			BUG_ON(1);
			return NULL;
		}
	}
	if ((data) && (subio_type != RAIZN_SUBIO_WP_LOG)) {
		p = is_vmalloc_addr(data) ? vmalloc_to_page(data) :
					    virt_to_page(data);
		if (bio_add_page(mdbio, p, len, 0) != len) {
			pr_err("Failed to add md data page\n");
			bio_endio(mdbio);
			BUG_ON(1);
			return NULL;
		}
	}
	// BUG_ON((mdbio->bi_iter.bi_sector >> dev->zone_shift) <
	//        ctx->params->num_zones);
	// BUG_ON(((round_up(len, PAGE_SIZE) + PAGE_SIZE) >> SECTOR_SHIFT) <
	//        bio_sectors(mdbio));
	return mdio;
}

// Header must not be null, but data can be null
// Returns 0 on success, nonzero on failure
int raizn_write_md(struct raizn_stripe_head *sh, sector_t lzoneno,
			  struct raizn_dev *dev, raizn_zone_type mdtype,
	   		  sub_io_type_t subio_type,
			  void *data, size_t len)
{
	struct raizn_sub_io *mdio =
		raizn_alloc_md(sh, lzoneno, dev, mdtype, subio_type, data, len);
#if defined (DUMMY_HDR) && defined (PP_OUTPLACE)
	if (!mdio) {
		pr_err("Fatal: Failed to write metadata\n");
		return -1;
	}
#else
	if (!mdio)
		return 0;
#endif
#ifdef RECORD_PP_AMOUNT
#if defined (DUMMY_HDR) && defined (PP_OUTPLACE)
	atomic64_add((len + PAGE_SIZE) >> SECTOR_SHIFT, &sh->ctx->pp_permanent);
#else
	atomic64_add((len) >> SECTOR_SHIFT, &sh->ctx->pp_permanent);
#endif
#endif
#ifdef TIMING
	uint64_t lba = mdio->bio->bi_iter.bi_sector;
	printk("pp %llu %d %d %d %llu %d\n", 
		ktime_get_ns(), smp_processor_id(), current->pid, get_dev_idx(sh->ctx, dev), lba, bio_sectors(mdio->bio));
#endif
#ifdef SMALL_ZONE_AGGR
	raizn_submit_bio_aggr(sh->ctx, __func__, mdio->bio, dev, 0);
#else
	raizn_submit_bio(sh->ctx, __func__, mdio->bio, 0);
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
			sh, &dev->bioset, RAIZN_MAX_BVECS, sub_io_type, NULL);
		if (!subio) {
			pr_err("Failed to allocate subio\n");
		}
		sh->bios[dev->idx] = subio->bio;
		subio->dev = dev;
		subio->dev_idx = dev->idx;
		subio->bio->bi_opf = sh->orig_bio->bi_opf;
		subio->bio->bi_iter.bi_sector =
			lba_to_pba_default(sh->ctx, lba);
		bio_set_dev(subio->bio, dev->dev->bdev);
		subio->zone = &dev->zones[lba_to_lzone(sh->ctx, lba)];
	}
	return sh->bios[dev->idx];
}

static int raizn_write(struct raizn_stripe_head *sh)
{
#ifdef DEBUG
	BUG_ON(in_interrupt());
#endif
	int num_xor_units = 0;
	struct raizn_ctx *ctx = sh->ctx;
	sector_t start_lba = sh->orig_bio->bi_iter.bi_sector;
	sector_t end_lba = bio_end_sector(sh->orig_bio);
	int start_stripe_id = lba_to_stripe(ctx, start_lba);
	int lzone_num = lba_to_lzone(ctx, start_lba);
	struct raizn_zone *lzone = &ctx->zone_mgr.lzones[lzone_num];
#ifdef DEBUG
	if (op_is_flush(bio_op(sh->orig_bio))) {
		printk("###FUA!");
	}
// #if 1
    if ((lzone_num == DEBUG_TARG_ZONE_1) // debug
    	|| (lzone_num == DEBUG_TARG_ZONE_2)) // debug
	printk("(%d)[%d:%d] %s: rw: %d, lba: %lluKB(%dKB) ~ %lluKB(%dKB), length: %dKB\n",
		current->pid, lzone_num, lba_to_stripe(ctx, start_lba),
		__func__, op_is_write(bio_op(sh->orig_bio)),
		start_lba/2, lba_to_stripe_offset(ctx, start_lba)/2,  // absolute LBA,  stripe offset
		end_lba/2, lba_to_stripe_offset(ctx, end_lba)/2, // absolute LBA,  stripe offset
		bio_sectors(sh->orig_bio)/2);
#endif
#ifdef RECORD_PP_AMOUNT
	atomic64_add(bio_sectors(sh->orig_bio), &ctx->total_write_amount);
	atomic64_inc(&ctx->total_write_count);
#endif
	// End LBA of the first stripe in this IO
	sector_t leading_stripe_end_lba =
		min(lba_to_stripe_addr(ctx, start_lba) +
			    ctx->params->stripe_sectors,
		    end_lba);
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
	// Maximum number of parity to write. This could be better, as it currently ignores cases where a subset of the final parity is known
	int parity_su = (bio_sectors(sh->orig_bio) - leading_substripe_sectors -
			 trailing_substripe_sectors) /
				ctx->params->stripe_sectors +
			// (leading_substripe_sectors > 0 ? 1 : 0) +
			// (trailing_substripe_sectors > 0 ? 1 : 0);
			2;
#ifdef DEBUG
// #if 1
	// printk("[DEBUG] [%s] start_lba: %d, start_stripe_id: %d\n", __func__, start_lba, start_stripe_id);
	// printk("[DEBUG] [%s] bio_sectors(sh->orig_bio): %llu, middle: %llu, lead: %llu, trail: %llu, parity_su: %d\n",
	// 	 __func__, 
	// 	 bio_sectors(sh->orig_bio), 
	// 	 bio_sectors(sh->orig_bio) - leading_substripe_sectors - trailing_substripe_sectors,
	// 	 leading_substripe_sectors,
	// 	 trailing_substripe_sectors,
	// 	 parity_su);
#endif
	struct bio *bio;
	struct bio_vec bv;
	struct bvec_iter iter;
	struct raizn_dev *dev;
	int parity_buf_adv = 0;
	unsigned int op_flags =
		op_is_flush(bio_op(sh->orig_bio)) ?
			((sh->orig_bio->bi_opf & REQ_FUA) | REQ_PREFLUSH) :
			0;
#ifndef BITMAP_OFF
	// Tracks which devices have been written to by a subio, and which have to be explicitly flushed (if necessary)
	DECLARE_BITMAP(dev_bitmap, RAIZN_MAX_DEVS);
#endif
	raizn_stripe_head_hold_completion(sh);
	BUG_ON(bio_sectors(sh->orig_bio) == 0);
	// Allocate buffer to hold all parity
#ifndef IGNORE_PARITY_BUF
	sh->parity_bufs =
		vzalloc(parity_su * (ctx->params->su_sectors << SECTOR_SHIFT));
		// kzalloc(parity_su * (ctx->params->su_sectors << SECTOR_SHIFT), GFP_KERNEL);
	// printk("--parity_su: %d--\n", parity_su);
	if (!sh->parity_bufs) {
		pr_err("Failed to allocate parity buffers\n");
		BUG_ON(1);
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
#ifdef PP_OUTPLACE
		// Always calculate full parity, but only use part of it
	// printk("--parity_buf 1: %p--\n", sh->parity_bufs);
		raizn_stripe_buffer_parity(ctx, start_lba, sh->parity_bufs);
		parity_buf_adv += ctx->params->su_sectors;
		// num_xor_units = ctx->params->stripe_width;
		// calc_part_parity(ctx, start_lba, num_xor_units, sh->parity_bufs);
#endif
#endif
#ifdef PP_OUTPLACE
		// if remaining is smaller than one chunk, full parity can be written instead of part parity
		if (ctx->params->stripe_sectors - lba_to_stripe_offset(ctx, leading_stripe_end_lba-1) >=  
				ctx->params->su_sectors) {
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
				lzone_num,
				lba_to_parity_dev(ctx, start_lba),
				RAIZN_ZONE_MD_PARITY_LOG,
				RAIZN_SUBIO_PP_OUTPLACE,
				sh->parity_bufs +
					leading_substripe_start_offset_bytes,
				leading_substripe_parity_bytes);
// #elif defined (PP_INPLACE)
// 			raizn_write_pp(sh, 
// 				sh->parity_bufs +
// 					leading_substripe_start_offset_bytes,
// 				leading_substripe_parity_bytes);
#endif
		}
#endif
	}
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
	// printk("--parity_buf 2: %p--\n", sh->parity_bufs);
	// printk("--parity buf before--\n");
	// 	print_buf(sh->parity_bufs);
	// printk("--parity buf before--\n");
	// 	print_buf(sh->parity_bufs + parity_buf_adv);
	// printk("\n");
		raizn_bio_parity(ctx, bio, sh->parity_bufs + parity_buf_adv);
	// printk("--parity buf after--\n");
	// 	print_buf(sh->parity_bufs);
	// printk("--parity buf after--\n");
	// 	print_buf(sh->parity_bufs + parity_buf_adv);
	// printk("\n");
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
				   end_lba);
		// Calculate partial parity
		// submit parity log, always starts at offset 0 in parity, may end before su_bytes
		raizn_stripe_buffer_parity(
			ctx, trailing_substripe_start_lba,
			sh->parity_bufs +
				(parity_su - 1) * ctx->params->su_sectors);
#ifdef PP_OUTPLACE
    	// num_xor_units = ctx->params->stripe_width;
		// calc_part_parity(
		// 	ctx, trailing_substripe_start_lba,
		// 	num_xor_units,
		// 	sh->parity_bufs +
		// 		(parity_su - 1) * ctx->params->su_sectors);
#endif
#endif

		// if remaining is smaller than one chunk, full parity can be written instead of part parity
		// if (ctx->params->stripe_sectors - lba_to_stripe_offset(ctx, bio_end_sector(sh->orig_bio) - 1) >=  
		// 		ctx->params->su_sectors) {
#ifndef IGNORE_PART_PARITY
#ifdef PP_OUTPLACE
			raizn_write_md(
				sh, lzone_num,
				lba_to_parity_dev(ctx, trailing_substripe_start_lba),
				RAIZN_ZONE_MD_PARITY_LOG,
				RAIZN_SUBIO_PP_OUTPLACE,
				//sh->parity_bufs, trailing_substripe_parity_bytes);
				sh->parity_bufs +
					(parity_su - 1) * ctx->params->su_sectors,
				trailing_substripe_parity_bytes);
// #elif defined (PP_INPLACE)
// 			raizn_write_pp(sh, 
// 				sh->parity_bufs +
// 					(parity_su - 1) * ctx->params->su_sectors,
// 				trailing_substripe_parity_bytes);
#endif
#endif
		// }
	}
#ifndef IGNORE_PART_PARITY
#if defined (PP_INPLACE)
	// if (!((lba_to_stripe_offset(ctx, start_lba)==0) && (lba_to_stripe_offset(ctx, end_lba)==0)))
		raizn_write_pp(sh, parity_su);
#endif
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
			// printk("lba: %d, chunk_end_lba: %d, su_remaining_bytes: %d, bv.bv_len: %d, data_pos: %d\n",
			// 	lba, chunk_end_lba, su_remaining_bytes, bv.bv_len, data_pos);
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
			// If we write the last sector of a stripe unit, add parity
			if ( (ctx->params->stripe_sectors - lba_to_stripe_offset(ctx, lba) <= ctx->params->su_sectors) ||
				unlikely(lba_to_lzone(ctx, lba) != lba_to_lzone(ctx, chunk_end_lba)) // the last chunk (lba_to_stripe_offset(ctx, lba) becomes 0 at the end of lzone)
			) {
				dev = lba_to_parity_dev(ctx, lba);
				bio = check_alloc_dev_bio(
					sh, dev,
					lba, RAIZN_SUBIO_FP);
				// printk("##parity bio add, dev: %d, lba: %d, pba: %llu, zone: %d, chunk_bytes: %d, bv.bv_offset + data_pos: %llu\n",
				// 	lba_to_parity_dev_idx(ctx, lba), lba, lba_to_pba_default(ctx, lba),
				// 	lba_to_lzone(ctx, lba), chunk_bytes, bv.bv_offset + data_pos);
				if (bio_add_page(bio,
						 vmalloc_to_page(
							 sh->parity_bufs +
							 (su_bytes *
							  (stripe_id -
							   start_stripe_id))),
						//  su_bytes, 0) < su_bytes) {
						 chunk_bytes, bv.bv_offset + data_pos) < chunk_bytes) {
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
submit:
	for (int subio_idx = 0; subio_idx <= atomic_read(&sh->subio_idx);
	     ++subio_idx) {
		struct raizn_sub_io *subio = sh->sub_ios[subio_idx];
		struct raizn_zone *zone = subio->zone;
		struct block_device *nvme_bdev;
		sector_t start_lba;
		int zone_idx; 
		int ret;
		if ((subio->sub_io_type == RAIZN_SUBIO_DATA) ||
			(subio->sub_io_type == RAIZN_SUBIO_PP_INPLACE) ||
			(subio->sub_io_type == RAIZN_SUBIO_FP))  {
			int bio_len = bio_sectors(subio->bio); 
			// if (atomic64_read(&subio->zone->wp) < 0) {
			// 	printk("[DEBUG] wp: %llu, bi_sector: %llu\n", 
			// 		subio->zone->wp, subio->bio->bi_iter.bi_sector);
			// 	return -1;
			// }
// #if 1
#ifdef DEBUG
			// printk("[DEBUG]1 %s tid: %d, dev: %d(%s), wp: %llu, bi_sector: %llu, start: %llu, subio: %p, zone: %p\n", 
			// 	__func__, current->pid,
			// 	get_bio_dev_idx(ctx, subio->bio),
			// 	get_bio_dev_idx(ctx, subio->bio) == lba_to_parity_dev_idx(ctx, pba_to_pzone(ctx, subio->bio->bi_iter.bi_sector) * ctx->params->stripe_sectors) ?
			// 		"PARITY":"DATA",
			// 	atomic64_read(&subio->zone->wp), subio->bio->bi_iter.bi_sector,
			// 	subio->zone->start, subio->bio, subio->zone);
#endif
			while (!subio_ready2submit(subio, 
				(subio->sub_io_type == RAIZN_SUBIO_DATA) || (subio->sub_io_type == RAIZN_SUBIO_FP) 
				)) {
#ifndef PERF_MODE
// #if 1
				if (lzone->waiting_data_lba == subio->bio->bi_iter.bi_sector) {
					int wc  = atomic_read(&lzone->wait_count_data);
					if (wc>=100) {
						if (wc%1000000 == 0) {
							sector_t allowed_range;
							if ((subio->sub_io_type == RAIZN_SUBIO_DATA) || (subio->sub_io_type == RAIZN_SUBIO_FP))
        						allowed_range = ZRWASZ/2 - bio_sectors(subio->bio);
							else
        						allowed_range = ZRWASZ - bio_sectors(subio->bio);
							printk("[raizn_write][%d:%d] lba: %llu, len: %llu, dev: %d(%s), wp: %llu, pba: %llu, diff: %llu, ZRWASZ: %d, allowed_range: %llu\n", 
								lzone_num,
								lba_to_stripe(ctx, sh->orig_bio->bi_iter.bi_sector),
								sh->orig_bio->bi_iter.bi_sector, bio_sectors(subio->bio),
								get_bio_dev_idx(ctx, subio->bio),
								get_bio_dev_idx(ctx, subio->bio) == lba_to_parity_dev_idx(ctx, sh->orig_bio->bi_iter.bi_sector) ?
									"PARITY":"DATA",
								subio->zone->pzone_wp, subio->bio->bi_iter.bi_sector,
								subio->bio->bi_iter.bi_sector - subio->zone->pzone_wp, 
								ZRWASZ,
								allowed_range);		
						}
						if (wc%100 == 0)
		    				usleep_range(10, 20);  // ## MAYBE Important code
					}
					atomic_inc(&lzone->wait_count_data);
				}
				else {
					atomic_set(&lzone->wait_count_data, 0);
					// usleep_range(10, 20);
					lzone->waiting_data_lba = subio->bio->bi_iter.bi_sector;
				}
#endif

#ifdef MQ_DEADLINE // ZRAID mq-deadline version (need to yield) --> maybe sleep() doesn't effect on performance
		    	usleep_range(10, 20);
#else
				udelay(2);
#endif
			}
			// mutex_lock(&subio->lock);
#ifdef TIMING
			if (op_is_write(bio_op(subio->bio))) {
				uint64_t lba = subio->bio->bi_iter.bi_sector;
				if (subio->sub_io_type == RAIZN_SUBIO_DATA)
					printk("data %llu %d %d %d %llu %d\n", 
						ktime_get_ns(), smp_processor_id(), current->pid, get_dev_idx(sh->ctx, subio->dev), lba, bio_sectors(subio->bio));
				else if (subio->sub_io_type == RAIZN_SUBIO_FP)
					printk("fp %llu %d %d %d %llu %d\n", 
						ktime_get_ns(), smp_processor_id(), current->pid, get_dev_idx(sh->ctx, subio->dev), lba, bio_sectors(subio->bio));
			}
#endif

#ifdef SMALL_ZONE_AGGR
			raizn_submit_bio_aggr(ctx, __func__, subio->bio, subio->dev, 0);
#else
			raizn_submit_bio(ctx, __func__, subio->bio, 0);
#endif
			// printk("[submit_data] lba: %llu ~ %llu, refcount: %d",
			// 	subio->sh->orig_bio->bi_iter.bi_sector,
			// 	bio_end_sector(subio->sh->orig_bio),
			// 	atomic_read(&subio->sh->refcount));
			/* There was a bug when "raizn_endio" processed between 
			 submit_bio_noacct(subio->bio) & zone->wp += bio_sectors(subio->bio)
			 "raizn_endio" frees the bio, bio_sectors(subio->bio) becomes to zero.
			 Thus WP isn't increased and the next write request will wait forever in udelay loop.
			 bio_sectors(subio->bio) is stored before the bio is submitted. 
			 After submit, the stored length is added to the zone's WP
			*/
#ifdef DEBUG
			// printk("[DEBUG]4 %s tid: %d, wp: %llu, bi_sector: %llu, add: %llu, start: %llu, subio: %p\n", 
			// 	__func__, current->pid,
			// 	// atomic64_read(&subio->zone->wp), subio->bio->bi_iter.bi_sector,
			// 	subio->zone->wp, subio->bio->bi_iter.bi_sector,
			// 	bio_len, subio->zone->start, subio->bio);
#endif
		}
	}
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
// #if 0
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

	struct bio *bio = sh->orig_bio;
	sector_t end_lba = bio_end_sector(bio);
#ifdef DEBUG
	// printk("(%d)[%d:%d] %s: bio_op: %d, lba: %lluKB(%dKB) ~ %lluKB(%dKB), length: %dKB\n",
	// 	current->pid, lba_to_lzone(ctx ,start_lba), lba_to_stripe(ctx, start_lba),
	// 	__func__, bio_op(bio),
	// 	start_lba/2, lba_to_stripe_offset(ctx, start_lba)/2,  // absolute LBA,  stripe offset
	// 	end_lba/2, lba_to_stripe_offset(ctx, end_lba)/2, // absolute LBA,  stripe offset
	// 	bio_sectors(bio)/2);
	if (bio_sectors(bio) == 0) {
		printk("zero read, imm return\n");
		bio_endio(bio);
		return 0;
	}
#endif

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
						RAIZN_SUBIO_REBUILD, NULL);
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
	printk("###FLUSH!");

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
	int zoneno = lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector), j, ret;
    struct raizn_zone *lzone = &ctx->zone_mgr.lzones[zoneno];
	raizn_stripe_head_hold_completion(sh);
	for (int devno = 0; devno < ctx->params->array_width; ++devno) {
		struct raizn_dev *dev = &ctx->devs[devno];
		struct raizn_zone *pzone = &dev->zones[zoneno];
		struct raizn_sub_io *subio = raizn_stripe_head_alloc_bio(
			sh, &dev->bioset, 1, RAIZN_SUBIO_DATA, NULL);
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
	int ret, j;
	int zoneno = lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector);
    struct raizn_zone *lzone = &ctx->zone_mgr.lzones[zoneno];
	raizn_stripe_head_hold_completion(sh);
	for (int devno = 0; devno < ctx->params->array_width; ++devno) {
		struct raizn_dev *dev = &ctx->devs[devno];
		struct raizn_zone *pzone = &dev->zones[zoneno];
		struct raizn_sub_io *subio = raizn_stripe_head_alloc_bio(
			sh, &dev->bioset, 1, RAIZN_SUBIO_DATA, NULL);
		subio->bio->bi_iter.bi_sector = lba_to_pba_default(
			ctx, sh->orig_bio->bi_iter.bi_sector);
		bio_set_op_attrs(subio->bio, REQ_OP_ZONE_FINISH, 0);
		bio_set_dev(subio->bio, dev->dev->bdev);
#ifdef SMALL_ZONE_AGGR
		subio->bio->bi_iter.bi_size = (ctx->params->num_zone_aggr << ctx->params->aggr_chunk_shift) << SECTOR_SHIFT;
		raizn_submit_bio_aggr(ctx, __func__, subio->bio, dev, 0);
#else
		raizn_submit_bio(ctx, __func__, subio->bio, 0);
		// struct block_device *nvme_bdev =  ctx->devs[devno].dev->bdev;
		// struct nvme_passthru_cmd *nvme_cmd = kzalloc(sizeof(struct nvme_passthru_cmd), GFP_KERNEL);
		// sector_t cmd_addr = pzone->start;
		// finish_zone(nvme_cmd, sector_to_block_addr(cmd_addr), NS_NUM, 0, 0); // 3rd parameter is nsid of device e.g.) nvme0n2 --> 2
		// ret = nvme_submit_passthru_cmd_sync(nvme_bdev, nvme_cmd);
		// if (ret != 0) {
		// 	printk("[Fail]\tzrwa finish zone ret: %d pba: %llu\n", ret, (cmd_addr));
		// }
// #ifdef DEBUG
// 		else
// 			printk("[Success]\tzrwa finish zone ret: %d pba: %llu\n", ret, (cmd_addr));
// #endif
// 		kfree(nvme_cmd);
#endif
#ifdef ATOMIC_WP
		spin_lock_irqsave(&pzone->pzone_wp_lock, flags);
		pzone->pzone_wp = pzone->start + pzone->capacity;
		spin_unlock_irqrestore(&pzone->pzone_wp_lock, flags);
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
	unsigned int flags;
	int ret;
	int zoneno = lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector);
	raizn_stripe_head_hold_completion(sh);
	ctx->zone_mgr.gen_counts[zoneno / RAIZN_GEN_COUNTERS_PER_PAGE]
		.zone_generation[zoneno % RAIZN_GEN_COUNTERS_PER_PAGE] += 1;
	for (int devno = 0; devno < ctx->params->array_width; ++devno) {
		struct raizn_dev *dev = &ctx->devs[devno];
		struct raizn_zone *pzone = &dev->zones[zoneno];
		struct raizn_sub_io *subio = raizn_stripe_head_alloc_bio(
			sh, &dev->bioset, 1, RAIZN_SUBIO_DATA, NULL);
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
		// struct block_device *nvme_bdev =  ctx->devs[devno].dev->bdev;
		// struct nvme_passthru_cmd *nvme_cmd = kzalloc(sizeof(struct nvme_passthru_cmd), GFP_KERNEL);
		// sector_t cmd_addr = pzone->start;
		// reset_zone(nvme_cmd, sector_to_block_addr(cmd_addr), NS_NUM, 0, 0); // 3rd parameter is nsid of device e.g.) nvme0n2 --> 2
		// ret = nvme_submit_passthru_cmd_sync(nvme_bdev, nvme_cmd);
		// if (ret != 0) {
		// 	printk("[Fail]\tzrwa reset zone ret: %d pba: %llu\n", ret, (cmd_addr));
		// }
// #ifdef DEBUG
// 		else
// 			printk("[Success]\tzrwa reset zone ret: %d pba: %llu\n", ret, (cmd_addr));
// #endif
		// kfree(nvme_cmd);
#endif
#ifdef ATOMIC_WP
		spin_lock_irqsave(&pzone->pzone_wp_lock, flags);
		pzone->pzone_wp = pzone->start;
		spin_unlock_irqrestore(&pzone->pzone_wp_lock, flags);
#else
		pzone->pzone_wp = pzone->start;
#endif
	}
	raizn_stripe_head_release_completion(sh);
	return DM_MAPIO_SUBMITTED;
}

static int raizn_zone_reset_top(struct raizn_stripe_head *sh)
{
	// printk("[raizn_zone_reset_top]");
	struct raizn_ctx *ctx = sh->ctx;
	struct raizn_dev *dev =
		lba_to_dev(ctx, sh->orig_bio->bi_iter.bi_sector);
	struct raizn_dev *parity_dev =
		lba_to_parity_dev(ctx, sh->orig_bio->bi_iter.bi_sector);
	int zoneno = lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector);
	struct raizn_stripe_head *log_sh =
		raizn_stripe_head_alloc(ctx, NULL, RAIZN_OP_ZONE_RESET_LOG);
	struct raizn_sub_io *devlog =
		raizn_alloc_md(sh, zoneno, dev, RAIZN_ZONE_MD_GENERAL, RAIZN_SUBIO_OTHER, NULL, 0);
	struct raizn_sub_io *pdevlog = raizn_alloc_md(
		sh, zoneno, parity_dev, RAIZN_ZONE_MD_GENERAL, RAIZN_SUBIO_OTHER, NULL, 0);
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
// #ifdef DEBUG
// #if 1
	// printk("### work thread pid: %d, core: %d\n", current->pid, smp_processor_id());
// #endif
#ifdef BATCH_WQ
	while (kfifo_out_spinlocked(&wq->work_fifo, &sh, 1, &wq->rlock)) {
#ifdef DEBUG
		BUG_ON(bio_op(sh->orig_bio) != REQ_OP_WRITE);
// #if 1
	sector_t start_lba = sh->orig_bio->bi_iter.bi_sector;
	sector_t end_lba = bio_end_sector(sh->orig_bio);
	struct raizn_ctx *ctx = sh->ctx;
    if ((lba_to_lzone(ctx, start_lba) == DEBUG_TARG_ZONE_1) // debug
    	|| (lba_to_lzone(ctx, start_lba) == DEBUG_TARG_ZONE_2)) // debug
	printk("(%d)[%d:%d] %s/ idx: %d rw: %d, lba: %lluKB(%dKB) ~ %lluKB(%dKB), length: %dKB\n",
		current->pid, lba_to_lzone(ctx ,start_lba), lba_to_stripe(ctx, start_lba),
		__func__, wq->idx, op_is_write(bio_op(sh->orig_bio)),
		start_lba/2, lba_to_stripe_offset(ctx, start_lba)/2,  // absolute LBA,  stripe offset
		end_lba/2, lba_to_stripe_offset(ctx, end_lba)/2, // absolute LBA,  stripe offset
		bio_sectors(sh->orig_bio)/2);
#endif
		raizn_write(sh);
	}
#else
	kfifo_out_spinlocked(&wq->work_fifo, &sh, 1, &wq->rlock);
	raizn_write(sh);
#endif
}

static int raizn_process_stripe_head(struct raizn_stripe_head *sh)
{
	struct raizn_ctx *ctx = sh->ctx;
	int ret;
#ifdef DEBUG
	// printk("[DEBUG] [%s] op = %d\n", __func__, sh->op);
#endif
	switch (sh->op) {
	case RAIZN_OP_READ:
		return raizn_read(sh);
	case RAIZN_OP_WRITE:
		// sh->orig_bio->bi_opf |= REQ_FUA;

		// Validate the write can be serviced
		if (raizn_zone_mgr_execute(sh) != 0) {
			pr_err("Failed to validate write\n");
			return DM_MAPIO_KILL;
		}
		if (ctx->num_io_workers > 1) {
#ifdef MULTI_FIFO
			int fifo_idx;
			// fifo_idx = (lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector) + lba_to_stripe(ctx, sh->orig_bio->bi_iter.bi_sector)) %
			fifo_idx = (lba_to_lzone(ctx, sh->orig_bio->bi_iter.bi_sector)) %
				min(ctx->num_cpus, ctx->num_io_workers);

#ifdef DEBUG
// #if 1
			sector_t start_lba = sh->orig_bio->bi_iter.bi_sector;
			sector_t end_lba = bio_end_sector(sh->orig_bio);
			if ((lba_to_lzone(ctx, start_lba) == DEBUG_TARG_ZONE_1) // debug
				|| (lba_to_lzone(ctx, start_lba) == DEBUG_TARG_ZONE_2)) // debug
			printk("(%d)[%d:%d] %s/ idx: %d rw: %d, lba: %lluKB(%dKB) ~ %lluKB(%dKB), length: %dKB\n",
				current->pid, lba_to_lzone(ctx ,start_lba), lba_to_stripe(ctx, start_lba),
				__func__, fifo_idx, op_is_write(bio_op(sh->orig_bio)),
				start_lba/2, lba_to_stripe_offset(ctx, start_lba)/2,  // absolute LBA,  stripe offset
				end_lba/2, lba_to_stripe_offset(ctx, end_lba)/2, // absolute LBA,  stripe offset
				bio_sectors(sh->orig_bio)/2);
#endif

			ret = kfifo_in_spinlocked(&ctx->io_workers[fifo_idx].work_fifo, &sh, 1,
					    &ctx->io_workers[fifo_idx].wlock);
			if (!ret) {
				pr_err("ERROR: %s kfifo insert failed!\n", __func__);
				BUG_ON(1);
			}
			queue_work(raizn_wq, &ctx->io_workers[fifo_idx].work);
#else
			// Push it onto the fifo
			ret = kfifo_in_spinlocked(&ctx->io_workers.work_fifo, &sh, 1,
					    &ctx->io_workers.wlock);
			if (!ret) {
				pr_err("ERROR: %s kfifo insert failed!\n", __func__);
				BUG_ON(1);
			}
			// udelay(10);
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
#ifdef DEBUG
// #if 1
	// profile_bio(sh);
	// print_bio_info(ctx, bio, __func__);
	if (bio_sectors(bio) > ctx->params->max_io_len) {
		printk("bio_sectors(bio) is too big: %lluKB > max(%lluKB)", bio_sectors(bio)/2, ctx->params->max_io_len);
		return DM_MAPIO_KILL;
	}
    // if ((lba_to_lzone(ctx, bio->bi_iter.bi_sector) == DEBUG_TARG_ZONE_1) // debug
    // 	|| (lba_to_lzone(ctx, bio->bi_iter.bi_sector) == DEBUG_TARG_ZONE_2)) // debug
	sector_t start_lba = bio->bi_iter.bi_sector;
	sector_t end_lba = bio_end_sector(bio);
	// if (raizn_op(bio) == RAIZN_OP_ZONE_RESET_LOG)
		printk("(%d)[%d:%d] %s: bio_op: %d, lba: %lluKB(%dKB) ~ %lluKB(%dKB), length: %dKB\n",
			current->pid, lba_to_lzone(ctx ,start_lba), lba_to_stripe(ctx, start_lba),
			__func__, bio_op(bio),
			start_lba/2, lba_to_stripe_offset(ctx, start_lba)/2,  // absolute LBA,  stripe offset
			end_lba/2, lba_to_stripe_offset(ctx, end_lba)/2, // absolute LBA,  stripe offset
			bio_sectors(bio)/2);
#endif
	return raizn_process_stripe_head(sh);
}

static void raizn_status(struct dm_target *ti, status_type_t type,
			 unsigned int status_flags, char *result,
			 unsigned int maxlen)
{
	struct raizn_ctx *ctx = ti->private;
	if (ctx->zone_mgr.rebuild_mgr.end) {
		pr_info("Rebuild took %llu ns\n",
			ktime_to_ns(
				ktime_sub(ctx->zone_mgr.rebuild_mgr.end,
					  ctx->zone_mgr.rebuild_mgr.start)));
	}
#ifdef PROFILING
	pr_info("write sectors = %llu\n",
		atomic64_read(&ctx->counters.write_sectors));
	pr_info("read sectors = %llu\n",
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
	struct gendisk *disk = dm_disk(dm_table_get_md(ti->table));
	// TODO: don't restict in here. bio must be splitted inside ZRAID
	blk_queue_max_hw_sectors(disk->queue, ctx->params->max_io_len);
	dm_set_target_max_io_len(ti, ctx->params->max_io_len);
	// ti->max_io_len = ctx->params->max_io_len;

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
#ifdef DEBUG
    // printk("[DEBUG] [%s:%i %s] \n", __FILE__, __LINE__, __func__);
    // printk("nr_zones: %d\n", nr_zones);
    printk("zoneno: %d\n", zoneno);
    printk("args->next_sector: %llu\n", args->next_sector);
    // printk("ctx->params->lzone_shift: %d\n", ctx->params->lzone_shift);
	// if (nr_zones > 5)
	// 	return 0;
#endif
	struct raizn_zone *zone = &ctx->zone_mgr.lzones[zoneno];
	struct blk_zone report;

	if (!nr_zones || zoneno > ctx->params->num_zones) {
		return args->zone_idx;
	}
	mutex_lock(&zone->lock);
	report.start = zone->start;
	report.len = ctx->params->lzone_size_sectors;
	report.wp = atomic64_read(&zone->lzone_wp);
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
	printk("[raizn_command] argc: %d, %d %d %d\n", 
		argc,
		strcmp(argv[0], RAIZN_DEV_TOGGLE_CMD),
		strcmp(argv[0], RAIZN_DEV_REBUILD_CMD),
		strcmp(argv[0], RAIZN_DEV_STAT_CMD)
		);
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
	pr_info("Received message, output buffer maxlen=%d\n", maxlen);
	for (idx = 0; idx < argc; ++idx) {
		pr_info("argv[%d] = %s\n", idx, argv[idx]);
	}
	raizn_command(ctx, argc, argv, result, maxlen);
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
