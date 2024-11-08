#include <linux/delay.h>

#include "raizn.h"
#include "util.h"
#include "pp.h"

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
	(void)sh->ctx;
	(void)sh;
#endif
}

inline void raizn_record_subio(struct raizn_stripe_head *sh, struct raizn_sub_io *subio)
{
	struct raizn_ctx *ctx = sh->ctx;
    u64 elapsed_time = ktime_get_ns() - subio->submit_time;
    if (subio->sub_io_type == RAIZN_SUBIO_DATA) {
        atomic64_add(elapsed_time, &ctx->subio_counters.d_t_tot);
        atomic64_inc(&(ctx->subio_counters.d_count)); // not working, don't know why. alternated with atomic64_add
    }
    else if (subio->sub_io_type == RAIZN_SUBIO_FP) {
        atomic64_add(elapsed_time, &ctx->subio_counters.fp_t_tot);
        atomic64_inc(&(ctx->subio_counters.fp_count));
    }
    else if (subio->sub_io_type == RAIZN_SUBIO_PP_INPLACE) {
        atomic64_add(elapsed_time, &ctx->subio_counters.pp_in_t_tot);
        atomic64_inc(&(ctx->subio_counters.pp_in_count));
    }
    else if (subio->sub_io_type == RAIZN_SUBIO_PP_OUTPLACE) {
        atomic64_add(elapsed_time, &ctx->subio_counters.pp_out_t_tot);
        atomic64_inc(&(ctx->subio_counters.pp_out_count));
    }

    // don't use switch-case. don't know why it fucked
	// if (sh->op != RAIZN_OP_GC) {
    //     switch (subio->sub_io_type) {
    //         // case RAIZN_SUBIO_DATA:
    //         //     atomic64_add(elapsed_time, &ctx->subio_counters.d_t_tot);
    //         //     atomic64_inc(&(ctx->subio_counters.d_count)); // not working, don't know why. alternated with atomic64_add
    //         //     // atomic64_add(1, &(ctx->subio_counters.d_count));
    //         //     break; 

    //         // case RAIZN_SUBIO_FP:
    //         //     atomic64_add(elapsed_time, &ctx->subio_counters.fp_t_tot);
    //         //     atomic64_inc(&(ctx->subio_counters.fp_count));
    //         //     // atomic64_add(1, &(ctx->subio_counters.fp_count));
    //         //     break; 
            
    //         // case RAIZN_SUBIO_PP_INPLACE:
    //         //     atomic64_add(elapsed_time, &ctx->subio_counters.pp_in_t_tot);
    //         //     atomic64_inc(&(ctx->subio_counters.pp_in_count));
    //         //     // atomic64_add(1, &(ctx->subio_counters.pp_in_count));
    //         //     break; 
            
    //         // case RAIZN_SUBIO_PP_OUTPLACE:
    //         //     atomic64_add(elapsed_time, &ctx->subio_counters.pp_out_t_tot);
    //         //     atomic64_inc(&(ctx->subio_counters.pp_out_count));
    //         //     // atomic64_add(1, &(ctx->subio_counters.pp_out_count));
    //         //     break; 

    //         default:
    //             // printk("ERROR! untracked subio, type: %d\n", subio->sub_io_type);
    //             return;
    //     }
    // }
}

void raizn_print_subio_counter(struct raizn_ctx *ctx)
{
    printk("raizn_print_subio_counter");
    if (atomic64_read(&ctx->subio_counters.d_count)) {
        u64 avg_lat = atomic64_read(&ctx->subio_counters.d_t_tot)/atomic64_read(&ctx->subio_counters.d_count);
        printk("---RAIZN_SUBIO_DATA---");
        printk("total: %lld, avg_lat(nsec): %lld\n", 
            atomic64_read(&ctx->subio_counters.d_count), avg_lat);
    }
    if (atomic64_read(&ctx->subio_counters.fp_count)) {
        u64 avg_lat = atomic64_read(&ctx->subio_counters.fp_t_tot)/atomic64_read(&ctx->subio_counters.fp_count);
        printk("---RAIZN_SUBIO_FP---");
        printk("total: %lld, avg_lat(nsec): %lld\n", 
            atomic64_read(&ctx->subio_counters.fp_count), avg_lat);
    }
    if (atomic64_read(&ctx->subio_counters.pp_in_count)) {
        u64 avg_lat = atomic64_read(&ctx->subio_counters.pp_in_t_tot)/atomic64_read(&ctx->subio_counters.pp_in_count);
        printk("---RAIZN_SUBIO_PP_INPLACE---");
        printk("total: %lld, avg_lat(nsec): %lld\n", 
            atomic64_read(&ctx->subio_counters.pp_in_count), avg_lat);
    }
    if (atomic64_read(&ctx->subio_counters.pp_out_count)) {
        u64 avg_lat = atomic64_read(&ctx->subio_counters.pp_out_t_tot)/atomic64_read(&ctx->subio_counters.pp_out_count);
        printk("---RAIZN_SUBIO_PP_OUTPLACE---");
        printk("total: %lld, avg_lat(nsec): %lld\n", 
            atomic64_read(&ctx->subio_counters.pp_out_count), avg_lat);
    }
}

void raizn_print_zf_counter(struct raizn_ctx *ctx)
{
    printk("raizn_print_zrwa_flush_counter");
    if (atomic64_read(&ctx->subio_counters.zf_cmd_count)) {
        u64 avg_lat = atomic64_read(&ctx->subio_counters.zf_cmd_t_tot)/atomic64_read(&ctx->subio_counters.zf_cmd_count);
        printk("---ZRWA_FLUSH_PER_CMD---");
        printk("num_total: %lld, avg_lat(nsec): %lld\n", 
            atomic64_read(&ctx->subio_counters.zf_cmd_count), avg_lat);
    }
    if (atomic64_read(&ctx->subio_counters.zf_wq_count)) {
        u64 avg_lat = atomic64_read(&ctx->subio_counters.zf_wq_t_tot)/atomic64_read(&ctx->subio_counters.zf_wq_count);
        printk("---ZRWA_FLUSH_WQ_ENQ2END---");
        printk("total: %lld, avg_lat(nsec): %lld\n", 
            atomic64_read(&ctx->subio_counters.zf_wq_count), avg_lat);
    }
    // if (atomic64_read(&ctx->subio_counters.pp_in_count)) {
    //     u64 avg_lat = atomic64_read(&ctx->subio_counters.pp_in_t_tot)/atomic64_read(&ctx->subio_counters.pp_in_count);
    //     printk("---RAIZN_SUBIO_PP_INPLACE---");
    //     printk("total: %lld, avg_lat(nsec): %lld\n", 
    //         atomic64_read(&ctx->subio_counters.pp_in_count), avg_lat);
    // }
    // if (atomic64_read(&ctx->subio_counters.pp_out_count)) {
    //     u64 avg_lat = atomic64_read(&ctx->subio_counters.pp_out_t_tot)/atomic64_read(&ctx->subio_counters.pp_out_count);
    //     printk("---RAIZN_SUBIO_PP_OUTPLACE---");
    //     printk("total: %lld, avg_lat(nsec): %lld\n", 
    //         atomic64_read(&ctx->subio_counters.pp_out_count), avg_lat);
    // }
}

void print_bio_info(struct raizn_ctx *ctx, struct bio *bio, char *funcname)
{
	if (bio==NULL) {
		printk("[print_bio_info] bio is NULL!!");
		return;
	}
    sector_t dev_lba = bio->bi_iter.bi_sector;
    sector_t stripe_start_lba = (dev_lba >> ctx->params->su_shift) * ctx->params->stripe_sectors;
	char rw[20], devtype[20];
    int dev_idx;
    struct raizn_dev *bio_dev;
    sector_t wp = 0;
	if (op_is_write(bio_op(bio)))
		strcpy(rw, "WRITE");
	else if ((bio_op(bio)) == REQ_OP_READ)
		strcpy(rw, "READ");
    else
		strcpy(rw, "OTHER");
    // printk("0 %s", rw);
    if (bio->bi_bdev!=NULL) {
        // printk("1");
        bio_dev = get_bio_dev(ctx, bio);
        if (bio_dev) {
            dev_idx = bio_dev->idx;
            wp = bio_dev->zones[pba_to_pzone(ctx, dev_lba)].pzone_wp;
        }
        else {
            strcpy(devtype, "ORIG_BIO");
            dev_idx = -2;
        }

        if (lba_to_parity_dev_idx(ctx, stripe_start_lba) == dev_idx)
            strcpy(devtype, "PARITY");
        else if (dev_idx != -1)
            strcpy(devtype, "DATA");
        // printk("2");

        // printk("3");
    }
    else {
        return;
        // printk("4");
        strcpy(devtype, "RAIZN_VIRTUAL");
        dev_idx = -1;
    }
    // printk("5");
	pr_err("(%d) [%s] err: %d, dev: %d(%s), op: %d, rw: %s, lba: %lld(KB), len: %dKB, wp: %lldKB, zone: %d, stripe: %d\n", 
		current->pid, funcname, bio->bi_status, dev_idx, devtype, bio_op(bio), rw,
		dev_lba/2, bio->bi_iter.bi_size/1024, wp/2,
        (dev_idx<0) ? lba_to_lzone(ctx, dev_lba): pba_to_pzone(ctx, dev_lba), 
        (dev_idx<0) ? lba_to_stripe(ctx, dev_lba): (dev_lba - bio_dev->zones[pba_to_pzone(ctx, dev_lba)].start >> ctx->params->su_shift));
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
    printk("(%d)[%d:%d] submit_bio from [%s] dev: %d(%s), rw: %s, pba: %lldKB, len: %dKB, last_comp_str: %d\n", 
        current->pid, lba_to_lzone(ctx, stripe_start_lba), lba_to_stripe(ctx, stripe_start_lba),
        funcname, get_bio_dev(ctx, bio)->idx, devtype, rw,
        dev_pba/2, bio->bi_iter.bi_size/1024,  lzone->last_complete_stripe);
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
inline int raizn_submit_bio_aggr(struct raizn_ctx *ctx, char *funcname, struct bio *bio, struct raizn_dev *dev, bool wait)
{
    // printk("raizn_submit_bio_aggr");
// #if 1
#ifdef DEBUG   
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
        printk("(%d)[%d:%d] submit_bio from [%s] dev: %d(%s), rw: %s, pba: %lldKB, len: %dKB, op: %d, last_comp_str: %d\n", 
            current->pid, lba_to_lzone(ctx, stripe_start_lba), lba_to_stripe(ctx, stripe_start_lba),
            funcname, get_bio_dev(ctx, bio)->idx, devtype, rw,
            dev_pba/2, bio->bi_iter.bi_size/1024,  
            bio_op(bio), lzone->last_complete_stripe);
        // printk("pba: %d, stripe_start_lba: %d\n", dev_pba, stripe_start_lba);
        // printk("round_up(bio->bi_iter.bi_sector + 1, AGGR_CHUNK_SECTOR): %lld, bio_end_sector(bio): %lld",
        //     round_up(bio->bi_iter.bi_sector + 1, AGGR_CHUNK_SECTOR), bio_end_sector(bio));
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
            atomic64_set(&mdzone->mdzone_wp, mdzone->capacity);
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
		if (bio_op(bio) == REQ_OP_ZONE_RESET) {
			printk("dev[%d]: %p", dev->idx, dev);
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
    if (bio_op(bio) == REQ_OP_ZONE_RESET) {
        printk("dev[%d]: %p", get_bio_dev_idx(ctx, bio), get_bio_dev(ctx, bio));
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

inline bool subio_ready2submit(struct raizn_sub_io *subio, bool data)
{
#ifdef IGNORE_ORDERING
    return true;
#endif
    s64 subio_pba = subio->bio->bi_iter.bi_sector;
    s64 allowed_range, dist_from_durable;
    if (data)
        allowed_range = ZRWASZ/2 - bio_sectors(subio->bio);
    else {
        allowed_range = ZRWASZ - bio_sectors(subio->bio);
    }

    // printk("allowed_range: %d, lba: %lld, wp: %lld, bool: %d\n",
    //      allowed_range,
    //      subio->bio->bi_iter.bi_sector,
    //      atomic64_read(&subio->zone->wp) ,
    //      (atomic64_read(&subio->zone->wp) < ((s64)(subio->bio->bi_iter.bi_sector) - (s64)(allowed_range))));
// #if defined (PP_INPLACE) || defined (HIGH_QD_SUPPORT)

    // dist_from_durable = subio_pba - max(
    //         (READ_ONCE(lzone->last_complete_stripe) * ctx->params->su_sectors),
    //         (s64)READ_ONCE(subio->zone->pzone_wp)
    //     );

    // if (dist_from_durable > allowed_range)
    //     return false; 

#ifdef ATOMIC_WP
			if ((s64)READ_ONCE(subio->zone->pzone_wp) <
#else //ATOMIC_WP
			if ((s64)subio->zone->pzone_wp <
#endif //ATOMIC_WP
                ((s64)(subio->bio->bi_iter.bi_sector) - (s64)(allowed_range)))
                return false; 

    return true;
}

inline struct raizn_dev *get_bio_dev(struct raizn_ctx *ctx,
					    struct bio *bio)
{
	int i;
	// dev_t bd_dev = bio->bi_bdev->bd_dev;
	// printk("0.0 %d\n",ctx->params->array_width);
	for (i = 0; i < ctx->params->array_width; i++) {
		// if (&ctx->devs[i] == NULL) {
		// 	// printk("0.1\n");
		// 	return NULL;
		// }
		// if (ctx->devs[i].dev == NULL) {
		// 	// printk("0.2\n");
		// 	return NULL;
		// }
		// if (ctx->devs[i].dev->bdev == NULL) {
		// 	// printk("0.3\n");
		// 	return NULL;	
		// }
		// printk("i: %d, ctx->devs[i]: %p, ctx->devs[i].dev: %p, ctx->devs[i].dev->bdev: %p, ctx->devs[i].dev->bdev->bd_dev: %p\n", 
		// 	i, &ctx->devs[i], ctx->devs[i].dev, ctx->devs[i].dev->bdev, ctx->devs[i].dev->bdev->bd_dev);
		// mdelay(10);
		if (ctx->devs[i].dev->bdev->bd_dev == bio->bi_bdev->bd_dev) {
			return &ctx->devs[i];
		}
	}
	return NULL;
}

// sequence starts from 1 ~ ends at array_width - 1
inline int get_dev_sequence(struct raizn_ctx *ctx, sector_t lba)
{
    int dev_idx = lba_to_dev_idx(ctx, lba), parity_idx = lba_to_parity_dev_idx(ctx, lba);
    if (dev_idx >= parity_idx)
        return (dev_idx - parity_idx);
    else
        return (ctx->params->array_width - parity_idx + dev_idx);
}

inline int get_dev_idx(struct raizn_ctx *ctx,
					    struct raizn_dev *dev)
{
    int i;
	for (i = 0; i < ctx->params->array_width; i++) {
		if (ctx->devs[i].dev == dev) {
			return i;
		}
	}
	return -1;
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



// From the beginning of the logical zone, which number stripe is LBA in
inline sector_t lba_to_stripe(struct raizn_ctx *ctx, sector_t lba)
{
    // printk("[lba_to_stripe] lzone_size: %lld, stripe_size: %lld",
    //     ctx->params->lzone_size_sectors, ctx->params->stripe_sectors);
#ifdef NON_POW_2_ZONE_SIZE
	return (lba % (ctx->params->lzone_size_sectors)) /
	       ctx->params->stripe_sectors;
#else
	return (lba & (ctx->params->lzone_size_sectors - 1)) /
	       ctx->params->stripe_sectors;
#endif
}
// From the beginning of the logical zone, which number stripe unit is LBA in
inline sector_t lba_to_su(struct raizn_ctx *ctx, sector_t lba)
{
#ifdef NON_POW_2_ZONE_SIZE
	return (lba % (ctx->params->lzone_size_sectors)) /
	       ctx->params->su_sectors;
#else
	return (lba & (ctx->params->lzone_size_sectors - 1)) >>
	       ctx->params->su_shift;
#endif
}

// return true if given lbas are in the same su
inline bool check_same_su(struct raizn_ctx *ctx, sector_t lba1, sector_t lba2)
{
    return ( (lba1 >> ctx->params->su_shift) == (lba2 >> ctx->params->su_shift) );
}

// return true if given lba is in the last su of the stripe
inline bool check_last_su(struct raizn_ctx *ctx, sector_t lba)
{
    return ( 
        (ctx->params->stripe_sectors - lba_to_stripe_offset(ctx, lba)) <= ctx->params->su_sectors
    );
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

// Which logical zone number is LBA in
inline sector_t lba_to_lzone(struct raizn_ctx *ctx, sector_t lba)
{
#ifdef NON_POW_2_ZONE_SIZE
	return lba / ctx->params->lzone_size_sectors;
#else
	return lba >> ctx->params->lzone_shift;
#endif
}

// Which device (index of the device in the array) holds the parity for data written in the stripe containing LBA
// Assuming RAID5 scheme
inline int lba_to_parity_dev_idx(struct raizn_ctx *ctx, sector_t lba)
{
#ifdef MOD_RAID4
    return ctx->params->array_width - 1;
#else
	return ctx->params->array_width - ((lba_to_stripe(ctx, lba) + lba_to_lzone(ctx, lba)) % ctx->params->array_width) - 1;
#endif
}
// Same as above, but returns the actual device object
struct raizn_dev *lba_to_parity_dev(struct raizn_ctx *ctx, sector_t lba)
{
	return &ctx->devs[lba_to_parity_dev_idx(ctx, lba)];
}
// Which device holds the data chunk associated with LBA
struct raizn_dev *lba_to_dev(struct raizn_ctx *ctx, sector_t lba)
{
	sector_t su_position = lba_to_su(ctx, lba) % ctx->params->stripe_width;
#ifndef IGNORE_FULL_PARITY
	// if (su_position >= lba_to_parity_dev_idx(ctx, lba)) {
	// 	su_position += 1;
	// }
    su_position = (lba_to_parity_dev_idx(ctx, lba) + su_position + 1) % ctx->params->array_width;
#endif
	return &ctx->devs[su_position];
}
// Which device holds the data chunk associated with LBA
inline int lba_to_dev_idx(struct raizn_ctx *ctx, sector_t lba)
{
	int su_position = lba_to_su(ctx, lba) % ctx->params->stripe_width;
#ifndef IGNORE_FULL_PARITY
	// if (su_position >= lba_to_parity_dev_idx(ctx, lba)) {
	// 	su_position += 1;
	// }
    su_position = (lba_to_parity_dev_idx(ctx, lba) + su_position + 1) % ctx->params->array_width;
#endif
	return su_position;
}
// What is the offset of LBA within the logical zone (in 512b sectors)
inline sector_t lba_to_lzone_offset(struct raizn_ctx *ctx, sector_t lba)
{
#ifdef NON_POW_2_ZONE_SIZE
	return lba % (ctx->params->lzone_size_sectors);
#else
	return lba & (ctx->params->lzone_size_sectors - 1);
#endif
}
// What is the offset of LBA within the stripe (in 512b sectors)
inline sector_t lba_to_stripe_offset(struct raizn_ctx *ctx, sector_t lba)
{
	return lba_to_lzone_offset(ctx, lba) & (ctx->params->stripe_sectors - 1);
}
// What is the offset of LBA within the stripe unit (in 512b sectors)
inline sector_t lba_to_su_offset(struct raizn_ctx *ctx, sector_t lba)
{
	return lba_to_lzone_offset(ctx, lba) & (ctx->params->su_sectors - 1);
}
// Same as above, except in bytes instead of sectors
inline sector_t bytes_to_stripe_offset(struct raizn_ctx *ctx,
					      uint64_t ptr)
{
	return (ptr & ((ctx->params->lzone_size_sectors << SECTOR_SHIFT) - 1)) %
	       (ctx->params->stripe_sectors << SECTOR_SHIFT);
}
// Return the starting LBA for the stripe containing lba (in sectors)
inline sector_t lba_to_stripe_addr(struct raizn_ctx *ctx, sector_t lba)
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
inline sector_t lba_to_pba_default(struct raizn_ctx *ctx, sector_t lba)
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
#endif

// block unit (= minimal read/write unit of real device, 4096b or etc.) to sector unit (512b)
inline sector_t block_to_sector_addr(sector_t block_addr)
{
    return (block_addr << DEV_BLOCKSHIFT) >> SECTOR_SHIFT;
}

inline sector_t sector_to_block_addr(sector_t sector_addr)
{
    return (sector_addr << SECTOR_SHIFT) >> DEV_BLOCKSHIFT;
}

inline void reset_stripe_buf(struct raizn_ctx *ctx, sector_t start)
{
    struct raizn_zone *lzone =
		&ctx->zone_mgr.lzones[lba_to_lzone(ctx, start)];
    
    if (lzone->stripe_buffers) {
        struct raizn_stripe_buffer *buf =
            &lzone->stripe_buffers[lba_to_stripe(ctx, start) &
                        STRIPE_BUFFERS_MASK];
        memset(buf->data, 0, ctx->params->stripe_sectors << SECTOR_SHIFT);
    }
}

// partial parity from the starting chunk of the stripe to the chunk of end_lba
// start_lba & end_lba must locate on the same stripe
// dst must be allocated and sufficiently large
// srcoff is the offset within the stripe
// Contents of dst are not included in parity calculation
void calc_part_parity(struct raizn_ctx *ctx,
					 sector_t start_lba, sector_t end_lba, void *dst)
{
    int i;
	void *stripe_units[RAIZN_MAX_DEVS];
    int num_xor_units = (lba_to_stripe_offset(ctx, end_lba - 1) >> ctx->params->su_shift) + 1;
    // printk("calc_part_parity: start: %lld, end: %lld, units: %d\n", start_lba, end_lba, num_xor_units);
	// return;
	struct raizn_zone *lzone =
		&ctx->zone_mgr.lzones[lba_to_lzone(ctx, start_lba)];
	struct raizn_stripe_buffer *buf =
		&lzone->stripe_buffers[lba_to_stripe(ctx, start_lba) &
				       STRIPE_BUFFERS_MASK];
	for (i = 0; i < ctx->params->stripe_width; ++i) {
		stripe_units[i] = buf->data +
				  i * (ctx->params->su_sectors << SECTOR_SHIFT);
	}
	xor_blocks(num_xor_units,
		   ctx->params->su_sectors << SECTOR_SHIFT, dst, stripe_units);
	return;
}
// inline bool check_need_fp(struct raizn_stripe_head *sh, sector_t lba)
// {
//     struct raizn_ctx *ctx = sh->ctx;
//     int stripe_id = lba_to_stripe(ctx, lba);
//     printk("stripe_id: %d, stripe_sectors: %d, lba_to_stripe_offset(ctx, lba - 1): %d, ctx->params->stripe_sectors - lba_to_stripe_offset(ctx, lba - 1): %d\n",
//         stripe_id, ctx->params->stripe_sectors, lba_to_stripe_offset(ctx, lba - 1), ctx->params->stripe_sectors - lba_to_stripe_offset(ctx, lba - 1));
// // printk("lba: %d, chunk_bytes: %d, bv_offset: %d, data_pos: %d\n",
// // 	lba, chunk_bytes, bv.bv_offset, data_pos);
//     (ctx->params->stripe_sectors - lba_to_stripe_offset(ctx, lba) <= ctx->params->su_sectors) 

//     || (stripe_id < lba_to_stripe(ctx, bio_end_sector(sh->orig_bio)))
//     return
// }


