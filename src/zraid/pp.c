#include <linux/module.h>
#include <linux/kernel.h>

#include <linux/init.h>
#include <linux/bitmap.h>
#include <linux/bio.h>
#include <linux/limits.h>
#include <linux/delay.h>
#include <linux/nvme_ioctl.h>
#include <linux/blkdev.h>
#include <linux/smp.h>
#include <linux/log2.h>

#include "raizn.h"
#include "pp.h"
#include "util.h"
#include "zrwa.h"

inline void convert_bit_pattern_64(unsigned long num, char* bit_pattern)
{
    int i;
    for(i=63; i>=0; i--) {
        bit_pattern[i] = (num & 1) + '0';
        num >>= 1;
    }
}

// return: 1 if pp can be written in same lzone (common case), 0 if pp should be redirected to reserved zone (the last stripe)
// distance between PP & the write target position is stored in argument <distance>
static int get_pp_distance(struct raizn_ctx *ctx, sector_t end_lba, int *pp_distance)
{
    sector_t ret;
    int chunks_per_zrwa = ctx->params->chunks_in_zrwa;
    struct raizn_zone *lzone = &ctx->zone_mgr.lzones[lba_to_lzone(ctx, end_lba - 1)];
    sector_t rem2end = lzone->start + ctx->params->lzone_capacity_sectors - end_lba;
    // printk("rem2end: %d, end_lba: %lld, chunks_per_zrwa: %d, ctx->params->stripe_sectors: %d\n", 
    //     rem2end, end_lba, chunks_per_zrwa, ctx->params->stripe_sectors);
#ifdef DEBUG
#endif 
    /* TODO: dynamic return for big sized request (larger than a stripe) */
    // TODO: configurable 1/2 recursive algorithm implementation
    if ((rem2end / ctx->params->stripe_sectors < chunks_per_zrwa/2)) { // near the last stripe
    // if (unlikely(rem2end / ctx->params->stripe_sectors < 1)) { // near the last stripe
        *pp_distance = -1;
        return 0;
    }
    *pp_distance = (chunks_per_zrwa/2);
    // *pp_distance = min((chunks_per_zrwa/2), ((u64)rem2end / (u64)ctx->params->stripe_sectors));
    return 1;
    /* TODO: get zrwa size from device */
    // chunks_per_zrwa = ctx->devs[0].zones[0].zrwasz
}

inline int get_pp_dev_idx(struct raizn_ctx *ctx, sector_t start_lba, sector_t end_lba, int pp_distance)
{
    int bio_dev_idx, pp_dev_idx;
    bio_dev_idx = lba_to_dev_idx(ctx, (end_lba - 1));
    pp_dev_idx = (bio_dev_idx + 1) % ctx->params->array_width;
#ifdef DEBUG
    printk("[get_pp_dev_idx] bio_dev_idx: %d, pp_dev_idx: %d, parity_dev: %d",
        bio_dev_idx,
        pp_dev_idx,
        lba_to_parity_dev_idx(ctx, end_lba)
        );
#endif
    return pp_dev_idx;
}

static inline void __get_pp_location(struct raizn_ctx *ctx, sector_t start_lba, sector_t end_lba, int pp_distance, int *dev_idx, sector_t *dev_lba)
{
    sector_t su_offset = 0, pzone_start;
    int bio_dev_idx;
    (*dev_idx) = get_pp_dev_idx(ctx, start_lba, end_lba, pp_distance); 
#ifdef DEBUG
// #if 1
    // printk("[%s] end_lba: %lld, ctx->params->stripe_sectors: %d, lba_to_stripe(ctx, end_lba - 1): %d, lba_to_su(ctx, end_lba): %d\n", 
    //     __func__, end_lba, ctx->params->stripe_sectors, lba_to_stripe(ctx, end_lba - 1), lba_to_su(ctx, end_lba - 1));
    // printk("[%s] bio_dev_idx: %d, dev_idx1 PP: %d, parity dev at PP: %d\n",
    //     __func__, bio_dev_idx, *dev_idx,  lba_to_parity_dev_idx(ctx, end_lba +
    //     pp_distance * ctx->params->stripe_sectors - 1));
#endif

    pzone_start = ctx->devs[*dev_idx].zones[lba_to_lzone(ctx, start_lba)].start;
#ifdef DEBUG
    // printk("[__get_pp_location], start: %lld, *dev_idx: %d, zone_idx: %d",
    //      pzone_start, *dev_idx, lba_to_lzone(ctx, start_lba));
#endif
    (*dev_lba) = pzone_start + ((lba_to_stripe(ctx, end_lba - 1) + pp_distance) << ctx->params->su_shift);
    if (check_same_su(ctx, start_lba, end_lba - 1)) // req is not spanning over chunks
        su_offset = lba_to_su_offset(ctx, start_lba);
    else // req is spanning more or equal than 2 chunks, offset starts from the start sector of the last spanned chunk
        su_offset = 0;
    (*dev_lba) += su_offset;
        
#ifdef DEBUG
    if ((*dev_idx) >= ctx->params->array_width) {
        pr_err("Fatal error! dev_idx is wrong: %d\n", *dev_idx);
        BUG_ON(1);
    }
    // printk("[%s] lba_to_stripe(ctx, end_lba) %d, pp_distance: %lld, lba_to_su_offset(ctx, end_lba): %lld\n", 
    //         __func__,
    //         lba_to_stripe(ctx, end_lba),
    //          pp_distance,
    //          lba_to_su_offset(ctx, start_lba)
    //          );
    // printk("[%s] dev_idx: %d, dev_lba: %lld\n", __func__, *dev_idx, *dev_lba);
#endif
}

// gap between start_lba & end_lba should be smaller or equal than a chunk
// return: 1 if PP can be written in same lzone (common case), idx & lba is passed through arguments
// return: 0 if PP should be redirected to reserved zone (the last stripe)
raizn_pp_location_t get_pp_location(struct raizn_ctx *ctx, sector_t start_lba, sector_t end_lba, int *dev_idx, sector_t *dev_lba)
{
    int pp_distance;
#ifdef DEBUG
    // printk("[%s] start_lba: %lld, end_lba: %lld\n", __func__,
    //     start_lba, end_lba);
#endif
    if (!get_pp_distance(ctx, end_lba, &pp_distance))
        return RAIZN_PP_REDIRECT;
    __get_pp_location(ctx, start_lba, end_lba, pp_distance, dev_idx, dev_lba);
    return RAIZN_PP_NORMAL;
}

int raizn_assign_pp_bio(struct raizn_stripe_head *sh, struct raizn_sub_io *ppio, int dev_idx, sector_t dev_lba, void *data, size_t len)
{
	struct page *p;
	struct bio *ppbio = ppio->bio;
#ifdef DEBUG
	BUG_ON(!ppbio);
	BUG_ON(!data);
#endif

#if defined (DUMMY_HDR) && defined (PP_INPLACE)
    p = is_vmalloc_addr(&ppio->header) ? vmalloc_to_page(&ppio->header) :
                        virt_to_page(&ppio->header);
	if (bio_add_page(ppbio, p, PAGE_SIZE, offset_in_page(&ppio->header)) !=
	    PAGE_SIZE) {
		pr_err("Failed to add dummy header page\n");
		bio_endio(ppbio);
		return NULL;
	}
	ppbio->bi_iter.bi_sector = dev_lba - PAGE_SIZE / SECTOR_SIZE;
#else
	ppbio->bi_iter.bi_sector = dev_lba;
#endif

    ppio->dev = &sh->ctx->devs[dev_idx];
    ppio->zone = &sh->ctx->devs[dev_idx].zones[pba_to_pzone(sh->ctx, dev_lba)];
    ppbio->bi_opf = sh->orig_bio->bi_opf;
	p = is_vmalloc_addr(data) ? vmalloc_to_page(data) :
					virt_to_page(data);
    bio_set_op_attrs(ppbio, REQ_OP_WRITE, 0);
	bio_set_dev(ppbio, sh->ctx->devs[dev_idx].dev->bdev);
#ifdef DEBUG
	BUG_ON(!p);
#endif
	if (bio_add_page(ppbio, p, len, 0) != len) {
		printk("Failed to add pp data page\n");
		bio_endio(ppbio);
		return 0;
	}
	return 1;
}

struct raizn_sub_io *raizn_alloc_pp(struct raizn_stripe_head *sh, struct raizn_dev *dev)
{
	struct raizn_ctx *ctx = sh->ctx;
	struct raizn_sub_io *ppio = raizn_stripe_head_alloc_bio(
		sh, &dev->bioset, 1, RAIZN_SUBIO_PP_INPLACE, NULL);
	return ppio;
}

// gap between start_lba & end_lba should be smaller or equal than a chunk
int __raizn_write_pp(struct raizn_stripe_head *sh, sector_t start_lba, sector_t end_lba, void *data)
{
    struct raizn_ctx *ctx = sh->ctx;
    size_t len = end_lba - start_lba;
#ifdef DEBUG
	// printk("[%s] start_lba: %lld, end_lba: %ld, data: %p\n",
    //     __func__, start_lba, end_lba, data);
// #if 1
	BUG_ON(!data);
	if (len <= 0) {
		printk("WARN! %s pp buf len is zero. start_lba: %lld, end_lba: %lld, len: %d\n",
            __func__, start_lba, end_lba, len);
	}
#endif
	raizn_pp_location_t ret;
	struct raizn_dev *dev;
	struct raizn_sub_io *ppio;
	int dev_idx;
	sector_t dev_lba;

	ret = get_pp_location(ctx, start_lba, end_lba, &dev_idx, &dev_lba);
#ifdef DEBUG
	printk("[%s] ret: %d, PP idx: %d, lba: %lld, len: %lld, lzone: %d, num_zones: %d\n", __func__, ret,
		dev_idx, dev_lba, len, lba_to_lzone(ctx, dev_lba), ctx->params->num_zones);
    if (ret == RAIZN_PP_NORMAL) {
        BUG_ON(dev_idx >= ctx->params->array_width);
        BUG_ON(lba_to_lzone(ctx, dev_lba) >= ctx->params->num_zones);
    }
#endif

	switch (ret)
	{
	case RAIZN_PP_NORMAL:
#ifdef DEBUG
        printk("[%s] RAIZN_PP_NORMAL, PP idx: %d, pba: %d, len: %d\n", __func__,
            dev_idx, dev_lba, len);
#endif
#ifdef RECORD_PP_AMOUNT
        atomic64_add(len, &ctx->pp_volatile);
#endif
		dev = &ctx->devs[dev_idx];
		ppio = raizn_alloc_pp(sh, dev);
		if (!raizn_assign_pp_bio(sh, ppio, dev_idx, dev_lba, data, len << SECTOR_SHIFT)) {
			pr_err("Fatal: Failed to assign pp bio\n");
			return 0;
		}
		return 1;

	case RAIZN_PP_REDIRECT:
#ifdef DEBUG
        printk("[%s] RAIZN_PP_REDIRECT, PP idx: %d, lba: %d, len: %d, data: %p\n", __func__,
            dev_idx, dev_lba, len, data);
#endif
		start_lba = sh->orig_bio->bi_iter.bi_sector;
		raizn_write_md(
			sh,
			lba_to_lzone(ctx, start_lba),
			lba_to_parity_dev(ctx, start_lba),
			RAIZN_ZONE_MD_GENERAL,
            RAIZN_SUBIO_PP_OUTPLACE,
			data, 
            len << SECTOR_SHIFT);
		return 1;

	default:
		printk("ERROR! not defined return from get_pp_location\n");
		break;
	}
	
	return 0;
}

int raizn_write_pp(struct raizn_stripe_head *sh, int parity_su)
{
	struct raizn_ctx *ctx = sh->ctx;
	uint8_t *parity_buf = sh->parity_bufs;

	sector_t start_lba = sh->orig_bio->bi_iter.bi_sector;
	sector_t end_lba = bio_end_sector(sh->orig_bio);
    sector_t start_su = lba_to_su(ctx, start_lba);
    sector_t end_su = lba_to_su(ctx, end_lba - 1);
    sector_t zone_start = sh->zone->start;
    sector_t last_stripe_start = zone_start + (lba_to_stripe(ctx, end_lba) << ctx->params->stripe_shift);
    sector_t end_su_start = (lba_to_su_offset(ctx, end_lba)) ?
        end_lba - lba_to_su_offset(ctx, end_lba) : 
        end_lba - ctx->params->su_sectors;

	// printk("[%s] start_lba: %lld, end_lba: %lld, start_su: %d, end_su: %d, end_su_start: %lld, su_offset: %d\n",
	// 	__func__, start_lba, end_lba, start_su, end_su, end_su_start, lba_to_su_offset(ctx, end_lba));

    if (start_su == end_su) {  // non-spanning request. easy to handle
        // printk("1");
        if (!check_last_su(ctx, end_lba - 1)) { // for the last-seq chunk, FP will be directly written
            // printk("1.1");
            calc_part_parity(ctx, last_stripe_start, end_su_start + ctx->params->su_sectors,
                parity_buf + ((parity_su - 2) << ctx->params->su_shift));
            __raizn_write_pp(sh, start_lba, end_lba, 
                parity_buf + ((parity_su - 2) << ctx->params->su_shift + lba_to_su_offset(ctx, start_lba)));
        }
    }
    else { // spanning request. 0 ~ 2 PP requests are needed.
        if (lba_to_su_offset(ctx, end_lba)) { // chunk-unaligned
            //     //  ------  ------  ------  ------  ------ 
            //     // |      ||      ||      ||  @   ||parity|    (end stripe in bio)
            //     //  ------  ------  ------  ------  ------      
            //                                  @: end lba
            // printk("2");
            // Partial chunk (final chunk of the bio)
            if (!check_last_su(ctx, end_lba - 1)) { 
            // printk("2.1");
                calc_part_parity(ctx, last_stripe_start, end_su_start + ctx->params->su_sectors,
                    parity_buf + ((parity_su - 2) << ctx->params->su_shift));
                __raizn_write_pp(sh, end_su_start, end_lba, 
                    parity_buf + ((parity_su - 2) << ctx->params->su_shift));
            }
            // Else, the last chunk in the stripe is partial. The last partial write will be protected by FP

            // Write full-chunk PP for previous of the final(partial) chunk. 
            if (!check_last_su(ctx, end_su_start - 1)) {
                sector_t pp_start_offset = 0;
                if (end_su - start_su > 1)
                    pp_start_offset = end_su_start - ctx->params->su_sectors;
                else
                    pp_start_offset = start_lba;
                    
                // printk("2.2");
                calc_part_parity(ctx, last_stripe_start, end_su_start,
                    parity_buf + ((parity_su - 1) << ctx->params->su_shift));
                __raizn_write_pp(sh, pp_start_offset, end_su_start, 
                    parity_buf + ((parity_su - 1) << ctx->params->su_shift));
            }
        }
        else { // chunk-aligned: single chunk PP corresponding to the end chunk
            // printk("3");
            if (!check_last_su(ctx, end_lba - 1)) { // for the last-seq chunk, FP will be directly written
            // printk("3.1");
            //     //  ------  ------  ------  ------  ------ 
            //     // |      ||      ||     @||      ||parity|    (end stripe in bio)
            //     //  ------  ------  ------  ------  ------      
            //                                  @: end lba
                calc_part_parity(ctx, last_stripe_start, end_su_start + ctx->params->su_sectors,
                    parity_buf + ((parity_su - 2) << ctx->params->su_shift));
                __raizn_write_pp(sh, end_su_start, end_su_start + ctx->params->su_sectors,
                    parity_buf + ((parity_su - 2) << ctx->params->su_shift));
            }
        }
    }
	return 0;
}

// int raizn_write_pp(struct raizn_stripe_head *sh, int parity_su)
// {
// 	struct raizn_ctx *ctx = sh->ctx;
// 	uint8_t *parity_buf = sh->parity_bufs;

// 	sector_t start_lba = sh->orig_bio->bi_iter.bi_sector;
// 	sector_t end_lba = bio_end_sector(sh->orig_bio);
//     sector_t zone_start = sh->zone->start;
//     int zone_num = lba_to_lzone(ctx, start_lba);
//     sector_t start_su = lba_to_su(ctx, start_lba);
//     sector_t end_su = lba_to_su(ctx, end_lba - 1);

// 	sector_t leading_subchunk_end_lba =
// 		min(((start_su + 1) << ctx->params->su_shift) + zone_start,
//             end_lba);
// 	sector_t leading_subchunk_sectors = lba_to_su_offset(ctx, start_lba) > 0 ?
//             leading_subchunk_end_lba - start_lba: 
//             0;
// 	sector_t trailing_subchunk_start_lba = 
//         max((end_su << ctx->params->su_shift) + zone_start,
//             start_lba);
// 	sector_t trailing_subchunk_sectors = end_lba - trailing_subchunk_start_lba;

// 	// printk("[%s] start_lba: %lld, end_lba: %lld, start_su: %d, end_su: %d, leading_subchunk_end_lba: %lld, leading_subchunk_sectors: %lld, trailing_subchunk_start_lba: %lld, trailing_subchunk_sectors: %lld\n",
// 	// 	__func__, start_lba, end_lba, start_su, end_su, leading_subchunk_end_lba, leading_subchunk_sectors, trailing_subchunk_start_lba, trailing_subchunk_sectors);


// 	//  ------  ------  ------  ------  ------ 
//     // |      ||  ^@  ||      ||      ||parity|    (bio stripe)
//     //  ------  ------  ------  ------  ------ 
//     // ^: start lba ,  @: end lba
//     if (end_su == start_su)
//     {
// 		if (!check_last_su(ctx, trailing_subchunk_start_lba)) { // for the last chunk, FP will be directly written, note: start_lba & end_lba can be on different stripes
// 			calc_part_parity(ctx, trailing_subchunk_start_lba, end_lba, parity_buf + (parity_su - 1) * ctx->params->su_sectors);
// 			__raizn_write_pp(sh, trailing_subchunk_start_lba, end_lba, parity_buf + (parity_su - 1) * ctx->params->su_sectors);
// 		}
//     }
//     /*
//      ------  ------  ------  ------  ------ 
//     |  ^   ||      ||  @   ||      ||parity|    (bio stripe)
//      ------  ------  ------  ------  ------      
//     ^: start lba ,  @: end lba
//     */
// 	else if (end_su - start_su >= 2) /* in this case, write two PP requests
// 	 1) XOR of the latest full chuck and all previous chunks (full chunk)
// 	 2) XOR of the trailing subchunk sectors and all previous subchunks (size of subchunk sectors)
// 	 PP write exception: the last chunk of the stripe (write FP instead)
// 	*/
// 	{
// 		// printk("full chunk + partial chunk %d\n", lba_to_su(ctx, end_lba));
// 		sector_t latest_full_su_start = zone_start + (end_su << ctx->params->su_shift);
// 		sector_t latest_full_su_end = zone_start + (end_su + 1 << ctx->params->su_shift);
// 		// printk("[%s] zone_start: %lld, latest_full_su_start: %lld, latest_full_su_end: %lld\n",
// 		// 	__func__, zone_start, latest_full_su_start, latest_full_su_end);
// 		if (!check_last_su(ctx, latest_full_su_start)) {
// 			calc_part_parity(ctx, latest_full_su_start, latest_full_su_end, parity_buf);
// 			__raizn_write_pp(sh, latest_full_su_start, latest_full_su_end, parity_buf);
// 		}
// 		if ((trailing_subchunk_sectors) && (!check_last_su(ctx, trailing_subchunk_start_lba))) { // for the last chunk, FP will be directly written, note: start_lba & end_lba can be on different stripes
// 			calc_part_parity(ctx, trailing_subchunk_start_lba, end_lba, parity_buf + (parity_su - 1) * ctx->params->su_sectors);
// 			__raizn_write_pp(sh, trailing_subchunk_start_lba, end_lba, parity_buf + (parity_su - 1) * ctx->params->su_sectors);
// 		}
// 	}
// 	// /*
//     //  ------  ------  ------  ------  ------ 
//     // |      ||  ^   ||  @   ||      ||parity|    (bio stripe)
//     //  ------  ------  ------  ------  ------      

// 	// OR

// 	//  ------  ------  ------  ------  ------ 
//     // |      ||      ||      ||  ^   ||parity|    (bio 1st stripe)
//     //  ------  ------  ------  ------  ------ 
// 	//  ------  ------  ------  ------  ------ 
//     // |  @   ||      ||      ||      ||parity|    (bio 2nd stripe)
//     //  ------  ------  ------  ------  ------ 

//     // ^: start lba ,  @: end lba
//     // */
// 	else /* in this case, write two PP request if start lba & end lba are on the different chunk. else, write 1 PP request.
// 	 1) XOR of the leading subchunk sectors and all previous subchunks (size of subchunk sectors) << this can make a full PP chunk with previously written subchunk
// 	 2) XOR of the trailing subchunk sectors and all previous subchunks (size of subchunk sectors)
// 	 PP write exception: the last chunk of the stripe (write FP instead)
// 	*/
// 	{
// 		// printk("2 partial chunks");
// 		if ((leading_subchunk_sectors) && (!check_last_su(ctx, start_lba))) { // for the last chunk, FP will be directly written
// 			calc_part_parity(ctx, start_lba, leading_subchunk_end_lba, parity_buf);
// 			__raizn_write_pp(sh, start_lba, leading_subchunk_end_lba, parity_buf);
// 		}
// 		if ((trailing_subchunk_sectors) && (!check_last_su(ctx, trailing_subchunk_start_lba))) { // for the last chunk, FP will be directly written, note: start_lba & end_lba can be on different stripes
// 			calc_part_parity(ctx, trailing_subchunk_start_lba, end_lba, parity_buf + (parity_su - 1) * ctx->params->su_sectors);
// 			__raizn_write_pp(sh, trailing_subchunk_start_lba, end_lba, parity_buf + (parity_su - 1) * ctx->params->su_sectors);
// 		}
// 	}
// 	return 0;
// }


int raizn_assign_wp_log_bio(struct raizn_stripe_head *sh, struct raizn_sub_io *wlio, int dev_idx, sector_t dev_lba)
{
	struct page *p;
	struct bio *wlbio = wlio->bio;
#ifdef DEBUG
	BUG_ON(!wlbio);
#endif
    p = is_vmalloc_addr(&wlio->header) ? vmalloc_to_page(&wlio->header) :
                        virt_to_page(&wlio->header);
	if (bio_add_page(wlbio, p, PAGE_SIZE, offset_in_page(&wlio->header)) !=
	    PAGE_SIZE) {
		pr_err("Failed to add dummy header page\n");
		bio_endio(wlbio);
		return NULL;
	}
	wlbio->bi_iter.bi_sector = dev_lba;
    wlio->dev = &sh->ctx->devs[dev_idx];
    wlio->zone = &sh->ctx->devs[dev_idx].zones[pba_to_pzone(sh->ctx, dev_lba)];
    wlbio->bi_opf = sh->orig_bio->bi_opf | REQ_FUA;
    bio_set_op_attrs(wlbio, REQ_OP_WRITE, 0);
	bio_set_dev(wlbio, sh->ctx->devs[dev_idx].dev->bdev);
#ifdef DEBUG
	BUG_ON(!p);
#endif
	return 1;
}

// struct raizn_sub_io *raizn_alloc_wp_log(struct raizn_stripe_head *sh, struct raizn_dev *dev, struct raizn_sub_io *wlio)
inline void raizn_alloc_wp_log(struct raizn_stripe_head *sh, struct raizn_dev *dev, struct raizn_sub_io *wlio)
{
	raizn_stripe_head_alloc_bio(
		sh, &dev->bioset, 1, RAIZN_SUBIO_WP_LOG, wlio);
}

static sector_t get_wp_pba(struct raizn_ctx *ctx, sector_t end_lba, int pp_distance, int wp_entry_idx)
{
    sector_t dev_pba, pzone_start;
    pzone_start = ctx->devs[0].zones[lba_to_lzone(ctx, end_lba)].start;
    dev_pba = pzone_start + ((lba_to_stripe(ctx, end_lba - 1) + pp_distance) << ctx->params->su_shift);
    dev_pba += wp_entry_idx * PAGE_SIZE / SECTOR_SIZE;
    return dev_pba;
}

static void generate_wp_log(struct raizn_stripe_head *sh, struct raizn_sub_io *wlio)
{
    // TODO: fill header with WPs of all opened zones
}

static void __raizn_write_wp_log(struct raizn_stripe_head *sh, sector_t end_lba, int wp_entry_idx)
{
    int pp_distance;
    struct raizn_ctx *ctx = sh->ctx;
    sector_t dev_pba;
    get_pp_distance(ctx, end_lba, &pp_distance);
    if (pp_distance >= 0) {
    // if (0) {
        struct raizn_sub_io *wlio1, *wlio2;
        int parity_dev_idx, wl_dev_idx;
        dev_pba = get_wp_pba(ctx, end_lba, pp_distance, wp_entry_idx);
        parity_dev_idx = lba_to_parity_dev_idx(ctx, end_lba - 1);
        wl_dev_idx = (parity_dev_idx + 1) % ctx->params->array_width; // next to parity dev (sequnece = 0)
    
    #if 1
        if (atomic_read(&sh->subio_idx) < 1) {
            printk("sub_io is fewer than 2. strange");
            BUG_ON(1);
        }
    #endif
        wlio1 = sh->sub_ios[0];
		raizn_alloc_wp_log(sh, &ctx->devs[parity_dev_idx], wlio1);
        generate_wp_log(sh, wlio1);
        if (!raizn_assign_wp_log_bio(sh, wlio1, parity_dev_idx, dev_pba)) {
			pr_err("Fatal: Failed to assign pp bio\n");
			BUG_ON(1);
		}
        wlio2 = sh->sub_ios[1];
		raizn_alloc_wp_log(sh, &ctx->devs[wl_dev_idx], wlio2);
		// wlio2 = raizn_alloc_wp_log(sh, &ctx->devs[wl_dev_idx]);
        generate_wp_log(sh, wlio2);
        if (!raizn_assign_wp_log_bio(sh, wlio2, wl_dev_idx, dev_pba)) {
			pr_err("Fatal: Failed to assign pp bio\n");
			BUG_ON(1);
		}
#ifdef SMALL_ZONE_AGGR
        raizn_submit_bio_aggr(ctx, __func__, wlio1->bio, &ctx->devs[parity_dev_idx], 0);
        raizn_submit_bio_aggr(ctx, __func__, wlio2->bio, &ctx->devs[wl_dev_idx], 0);
#else
        raizn_submit_bio(ctx, __func__, wlio1->bio, 0);
        raizn_submit_bio(ctx, __func__, wlio2->bio, 0);
#endif
    }
    else // WP log to separated zone
        raizn_write_md(
			sh,
			lba_to_lzone(ctx, end_lba),
			lba_to_parity_dev(ctx, end_lba),
			RAIZN_ZONE_MD_GENERAL,
            RAIZN_SUBIO_WP_LOG,
			sh->sub_ios[0], 
            0);
}


int raizn_write_wp_log(struct raizn_stripe_head *sh, sector_t end_lba)
{
    struct raizn_ctx *ctx = sh->ctx;
    int wp_entry_idx;
    struct raizn_zone *lzone = &ctx->zone_mgr.lzones[lba_to_lzone(ctx, end_lba)];
    if ((s32)lba_to_stripe(ctx, end_lba) <= (s32)READ_ONCE(lzone->last_complete_stripe)) {
        printk("[raizn_write_wp_log] Skipped. zone: %d, end_stripe: %d, last_complete_stripe: %d",
            lba_to_lzone(ctx, end_lba), lba_to_stripe(ctx, end_lba), READ_ONCE(lzone->last_complete_stripe));
        return 0;
    }

    sh->op = RAIZN_OP_WP_LOG;
    // TODO: lock needed? Can different threads simultaneoulsy read same wp_entry_idx value?
    wp_entry_idx = atomic_read(&lzone->wp_entry_idx);
    __raizn_write_wp_log(sh, end_lba, wp_entry_idx);
    // bio_endio(sh->orig_bio); return;
    if (++wp_entry_idx >= ctx->params->su_sectors / (PAGE_SIZE / SECTOR_SIZE))
        wp_entry_idx = 0;
    atomic_set(&lzone->wp_entry_idx, wp_entry_idx);
    // atomic_inc(&lzone->wp_entry_idx);
    return 1;
}

/* Below are manage functions */
void print_bitmap(struct raizn_ctx *ctx, struct raizn_zone *lzone)
{
    unsigned long *bitmap = lzone->stripe_prog_bitmap;
    unsigned long i, j, k;
    // mutex_lock(&lzone->prog_bitmap_lock);
    for (i=0; i<ctx->params->stripes_in_stripe_prog_bitmap; i++) {
        printk("stripe bitmap [%d] addr: %p\n", i, bitmap + i *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)));
        for (j=0; j<( BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors))); j++) {
            for (k=0; k<8; k++) {
                printk("%llu%llu%llu%llu%llu%llu%llu%llu\n",
                    ((*(bitmap + i *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)) & (1LL<<(k*8+0))) ? 1 : 0,
                    ((*(bitmap + i *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)) & (1LL<<(k*8+1))) ? 1 : 0,
                    ((*(bitmap + i *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)) & (1LL<<(k*8+2))) ? 1 : 0,
                    ((*(bitmap + i *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)) & (1LL<<(k*8+3))) ? 1 : 0,
                    ((*(bitmap + i *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)) & (1LL<<(k*8+4))) ? 1 : 0,
                    ((*(bitmap + i *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)) & (1LL<<(k*8+5))) ? 1 : 0,
                    ((*(bitmap + i *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)) & (1LL<<(k*8+6))) ? 1 : 0,
                    ((*(bitmap + i *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)) & (1LL<<(k*8+7))) ? 1 : 0
                );
            }
        }
    }
    // mutex_unlock(&lzone->prog_bitmap_lock);
}

void print_bitmap_one_stripe(struct raizn_ctx *ctx, struct raizn_zone *lzone, int stripe_num)
{
    int stripe_idx = stripe_num % ctx->params->stripes_in_stripe_prog_bitmap;
    unsigned long *bitmap = lzone->stripe_prog_bitmap;
    unsigned long i, j, k;
    // mutex_lock(&lzone->prog_bitmap_lock);
    printk("stripe bitmap [%d] addr: %p\n", stripe_num, bitmap + stripe_idx *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)));
    for (j=0; j<( BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors))); j++) {
        for (k=0; k<8; k++) {
            printk("%llu%llu%llu%llu%llu%llu%llu%llu\n",
                ((*(bitmap + stripe_idx *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)) & (1LL<<(k*8+0))) ? 1 : 0,
                ((*(bitmap + stripe_idx *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)) & (1LL<<(k*8+1))) ? 1 : 0,
                ((*(bitmap + stripe_idx *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)) & (1LL<<(k*8+2))) ? 1 : 0,
                ((*(bitmap + stripe_idx *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)) & (1LL<<(k*8+3))) ? 1 : 0,
                ((*(bitmap + stripe_idx *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)) & (1LL<<(k*8+4))) ? 1 : 0,
                ((*(bitmap + stripe_idx *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)) & (1LL<<(k*8+5))) ? 1 : 0,
                ((*(bitmap + stripe_idx *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)) & (1LL<<(k*8+6))) ? 1 : 0,
                ((*(bitmap + stripe_idx *  BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)) & (1LL<<(k*8+7))) ? 1 : 0
            );
        }
    }
    // mutex_unlock(&lzone->prog_bitmap_lock);
}

void raizn_update_prog_bitmap(struct raizn_ctx *ctx, sector_t start_lba, sector_t end_lba)
{
    struct raizn_zone *lzone = &ctx->zone_mgr.lzones[lba_to_lzone(ctx, start_lba)];
    char bit_pattern[64];
    volatile unsigned long *bitmap = lzone->stripe_prog_bitmap;
    int bit_loc = 0, total_bits = sector_to_block_addr(end_lba - start_lba);
    int start_stripe_num = lba_to_stripe(ctx, start_lba);
    int start_stripe_idx = start_stripe_num % ctx->params->stripes_in_stripe_prog_bitmap;
    sector_t start_stripe_offset = lba_to_stripe_offset(ctx, start_lba);
    int end_stripe_num = lba_to_stripe(ctx, end_lba-1);
    int i=0, j;
    int stripe_num = start_stripe_idx;
    int bit = 0;
#ifdef DEBUG
// #if 1
    printk("---raizn_update_prog_bitmap--- %lld, %lld, zone_idx: %d, start_str: %d, end_str: %d\n", 
        start_lba, end_lba, lba_to_lzone(ctx, start_lba),
        lba_to_stripe(ctx, start_lba),lba_to_stripe(ctx, end_lba));
    BUG_ON(total_bits <= 0);
    // printk("addr###%d:%d\t%p\n", lba_to_lzone(ctx, start_lba), lba_to_stripe(ctx, start_lba),
    //     bitmap + start_stripe_idx * BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)));
    // print_bitmap(ctx, lzone);
    for (i=start_stripe_num; i<=end_stripe_num; i++) {
        for (j=0; j<BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)); j++) {
            convert_bit_pattern_64(
                (*(bitmap + (i% ctx->params->stripes_in_stripe_prog_bitmap) * BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)),
                bit_pattern    
            );
            printk("before###%d:%d\t%s, value: %llu, addr: %p\n", lba_to_lzone(ctx, start_lba), i, bit_pattern, 
                *(bitmap + (i% ctx->params->stripes_in_stripe_prog_bitmap) * BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j),
                (bitmap + (i% ctx->params->stripes_in_stripe_prog_bitmap) * BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j));
        }    
    }
#endif

// #ifdef DEBUG
#if 0
    while(total_bits) { 
        // printk("addr: %p, stripe_idx: %d, stripe_num: %d, sector_to_block_addr(lba_to_stripe_offset(ctx, start_lba)): %lld, total_bits: %d, bit_loc: %lld, add: %lld, mod: %lld\n",
        //     bitmap + start_stripe_idx * sector_to_block_addr(ctx->params->stripe_sectors) +
        //     sector_to_block_addr(lba_to_stripe_offset(ctx, start_lba)) + bit_loc,
        //     start_stripe_idx, stripe_num,
        //     sector_to_block_addr(lba_to_stripe_offset(ctx, start_lba)),
        //     total_bits, bit_loc, (sector_to_block_addr(lba_to_stripe_offset(ctx, start_lba)) + bit_loc), (sector_to_block_addr(lba_to_stripe_offset(ctx, start_lba)) + bit_loc)%sector_to_block_addr(ctx->params->stripe_sectors));
        // printk("bit_Loc: %d, total_bit: %d, start_stripe_idx: %d\n", bit_loc, total_bits, start_stripe_idx);
        bit = test_and_set_bit(start_stripe_idx * sector_to_block_addr(ctx->params->stripe_sectors) +
            sector_to_block_addr(start_stripe_offset) + bit_loc, bitmap);
        if (bit) {
            // print_bitmap_one_stripe(ctx, lzone, stripe_num);
            // BUG_ON(1);
            convert_bit_pattern_64(
                (*(bitmap + (stripe_num % ctx->params->stripes_in_stripe_prog_bitmap) * BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)))),
                bit_pattern    
            );
            printk("BIT conflict!!###%d:%d\t%s, addr: %p\n", lba_to_lzone(ctx, start_lba), lba_to_stripe(ctx, start_lba), bit_pattern, 
                (bitmap + (stripe_num)% ctx->params->stripes_in_stripe_prog_bitmap * BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors))));
        }
        bit_loc++;
        total_bits--;

        if ((sector_to_block_addr(start_stripe_offset) + bit_loc) % sector_to_block_addr(ctx->params->stripe_sectors) == 0) {
            stripe_num++;
        }
        if (stripe_num >= ctx->params->stripes_in_stripe_prog_bitmap) {
            start_stripe_idx = 0;
            bit_loc = 0;
        }
    }
#else
    if ((start_stripe_num/ctx->params->stripes_in_stripe_prog_bitmap) == 
            (end_stripe_num/ctx->params->stripes_in_stripe_prog_bitmap))
        bitmap_set(bitmap, start_stripe_idx * sector_to_block_addr(ctx->params->stripe_sectors) +
            sector_to_block_addr(start_stripe_offset), 
            total_bits);
    else {
        unsigned long bits_to_bitmap_end = 
            (ctx->params->stripes_in_stripe_prog_bitmap - start_stripe_idx) * sector_to_block_addr(ctx->params->stripe_sectors)
             - sector_to_block_addr(start_stripe_offset);
        // printk("bits 1: %lld, bits 2: %lld\n", 
        //     bits_to_bitmap_end, total_bits - bits_to_bitmap_end);
        // bitmap start to end stripe
        bitmap_set(bitmap, 0, 
            total_bits - bits_to_bitmap_end);
        // start stripe to bitmap end
        bitmap_set(bitmap, start_stripe_idx * sector_to_block_addr(ctx->params->stripe_sectors) +
            sector_to_block_addr(start_stripe_offset), bits_to_bitmap_end);
    }
#endif
#ifdef DEBUG
// #if 1
    for (i=start_stripe_num; i<=end_stripe_num; i++) {
        for (j=0; j<BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)); j++) {
            convert_bit_pattern_64(
                (*(bitmap + (i% ctx->params->stripes_in_stripe_prog_bitmap) * BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j)),
                bit_pattern    
            );
            printk("after###%d:%d\t%s, value: %llu, addr: %p\n", lba_to_lzone(ctx, start_lba), i, bit_pattern, 
                *(bitmap + (i% ctx->params->stripes_in_stripe_prog_bitmap) * BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j),
                (bitmap + (i% ctx->params->stripes_in_stripe_prog_bitmap) * BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + j));
        }    
    }
#endif

    // print_bitmap(ctx, lzone);
}

// reset only one stripe
inline void raizn_reset_prog_bitmap_stripe(struct raizn_ctx *ctx, struct raizn_zone *lzone, unsigned int stripe_num)
{
    unsigned int stripe_idx = stripe_num % ctx->params->stripes_in_stripe_prog_bitmap; // important! we allocated memory for only ZRWA area
    char *bitmap = (char *) lzone->stripe_prog_bitmap;
#ifdef DEBUG
    printk("(%d)raizn_reset_prog_bitmap_stripe [%d:%d] addr: %p\n",
        current->pid, 
        lba_to_lzone(ctx, lzone->start), stripe_num, 
        bitmap + BITS_TO_BYTES(stripe_idx * sector_to_block_addr(ctx->params->stripe_sectors)));
#endif
    // printk("before### %d", stripe_num);
    // print_bitmap(ctx, lzone);
    // mutex_lock(&lzone->prog_bitmap_lock);
	memset(bitmap + BITS_TO_BYTES(stripe_idx * sector_to_block_addr(ctx->params->stripe_sectors)), 0,
        BITS_TO_BYTES(sector_to_block_addr(ctx->params->stripe_sectors)));
    // mutex_unlock(&lzone->prog_bitmap_lock);
    // printk("after###");
    // print_bitmap(ctx, lzone);
    // printk("*********************************************************");
} 

// reset the whole zone
inline void raizn_reset_prog_bitmap_zone(struct raizn_ctx *ctx, struct raizn_zone *lzone)
{
    // printk("[raizn_reset_prog_bitmap_zone: %d] before###\n", lba_to_lzone(ctx, lzone->start));
    // print_bitmap(ctx, lzone);
    char *bitmap = (char *) lzone->stripe_prog_bitmap;
    // mutex_lock(&lzone->prog_bitmap_lock);
	memset(bitmap, 0, ctx->params->stripe_prog_bitmap_size_bytes);
    // mutex_unlock(&lzone->prog_bitmap_lock);
    // printk("after### %llu\n", (unsigned long)*bitmap);
    // print_bitmap(ctx, lzone);
    // int i;
    // unsigned long *bitmap2 = lzone->stripe_prog_bitmap;
    // for(i=0; i<BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)); i++)
    //     if (*(bitmap2+i) > 0) {
    //         pr_err("not initialized!!");
    //     }
    //     else   
    //         printk("bitmap init success");
}

// wait until given stripe becomes durable. bitmap is checked from the start of the stripe to the end_offset
inline bool check_stripe_complete(struct raizn_ctx *ctx, struct raizn_zone *lzone, int stripe_num)
{
    // printk("[check_stripe_complete] %llu", BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)));
    int i;
    char bit_pattern[64];
    unsigned long content = 0;
    volatile unsigned long *bitmap = (unsigned long *)lzone->stripe_prog_bitmap;
    unsigned long mask = ULONG_MAX;
    int stripe_idx = stripe_num % ctx->params->stripes_in_stripe_prog_bitmap;
    // print_bitmap(ctx, lzone);

    for(i=0; i<BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)); i++)
    {
        // mutex_lock(&lzone->prog_bitmap_lock);
        content = READ_ONCE(*(bitmap + stripe_idx * BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + i));
        // mutex_unlock(&lzone->prog_bitmap_lock);
        if ((content & mask) != mask) {
#ifdef DEBUG
            // print_bitmap_one_stripe(ctx, lzone, stripe_idx);
            convert_bit_pattern_64(mask - (content & mask), bit_pattern);
            printk("[check_stripe_complete] not match bit(%s)! stripe[%d:%d]\n", 
                bit_pattern,
                lba_to_lzone(ctx,lzone->start),
                stripe_num);
#endif
            return false;
        }
    }
    // printk("[check_stripe_complete] match bit!! stripe[%d]", 
    //     stripe_num);
    return true;
}

// wait all previous requests are persisted.
// TODO: manage last durable stripe-num (atomic type) and check from there
inline int check_stripe_complete_range(struct raizn_ctx *ctx, struct raizn_zone *lzone, sector_t start_stripe_num, sector_t end_stripe_num)
{
    // printk("(%d)[check_stripe_complete_range] start: %lld, end: %lld",
    //     current->pid, start_stripe_num, end_stripe_num);
    int stripe_num;
    for (stripe_num=start_stripe_num; stripe_num<=end_stripe_num; stripe_num++) { 
        if (!check_stripe_complete(ctx, lzone, stripe_num)) {
            if (stripe_num == start_stripe_num)
                return -1;
            else
                return stripe_num - 1;
        }
    }
    return end_stripe_num;
}

void raizn_update_complete_stripe(struct raizn_ctx *ctx, sector_t start_lba, sector_t end_lba)
{
    int stripe_num, new_comp_str_num, prev_comp_str_num, end_stripe_num = lba_to_stripe(ctx, end_lba - 1);
    struct raizn_zone *lzone = &ctx->zone_mgr.lzones[lba_to_lzone(ctx, start_lba)];
#ifdef DEBUG
    printk("[raizn_update_complete_stripe][%d] lba: %lld~%lld, prev: %d, end_str: %d\n",
        lba_to_lzone(ctx, start_lba), start_lba, end_lba,
        lzone->last_complete_stripe, end_stripe_num);
#endif
    prev_comp_str_num = lzone->last_complete_stripe;
    if (prev_comp_str_num >= end_stripe_num) {
        return;
    }
    new_comp_str_num = check_stripe_complete_range(ctx, lzone, 
       prev_comp_str_num + 1, end_stripe_num + ctx->params->chunks_in_zrwa); 
       // Reason why check further than end_stripe: request completion can be reordered
       // ctx->chunks_in_zrwa/2? gap between request stripes can't be greater than ctx->chunks_in_zrwa/2

    // spin_lock(&lzone->last_comp_str_lock);
#ifdef DEBUG
    printk("[raizn_update_complete_stripe] prev: %d, new: %d\n",
        prev_comp_str_num, new_comp_str_num);
#endif
    // TODO: maybe lock or barrier needed..?
    if (new_comp_str_num > READ_ONCE(lzone->last_complete_stripe)) {
        WRITE_ONCE(lzone->last_complete_stripe, new_comp_str_num);
        // spin_unlock(&lzone->last_comp_str_lock);
        for (stripe_num=prev_comp_str_num + 1; stripe_num<=new_comp_str_num; stripe_num++) {
            raizn_reset_prog_bitmap_stripe(ctx, lzone, stripe_num);
        }
#ifdef DEBUG
        printk("[raizn_update_complete_stripe][%d] lba: %lld~%lld, prev: %d, new: %d",
            lba_to_lzone(ctx, start_lba), start_lba, end_lba,
            prev_comp_str_num, new_comp_str_num);
#endif
    }
    // else
        // spin_unlock(&lzone->last_comp_str_lock);
}

// wait until given stripe becomes durable. bitmap is checked from the start of the stripe to the end_offset
inline void wait_stripe_durable(struct raizn_ctx *ctx, struct raizn_zone *lzone, int stripe_num, int check_end_offset)
{
    int i;
    volatile unsigned long content = 0;
    volatile unsigned long *bitmap = lzone->stripe_prog_bitmap;
    unsigned long mask = (unsigned long)((unsigned long)1 << (unsigned long)sector_to_block_addr(check_end_offset)) - 1;
    int stripe_idx = stripe_num % ctx->params->stripes_in_stripe_prog_bitmap;
    // print_bitmap(ctx, lzone);
    char bit_pattern[64];
    // printk("[wait_stripe_durable] check_end_offset:%d, sector_to_block_addr(check_end_offset): %d, mask:%llu\n", 
    //     check_end_offset, sector_to_block_addr(check_end_offset), mask);

    for(i=0; i<BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)); i++)
    {
        // mutex_lock(&lzone->prog_bitmap_lock);
        content = READ_ONCE(*(bitmap + stripe_idx * BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + i));
        // mutex_unlock(&lzone->prog_bitmap_lock);
        // print_bitmap(ctx, lzone);
#ifdef DEBUG
        printk("stripe[%d] %p: %llu, mask: %llu, &: %llu\n", stripe_idx, 
            bitmap + stripe_idx * BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors) + i),
            content,
            mask,
            content & mask);
#endif
        while (1) {
            if ((content & mask) == mask)
                break;
            if (READ_ONCE(lzone->last_complete_stripe)>=stripe_num)
                break;
            if ((atomic_read(&lzone->cond) != BLK_ZONE_COND_IMP_OPEN)&&(atomic_read(&lzone->cond) != BLK_ZONE_COND_EXP_OPEN))
                return; // zone is finished
#ifdef DEBUG
// #if 1
            // printk("(%d)[%s] waiting for write completion of preceding requests of the same stripe",
            //     current->pid, __func__);
            printk("stripe[%d:%d] %p: %llu, mask: %llu, &: %llu, check_end_offset: %d\n", 
                lba_to_lzone(ctx, lzone->start),
                stripe_num, 
                bitmap + stripe_idx * BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors) + i),
                content,
                mask,
                content & mask,
                check_end_offset);
            // print_bitmap_one_stripe(ctx, lzone, stripe_idx);
            // mdelay(1000);
            msleep(1000);
            // usleep_range(10, 20);
#else
            content = READ_ONCE(*(bitmap + stripe_idx * BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors)) + i));
// #ifndef PERF_MODE
#if 1
            if (lzone->waiting_str_num2 == stripe_num) {
                int wc  = atomic_read(&lzone->wait_count2);
                if (wc>=1000) {
                    if (wc%100000 == 0) {
                        convert_bit_pattern_64(mask - (content & mask), bit_pattern);
                        printk("[wait_stripe_durable] not match bit(%s)! stripe[%d:%d] %p: %llu, mask: %llu, &: %llu", 
                            bit_pattern,
                            lba_to_lzone(ctx, lzone->start),
                            stripe_num, 
                            bitmap + stripe_idx * BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors) + i),
                            content,
                            mask,
                            content & mask);
                        // print_bitmap_one_stripe(ctx, lzone, stripe_idx);
                    }
                    // print_bitmap_one_stripe(ctx, lzone, stripe_num);
                    // usleep_range(10, 20);
                    // mleep(1);
                }
                atomic_inc(&lzone->wait_count2);
            }
            else {
                atomic_set(&lzone->wait_count2, 0);
                // usleep_range(10, 20);
                lzone->waiting_str_num2 = stripe_num;
            }
#endif

            // printk("(%d)[%s] waiting for write completion of preceding requests [%d:%d]\n",
            //     current->pid, __func__, lba_to_lzone(ctx, lzone->start), stripe_num);
		    // udelay(2);
            // msleep(1000);
            usleep_range(10, 20);
		    // usleep_range(2, 5);
#endif
            // mutex_lock(&lzone->prog_bitmap_lock);
            // mutex_unlock(&lzone->prog_bitmap_lock);
        }
    }
}

// wait all previous requests are persisted.
// TODO: manage last durable stripe-num (atomic type) and check from there
void raizn_wait_prog(struct raizn_ctx *ctx, sector_t start_lba, sector_t end_lba)
{ 
    int start_stripe_num = lba_to_stripe(ctx, start_lba);
    int end_stripe_num = lba_to_stripe(ctx, end_lba - 1);
    struct raizn_zone *lzone = &ctx->zone_mgr.lzones[lba_to_lzone(ctx, start_lba)];
#ifdef DEBUG
    printk("(%d)[raizn_wait_prog] start: %lld, end: %lld, end_str: %d, last_complete_stripe: %d, bool %d",
        current->pid, start_lba, end_lba, end_stripe_num,
        (lzone->last_complete_stripe),
        (lzone->last_complete_stripe) >= end_stripe_num - 1);
#endif
    int stripe_num, start_offset = 0, check_end_offset = 0;

    // wait until last completed stripe reaches at end_stripe_num -1
    while (1) {
        if (READ_ONCE(lzone->last_complete_stripe) >= end_stripe_num - 1) {
#ifdef DEBUG
            printk("last_complete_stripe reaches at end_stripe_num - 1, last: %d, end-1: %d",
                READ_ONCE(lzone->last_complete_stripe), end_stripe_num - 1);
#endif
            break;
        }
        int zone_cond = atomic_read(&lzone->cond);
        if ((zone_cond != BLK_ZONE_COND_IMP_OPEN)&&(zone_cond != BLK_ZONE_COND_EXP_OPEN)) {
#ifdef DEBUG
            printk("zone is finished, zone->cond: %d, BLK_ZONE_COND_IMP_OPEN: %d",
                zone_cond, BLK_ZONE_COND_IMP_OPEN);
#endif
            return; // zone is finished
        }
#ifdef DEBUG
        printk("[raizn_wait_prog] last_complete_stripe: %d, end_str: %d", 
            READ_ONCE(lzone->last_complete_stripe), end_stripe_num);
        msleep(1000);
        // usleep_range(10, 20);
        // mdelay(1000);
#else
        // msleep(1000);
// #ifndef PERF_MODE
#if 1
        if (lzone->waiting_str_num == end_stripe_num) {
            int wc  = atomic_read(&lzone->wait_count);
            if (wc>=1000) {
                if (wc%100000 == 0) {
                    printk("[raizn_wait_prog][%d] last_complete_stripe: %d, end_str: %d", 
                        lba_to_lzone(ctx, start_lba),
                        READ_ONCE(lzone->last_complete_stripe), end_stripe_num);
                    // print_bitmap_one_stripe(ctx, lzone, READ_ONCE(lzone->last_complete_stripe)+1);
                    // print_bitmap_one_stripe(ctx, lzone, READ_ONCE(lzone->last_complete_stripe)+2);
                    // print_bitmap_one_stripe(ctx, lzone, READ_ONCE(lzone->last_complete_stripe)+3);
                    // print_bitmap_one_stripe(ctx, lzone, READ_ONCE(lzone->last_complete_stripe)+4);
                    // print_bitmap_one_stripe(ctx, lzone, READ_ONCE(lzone->last_complete_stripe)+5);
                    int i, j;
                    unsigned long content = 0;
                    volatile unsigned long *bitmap = lzone->stripe_prog_bitmap;
                    char bit_pattern[64];
                    for (i=lzone->last_complete_stripe+1; i<=lzone->last_complete_stripe+5; i++) {
                        for (j=0;j<BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors));j++) {
                            content = READ_ONCE(*(bitmap + (i%ctx->params->stripes_in_stripe_prog_bitmap) * BITS_TO_LONGS(sector_to_block_addr(ctx->params->stripe_sectors))+j));
                            convert_bit_pattern_64(content, bit_pattern);
                            printk("[raizn_wait_prog] stripe[%d:%d] %s\n", 
                                lba_to_lzone(ctx, lzone->start),
                                i, 
                                bit_pattern);
                        }
                    }
                }
                // print_bitmap_one_stripe(ctx, lzone, end_stripe_num);
                // usleep_range(10, 20);
                // mleep(1);
            }
            atomic_inc(&lzone->wait_count);
        }
        else {
            atomic_set(&lzone->wait_count, 0);
            // usleep_range(10, 20);
            lzone->waiting_str_num = end_stripe_num;
        }
#endif
        // printk("[raizn_wait_prog][%d] last_complete_stripe: %d, end_str: %d\n", 
        //     lba_to_lzone(ctx, start_lba),
        //     READ_ONCE(lzone->last_complete_stripe), end_stripe_num);
        usleep_range(10, 20);
        // udelay(2);
#endif
    }
    if (start_stripe_num != end_stripe_num)
        return; // bitmap progress end stripe is already filled by this request
    
    // check until start_lba. this needs only when request is included in single stripe
    check_end_offset = lba_to_stripe_offset(ctx, start_lba); 
    if (check_end_offset != 0)
        wait_stripe_durable(ctx, lzone, end_stripe_num, check_end_offset);
#ifdef DEBUG
    printk("[wait end] end: %lld",end_stripe_num);
#endif
}

inline bool wp_advance_needed(struct raizn_ctx *ctx, sector_t start_lba, sector_t end_lba)
{
    // preceding chunk(s) are completed
    if (lba_to_su(ctx, start_lba) != lba_to_su(ctx, end_lba))
        return true;
    // request is included in a single chunk, but the chunk is completed by this request
    else if (lba_to_su_offset(ctx, end_lba) == 0)
        return true;

    return false;
}

// flush_lba is on lzone's address space. zrwa flush handler will convert this lba to pba & dev_idx.
void raizn_request_zrwa_flush(struct raizn_ctx *ctx, sector_t start_lba, sector_t end_lba)
{
#ifdef DEBUG
    printk("##raizn_request_zrwa_flush##[%d:%d] lba: %lld ~ %lld",
        lba_to_lzone(ctx, start_lba), lba_to_stripe(ctx, start_lba),
        start_lba, end_lba);
#endif
	int fifo_idx, ret;
    struct raizn_stripe_head *sh =
		raizn_stripe_head_alloc(ctx, NULL, RAIZN_OP_ZONE_ZRWA_FLUSH);
    sh->start_lba = start_lba;
    sh->end_lba = end_lba;
    sh->zf_submit_time = ktime_get_ns();
    sh->zf_submitted = true;
	// fifo_idx = lba_to_lzone(ctx, start_lba) %
#ifdef MULTI_FIFO
    // WARN: if fifo idx is based on lba_to_stripe(), req order is really messed
    // int aligned = (lba_to_stripe_offset(ctx, end_lba) == 0);
	// fifo_idx = (lba_to_lzone(ctx, end_lba) + lba_to_stripe(ctx, end_lba - 1) - 1 + aligned) %
	fifo_idx = (lba_to_lzone(ctx, end_lba)) %
	    	min(ctx->num_cpus, ctx->num_manage_workers);
	ret = kfifo_in_spinlocked(
		&ctx->zone_manage_workers[fifo_idx].work_fifo, &sh,
		1, &ctx->zone_manage_workers[fifo_idx].wlock);
    if (!ret) {
		pr_err("ERROR: %s kfifo insert failed!\n", __func__);
		return;
	}
	raizn_queue_manage(ctx, fifo_idx);
#else
    // Push it onto the fifo
    ret = kfifo_in_spinlocked(&ctx->zone_manage_workers.work_fifo, &sh, 1,
                &ctx->zone_manage_workers.wlock);
    if (!ret) {
		pr_err("ERROR: %s kfifo insert failed!\n", __func__);
		return;
	}
    // udelay(10);
	raizn_queue_manage(ctx, 0);
#endif
}

// given lba is converted to corresponding pba in each dev
static void __raizn_do_zrwa_flush(struct raizn_ctx *ctx, int dev_idx, sector_t dev_pba, bool to_parity)
{
    // return;
#ifdef DEBUG
    printk("[%s] dev_idx: %d, dev_pba: %lld", __func__, dev_idx, dev_pba);
    BUG_ON((dev_idx < 0) || (dev_idx > ctx->params->array_width));
    BUG_ON(pba_to_pzone(ctx, dev_pba) >= ctx->params->num_zones);
#endif
#ifdef SAMSUNG_MODE
	struct block_device *nvme_bdev = ctx->raw_bdev;
    int pzone_idx = pba_to_pzone(ctx, dev_pba);
#else
	struct block_device *nvme_bdev = ctx->devs[dev_idx].dev->bdev;
#endif
	int ret, j;
    bool need_free = 0;
    unsigned long flags;
	// struct raizn_zone *pzone = &ctx->devs[dev_idx].zones[pzone_idx];
    struct raizn_zone *pzone = &ctx->devs[dev_idx].zones[pba_to_pzone(ctx, dev_pba)];
    if (dev_pba+1 >= pzone->start + pzone->capacity) {
#ifdef DEBUG
// #if 1
		printk("[Skipped]\tzrwa flush exceeds zone boundary dev: %d, pba(sector): %lld, pba: %lldKB, wp: %lldKB\n",
			dev_idx, dev_pba, dev_pba/2, pzone->pzone_wp/2);
#endif
        goto wp_update;
        // return;
    }

	//  no need to submit zrwa flush, already flushed to former address by following request
	if (pzone->pzone_wp > dev_pba) {
#ifdef DEBUG
// #if 1
		printk("[Skipped]\tzrwa flush zone dev: %d, pba(sector): %lld, pba: %lldKB, wp: %lldKB\n",
			dev_idx, dev_pba, dev_pba/2, pzone->pzone_wp/2);
#endif
	}
    else {
#ifdef DEBUG
        sector_t stripe_start_lba = (dev_pba >> ctx->params->su_shift) * ctx->params->stripe_sectors;
        struct raizn_zone *lzone = &ctx->zone_mgr.lzones[lba_to_lzone(ctx, stripe_start_lba)];
        printk("[%s] [%d:%d] dev_idx: %d, pba(sector): %lld, pba: %lldKB, wp: %lldKB\n",
        	__func__, lba_to_lzone(ctx, stripe_start_lba), lba_to_stripe(ctx, stripe_start_lba),
            dev_idx, dev_pba, dev_pba/2, pzone->pzone_wp/2);
#endif
    #ifdef RECORD_ZFLUSH
        u64 before = ktime_get_ns();
    #endif
        // ret = 0;
        struct nvme_passthru_cmd *nvme_cmd = kzalloc(sizeof(struct nvme_passthru_cmd), GFP_KERNEL);
        if (!nvme_cmd) {
            printk("cmd alloc failed!");
            BUG_ON(1);
            return;
        }
        need_free = 1;
#ifdef SAMSUNG_MODE
		sector_t pzone_base_addr = dev_idx * ctx->params->div_capacity +
			(pzone_idx * ctx->params->gap_zone_aggr * ctx->devs[0].zones[0].phys_len);
#ifdef NON_POW_2_ZONE_SIZE
	    sector_t pzone_offset = dev_pba % (ctx->devs[0].zones[0].len);
#else
	    sector_t pzone_offset = dev_pba & (ctx->devs[0].zones[0].len - 1);
#endif
	    sector_t aggr_chunk_offset = pzone_offset & (ctx->params->aggr_chunk_sector - 1);
	    sector_t aggr_stripe_id = (pzone_offset / ctx->params->aggr_chunk_sector) >> ctx->params->aggr_zone_shift; // col num in 2-dimensional chunk array
        int azone_idx = pba_to_aggr_zone(ctx, dev_pba);
        int old_azone_idx = pba_to_aggr_zone(ctx, pzone->pzone_wp);
        sector_t cmd_addr;
        // printk("pzone_base_addr: %lld, pzone_offset: %lld, aggr_chunk_offset: %d, aggr_stripe_id: %d",
        //     pzone_base_addr, pzone_offset, aggr_chunk_offset, aggr_stripe_id
        // );
        // printk("azone_idx: %d, old_azone_idx: %d, (ctx->params->num_zone_aggr << (ctx->params->su_shift - 1)): %lld",
        //     azone_idx, old_azone_idx, (ctx->params->num_zone_aggr << (ctx->params->su_shift - 1)));

        if ((dev_pba - pzone->pzone_wp) >= 
            (ctx->params->num_zone_aggr << (ctx->params->su_shift - 1))) {
            // for (j=0; j<ctx->params->num_zone_aggr; j++) {
            for (j=azone_idx; j<=azone_idx; j++) {
                if (j <= azone_idx)
                    cmd_addr = pzone_base_addr + j * ctx->devs[0].zones[0].phys_len +
                        (aggr_stripe_id << ctx->params->aggr_chunk_shift) +
                        aggr_chunk_offset;
                else
                    cmd_addr = pzone_base_addr + j * ctx->devs[0].zones[0].phys_len +
                        (aggr_stripe_id << ctx->params->aggr_chunk_shift) +
                        aggr_chunk_offset -
                        (ctx->params->su_sectors >> 1);
#ifdef DEBUG
                printk("[__raizn_do_zrwa_flush] 1 cmd_addr: %lld(sector), %lld(KB)", 
                    cmd_addr, sector_to_block_addr(cmd_addr));
#endif                
                zrwa_flush_zone(nvme_cmd, sector_to_block_addr(cmd_addr), NS_NUM, 0); // 3rd parameter is nsid of device e.g.) nvme0n2 --> 2
                ret = nvme_submit_passthru_cmd_sync(nvme_bdev, nvme_cmd);
// #if 1
#ifdef DEBUG
                if (ret != 0) {
                    printk("(%d)[Fail]\tzrwa flush zone[%d:%d] ret: %d, dev: %d, pba(sector): %lld, pba: %lldKB, wp: %lldKB\n",
                        current->pid, pba_to_pzone(ctx, dev_pba), j, ret, 
                        dev_idx, cmd_addr-pzone->start, (cmd_addr-pzone->start)/2, (pzone->pzone_wp-pzone->start)/2);
                }
                else {
                    printk("(%d)[Success]\tzrwa flush zone[%d:%d] dev: %d, pba(sector): %lld, pba: %lldKB, wp: %lldKB\n",
                        current->pid, pba_to_pzone(ctx, dev_pba), j,
                        dev_idx, cmd_addr-pzone->start, (cmd_addr-pzone->start)/2, (pzone->pzone_wp-pzone->start)/2);
                }
#endif
            }
        }
        else {
            if (old_azone_idx > azone_idx) { 
                // for (j=0; j<=azone_idx; j++) {
                for (j=azone_idx; j<=azone_idx; j++) {
                    cmd_addr = pzone_base_addr + j * ctx->devs[0].zones[0].phys_len +
                        (aggr_stripe_id << ctx->params->aggr_chunk_shift) +
                        aggr_chunk_offset;
                    // cmd_addr = pba_to_aggr_addr(ctx, dev_pba + 
                    //     ((j - azone_idx) << (ctx->params->aggr_chunk_shift)));
#ifdef DEBUG
                    printk("[__raizn_do_zrwa_flush] 2.11 cmd_addr: %lld(sector), %lld(4KB blk)", 
                        cmd_addr, sector_to_block_addr(cmd_addr));
#endif
                    zrwa_flush_zone(nvme_cmd, sector_to_block_addr(cmd_addr), NS_NUM, 0); // 3rd parameter is nsid of device e.g.) nvme0n2 --> 2
                    ret = nvme_submit_passthru_cmd_sync(nvme_bdev, nvme_cmd);
// #if 1
#ifdef DEBUG
                    if (ret != 0) {
                        printk("(%d)[Fail]\tzrwa flush zone[%d:%d] ret: %d, dev: %d, pba(sector): %lld, pba: %lldKB, wp: %lldKB\n",
                            current->pid, pba_to_pzone(ctx, dev_pba), j, ret, 
                            dev_idx, cmd_addr-pzone->start, (cmd_addr-pzone->start)/2, (pzone->pzone_wp-pzone->start)/2);
                    }
                    else {
                        printk("(%d)[Success]\tzrwa flush zone[%d:%d] dev: %d, pba(sector): %lld, pba: %lldKB, wp: %lldKB\n",
                            current->pid, pba_to_pzone(ctx, dev_pba), j,
                            dev_idx, cmd_addr-pzone->start, (cmd_addr-pzone->start)/2, (pzone->pzone_wp-pzone->start)/2);
                    }
#endif
                }
                // for (j=old_azone_idx; j<ctx->params->num_zone_aggr; j++) {
                if (0) {
                    cmd_addr = pzone_base_addr + j * ctx->devs[0].zones[0].phys_len +
                        (aggr_stripe_id << ctx->params->aggr_chunk_shift) +
                        aggr_chunk_offset -
                        (ctx->params->su_sectors >> 1);
                    // cmd_addr = pba_to_aggr_addr(ctx, dev_pba + 
                    //     ((j - azone_idx) << (ctx->params->aggr_chunk_shift)) - 
                    //     (ctx->params->su_sectors >> 1));
#ifdef DEBUG
                    printk("[__raizn_do_zrwa_flush] 2.12 cmd_addr: %lld(sector), %lld(KB)", 
                        cmd_addr, sector_to_block_addr(cmd_addr));
#endif
                    zrwa_flush_zone(nvme_cmd, sector_to_block_addr(cmd_addr), NS_NUM, 0); // 3rd parameter is nsid of device e.g.) nvme0n2 --> 2
                    ret = nvme_submit_passthru_cmd_sync(nvme_bdev, nvme_cmd);
// #if 1
#ifdef DEBUG
                    if (ret != 0) {
                        printk("(%d)[Fail]\tzrwa flush zone[%d:%d] ret: %d, dev: %d, pba(sector): %lld, pba: %lldKB, wp: %lldKB\n",
                            current->pid, pba_to_pzone(ctx, dev_pba), j, ret, 
                            dev_idx, cmd_addr-pzone->start, (cmd_addr-pzone->start)/2, (pzone->pzone_wp-pzone->start)/2);
                    }
                    else {
                        printk("(%d)[Success]\tzrwa flush zone[%d:%d] dev: %d, pba(sector): %lld, pba: %lldKB, wp: %lldKB\n",
                            current->pid, pba_to_pzone(ctx, dev_pba), j,
                            dev_idx, cmd_addr-pzone->start, (cmd_addr-pzone->start)/2, (pzone->pzone_wp-pzone->start)/2);
                    }
#endif
                }
            }
            else {
                // printk("__raizn_do_zrwa_flush 2.2 old_azone_idx: %d, azone_idx: %d", old_azone_idx, azone_idx);
                // for (j=old_azone_idx; j<=azone_idx; j++) {
                for (j=azone_idx; j<=azone_idx; j++) {
                    cmd_addr = pzone_base_addr + j * ctx->devs[0].zones[0].phys_len +
                        (aggr_stripe_id << ctx->params->aggr_chunk_shift) +
                        aggr_chunk_offset;
                    // cmd_addr = pba_to_aggr_addr(ctx, dev_pba + 
                    //     ((j - azone_idx) << (ctx->params->aggr_chunk_shift)));
#ifdef DEBUG
                    printk("[__raizn_do_zrwa_flush] 2.2 idx: %d, cmd_addr: %lld(sector), %lld(4KB blk)", 
                        j, cmd_addr, sector_to_block_addr(cmd_addr));
#endif
                    zrwa_flush_zone(nvme_cmd, sector_to_block_addr(cmd_addr), NS_NUM, 0); // 3rd parameter is nsid of device e.g.) nvme0n2 --> 2
                    ret = nvme_submit_passthru_cmd_sync(nvme_bdev, nvme_cmd);
#ifdef DEBUG
// #if 1
                    if (ret != 0) {
                        printk("(%d)[Fail]\tzrwa flush zone[%d:%d] ret: %d, dev: %d, pba(sector): %lld, pba: %lldKB, wp: %lldKB\n",
                            current->pid, pba_to_pzone(ctx, dev_pba), j, ret, 
                            dev_idx, cmd_addr-pzone->start, (cmd_addr-pzone->start)/2, (pzone->pzone_wp-pzone->start)/2);
                    }
                    else {
                        printk("(%d)[Success]\tzrwa flush zone[%d:%d] dev: %d, pba(sector): %lld, pba: %lldKB, wp: %lldKB\n",
                            current->pid, pba_to_pzone(ctx, dev_pba), j,
                            dev_idx, cmd_addr-pzone->start, (cmd_addr-pzone->start)/2, (pzone->pzone_wp-pzone->start)/2);
                    }
#endif
                }
            }
        }
#else // SAMSUNG_MODE
	    zrwa_flush_zone(nvme_cmd, sector_to_block_addr(dev_pba), NS_NUM, 0); // 3rd parameter is nsid of device e.g.) nvme0n2 --> 2
        ret = nvme_submit_passthru_cmd_sync(nvme_bdev, nvme_cmd);
#ifdef RECORD_ZFLUSH
        u64 elapsed_time = ktime_get_ns() - before;
        atomic64_add(elapsed_time, &ctx->subio_counters.zf_cmd_t_tot);
        atomic64_inc(&(ctx->subio_counters.zf_cmd_count));
#endif
#ifdef DEBUG
        if (ret != 0) {
            printk("(%d)[Fail]\tzrwa flush zone[%d] ret: %d, dev: %d, pba(sector): %lld, pba: %lldKB, wp: %lldKB\n",
                current->pid, pba_to_pzone(ctx, dev_pba), ret, 
                dev_idx, dev_pba-pzone->start, (dev_pba-pzone->start)/2, (pzone->pzone_wp-pzone->start)/2);
        }
        else {
            printk("(%d)[Success]\tzrwa flush zone[%d] dev: %d, pba(sector): %lld, pba: %lldKB, wp: %lldKB\n",
                current->pid, pba_to_pzone(ctx, dev_pba),
                dev_idx, dev_pba-pzone->start, (dev_pba-pzone->start)/2, (pzone->pzone_wp-pzone->start)/2);
        }
#endif
#endif // SAMSUNG_MODE
wp_update:
#ifdef ATOMIC_WP
        // spin_lock_irqsave(&pzone->pzone_wp_lock, flags);
        spin_lock(&pzone->pzone_wp_lock);
#endif
        sector_t p_wp = READ_ONCE(pzone->pzone_wp);
        if (p_wp < min(dev_pba + 1, pzone->start + pzone->capacity)) {
            WRITE_ONCE(pzone->pzone_wp, min(dev_pba + 1, pzone->start + pzone->capacity));
#ifdef DEBUG
// #if 1
            printk("__raizn_do_zrwa_flush changed change[%d] prev: %lld, new: %lld",
                dev_idx,
                p_wp, min(dev_pba + 1, pzone->start + pzone->capacity));
#endif
        }
#ifdef DEBUG
// #if 1
        else
            printk("__raizn_do_zrwa_flush no change[%d] prev: %lld, cmd pba: %lld",
                dev_idx,
                p_wp, min(dev_pba + 1, pzone->start + pzone->capacity));
#endif
#ifdef ATOMIC_WP
        // spin_unlock_irqrestore(&pzone->pzone_wp_lock, flags);
        spin_unlock(&pzone->pzone_wp_lock);
#endif
        // printk("dev_idx: %d, WP is now %lld", dev_idx, atomic64_read(&pzone->wp));
        if (need_free)
            kfree(nvme_cmd);
    }
}

void raizn_do_zrwa_flush(struct raizn_stripe_head *sh, sector_t start_lba, sector_t end_lba)
{
    struct raizn_ctx *ctx = sh->ctx;
#ifdef DEBUG
    printk("##raizn_do_zrwa_flush## start_lba: %lld, end_lba: %lld", start_lba, end_lba);
#endif
    raizn_wait_prog(ctx, start_lba, end_lba);

    sector_t flush_lba_1, flush_lba_2;
    int zone_idx = lba_to_lzone(ctx, start_lba);
    struct raizn_zone *lzone = &ctx->zone_mgr.lzones[zone_idx];
    struct raizn_zone *pzone = &ctx->devs[0].zones[zone_idx];
    sector_t pzone_start = pzone->start;
    int end_su_num, i, prev_dev_idx = -1, curr_dev_idx = -1;
    sector_t endsu_last_sector;
    int stripe_num, start_stripe_num, end_stripe_num;
    bool end_dev_is_first;

    endsu_last_sector = ((end_lba >> ctx->params->su_shift) << ctx->params->su_shift) - 1;
    end_stripe_num = lba_to_stripe(ctx, endsu_last_sector);
    end_dev_is_first = (get_dev_sequence(ctx, endsu_last_sector) == 1);
    // printk("[%s] start_lba: %lld, end_lba: %lld, endsu_last_sector: %lld, end_stripe_num: %lld", __func__, start_lba, end_lba,endsu_last_sector, end_stripe_num);

    // WP advancement should go to previous stripe's last sequence
    if (end_dev_is_first) {
        sector_t prev_str_first_sector = lba_to_stripe_addr(ctx, end_stripe_num - 1);
        curr_dev_idx = (lba_to_dev_idx(ctx, endsu_last_sector));
        prev_dev_idx = lba_to_parity_dev_idx(ctx, prev_str_first_sector) - 1;
        if (prev_dev_idx < 0) {
            prev_dev_idx += ctx->params->array_width;
        }
    }
    else {
        curr_dev_idx = (lba_to_dev_idx(ctx, endsu_last_sector));
        prev_dev_idx = (curr_dev_idx - 1);
        if (prev_dev_idx < 0) {
            prev_dev_idx += ctx->params->array_width;
        }
    }
    // printk("end_dev_is_first: %d, prev_dev: %d, curr_dev: %d", end_dev_is_first, prev_dev_idx, curr_dev_idx);
    // advance WP to 2nd step (prev device). Only exception: the first chunk
    // TODO: first chunk of the zone (corner case) valid log
    if (likely((endsu_last_sector%ctx->params->lzone_size_sectors)> ctx->params->su_sectors)) { // except first chunk of the zone (corner case)
        if (end_dev_is_first)
            flush_lba_1 = pzone_start + (end_stripe_num << ctx->params->su_shift) - 1;
        else
            flush_lba_1 = pzone_start + (end_stripe_num + 1 << ctx->params->su_shift) - 1;
        // printk("flush_lba_1: %lld, pzone_start: %lld, end_stripe_num: %d, dev_seq: %d", 
        //     flush_lba_1, pzone_start, end_stripe_num, get_dev_sequence(ctx,endsu_last_sector));
        __raizn_do_zrwa_flush(ctx, prev_dev_idx, flush_lba_1, false);
    }
    // advance WP to 1st step (curr device)
    flush_lba_2 = pzone_start + (end_stripe_num << ctx->params->su_shift)
                    + (ctx->params->su_sectors >> 1) - 1; // half of the chunk
        // printk("flush_lba_2: %lld, pzone_start: %lld, end_stripe_num: %d", 
        //     flush_lba_2, pzone_start, end_stripe_num);
    __raizn_do_zrwa_flush(ctx, curr_dev_idx, flush_lba_2, false);

    // printk("[%s] start_lba: %lld, end_lba: %lld, same stripe: %d", __func__, start_lba, end_lba,
    //     lba_to_stripe(ctx, start_lba) == lba_to_stripe(ctx, end_lba));
    // if stripe(s) is complete, the bitmap for this stripe should be initialized & reused
    if (lba_to_stripe(ctx, start_lba) != lba_to_stripe(ctx, end_lba)) {
        start_stripe_num = lba_to_stripe(ctx, start_lba);  
        end_stripe_num = (lba_to_stripe_offset(ctx, endsu_last_sector + 1) == 0) ? // is the stripe end chunk?
            end_stripe_num :
            end_stripe_num - 1;
        // printk("(%d)[%s] start_lba: %lld, end_lba: %lld, start_str: %d, end_str: %d, same stripe: %d", 
        //     current->pid, __func__, start_lba, end_lba,
        //     lba_to_stripe(ctx, start_lba),
        //     lba_to_stripe(ctx, end_lba),
        //     lba_to_stripe(ctx, start_lba) == lba_to_stripe(ctx, end_lba));

        // printk("prev_dev: %d, curr_dev: %d", prev_dev_idx, curr_dev_idx);
        flush_lba_1 = pzone_start + (end_stripe_num + 1 << ctx->params->su_shift) - 1;
		for (i=0; i<ctx->params->array_width; i++) { // stripe is completed, flush all devices (include parity) in the stripe
            if ((i==prev_dev_idx) || (i==curr_dev_idx))
                continue; // already queued
			// TODO: Warning! care about the ordering. This flush must be sent after the flush command is returned
            // 종속성이 있는 디바이스를 나타내는 멤버를 sh에 추가? 그리고 zrwa flush에 대한 sequence number 관리?
            // printk("pzone->start: %lld, end_stripe_start_sector: %lld, flush_lba_1: %lld, lba_to_su_offset(ctx, flush_lba_1): %lld", 
            //     pzone->start, end_stripe_start_sector, flush_lba_1, lba_to_su_offset(ctx, flush_lba_1));
            // printk("end_lba: %lld, lba_to_stripe(ctx, end_lba): %lld, end_stripe_num: %d, idx: %d, wp: %lld, flush_lba: %lld", 
            //     end_lba, lba_to_stripe(ctx, end_lba), end_stripe_num, i, atomic64_read(&pzone->wp), l2p_addr);
            pzone = &ctx->devs[i].zones[zone_idx];
            if (pzone->pzone_wp < flush_lba_1) // else already flushed before
            {
                // printk("flush_lba_3: %lld, pzone_start: %lld, end_stripe_num: %d", 
                //     flush_lba_1, pzone_start, end_stripe_num);
                // printk("__raizn_do_zrwa_flush sent. idx: %d, wp: %lld, flush_lba: %lld",
                //     i, atomic64_read(&pzone->wp), flush_lba_1);
                __raizn_do_zrwa_flush(ctx, i, flush_lba_1, false);
            }
            else {
                // printk("__raizn_do_zrwa_flush skipped. idx: %d, wp: %lld, flush_lba: %lld",
                //     i, atomic64_read(&pzone->wp), flush_lba_1);
            }
		}
    }

#ifdef RECORD_ZFLUSH
    if (sh->zf_submitted) {
        u64 elapsed_time = ktime_get_ns() - sh->zf_submit_time;
        atomic64_add(elapsed_time, &ctx->subio_counters.zf_wq_t_tot);
        atomic64_inc(&(ctx->subio_counters.zf_wq_count));
    }
#endif
}

void raizn_pp_manage(struct raizn_ctx *ctx, sector_t start_lba, sector_t end_lba)
{
    unsigned long flags;
    struct raizn_zone *lzone = &ctx->zone_mgr.lzones[lba_to_lzone(ctx, start_lba)];
#ifdef DEBUG
// #if 1
    printk("##raizn_pp_manage##[%d:%d] lba: %lldKB ~ %lldKB, offset: %lldKB, len: %lldKB, last: %d\n",
        lba_to_lzone(ctx, start_lba), lba_to_stripe(ctx, start_lba),
        start_lba/2, end_lba/2, lba_to_stripe_offset(ctx, start_lba)/2, (end_lba-start_lba)/2, lzone->last_complete_stripe);
#endif
    if (lba_to_su(ctx, end_lba) != lba_to_su(ctx, start_lba)) {
        raizn_request_zrwa_flush(ctx, start_lba, end_lba);
    }
    else {
        // printk("!!!!!!!!!!!!!!!!!!!!!no raizn_request_zrwa_flush");
    }
    spin_lock_irqsave(&lzone->prog_bitmap_lock, flags);
    raizn_update_prog_bitmap(ctx, start_lba, end_lba);
    raizn_update_complete_stripe(ctx, start_lba, end_lba);
    spin_unlock_irqrestore(&lzone->prog_bitmap_lock, flags);
    //     raizn_write_wp_log();
}


