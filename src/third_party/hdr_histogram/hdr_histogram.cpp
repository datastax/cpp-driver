/**
 * hdr_histogram.c
 * Written by Michael Barker and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

#define __STDC_CONSTANT_MACROS
#define __STDC_LIMIT_MACROS

#include <stdlib.h>
#include <math.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>

#if defined(_MSC_VER)
#include <intrin.h>
#else
#include <inttypes.h>
#endif

#include "hdr_histogram.hpp"

inline int32_t hdr_clz64(uint64_t x) {
#if defined(_MSC_VER)
#  if defined(_M_AMD64)
  return (int32_t)__lzcnt64(x);
#  else
  // On 32-bit this needs to be split into two operations
  int32_t lz = (int32_t)__lzcnt((unsigned long)(x >> 32));
  if (lz == 32) {
    // Scan the last 32 bits by truncating the 64-bit value
    return (int32_t)__lzcnt((unsigned long)x) + 32;
  }
  return lz;
#  endif
#else
  return (int32_t)__builtin_clzll(x);
#endif
}

//  ######   #######  ##     ## ##    ## ########  ######
// ##    ## ##     ## ##     ## ###   ##    ##    ##    ##
// ##       ##     ## ##     ## ####  ##    ##    ##
// ##       ##     ## ##     ## ## ## ##    ##     ######
// ##       ##     ## ##     ## ##  ####    ##          ##
// ##    ## ##     ## ##     ## ##   ###    ##    ##    ##
//  ######   #######   #######  ##    ##    ##     ######

static int32_t normalize_index(struct hdr_histogram* h, int32_t index)
{
    if (h->normalizing_index_offset == 0)
    {
        return index;
    }

    int32_t normalized_index = index - h->normalizing_index_offset;
    int32_t adjustment = 0;

    if (normalized_index < 0)
    {
        adjustment = h->counts_len;
    }
    else if (normalized_index >= h->counts_len)
    {
        adjustment = -h->counts_len;
    }

    return normalized_index + adjustment;
}

static int64_t counts_get_direct(struct hdr_histogram* h, int32_t index)
{
    return h->counts[index];
}

static int32_t counts_get_normalised(struct hdr_histogram* h, int32_t index)
{
    return counts_get_direct(h, normalize_index(h, index));
}

static void counts_inc_normalised(
    struct hdr_histogram* h, int32_t index, int64_t value)
{
    int32_t normalised_index = normalize_index(h, index);
    h->counts[normalised_index] += value;
    h->total_count += value;
}

static void counts_set_direct(struct hdr_histogram* h, int32_t index, int64_t value)
{
    h->counts[index] = value;
}

static void counts_set_normalised(struct hdr_histogram* h, int32_t index, int64_t value)
{
    int32_t normalised_index = normalize_index(h, index);
    counts_set_direct(h, normalised_index, value);
}

static void counts_set_min_max(struct hdr_histogram* h, int64_t min, int64_t max)
{
    h->min_value = min;
    h->max_value = max;
}

static void update_min_max(struct hdr_histogram* h, int64_t value)
{
    h->min_value = (value < h->min_value && value != 0) ? value : h->min_value;
    h->max_value = (value > h->max_value) ? value : h->max_value;
}

// ##     ## ######## #### ##       #### ######## ##    ##
// ##     ##    ##     ##  ##        ##     ##     ##  ##
// ##     ##    ##     ##  ##        ##     ##      ####
// ##     ##    ##     ##  ##        ##     ##       ##
// ##     ##    ##     ##  ##        ##     ##       ##
// ##     ##    ##     ##  ##        ##     ##       ##
//  #######     ##    #### ######## ####    ##       ##

static int64_t power(int64_t base, int64_t exp)
{
    int result = 1;
    while(exp)
    {
        result *= base; exp--;
    }
    return result;
}

static int32_t get_bucket_index(struct hdr_histogram* h, int64_t value)
{
    int32_t pow2ceiling = 64 - hdr_clz64(value | h->sub_bucket_mask); // smallest power of 2 containing value
    return pow2ceiling - h->unit_magnitude - (h->sub_bucket_half_count_magnitude + 1);
}

static int32_t get_sub_bucket_index(int64_t value, int32_t bucket_index, int32_t unit_magnitude)
{
    return (int32_t)(value >> (bucket_index + unit_magnitude));
}

static int32_t counts_index(struct hdr_histogram* h, int32_t bucket_index, int32_t sub_bucket_index)
{
    // Calculate the index for the first entry in the bucket:
    // (The following is the equivalent of ((bucket_index + 1) * subBucketHalfCount) ):
    int32_t bucket_base_index = (bucket_index + 1) << h->sub_bucket_half_count_magnitude;
    // Calculate the offset in the bucket:
    int32_t offset_in_bucket = sub_bucket_index - h->sub_bucket_half_count;
    // The following is the equivalent of ((sub_bucket_index  - subBucketHalfCount) + bucketBaseIndex;
    return bucket_base_index + offset_in_bucket;
}

static int32_t counts_index_for(struct hdr_histogram* h, int64_t value)
{
    int32_t bucket_index     = get_bucket_index(h, value);
    int32_t sub_bucket_index = get_sub_bucket_index(value, bucket_index, h->unit_magnitude);

    return counts_index(h, bucket_index, sub_bucket_index);
}

static int64_t value_from_index(int32_t bucket_index, int32_t sub_bucket_index, int32_t unit_magnitude)
{
    return ((int64_t) sub_bucket_index) << (bucket_index + unit_magnitude);
}

int64_t hdr_value_at_index(struct hdr_histogram *h, int32_t index)
{
    int32_t bucket_index = (index >> h->sub_bucket_half_count_magnitude) - 1;
    int32_t sub_bucket_index = (index & (h->sub_bucket_half_count - 1)) + h->sub_bucket_half_count;

    if (bucket_index < 0)
    {
        sub_bucket_index -= h->sub_bucket_half_count;
        bucket_index = 0;
    }

    return value_from_index(bucket_index, sub_bucket_index, h->unit_magnitude);
}

static int64_t get_count_at_index(
        struct hdr_histogram* h,
        int32_t bucket_index,
        int32_t sub_bucket_index)
{
    return counts_get_normalised(h, counts_index(h, bucket_index, sub_bucket_index));
}

int64_t hdr_size_of_equivalent_value_range(struct hdr_histogram* h, int64_t value)
{
    int32_t bucket_index     = get_bucket_index(h, value);
    int32_t sub_bucket_index = get_sub_bucket_index(value, bucket_index, h->unit_magnitude);
    int32_t adjusted_bucket  = (sub_bucket_index >= h->sub_bucket_count) ? (bucket_index + 1) : bucket_index;
    return INT64_C(1) << (h->unit_magnitude + adjusted_bucket);
}

static int64_t lowest_equivalent_value(struct hdr_histogram* h, int64_t value)
{
    int32_t bucket_index     = get_bucket_index(h, value);
    int32_t sub_bucket_index = get_sub_bucket_index(value, bucket_index, h->unit_magnitude);
    return value_from_index(bucket_index, sub_bucket_index, h->unit_magnitude);
}

int64_t hdr_next_non_equivalent_value(struct hdr_histogram *h, int64_t value)
{
    return lowest_equivalent_value(h, value) + hdr_size_of_equivalent_value_range(h, value);
}

static int64_t highest_equivalent_value(struct hdr_histogram* h, int64_t value)
{
    return hdr_next_non_equivalent_value(h, value) - 1;
}

int64_t hdr_median_equivalent_value(struct hdr_histogram *h, int64_t value)
{
    return lowest_equivalent_value(h, value) + (hdr_size_of_equivalent_value_range(h, value) >> 1);
}

static int64_t non_zero_min(struct hdr_histogram* h)
{
    if (INT64_MAX == h->min_value)
    {
        return INT64_MAX;
    }

    return lowest_equivalent_value(h, h->min_value);
}

void hdr_reset_internal_counters(struct hdr_histogram* h)
{
    int min_non_zero_index = -1;
    int max_index = -1;
    int64_t observed_total_count = 0;

    for (int i = 0; i < h->counts_len; i++)
    {
        int64_t count_at_index;

        if ((count_at_index = counts_get_direct(h, i)) > 0)
        {
            observed_total_count += count_at_index;
            max_index = i;
            if (min_non_zero_index == -1 && i != 0)
            {
                min_non_zero_index = i;
            }
        }
    }

    if (max_index == -1)
    {
        h->max_value = 0;
    }
    else
    {
        int64_t max_value = hdr_value_at_index(h, max_index);
        h->max_value = highest_equivalent_value(h, max_value);
    }

    if (min_non_zero_index == -1)
    {
        h->min_value = INT64_MAX;
    }
    else
    {
        h->min_value = hdr_value_at_index(h, min_non_zero_index);
    }

    h->total_count = observed_total_count;
}

int32_t buckets_needed_to_cover_value(int64_t value, int32_t sub_bucket_count, int32_t unit_magnitude)
{
    int64_t smallest_untrackable_value = ((int64_t) sub_bucket_count) << unit_magnitude;
    int32_t buckets_needed = 1;
    while (smallest_untrackable_value <= value)
    {
        if (smallest_untrackable_value > INT64_MAX / 2)
        {
            return buckets_needed + 1;
        }
        smallest_untrackable_value <<= 1;
        buckets_needed++;
    }

    return buckets_needed;
}

// ##     ## ######## ##     ##  #######  ########  ##    ##
// ###   ### ##       ###   ### ##     ## ##     ##  ##  ##
// #### #### ##       #### #### ##     ## ##     ##   ####
// ## ### ## ######   ## ### ## ##     ## ########     ##
// ##     ## ##       ##     ## ##     ## ##   ##      ##
// ##     ## ##       ##     ## ##     ## ##    ##     ##
// ##     ## ######## ##     ##  #######  ##     ##    ##

int hdr_calculate_bucket_config(
        int64_t lowest_trackable_value,
        int64_t highest_trackable_value,
        int significant_figures,
        struct hdr_histogram_bucket_config* cfg)
{
    if (lowest_trackable_value < 1 ||
            significant_figures < 1 || 5 < significant_figures)
    {
        return EINVAL;
    }
    else if (lowest_trackable_value * 2 > highest_trackable_value)
    {
        return EINVAL;
    }

    cfg->lowest_trackable_value = lowest_trackable_value;
    cfg->significant_figures = significant_figures;
    cfg->highest_trackable_value = highest_trackable_value;

    int64_t largest_value_with_single_unit_resolution = 2 * power(10, significant_figures);
    int32_t sub_bucket_count_magnitude = (int32_t) ceil(log((double)largest_value_with_single_unit_resolution) / log(2.0));
    cfg->sub_bucket_half_count_magnitude = ((sub_bucket_count_magnitude > 1) ? sub_bucket_count_magnitude : 1) - 1;

    cfg->unit_magnitude = (int32_t) floor(log((double)lowest_trackable_value) / log(2.0));

    cfg->sub_bucket_count      = (int32_t) pow(2.0, (cfg->sub_bucket_half_count_magnitude + 1));
    cfg->sub_bucket_half_count = cfg->sub_bucket_count / 2;
    cfg->sub_bucket_mask       = ((int64_t) cfg->sub_bucket_count - 1) << cfg->unit_magnitude;

    // determine exponent range needed to support the trackable value with no overflow:
    cfg->bucket_count = buckets_needed_to_cover_value(highest_trackable_value, cfg->sub_bucket_count, cfg->unit_magnitude);
    cfg->counts_len = (cfg->bucket_count + 1) * (cfg->sub_bucket_count / 2);

    return 0;
}

void hdr_init_preallocated(struct hdr_histogram* h, struct hdr_histogram_bucket_config* cfg)
{
    h->lowest_trackable_value          = cfg->lowest_trackable_value;
    h->highest_trackable_value         = cfg->highest_trackable_value;
    h->unit_magnitude                  = cfg->unit_magnitude;
    h->significant_figures             = cfg->significant_figures;
    h->sub_bucket_half_count_magnitude = cfg->sub_bucket_half_count_magnitude;
    h->sub_bucket_half_count           = cfg->sub_bucket_half_count;
    h->sub_bucket_mask                 = cfg->sub_bucket_mask;
    h->sub_bucket_count                = cfg->sub_bucket_count;
    h->min_value                       = INT64_MAX;
    h->max_value                       = 0;
    h->normalizing_index_offset        = 0;
    h->conversion_ratio                = 1.0;
    h->bucket_count                    = cfg->bucket_count;
    h->counts_len                      = cfg->counts_len;
    h->total_count                     = 0;
}

int hdr_init(
        int64_t lowest_trackable_value,
        int64_t highest_trackable_value,
        int significant_figures,
        struct hdr_histogram** result)
{
    struct hdr_histogram_bucket_config cfg;

    int r = hdr_calculate_bucket_config(lowest_trackable_value, highest_trackable_value, significant_figures, &cfg);
    if (r)
    {
        return r;
    }

    size_t histogram_size           = sizeof(struct hdr_histogram) + cfg.counts_len * sizeof(int64_t);
    struct hdr_histogram* histogram = (struct hdr_histogram*)malloc(histogram_size);

    if (!histogram)
    {
        return ENOMEM;
    }

    // memset will ensure that all of the function pointers are null.
    memset((void*) histogram, 0, histogram_size);

    hdr_init_preallocated(histogram, &cfg);
    *result = histogram;

    return 0;
}


int hdr_alloc(int64_t highest_trackable_value, int significant_figures, struct hdr_histogram** result)
{
    return hdr_init(1, highest_trackable_value, significant_figures, result);
}

// reset a histogram to zero.
void hdr_reset(struct hdr_histogram *h)
{
     h->total_count=0;
     h->min_value = INT64_MAX;
     h->max_value = 0;
     memset((void *) &h->counts, 0, (sizeof(int64_t) * h->counts_len));
     return;
}

size_t hdr_get_memory_size(struct hdr_histogram *h)
{
    return sizeof(struct hdr_histogram) + h->counts_len * sizeof(int64_t);
}

void shift_lowest_half_bucket_contents_left(struct hdr_histogram* h, int32_t shift_amount)
{
    int32_t binary_orders_of_magnitude = shift_amount >> h->sub_bucket_half_count_magnitude;

    for (int from_index = 1; from_index < h->sub_bucket_half_count; from_index++)
    {
        int64_t to_value = hdr_value_at_index(h, from_index) << binary_orders_of_magnitude;
        int32_t to_index = counts_index_for(h, to_value);
        int64_t count_at_from_index = counts_get_direct(h, from_index);
        counts_set_normalised(h, to_index, count_at_from_index);
        counts_set_direct(h, from_index, 0);
    }
}

// TODO: Concurrency????
static void shift_normalizing_index_by_offset(struct hdr_histogram *h, int32_t shift_amount, bool populated)
{
    int64_t zero_value_count = hdr_count_at_index(h, 0);
    counts_set_normalised(h, 0, 0);

    h->normalizing_index_offset += shift_amount;

    if (populated)
    {
        shift_lowest_half_bucket_contents_left(h, shift_amount);
    }

    counts_set_normalised(h, 0, zero_value_count);
}

bool hdr_shift_values_left(struct hdr_histogram* h, int32_t binary_orders_of_magnitude)
{
    if (binary_orders_of_magnitude < 0)
    {
        return false;
    }
    else if (binary_orders_of_magnitude == 0)
    {
        return true;
    }

    if (h->total_count == hdr_count_at_index(h, 0))
    {
        return true;
    }

    int32_t shift_amount = binary_orders_of_magnitude << h->sub_bucket_half_count_magnitude;
    int32_t max_value_index = counts_index_for(h, hdr_max(h));

    if (max_value_index >= (h->counts_len - shift_amount))
    {
        return false;
    }

    int64_t max_before_shift = h->max_value;
    int64_t min_before_shift = h->min_value;
    counts_set_min_max(h, INT64_MAX, 0);

    bool lowest_half_bucket_populated = (min_before_shift < h->sub_bucket_half_count);

    shift_normalizing_index_by_offset(h, shift_amount, lowest_half_bucket_populated);

    update_min_max(h, max_before_shift << binary_orders_of_magnitude);
    if (min_before_shift < INT64_MAX)
    {
        update_min_max(h, min_before_shift << binary_orders_of_magnitude);
    }

    return true;
}

bool hdr_shift_values_right(struct hdr_histogram* h, int32_t binary_orders_of_magnitude)
{
    if (binary_orders_of_magnitude < 0)
    {
        return false;
    }
    else if (binary_orders_of_magnitude == 0)
    {
        return true;
    }

    if (h->total_count == hdr_count_at_index(h, 0))
    {
        return true;
    }

    int32_t shift_amount = h->sub_bucket_half_count * binary_orders_of_magnitude;
    int32_t min_value_index = counts_index_for(h, non_zero_min(h));

    if (min_value_index < shift_amount + h->sub_bucket_half_count)
    {
        return false;
    }

    int64_t max_value_before_shift = h->max_value;
    int64_t min_value_before_shift = h->min_value;
    counts_set_min_max(h, INT64_MAX, 0);

    shift_normalizing_index_by_offset(h, -shift_amount, false);

    update_min_max(h, max_value_before_shift >> binary_orders_of_magnitude);
    if (min_value_before_shift < INT64_MAX)
    {
        update_min_max(h, min_value_before_shift >> binary_orders_of_magnitude);
    }

    return true;
}

// ##     ## ########  ########     ###    ######## ########  ######
// ##     ## ##     ## ##     ##   ## ##      ##    ##       ##    ##
// ##     ## ##     ## ##     ##  ##   ##     ##    ##       ##
// ##     ## ########  ##     ## ##     ##    ##    ######    ######
// ##     ## ##        ##     ## #########    ##    ##             ##
// ##     ## ##        ##     ## ##     ##    ##    ##       ##    ##
//  #######  ##        ########  ##     ##    ##    ########  ######


bool hdr_record_value(struct hdr_histogram* h, int64_t value)
{
    return hdr_record_values(h, value, 1);
}

bool hdr_record_values(struct hdr_histogram* h, int64_t value, int64_t count)
{
    if (value < 0)
    {
        return false;
    }

    int32_t counts_index = counts_index_for(h, value);

    if (counts_index < 0 || h->counts_len <= counts_index)
    {
        return false;
    }

    counts_inc_normalised(h, counts_index, count);
    update_min_max(h, value);

    return true;
}

bool hdr_record_corrected_value(struct hdr_histogram* h, int64_t value, int64_t expected_interval)
{
    return hdr_record_corrected_values(h, value, 1, expected_interval);
}


bool hdr_record_corrected_values(struct hdr_histogram* h, int64_t value, int64_t count, int64_t expected_interval)
{
    if (!hdr_record_values(h, value, count))
    {
        return false;
    }

    if (expected_interval <= 0 || value <= expected_interval)
    {
        return true;
    }

    int64_t missing_value = value - expected_interval;
    for (; missing_value >= expected_interval; missing_value -= expected_interval)
    {
        if (!hdr_record_values(h, missing_value, count))
        {
            return false;
        }
    }

    return true;
}

int64_t hdr_add(struct hdr_histogram* h, struct hdr_histogram* from)
{
    struct hdr_iter iter;
    hdr_iter_recorded_init(&iter, from);
    int64_t dropped = 0;

    while (hdr_iter_next(&iter))
    {
        int64_t value = iter.value_from_index;
        int64_t count = iter.count_at_index;

        if (!hdr_record_values(h, value, count))
        {
            dropped += count;
        }
    }

    return dropped;
}

int64_t hdr_add_while_correcting_for_coordinated_omission(
        struct hdr_histogram* h, struct hdr_histogram* from, int64_t expected_interval)
{
    struct hdr_iter iter;
    hdr_iter_recorded_init(&iter, from);
    int64_t dropped = 0;

    while (hdr_iter_next(&iter))
    {
        int64_t value = iter.value_from_index;
        int64_t count = iter.count_at_index;

        if (!hdr_record_corrected_values(h, value, count, expected_interval))
        {
            dropped += count;
        }
    }

    return dropped;
}



// ##     ##    ###    ##       ##     ## ########  ######
// ##     ##   ## ##   ##       ##     ## ##       ##    ##
// ##     ##  ##   ##  ##       ##     ## ##       ##
// ##     ## ##     ## ##       ##     ## ######    ######
//  ##   ##  ######### ##       ##     ## ##             ##
//   ## ##   ##     ## ##       ##     ## ##       ##    ##
//    ###    ##     ## ########  #######  ########  ######


int64_t hdr_max(struct hdr_histogram* h)
{
    if (0 == h->max_value)
    {
        return 0;
    }

    return highest_equivalent_value(h, h->max_value);
}

int64_t hdr_min(struct hdr_histogram* h)
{
    if (0 < hdr_count_at_index(h, 0))
    {
        return 0;
    }

    return non_zero_min(h);
}

int64_t hdr_value_at_percentile(struct hdr_histogram* h, double percentile)
{
    struct hdr_iter iter;
    hdr_iter_init(&iter, h);

    double requested_percentile = percentile < 100.0 ? percentile : 100.0;
    int64_t count_at_percentile =
        (int64_t) (((requested_percentile / 100) * h->total_count) + 0.5);
    count_at_percentile = count_at_percentile > 1 ? count_at_percentile : 1;
    int64_t total = 0;

    while (hdr_iter_next(&iter))
    {
        total += iter.count_at_index;

        if (total >= count_at_percentile)
        {
            int64_t value_from_index = iter.value_from_index;
            return highest_equivalent_value(h, value_from_index);
        }
    }

    return 0;
}

double hdr_mean(struct hdr_histogram* h)
{
    struct hdr_iter iter;
    int64_t total = 0;

    hdr_iter_init(&iter, h);

    while (hdr_iter_next(&iter))
    {
        if (0 != iter.count_at_index)
        {
            total += iter.count_at_index * hdr_median_equivalent_value(h, iter.value_from_index);
        }
    }

    return (total * 1.0) / h->total_count;
}

double hdr_stddev(struct hdr_histogram* h)
{
    double mean = hdr_mean(h);
    double geometric_dev_total = 0.0;

    struct hdr_iter iter;
    hdr_iter_init(&iter, h);

    while (hdr_iter_next(&iter))
    {
        if (0 != iter.count_at_index)
        {
            double dev = (hdr_median_equivalent_value(h, iter.value_from_index) * 1.0) - mean;
            geometric_dev_total += (dev * dev) * iter.count_at_index;
        }
    }

    return sqrt(geometric_dev_total / h->total_count);
}

bool hdr_values_are_equivalent(struct hdr_histogram* h, int64_t a, int64_t b)
{
    return lowest_equivalent_value(h, a) == lowest_equivalent_value(h, b);
}

int64_t hdr_lowest_equivalent_value(struct hdr_histogram* h, int64_t value)
{
    return lowest_equivalent_value(h, value);
}

int64_t hdr_count_at_value(struct hdr_histogram* h, int64_t value)
{
    return counts_get_normalised(h, counts_index_for(h, value));
}

int64_t hdr_count_at_index(struct hdr_histogram* h, int32_t index)
{
    return counts_get_normalised(h, index);
}


// #### ######## ######## ########     ###    ########  #######  ########   ######
//  ##     ##    ##       ##     ##   ## ##      ##    ##     ## ##     ## ##    ##
//  ##     ##    ##       ##     ##  ##   ##     ##    ##     ## ##     ## ##
//  ##     ##    ######   ########  ##     ##    ##    ##     ## ########   ######
//  ##     ##    ##       ##   ##   #########    ##    ##     ## ##   ##         ##
//  ##     ##    ##       ##    ##  ##     ##    ##    ##     ## ##    ##  ##    ##
// ####    ##    ######## ##     ## ##     ##    ##     #######  ##     ##  ######


static bool has_buckets(struct hdr_iter* iter)
{
    return iter->bucket_index < iter->h->bucket_count;
}

static bool has_next(struct hdr_iter* iter)
{
    return iter->count_to_index < iter->h->total_count;
}

static void increment_bucket(struct hdr_histogram* h, int32_t* bucket_index, int32_t* sub_bucket_index)
{
    (*sub_bucket_index)++;

    if (*sub_bucket_index >= h->sub_bucket_count)
    {
        *sub_bucket_index = h->sub_bucket_half_count;
        (*bucket_index)++;
    }
}

static bool move_next(struct hdr_iter* iter)
{
    increment_bucket(iter->h, &iter->bucket_index, &iter->sub_bucket_index);

    if (!has_buckets(iter))
    {
        return false;
    }

    iter->count_at_index  = get_count_at_index(iter->h, iter->bucket_index, iter->sub_bucket_index);
    iter->count_to_index += iter->count_at_index;

    iter->value_from_index = value_from_index(iter->bucket_index, iter->sub_bucket_index, iter->h->unit_magnitude);
    iter->highest_equivalent_value = highest_equivalent_value(iter->h, iter->value_from_index);

    return true;
}

static int64_t peek_next_value_from_index(struct hdr_iter* iter)
{
    int32_t bucket_index     = iter->bucket_index;
    int32_t sub_bucket_index = iter->sub_bucket_index;

    increment_bucket(iter->h, &bucket_index, &sub_bucket_index);

    return value_from_index(bucket_index, sub_bucket_index, iter->h->unit_magnitude);
}

bool _basic_iter_next(struct hdr_iter *iter)
{
    if (!has_next(iter))
    {
        return false;
    }

    move_next(iter);

    return true;
}

void hdr_iter_init(struct hdr_iter* itr, struct hdr_histogram* h)
{
    itr->h = h;

    itr->bucket_index       =  0;
    itr->sub_bucket_index   = -1;
    itr->count_at_index     =  0;
    itr->count_to_index     =  0;
    itr->value_from_index   =  0;
    itr->highest_equivalent_value = 0;

    itr->_next_fp = _basic_iter_next;
}

bool hdr_iter_next(struct hdr_iter* iter)
{
    return iter->_next_fp(iter);
}

// ########  ######## ########   ######  ######## ##    ## ######## #### ##       ########  ######
// ##     ## ##       ##     ## ##    ## ##       ###   ##    ##     ##  ##       ##       ##    ##
// ##     ## ##       ##     ## ##       ##       ####  ##    ##     ##  ##       ##       ##
// ########  ######   ########  ##       ######   ## ## ##    ##     ##  ##       ######    ######
// ##        ##       ##   ##   ##       ##       ##  ####    ##     ##  ##       ##             ##
// ##        ##       ##    ##  ##    ## ##       ##   ###    ##     ##  ##       ##       ##    ##
// ##        ######## ##     ##  ######  ######## ##    ##    ##    #### ######## ########  ######

bool _percentile_iter_next(struct hdr_iter* iter)
{
    struct hdr_iter_percentiles* percentiles = &iter->specifics.percentiles;

    if (!has_next(iter))
    {
        if (percentiles->seen_last_value)
        {
            return false;
        }

        percentiles->seen_last_value = true;
        percentiles->percentile = 100.0;

        return true;
    }

    if (iter->sub_bucket_index == -1 && !_basic_iter_next(iter))
    {
        return false;
    }

    do
    {
        double current_percentile = (100.0 * (double) iter->count_to_index) / iter->h->total_count;
        if (iter->count_at_index != 0 &&
                percentiles->percentile_to_iterate_to <= current_percentile)
        {
            percentiles->percentile = percentiles->percentile_to_iterate_to;

            int64_t half_distance = (int64_t) pow(2.0, (double)((int64_t)(log(100.0 / (100.0 - (percentiles->percentile_to_iterate_to))) / log(2.0)) + 1));
            int64_t percentile_reporting_ticks = percentiles->ticks_per_half_distance * half_distance;
            percentiles->percentile_to_iterate_to += 100.0 / percentile_reporting_ticks;

            return true;
        }
    }
    while (_basic_iter_next(iter));

    return true;
}

void hdr_iter_percentile_init(struct hdr_iter* iter, struct hdr_histogram* h, int32_t ticks_per_half_distance)
{
    iter->h = h;

    hdr_iter_init(iter, h);

    iter->specifics.percentiles.seen_last_value          = false;
    iter->specifics.percentiles.ticks_per_half_distance  = ticks_per_half_distance;
    iter->specifics.percentiles.percentile_to_iterate_to = 0.0;
    iter->specifics.percentiles.percentile               = 0.0;

    iter->_next_fp = _percentile_iter_next;
}

// ########  ########  ######   #######  ########  ########  ######## ########
// ##     ## ##       ##    ## ##     ## ##     ## ##     ## ##       ##     ##
// ##     ## ##       ##       ##     ## ##     ## ##     ## ##       ##     ##
// ########  ######   ##       ##     ## ########  ##     ## ######   ##     ##
// ##   ##   ##       ##       ##     ## ##   ##   ##     ## ##       ##     ##
// ##    ##  ##       ##    ## ##     ## ##    ##  ##     ## ##       ##     ##
// ##     ## ########  ######   #######  ##     ## ########  ######## ########


bool _recorded_iter_next(struct hdr_iter* iter)
{
    while (_basic_iter_next(iter))
    {
        if (iter->count_at_index != 0)
        {
            iter->specifics.recorded.count_added_in_this_iteration_step = iter->count_at_index;
            return true;
        }
    }

    return false;
}

void hdr_iter_recorded_init(struct hdr_iter* iter, struct hdr_histogram* h)
{
    hdr_iter_init(iter, h);

    iter->specifics.recorded.count_added_in_this_iteration_step = 0;

    iter->_next_fp = _recorded_iter_next;
}

// ##       #### ##    ## ########    ###    ########
// ##        ##  ###   ## ##         ## ##   ##     ##
// ##        ##  ####  ## ##        ##   ##  ##     ##
// ##        ##  ## ## ## ######   ##     ## ########
// ##        ##  ##  #### ##       ######### ##   ##
// ##        ##  ##   ### ##       ##     ## ##    ##
// ######## #### ##    ## ######## ##     ## ##     ##


bool _iter_linear_next(struct hdr_iter* iter)
{
    struct hdr_iter_linear* linear = &iter->specifics.linear;

    linear->count_added_in_this_iteration_step = 0;

    if (has_next(iter) ||
        peek_next_value_from_index(iter) > linear->next_value_reporting_level_lowest_equivalent)
    {
        do
        {
            if (iter->value_from_index >= linear->next_value_reporting_level_lowest_equivalent)
            {
                linear->next_value_reporting_level += linear->value_units_per_bucket;
                linear->next_value_reporting_level_lowest_equivalent = lowest_equivalent_value(iter->h, linear->next_value_reporting_level);

                return true;
            }

            if (!move_next(iter))
            {
                break;
            }
            linear->count_added_in_this_iteration_step += iter->count_at_index;
        }
        while (true);
    }

    return false;
}


void hdr_iter_linear_init(struct hdr_iter* iter, struct hdr_histogram* h, int64_t value_units_per_bucket)
{
    hdr_iter_init(iter, h);

    iter->specifics.linear.count_added_in_this_iteration_step = 0;
    iter->specifics.linear.value_units_per_bucket = value_units_per_bucket;
    iter->specifics.linear.next_value_reporting_level = value_units_per_bucket;
    iter->specifics.linear.next_value_reporting_level_lowest_equivalent = lowest_equivalent_value(h, value_units_per_bucket);

    iter->_next_fp = _iter_linear_next;
}

// ##        #######   ######      ###    ########  #### ######## ##     ## ##     ## ####  ######
// ##       ##     ## ##    ##    ## ##   ##     ##  ##     ##    ##     ## ###   ###  ##  ##    ##
// ##       ##     ## ##         ##   ##  ##     ##  ##     ##    ##     ## #### ####  ##  ##
// ##       ##     ## ##   #### ##     ## ########   ##     ##    ######### ## ### ##  ##  ##
// ##       ##     ## ##    ##  ######### ##   ##    ##     ##    ##     ## ##     ##  ##  ##
// ##       ##     ## ##    ##  ##     ## ##    ##   ##     ##    ##     ## ##     ##  ##  ##    ##
// ########  #######   ######   ##     ## ##     ## ####    ##    ##     ## ##     ## ####  ######

bool _log_iter_next(struct hdr_iter *iter)
{
    struct hdr_iter_log* logarithmic = &iter->specifics.log;

    logarithmic->count_added_in_this_iteration_step = 0;

    if (has_next(iter) ||
        peek_next_value_from_index(iter) > logarithmic->next_value_reporting_level_lowest_equivalent)
    {
        do
        {
            if (iter->value_from_index >= logarithmic->next_value_reporting_level_lowest_equivalent)
            {
                logarithmic->next_value_reporting_level *= (int64_t)logarithmic->log_base;
                logarithmic->next_value_reporting_level_lowest_equivalent = lowest_equivalent_value(iter->h, logarithmic->next_value_reporting_level);

                return true;
            }

            if (!move_next(iter))
            {
                break;
            }

            logarithmic->count_added_in_this_iteration_step += iter->count_at_index;
        }
        while (true);
    }

    return false;
}

void hdr_iter_log_init(
        struct hdr_iter* iter,
        struct hdr_histogram* h,
        int64_t value_units_first_bucket,
        double log_base)
{
    hdr_iter_init(iter, h);
    iter->specifics.log.count_added_in_this_iteration_step = 0;
    iter->specifics.log.value_units_first_bucket = value_units_first_bucket;
    iter->specifics.log.log_base = log_base;
    iter->specifics.log.next_value_reporting_level = value_units_first_bucket;
    iter->specifics.log.next_value_reporting_level_lowest_equivalent = lowest_equivalent_value(h, value_units_first_bucket);

    iter->_next_fp = _log_iter_next;
}
