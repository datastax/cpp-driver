/**
 * hdr_histogram.h
 * Written by Michael Barker and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * The source for the hdr_histogram utilises a few C99 constructs, specifically
 * the use of stdint/stdbool and inline variable declaration.
 */

#ifndef HDR_HISTOGRAM_H
#define HDR_HISTOGRAM_H 1

#include <stdint.h>
#ifndef _MSC_VER
#include <stdbool.h>
#endif
#include <stdio.h>

struct hdr_histogram
{
    int64_t lowest_trackable_value;
    int64_t highest_trackable_value;
    int64_t unit_magnitude;
    int64_t significant_figures;
    int32_t sub_bucket_half_count_magnitude;
    int32_t sub_bucket_half_count;
    int64_t sub_bucket_mask;
    int32_t sub_bucket_count;
    int32_t bucket_count;
    int64_t min_value;
    int64_t max_value;
    int32_t normalizing_index_offset;
    double conversion_ratio;
    int32_t counts_len;
    int64_t total_count;
    int64_t counts[1];
};

/**
 * Allocate the memory and initialise the hdr_histogram.
 *
 * Due to the size of the histogram being the result of some reasonably
 * involved math on the input parameters this function it is tricky to stack allocate.
 * The histogram is allocated in a single contigious block so can be delete via free,
 * without any structure specific destructor.
 *
 * @param lowest_trackable_value The smallest possible value to be put into the
 * histogram.
 * @param highest_trackable_value The largest possible value to be put into the
 * histogram.
 * @param significant_figures The level of precision for this histogram, i.e. the number
 * of figures in a decimal number that will be maintained.  E.g. a value of 3 will mean
 * the results from the histogram will be accurate up to the first three digits.  Must
 * be a value between 1 and 5 (inclusive).
 * @param result Output parameter to capture allocated histogram.
 * @return 0 on success, EINVAL if lowest_trackable_value is < 1 or the
 * significant_figure value is outside of the allowed range, ENOMEM if malloc
 * failed.
 */
int hdr_init(
        int64_t lowest_trackable_value,
        int64_t highest_trackable_value,
        int significant_figures,
        struct hdr_histogram** result);

/**
 * Allocate the memory and initialise the hdr_histogram.  This is the equivalent of calling
 * hdr_init(1, highest_trackable_value, significant_figures, result);
 *
 * @deprecated use hdr_init.
 */
int hdr_alloc(int64_t highest_trackable_value, int significant_figures, struct hdr_histogram** result);


/**
 * Reset a histogram to zero - empty out a histogram and re-initialise it
 *
 * If you want to re-use an existing histogram, but reset everything back to zero, this
 * is the routine to use.
 *
 * @param h The histogram you want to reset to empty.
 *
 */
void hdr_reset(struct hdr_histogram *h);

/**
 * Get the memory size of the hdr_histogram.
 *
 * @param h "This" pointer
 * @return The amount of memory used by the hdr_histogram in bytes
 */
size_t hdr_get_memory_size(struct hdr_histogram *h);

/**
 * Records a value in the histogram, will round this value of to a precision at or better
 * than the significant_figure specified at construction time.
 *
 * @param h "This" pointer
 * @param value Value to add to the histogram
 * @return false if the value is larger than the highest_trackable_value and can't be recorded,
 * true otherwise.
 */
bool hdr_record_value(struct hdr_histogram* h, int64_t value);

/**
 * Records count values in the histogram, will round this value of to a
 * precision at or better than the significant_figure specified at construction
 * time.
 *
 * @param h "This" pointer
 * @param value Value to add to the histogram
 * @return false if the value is larger than the highest_trackable_value and can't be recorded,
 * true otherwise.
 */
bool hdr_record_values(struct hdr_histogram* h, int64_t value, int64_t count);


/**
 * Record a value in the histogram and backfill based on an expected interval.
 *
 * Records a value in the histogram, will round this value of to a precision at or better
 * than the significant_figure specified at contruction time.  This is specifically used
 * for recording latency.  If the value is larger than the expected_interval then the
 * latency recording system has experienced co-ordinated omission.  This method fills in the
 * values that would have occured had the client providing the load not been blocked.

 * @param h "This" pointer
 * @param value Value to add to the histogram
 * @param expected_interval The delay between recording values.
 * @return false if the value is larger than the highest_trackable_value and can't be recorded,
 * true otherwise.
 */
bool hdr_record_corrected_value(struct hdr_histogram* h, int64_t value, int64_t expexcted_interval);
bool hdr_record_corrected_values(struct hdr_histogram* h, int64_t value, int64_t count, int64_t expected_interval);

/**
 * Adds all of the values from 'from' to 'this' histogram.  Will return the
 * number of values that are dropped when copying.  Values will be dropped
 * if they around outside of h.lowest_trackable_value and
 * h.highest_trackable_value.
 *
 * @param h "This" pointer
 * @param from Histogram to copy values from.
 * @return The number of values dropped when copying.
 */
int64_t hdr_add(struct hdr_histogram* h, struct hdr_histogram* from);
int64_t hdr_add_while_correcting_for_coordinated_omission(
        struct hdr_histogram* h, struct hdr_histogram* from, int64_t expected_interval);

int64_t hdr_min(struct hdr_histogram* h);
int64_t hdr_max(struct hdr_histogram* h);
int64_t hdr_value_at_percentile(struct hdr_histogram* h, double percentile);

double hdr_mean(struct hdr_histogram* h);
double hdr_stddev(struct hdr_histogram* h);
bool hdr_values_are_equivalent(struct hdr_histogram* h, int64_t a, int64_t b);
int64_t hdr_lowest_equivalent_value(struct hdr_histogram* h, int64_t value);
int64_t hdr_count_at_value(struct hdr_histogram* h, int64_t value);
int64_t hdr_count_at_index(struct hdr_histogram* h, int32_t index);
int64_t hdr_value_at_index(struct hdr_histogram* h, int32_t index);

struct hdr_iter_percentiles
{
    bool seen_last_value;
    int32_t ticks_per_half_distance;
    double percentile_to_iterate_to;
    double percentile;
};

struct hdr_iter_recorded
{
    int64_t count_added_in_this_iteration_step;
};

struct hdr_iter_linear
{
    int64_t value_units_per_bucket;
    int64_t count_added_in_this_iteration_step;
    int64_t next_value_reporting_level;
    int64_t next_value_reporting_level_lowest_equivalent;
};

struct hdr_iter_log
{
    int64_t value_units_first_bucket;
    double log_base;
    int64_t count_added_in_this_iteration_step;
    int64_t next_value_reporting_level;
    int64_t next_value_reporting_level_lowest_equivalent;
};

/**
 * The basic iterator.  This is the equivlent of the
 * AllValues iterator from the Java implementation.  It iterates
 * through all entries in the histogram whether or not a value
 * is recorded.
 */
struct hdr_iter
{
    struct hdr_histogram* h;
    int32_t bucket_index;
    int32_t sub_bucket_index;
    int64_t count_at_index;
    int64_t count_to_index;
    int64_t value_from_index;
    int64_t highest_equivalent_value;

    union
    {
        struct hdr_iter_percentiles percentiles;
        struct hdr_iter_recorded recorded;
        struct hdr_iter_linear linear;
        struct hdr_iter_log log;
    } specifics;

    bool (*_next_fp)(struct hdr_iter* iter);
};

/**
 * Initalises the basic iterator.
 *
 * @param itr 'This' pointer
 * @param h The histogram to iterate over
 */
void hdr_iter_init(struct hdr_iter* iter, struct hdr_histogram* h);

/**
 * Initialise the iterator for use with percentiles.
 */
void hdr_iter_percentile_init(struct hdr_iter* iter, struct hdr_histogram* h, int32_t ticks_per_half_distance);

/**
 * Initialise the iterator for use with recorded values.
 */
void hdr_iter_recorded_init(struct hdr_iter* iter, struct hdr_histogram* h);

/**
 * Initialise the iterator for use with linear values.
 */
void hdr_iter_linear_init(
        struct hdr_iter* iter,
        struct hdr_histogram* h,
        int64_t value_units_per_bucket);

/**
 * Initialise the iterator for use with logarithmic values
 */
void hdr_iter_log_init(
        struct hdr_iter* iter,
        struct hdr_histogram* h,
        int64_t value_units_first_bucket,
        double log_base);

/**
 * Iterate to the next value for the iterator.  If there are no more values
 * available return faluse.
 *
 * @param itr 'This' pointer
 * @return 'false' if there are no values remaining for this iterator.
 */
bool hdr_iter_next(struct hdr_iter* iter);

/**
* Internal allocation methods, used by hdr_dbl_histogram.
*/
struct hdr_histogram_bucket_config
{
    int64_t lowest_trackable_value;
    int64_t highest_trackable_value;
    int64_t unit_magnitude;
    int64_t significant_figures;
    int32_t sub_bucket_half_count_magnitude;
    int32_t sub_bucket_half_count;
    int64_t sub_bucket_mask;
    int32_t sub_bucket_count;
    int32_t bucket_count;
    int32_t counts_len;
};

int hdr_calculate_bucket_config(
        int64_t lowest_trackable_value,
        int64_t highest_trackable_value,
        int significant_figures,
        struct hdr_histogram_bucket_config* cfg);

void hdr_init_preallocated(struct hdr_histogram* h, struct hdr_histogram_bucket_config* cfg);

bool hdr_shift_values_left(struct hdr_histogram* h, int32_t binary_orders_of_magnitude);
bool hdr_shift_values_right(struct hdr_histogram* h, int32_t shift);
int64_t hdr_size_of_equivalent_value_range(struct hdr_histogram *h, int64_t value);
int64_t hdr_next_non_equivalent_value(struct hdr_histogram *h, int64_t value);
int64_t hdr_median_equivalent_value(struct hdr_histogram *h, int64_t value);

#endif
