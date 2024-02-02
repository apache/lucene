#ifndef POSTINGS_CACHE_H_
#define POSTINGS_CACHE_H_

#include <stdint.h>

#include "common.h"

/**
 * structure to hold information on postings : address and byte size
 */
typedef struct {
    mram_ptr_t addr;
    uint32_t size;
} postings_info_t;

/**
 * store postings information in the cache for the given query, term and DPU segment
 */
void
set_postings_in_cache(uint32_t query_id, uint8_t term_id, uint8_t nr_segments, const postings_info_t *elems);

/**
 * get postings information from the cache for the given query, term and DPU segment
 */
void
get_postings_from_cache(uint32_t query_id, uint8_t nr_terms, uint8_t segment_id, postings_info_t *elems);

/**
 * update postings information in the cache for the given query, term and DPU segment
 */
void
update_postings_in_cache(uint32_t query_id, uint8_t nr_terms, uint8_t segment_id, postings_info_t *elems);

#endif
