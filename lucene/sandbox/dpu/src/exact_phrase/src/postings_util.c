#include <mram.h>
#include <stdint.h>

#include "postings_util.h"

__mram_noinit postings_info_t postings_cache[DPU_MAX_BATCH_SIZE][MAX_NR_SEGMENTS * MAX_NR_TERMS];

void
set_postings_in_cache(uint32_t query_id, uint8_t term_id, uint8_t nr_segments, const postings_info_t *elem)
{

    for (uint8_t i = 0; i < nr_segments; ++i) {
        mram_write(elem + i, postings_cache[query_id] + i * MAX_NR_TERMS + term_id, sizeof(postings_info_t));
    }
}

void
get_postings_from_cache(uint32_t query_id, uint8_t nr_terms, uint8_t segment_id, postings_info_t *elems)
{

    // load the postings for all terms and the current segment
    mram_read(&postings_cache[query_id][segment_id * MAX_NR_TERMS], elems, nr_terms * sizeof(postings_info_t));
}

void
update_postings_in_cache(uint32_t query_id, uint8_t nr_terms, uint8_t segment_id, postings_info_t *elems)
{

    mram_write(elems, &postings_cache[query_id][segment_id * MAX_NR_TERMS], nr_terms * sizeof(postings_info_t));
}
