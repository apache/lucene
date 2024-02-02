#ifndef SCORE_LOWER_BOUND_H_
#define SCORE_LOWER_BOUND_H_

#include <mram.h>
#include <stdint.h>

#include "common.h"
#include "matcher.h"

#define MAX_NB_SCORES (1 << MAX_NB_SCORES_LOG2)

/**
 * a structure to store a current score of DPU result
 * this will be retrieved by the host to compute a lower bound
 * to be sent back to all DPUs.
 */
typedef struct {
    // quantized score
    uint32_t score_quant;
    // freq is stored in the 3 LSB and norm in the MSB
    uint32_t freq_and_norm;
} score_t;

void
set_query_no_norms(uint32_t query_id);

void
set_query_doc_norms_addr(uint32_t query_id, __mram_ptr NORM_INV_TYPE *addr);

void reset_scores(uint32_t);

void
reset_score_lower_bounds(uint32_t nb_queries);

bool
is_score_competitive(uint32_t query_id, uint32_t did, did_matcher_t *matchers, uint32_t nr_terms);

void
add_match_for_best_scores(uint8_t query_id, uint32_t doc_id, uint32_t freq);

uint8_t
get_doc_norm(uint32_t query_id, uint32_t doc_id);

#endif /* SCORE_LOWER_BOUND_H_ */