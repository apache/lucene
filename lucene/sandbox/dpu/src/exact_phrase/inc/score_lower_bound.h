#ifndef SCORE_LOWER_BOUND_H_
#define SCORE_LOWER_BOUND_H_

#include <stdint.h>
#include <mram.h>
#include "common.h"
#include "matcher.h"

/**
 * a structure to store a current score of DPU result
 * this will be retrieved by the host to compute a lower bound
 * to be sent back to all DPUs.
 */
typedef struct _score {
    // quantized score
    uint32_t score_quant;
    // freq is stored in the 3 LSB and norm in the MSB
    uint32_t freq_and_norm;
} score_t;

void set_query_doc_norms_addr(uint32_t query_id, uintptr_t addr);

void reset_scores(uint32_t);

bool is_score_competitive(uint32_t query_id, uint32_t did, did_matcher_t *matchers, uint32_t nr_terms);

void add_match_for_best_scores(uint8_t query_id, uint32_t doc_id, uint32_t freq);

#endif /* SCORE_LOWER_BOUND_H_ */