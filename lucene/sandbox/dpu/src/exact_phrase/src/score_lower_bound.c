#include <score_lower_bound.h>
#include <mutex_pool.h>
#include <defs.h>
#include <mram_unaligned.h>
#include "decoder.h"

// store all scores found so far
__mram_noinit score_t best_scores[DPU_MAX_BATCH_SIZE][MAX_NB_SCORES];
// number of scores stored
__host uint8_t nb_best_scores[DPU_MAX_BATCH_SIZE] = { 0 };

// lower bound on score for each query, to be set by the host
__host uint32_t score_lower_bound[DPU_MAX_BATCH_SIZE] = { 0 };

uintptr_t doc_norms_addr[DPU_MAX_BATCH_SIZE];

#define DEFAULT_NORM_VALUE 1
#define MIN_FREQ_COMPETITIVE 10000

MUTEX_POOL_INIT(mut_pool, 8);

uint8_t
get_doc_norm(uint32_t query_id, uint32_t doc_id)
{

    if (doc_norms_addr[query_id] == UINTPTR_MAX) {
        // no norms for this field, default value
        return DEFAULT_NORM_VALUE;
    }

    // printf("doc_id=%u nb_docs=%u ind=%u\n", doc_id, nb_docs, ind);
    uint64_t norm_buff;
    uint8_t *norm_vec = mram_read_unaligned((__mram_ptr uint8_t *)doc_norms_addr[query_id] + doc_id, &norm_buff, 1);
    uint8_t norm = *norm_vec;
    // printf("norm=%u\n", norm);
    return norm;
}

static NORM_INV_TYPE
get_norm_inverse(uint32_t query_id, uint8_t norm)
{

    uint64_t norm_buff;
    NORM_INV_TYPE *norm_inverse
        = mram_read_unaligned((__mram_ptr uint8_t *)doc_norms_addr[query_id] - ((256 - norm) * sizeof(NORM_INV_TYPE)),
            &norm_buff,
            sizeof(NORM_INV_TYPE));
    return *norm_inverse;
}

void
reset_scores(uint32_t nb_queries)
{

    for (int i = 0; i < nb_queries; ++i)
        nb_best_scores[i] = 0;
}

void
reset_score_lower_bounds(uint32_t nb_queries)
{

    for (int i = 0; i < nb_queries; ++i)
        score_lower_bound[i] = 0;
}

void
set_query_no_norms(uint32_t query_id)
{

    doc_norms_addr[query_id] = UINTPTR_MAX;
}

void
set_query_doc_norms_addr(uint32_t query_id, uintptr_t addr)
{

    // store the address where to read the norms
    doc_norms_addr[query_id] = addr + 256 * sizeof(NORM_INV_TYPE) /*norm inverse cache*/;
}

static uint32_t
get_score_quant(uint8_t query_id, uint8_t norm, uint32_t freq)
{

    if (doc_norms_addr[query_id] == UINTPTR_MAX) {
        // no norms for the field of this query
        // The default norm will apply, this is the same value for every doc.
        // Hence the norm inverse value has no effect in the DPU scoring
        return freq;
    }

    uint16_t norm_inv_quant = get_norm_inverse(query_id, norm);
    uint32_t score_quant = norm_inv_quant * freq;
    return score_quant;
}

bool
is_score_competitive(uint32_t query_id, uint32_t did, did_matcher_t *matchers, uint32_t nr_terms)
{

    // first find the minimum frequency of a term
    // this gives an upper bound on the exact phrase's frequency in the document
    uint32_t min_freq = UINT32_MAX;
    // TODO #pragma unroll ?
    for (int i = 0; i < nr_terms; ++i) {
        uint32_t term_freq = matcher_get_curr_freq(matchers, i);
        if (min_freq > term_freq)
            min_freq = term_freq;
    }
    // avoid possible overflow, if the frequence is too large, assume the score to be competitive
    if (min_freq > MIN_FREQ_COMPETITIVE)
        return true;
    return get_score_quant(query_id, get_doc_norm(query_id, did), min_freq) >= score_lower_bound[query_id];
}

__dma_aligned score_t score_buffer[NR_TASKLETS];

void
add_match_for_best_scores(uint8_t query_id, uint32_t doc_id, uint32_t freq)
{

    uint8_t norm = get_doc_norm(query_id, doc_id);
    uint32_t score_quant = get_score_quant(query_id, norm, freq);
    score_t new_score = { score_quant, freq };
    if ((freq & ~0xFFFFFF) == 0) {
        new_score.freq_and_norm |= (norm << 24);
    } else {
        // error, frequence is too large, should not happen
        // TODO
    }

    uint32_t score_id = MAX_NB_SCORES;
    mutex_pool_lock(&mut_pool, query_id);
    if (nb_best_scores[query_id] < MAX_NB_SCORES) {
        score_id = nb_best_scores[query_id]++;
    } else {
        // if the buffer of scores is full, make sure to at least keep the best score
        // remove a next element in round-robin only if the new score is better
        uint8_t id = nb_best_scores[query_id] & (MAX_NB_SCORES - 1);
        mram_read(&best_scores[query_id][id], &score_buffer[me()], 8);
        if (score_quant > score_buffer[me()].score_quant) {
            score_buffer[me()] = new_score;
            mram_write(&score_buffer[me()], &best_scores[query_id][id], 8);
        }
    }
    mutex_pool_unlock(&mut_pool, query_id);
    if (score_id < MAX_NB_SCORES)
        mram_write(&new_score, &best_scores[query_id][score_id], 8);
}