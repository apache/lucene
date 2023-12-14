#include <score_lower_bound.h>
#include <mutex_pool.h>
#include <defs.h>
#include "decoder.h"

// store all scores found so far
__mram_noinit score_t best_scores[DPU_MAX_BATCH_SIZE][MAX_NB_SCORES];
// number of scores stored
__host uint8_t nb_best_scores[DPU_MAX_BATCH_SIZE];

// norms cache set by the host
// those values are floating point numbers on the host, quantized for the DPU
// TODO store in MRAM ? use uint8_t ?
__host uint16_t quantized_norm_inv_cache[256];
// lower bound on score for each query, to be set by the host
__host uint32_t score_lower_bound[DPU_MAX_BATCH_SIZE] = {UINT32_MAX};

uint32_t nb_docs;
uintptr_t doc_norms_addr;

MUTEX_POOL_INIT(mut_pool, 8);

//TODO check simpler hash functions
static inline uint32_t wang_hash_func(uint32_t key) {
  key += ~(key << 15);
  key ^= (key >> 10);
  key += (key << 3);
  key ^= (key >> 6);
  key += ~(key << 11);
  key ^= (key >> 16);
  return key;
}

static uint8_t get_doc_norm(uint32_t doc_id) {

    uint32_t hash = wang_hash_func(doc_id);
    uint32_t index = hash & (nb_docs - 1);
    uint64_t norm_vec;
    mram_read((__mram_ptr uint64_t*)doc_norms_addr + (index >> 2), &norm_vec, 8);
    uint16_t norm = (norm_vec >> ((index & 3) << 4)) & 0xFFFF;
    uint8_t collision_hint = norm & 0xFF;
    if(!collision_hint)
        return norm & 0xFF00;

    // when there is a collision, start linear scan from address hint
    // this should be of low probability to reach this code
    uintptr_t start_addr = doc_norms_addr + (nb_docs << 1) + collision_hint;
    // get a decoder from the pool
    decoder_t* decoder = decoder_pool_get_one();
    initialize_decoder(decoder, start_addr);
    uint32_t key = decode_vint_from(decoder);
    uint32_t count = 0;
    while (key != doc_id && count < nb_docs) {
        skip_bytes_decoder(decoder, 1);
        key = decode_vint_from(decoder);
        count++;
    }
    //TODO error handling
    assert(count < nb_docs);

    norm = decode_byte_from(decoder);
    decoder_pool_release_one(decoder);

    return norm;
}

void init_lower_bound_globals(uint32_t ndocs, uintptr_t addr) {
    nb_docs = ndocs;
    doc_norms_addr = addr;
}

void reset_scores(uint32_t nb_queries) {

    for(int i = 0; i < nb_queries; ++i)
        nb_best_scores[i] = 0;
}

uint32_t get_score_quant(uint8_t norm, uint32_t freq) {

    uint16_t norm_inv_quant = quantized_norm_inv_cache[norm];
    // TODO possible overflow ?
    uint32_t score_quant = norm_inv_quant * freq;
    return score_quant;
}

bool is_score_competitive(uint32_t query_id, uint32_t did, did_matcher_t *matchers, uint32_t nr_terms) {

    // first find the minimum frequency of a term
    // this gives an upper bound on the exact phrase's frequency in the document
    uint32_t min_freq = UINT32_MAX;
    for(int i = 0; i < nr_terms; ++i) {
        uint32_t term_freq = matcher_get_curr_freq(matchers, i);
        if(min_freq > term_freq)
            min_freq = term_freq;
    }
    return get_score_quant(get_doc_norm(did), min_freq) > score_lower_bound[query_id];
}

__dma_aligned score_t score_buffer[NR_TASKLETS];

void add_match_for_best_scores(uint8_t query_id, uint32_t doc_id, uint32_t freq) {

    uint8_t norm = get_doc_norm(doc_id);
    uint32_t score_quant = get_score_quant(norm, freq);
    score_t new_score = {score_quant, freq};
    if((freq & ~0xFFFFFF) == 0) {
        new_score.freq_and_norm |= (norm << 24);
    }
    else {
        // error, frequence is too large, should not happen
        // TODO
    }

    uint32_t score_id = MAX_NB_SCORES;
    mutex_pool_lock(&mut_pool, query_id);
    if(nb_best_scores[query_id] < MAX_NB_SCORES) {
        score_id = nb_best_scores[query_id]++;
    }
    else {
        // if the buffer of scores is full, make sure to at least keep the best score
        mram_read(&best_scores[query_id][0], &score_buffer[me()], 8);
        if(score_quant > score_buffer[me()].score_quant) {
            score_buffer[me()] = new_score;
            mram_write(&score_buffer[me()], &best_scores[query_id][0], 8);
        }
    }
    mutex_pool_unlock(&mut_pool, query_id);
    if(score_id < MAX_NB_SCORES)
        mram_write(&new_score, &best_scores[query_id][score_id], 8);

    /*
    mutex_pool_lock(mut_pool, query_id);
    if(!nb_best_scores[query_id]) {
        mram_write(&new_score, &best_scores[query_id][0], 8);
        nb_best_scores[query_id]++;
    }
    else if(nb_best_scores[query_id] == 1) {
        mram_read(&best_scores[query_id][0], score_buffer[me()], 8);
        if(score_quant > score_buffer[me()][0].score_quant) {
            score_buffer[me()][1] = score_buffer[me()][0];
            score_buffer[me()][0] = new_score;
            mram_write(score_buffer[me()], &best_scores[query_id][0], 16);
        }
        else {
            score_buffer[me()][1] = new_score;
            mram_write(&score_buffer[me()][1], &best_scores[query_id][1], 8);
        }
        nb_best_scores[query_id]++;
    }
    else {
       mram_read(&best_scores[query_id][0], score_buffer[me()], 16);
       if(score_quant > score_buffer[me()][0].score_quant) {
            score_buffer[me()][1] = score_buffer[me()][0];
            score_buffer[me()][0] = new_score;
            mram_write(score_buffer[me()], &best_scores[query_id][0], 16);
       }
       else if(score_quant > score_buffer[me()][1].score_quant) {
            score_buffer[me()][1] = new_score;
            mram_write(score_buffer[me()][1], &best_scores[query_id][1], 8);
       }
    }
    mutex_pool_unlock(mut_pool, query_id);
    */

    /*
    if(nb_best_scores[query_id] < 2) {
        if(nb_best_scores[query_id] && score_quant > best_scores[query_id][0].score_quant) {
            // swap
            mram_write(&best_scores[query_id][0], &best_scores[query_id][nb_best_scores[query_id]], 8);
            mram_write(&new_score, &best_scores[query_id][0], 8);
        }
        else {
            mram_write(&new_score, &best_scores[query_id][nb_best_scores[query_id]], 8);
        }
        nb_best_scores[query_id]++;
    }
    else {
        // two many best scores, erase the minimum score
        mram_write(&new_score, &best_scores[query_id][0], 8);
        //TODO the logic to keep the minimum score is broken
        // as here we should put the new minimum in place of the zero index
    }
    mutex_pool_unlock(mut_pool, query_id);
    */
}