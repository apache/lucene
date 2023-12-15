#include <score_lower_bound.h>
#include <mutex_pool.h>
#include <defs.h>
#include <mram_unaligned.h>
#include "decoder.h"

// store all scores found so far
__mram_noinit score_t best_scores[DPU_MAX_BATCH_SIZE][MAX_NB_SCORES];
// number of scores stored
__host uint8_t nb_best_scores[DPU_MAX_BATCH_SIZE];

// norms cache set by the host
// those values are floating point numbers on the host, quantized for the DPU
// TODO store in MRAM ? use uint8_t ?
__host uint16_t quantized_norm_inv_cache[256] = {0};
// lower bound on score for each query, to be set by the host
__host uint32_t score_lower_bound[DPU_MAX_BATCH_SIZE] = {0};

uint8_t nb_docs_log2[DPU_MAX_BATCH_SIZE];
uintptr_t doc_norms_addr[DPU_MAX_BATCH_SIZE];

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

static uint8_t get_doc_norm(uint32_t query_id, uint32_t doc_id) {

    uint32_t hash = wang_hash_func(doc_id);
    uint32_t nb_docs = 1 << nb_docs_log2[query_id];
    uint32_t ind = hash & (nb_docs - 1);
    //printf("doc_id=%u nb_docs=%u ind=%u\n", doc_id, nb_docs, ind);
    uint64_t norm_buff;
    uint8_t *norm_vec = mram_read_unaligned(
                            (__mram_ptr uint8_t*)doc_norms_addr[query_id] + ind, &norm_buff, 1);
    uint8_t norm = *norm_vec;
    //printf("norm=%u\n", norm);

    // if the norm is not zero, this is a valid value
    if(norm) return norm;

    // the norm is zero, meaning there is a collision
    // start linear scan from the end of the hash table
    // the collision rate should be low
    uintptr_t start_addr = doc_norms_addr[query_id] + nb_docs;
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

void reset_scores(uint32_t nb_queries) {

    for(int i = 0; i < nb_queries; ++i)
        nb_best_scores[i] = 0;

    // TODO hack for test
    for(int i = 0; i < 256; ++i)
        quantized_norm_inv_cache[i] = 1;
}

void set_query_doc_norms_addr(uint32_t query_id, uintptr_t addr) {

    // read number of docs log2 and store it
    uint64_t buff;
    //TODO alignement may not be right
    uint8_t *val = mram_read_unaligned((__mram_ptr uint64_t*)addr, &buff, 1);
    //printf("nb_docs_log2=%u\n", *val);
    nb_docs_log2[query_id] = *val;
    // store the address where to read the norms
    doc_norms_addr[query_id] = addr + 1;
}

static uint32_t get_score_quant(uint8_t norm, uint32_t freq) {

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
    //printf("min_freq=%d lower_bound=%u score=%u\n", min_freq, score_lower_bound[query_id], get_score_quant(get_doc_norm(query_id, did), min_freq));
    return get_score_quant(get_doc_norm(query_id, did), min_freq) > score_lower_bound[query_id];
}

__dma_aligned score_t score_buffer[NR_TASKLETS];

void add_match_for_best_scores(uint8_t query_id, uint32_t doc_id, uint32_t freq) {

    uint8_t norm = get_doc_norm(query_id, doc_id);
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
}