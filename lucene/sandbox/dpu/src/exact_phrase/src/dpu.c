#include <alloc.h>                 // for mem_reset
#include <assert.h>                // for assert
#include <attributes.h>            // for __host, __mram_noinit, __dma_aligned
#include <barrier.h>               // for barrier_wait, BARRIER_INIT
#include <defs.h>                  // for me
#include <dpuruntime.h>            // for unreachable
#include <limits.h>                // for UINT16_MAX, UINT8_MAX, INT32_MAX
#include <mram.h>                  // for mram_write, mram_read, DPU_MRAM_HE...
#include <mram_unaligned.h>        // for mram_read_unaligned, mram_write_un...
#include <mutex.h>                 // for mutex_lock, mutex_unlock, MUTEX_INIT
#include <stdbool.h>               // for true
#include <stdint.h>                // for uint32_t, uint16_t, uint8_t, uint64_t
#include <stdlib.h>                // for abort
#include <string.h>                // for memset
#ifdef DBG_print
#include <stdio.h>
#endif
//#define PERF_MESURE
#ifdef PERF_MESURE
#include <perfcounter.h>
#include <stdio.h>
uint64_t total_cycles = 0;
#endif

#include "common.h"                // for DPU_RESULTS_CACHE_SIZE, mram_ptr_t
#include "context_save_restore.h"  // for restore_context, save_context
#include "decoder.h"               // for decode_byte_from, decode_short_from
#include "matcher.h"               // for did_matcher_t, matcher_get_curr_did
#include "postings_util.h"         // for postings_info_t, update_postings_i...
#include "query_parser.h"          // for init_query_parser, read_field, rea...
#include "query_result.h"          // for query_buffer_elem_t, query_buffer_...
#include "score_lower_bound.h"     // for reset_scores, add_match_for_best_s...
#include "term.h"                  // for decoder_t, term_t
#include "term_lookup.h"           // for get_field_addresses, get_term_post...

__host uint32_t index_loaded = 0;

/**
  Input queries
  */
__host uint32_t nb_queries_in_batch;
__host uint32_t nb_bytes_in_batch;
__mram_noinit uint8_t query_batch[DPU_QUERY_BATCH_BYTE_SIZE];
__host uint32_t query_offset_in_batch[DPU_MAX_BATCH_SIZE];
__host uint32_t nb_max_doc_match;
__host uint32_t new_query;

/**
  Output results
  */
__mram_noinit uint8_t results_batch[DPU_RESULTS_MAX_BYTE_SIZE];
__mram_noinit uint8_t results_batch_sorted[DPU_RESULTS_MAX_BYTE_SIZE];
__host uint32_t results_index[DPU_MAX_BATCH_SIZE] = { 0 };
__mram uint32_t results_index_lucene_segments[DPU_MAX_BATCH_SIZE * DPU_MAX_NR_LUCENE_SEGMENTS] = { 0 };
__mram_noinit uint32_t results_segment_offset[DPU_MAX_BATCH_SIZE * MAX_NR_SEGMENTS];
__host uint64_t search_done;

/* Results WRAM caches */
__dma_aligned query_buffer_elem_t results_cache[NR_TASKLETS][DPU_RESULTS_CACHE_SIZE];
#define NB_ELEM_TRANSFER 8
__dma_aligned uint32_t segment_offset_cache[NR_TASKLETS][NB_ELEM_TRANSFER];
MUTEX_INIT(results_mutex);
uint32_t results_buffer_index = 0;

/* Number of terms for each query */
uint8_t queries_nb_terms[DPU_MAX_BATCH_SIZE];

/* WRAM cache for postings info */
#define POSTINGS_CACHE_SIZE (MAX_NR_TERMS > MAX_NR_SEGMENTS ? MAX_NR_TERMS : MAX_NR_SEGMENTS)
//NOLINTNEXTLINE(bugprone-branch-clone)
__dma_aligned postings_info_t postings_cache_wram[NR_TASKLETS][POSTINGS_CACHE_SIZE];

/* number of DPUs in the index and dpu id.
 * This info is used to retrieve the correct doc id from the relative doc id.
 * On the DPU, doc ids are stored as relative doc ids 0,1,2 ... which correspond
 * to other doc ids in Lucene's context. */
uint16_t nr_dpus;
uint16_t dpu_id;

/* Lucene segments maxDoc */
uint16_t nr_lucene_segments;
uint32_t lucene_segment_maxdoc[DPU_MAX_NR_LUCENE_SEGMENTS];
uint16_t current_segment[NR_TASKLETS];

uint8_t nr_segments_log2 = 0;
mram_ptr_t dpu_index = 0;

uint32_t batch_num = 0;
MUTEX_INIT(batch_mutex);
BARRIER_INIT(barrier, NR_TASKLETS);

#ifdef TEST1
#define TEST
#define DEBUG
#include "../test/test1.h"
#endif

//#define DEBUG

#ifndef NDEBUG
uint16_t nb_did_skipped[DPU_MAX_BATCH_SIZE];
#endif

static uint32_t
perform_did_and_pos_matching(uint32_t query_id, uint16_t segment_id, did_matcher_t *matchers, uint32_t nr_terms);
static void
init_results_cache(uint32_t query_id, uint32_t buffer_id, uint8_t segment_id);
static void
lookup_postings_info_for_query(mram_ptr_t index, uint32_t query_id);
static void
prefix_sum_each_query(__mram_ptr uint32_t *array, uint32_t sz);
static void
sort_query_results();
static void
flush_query_buffer();
static void
get_segments_info(mram_ptr_t index);
static uint32_t
get_abs_doc_id(int rel_doc);
static void
adder(int *i, int args)
{
    *i += args;
}
static void
early_exit(uint32_t query_id, uint32_t segment_id, uint32_t nr_terms, did_matcher_t *matchers);
static void
normal_exit(uint32_t query_id, uint32_t segment_id, uint32_t nr_terms);

int
main()
{

#ifndef TEST
    if (!index_loaded) {
#ifdef DBG_PRINT
        printf("No index loaded\n");
#endif
        // this DPU has no index loaded
        // All information read by the host need
        // to be reset (search done, flags, number of results to 0 etc.)
        search_done = 1;
        reset_scores(nb_queries_in_batch);
        for (uint32_t i = me(); i < nb_queries_in_batch; i += NR_TASKLETS) {
            results_index[i] = 0;
        }
        memset(results_index_lucene_segments,
            0,
            nb_queries_in_batch * (nr_lucene_segments + (nr_lucene_segments & 1U)) * sizeof(uint32_t));
        return 0;
    }
#endif
    if (me() == 0) {

        if (new_query) {

#ifdef PERF_MESURE
            perfcounter_config(COUNT_CYCLES, true);
            printf("Number of queries: %d\n", nb_queries_in_batch);
#endif
            mem_reset();
            results_buffer_index = 0;
            initialize_decoder_pool();
#ifdef TEST
            // in test mode set the queries inputs correctly
            nb_queries_in_batch = test_nb_queries_in_batch;
            nb_bytes_in_batch = test_nb_bytes_in_batch;
            mram_write(test_query_batch, query_batch, ((test_nb_bytes_in_batch + 7) >> 3) << 3);
            memcpy(query_offset_in_batch, test_query_offset_in_batch, test_nb_queries_in_batch * sizeof(uint32_t));
            dpu_index = (uintptr_t)(&index_mram[0]);
#else
            dpu_index = DPU_MRAM_HEAP_POINTER;
#endif
            get_segments_info(dpu_index);
            assert(nr_segments_log2 < 8);
            assert(nr_lucene_segments < DPU_MAX_NR_LUCENE_SEGMENTS);
            memset(results_index_lucene_segments,
                0,
                nb_queries_in_batch * (nr_lucene_segments + (nr_lucene_segments & 1U)) * sizeof(uint32_t));
            reset_score_lower_bounds(nb_queries_in_batch);
        }
        batch_num = 0;
        search_done = 1;
        reset_scores(nb_queries_in_batch);
    }
    // TODO(jlegriel): is this barrier really useful ?
    barrier_wait(&barrier);

    // first lookup the postings addresses for each query/term/segment
    // store them in MRAM for later use by the tasklets to find matching document/positions
    if (new_query) {
        for (uint32_t i = me(); i < nb_queries_in_batch; i += NR_TASKLETS) {
            lookup_postings_info_for_query(dpu_index, i);
            results_index[i] = 0;
#ifndef NDEBUG
            nb_did_skipped[i] = 0;
#endif
        }
        // TODO(jlegriel): avoid a barrier here ? Load balancing of lookup postings operation is not very good
        barrier_wait(&barrier);
    }

    // each tasklet loops and take the next pair (query/segment) in the batch, until no more queries
    while (1) {
        mutex_lock(batch_mutex);
        uint32_t batch_num_tasklet = batch_num++;
        mutex_unlock(batch_mutex);
        if (batch_num_tasklet >= nb_queries_in_batch << nr_segments_log2) {
            break;
        }

        // TODO(jlegriel): instead make several tasklet work on different queries in parallel
        // uint32_t segment_id = batch_num_tasklet / nb_queries_in_batch;
        // uint32_t query_id = batch_num_tasklet - ((1 << nr_segments_log2) * nb_queries_in_batch);
        assert(batch_num_tasklet >> nr_segments_log2 <= UINT16_MAX);
        uint16_t query_id = (uint16_t)(batch_num_tasklet >> nr_segments_log2);
        assert(batch_num_tasklet - ((uint32_t)query_id << nr_segments_log2) <= UINT8_MAX);
        uint8_t segment_id = (uint8_t)(batch_num_tasklet - ((uint32_t)query_id << nr_segments_log2));
        uint8_t nr_terms = queries_nb_terms[query_id];
        uint32_t nr_results = 0;
        current_segment[me()] = 0;

        // printf("tid:%d start query %d segment %d nr_terms %d\n", me(), query_id, segment_id, nr_terms);

        // nr_terms is set to zero if the field was not found, or no postings were found for some of the terms
        if (nr_terms != 0) {

            init_results_cache(query_id, 0, segment_id);

            get_postings_from_cache(query_id, nr_terms, segment_id, postings_cache_wram[me()]);

            uint32_t start_did = 0;
            if (!new_query) {
                if (postings_cache_wram[me()][0].size == 0) {
                    continue;
                } // when this is a subsequent run for the same query, restore the context
                restore_context(query_id, segment_id, &start_did, results_cache[me()], results_batch);
            }

            did_matcher_t *matchers = setup_matchers(nr_terms, postings_cache_wram[me()], start_did);

#ifdef DBG_PRINT
            printf("Query %d: %d terms matchers %x\n", query_id, nr_terms, (uintptr_t)matchers);
#endif
            // a null matchers means one of the term of the query does not have postings in this segment
            // we can skip it
            if (matchers != 0) {
                nr_results = perform_did_and_pos_matching(query_id, segment_id, matchers, nr_terms);
            } else {
                normal_exit(query_id, segment_id, nr_terms);
            }

            release_matchers(matchers, nr_terms);

            // TODO(jlegriel): could use a mutex pool here, but is it worth ?
            if (nr_results != 0) {
                mutex_lock(results_mutex);
                results_index[query_id] += nr_results;
                mutex_unlock(results_mutex);
            }
        }
#ifdef DBG_PRINT
        printf("tid %d nr_results for query %d:%d = %u\n", me(), query_id, segment_id, nr_results);
#endif
        if (new_query) {
            // NOLINTNEXTLINE(performance-no-int-to-ptr)
            mram_write_int_atomic(&results_segment_offset[query_id * (1U << nr_segments_log2) + segment_id], (int)nr_results);
        } else if (nr_results) {
            // NOLINTNEXTLINE(performance-no-int-to-ptr)
            mram_update_int_atomic(
                &results_segment_offset[query_id * (1U << nr_segments_log2) + segment_id], adder, (int)nr_results);
        }
    }

    barrier_wait(&barrier);

    // if not all documents have been searched (early exit for lower bound on score)
    // return here
    if (!search_done) {
        return 0;
    }

    // prefix sum of the query index values
    if (me() == 0) {
        for (uint32_t i = 1; i < nb_queries_in_batch; ++i) {
            results_index[i] += results_index[i - 1];
        }
    }
    // read the segment offsets for this query and prefix sum the values and write it back
    prefix_sum_each_query(results_segment_offset, 1U << nr_segments_log2);

    barrier_wait(&barrier);

    sort_query_results();

#ifdef DBG_PRINT
    barrier_wait(&barrier);
    if (me() == 0) {
        printf("\nQUERIES RESULTS:\n");
        for (uint32_t i = 0; i < nb_queries_in_batch; ++i) {
            printf("nb did skipped: %u\n", nb_did_skipped[i]);
            printf("Query %u results:\n", i);
            uint32_t start = 0;
            if (i) {
                start = results_index[i - 1];
            }
            for (uint32_t j = start; j < results_index[i]; j++) {
                uint64_t res = 0;
                mram_read(&results_batch_sorted[j * 8], &res, 8);
                printf("doc:%u freq:%u norm:%u\n", *((uint32_t *)&res), *((uint16_t *)(&res) + 3), *((uint16_t *)(&res) + 2));
            }
            /*
            printf("\nnb results per lucene segments:\n");
            for(int j = 0; j < nr_lucene_segments; ++j) {
                printf("segment%d: %d\n", j, results_index_lucene_segments[i * nr_lucene_segments + j]);
            }*/
        }
    }
#endif

#ifdef PERF_MESURE
    barrier_wait(&barrier);
    if (me() == 0) {
        total_cycles += perfcounter_get();
        printf("Nb cycles=%lu total=%lu\n", perfcounter_get(), total_cycles);
    }
#endif
    return 0;
}

static void
store_query_result(uint16_t query_id, uint32_t did, __attribute((unused)) uint32_t pos);

static uint32_t
perform_pos_matching_for_did(uint32_t query_id, did_matcher_t *matchers, unsigned int nr_terms, uint32_t did)
{
    uint32_t nr_results = 0;
    start_pos_matching(matchers, nr_terms);

    if (!matchers_has_next_pos(matchers, nr_terms)) {
        goto end;
    }

    while (true) {
        uint32_t max_pos = 0;
        uint32_t index = 0;

        get_max_pos_and_index(matchers, nr_terms, &index, &max_pos);

        switch (seek_pos(matchers, nr_terms, max_pos, index)) {
            case POSITIONS_FOUND: {
                assert(query_id <= UINT16_MAX);
                store_query_result((uint16_t)query_id, did, max_pos - index);
                nr_results++;
#ifdef DBG_PRINT
                printf("Found a result for query %u: did=%u, pos=%u\n", query_id, did, max_pos - index);
#endif
                // switch to next position
                if (!matchers_has_next_pos(matchers, nr_terms)) {
                    goto end;
                }
                __attribute__((fallthrough));
            }
            case POSITIONS_NOT_FOUND:
                break;
            case END_OF_POSITIONS:
                goto end;
        }
    }
end:
    stop_pos_matching(matchers, nr_terms);
    return nr_results;
}

static uint32_t
perform_did_and_pos_matching(uint32_t query_id, uint16_t segment_id, did_matcher_t *matchers, uint32_t nr_terms)
{
    uint32_t nr_doc_match = 0;
    uint32_t nr_results = 0;
    while (true) {
        // This is either the initial loop, or we come back from a
        // set of matching DIDs. Whatever the case is, need to
        // warm up the iterator again by fetching next DIDs.
        if (!matchers_has_next_did(matchers, nr_terms)) {
            goto end;
        }

        seek_did_t did_status = DID_NOT_FOUND;
        do {
            uint32_t did = get_max_did(matchers, nr_terms);
            did_status = seek_did(matchers, nr_terms, did);
            switch (did_status) {
                case END_OF_INDEX_TABLE:
                    goto end;
                case DID_FOUND: {
#ifdef DBG_PRINT
                    printf("Found did %u query %u segment_id %u\n", did, query_id, segment_id);
#endif
                    // if the upper bound on this doc's score is lower than the lower bound, skip it
                    if (is_score_competitive(query_id, did, matchers, nr_terms)) {

                        uint32_t freq = perform_pos_matching_for_did(query_id, matchers, nr_terms, did);
                        if (freq) {
                            nr_results++;
                            assert(query_id <= UINT8_MAX);
                            add_match_for_best_scores((uint8_t)query_id, did, freq);
                        }

                        if (++nr_doc_match == nb_max_doc_match) {
                            // should stop here, early exit to allow the host to provide a lower bound on score
                            early_exit(query_id, segment_id, nr_terms, matchers);
                            return nr_results;
                        }

#ifdef DBG_PRINT
                        printf("nr_results after pos match did=%u %u\n", did, nr_results);
#endif
                    } else {
                        abort_did(matchers, nr_terms);
#ifndef NDEBUG
                        mutex_lock(results_mutex);
                        nb_did_skipped[query_id]++;
                        mutex_unlock(results_mutex);
#endif
                    }
                } break;
                case DID_NOT_FOUND:
                    break;
                default:
                    unreachable();
                    abort();
            }
        } while (did_status == DID_NOT_FOUND);
    }

end:
    normal_exit(query_id, segment_id, nr_terms);
    return nr_results;
}

static void
flush_query_buffer()
{

    if (results_cache[me()][0].info.buffer_size == 0) {
        return;
    }

    uint32_t mram_buffer_id = 0;
    if (results_cache[me()][0].info.mram_id == UINT16_MAX) {
        mutex_lock(results_mutex);
        mram_buffer_id = results_buffer_index++;
        mutex_unlock(results_mutex);
        assert(mram_buffer_id < UINT16_MAX);
        if (mram_buffer_id * DPU_RESULTS_CACHE_SIZE * sizeof(query_buffer_elem_t) >= DPU_RESULTS_MAX_BYTE_SIZE) {
            // the size of results is exceeded, we need to send a corresponding status to the host
            // TODO(jlegriel)
            return;
        }
        assert(mram_buffer_id <= UINT16_MAX);
        results_cache[me()][0].info.mram_id = (uint16_t)mram_buffer_id;
    } else {
        mram_buffer_id = results_cache[me()][0].info.mram_id;
    }

    // TODO(jlegriel): possibly avoid writting the cache fully for the last buffer where it is not full
    mram_write(results_cache[me()],
        &results_batch[mram_buffer_id * DPU_RESULTS_CACHE_SIZE * sizeof(query_buffer_elem_t)],
        DPU_RESULTS_CACHE_SIZE * sizeof(query_buffer_elem_t));
}

static void
init_results_cache(uint32_t query_id, uint32_t buffer_id, uint8_t segment_id)
{
    assert(buffer_id < UINT16_MAX);
    results_cache[me()][0].info.buffer_id = (uint16_t)buffer_id;
    results_cache[me()][0].info.buffer_size = 0;
    results_cache[me()][0].info.segment_id = segment_id;
    assert(query_id < UINT16_MAX);
    results_cache[me()][0].info.query_id = (uint16_t)query_id;
    results_cache[me()][0].info.mram_id = UINT16_MAX;
}

static void
store_query_result(uint16_t query_id, uint32_t did, __attribute((unused)) uint32_t pos)
{

    // there are different cases:
    // 1) doc id is the same as last result => increase the frequency of last result
    // 2) the results cache is full => flush the buffer
    // 3) the buffer has space available => insert the result in the current buffer
    assert(query_id == results_cache[me()][0].info.query_id);

    // change did to its absolute value
    uint32_t rel_did = did;
    assert(did < INT32_MAX);
    did = get_abs_doc_id((int)did);

    uint16_t buffer_size = results_cache[me()][0].info.buffer_size;
    if (buffer_size > 0) {
        // the buffer contains a result, check if the did is the same
        if (results_cache[me()][buffer_size].result.doc_id == did) {
            // same did, increase the frequency
            results_cache[me()][buffer_size].result.freq++;
            return;
        }
    }

    // first check if the buffer is full, in which case
    // we need to flush the buffer to MRAM
    if (buffer_size >= DPU_RESULTS_CACHE_SIZE - 1) {
        // write the buffer to MRAM
        flush_query_buffer();
        init_results_cache(query_id, results_cache[me()][0].info.buffer_id + 1, results_cache[me()][0].info.segment_id);
    }

    // insert the new result in the WRAM cache
    buffer_size = ++results_cache[me()][0].info.buffer_size;
    results_cache[me()][buffer_size].result.doc_id = did;
    results_cache[me()][buffer_size].result.norm = get_doc_norm(query_id, rel_did);
    results_cache[me()][buffer_size].result.freq = 1;

    // update lucene segment for the current did, then add 1 to the count of results per lucene segment
    while (current_segment[me()] < nr_lucene_segments && did >= lucene_segment_maxdoc[current_segment[me()]]) {
        current_segment[me()]++;
    }
    assert(current_segment[me()] < nr_lucene_segments);

    // atomic increment of the results per lucene segments info
    // NOLINTNEXTLINE(performance-no-int-to-ptr)
    mram_update_int_atomic(
        &results_index_lucene_segments[query_id * nr_lucene_segments + current_segment[me()]], adder, 1);
}

static void
sort_query_results()
{
    for (uint32_t b_id = me(); b_id < results_buffer_index; b_id += NR_TASKLETS) {

        mram_read(&results_batch[b_id * DPU_RESULTS_CACHE_SIZE * sizeof(query_buffer_elem_t)],
            results_cache[me()],
            DPU_RESULTS_CACHE_SIZE * sizeof(query_buffer_elem_t));

        uint32_t buffer_id = results_cache[me()][0].info.buffer_id;
        uint8_t buffer_size = results_cache[me()][0].info.buffer_size;
        uint8_t segment_id = results_cache[me()][0].info.segment_id;
        uint16_t query_id = results_cache[me()][0].info.query_id;
        uint32_t offset = 0;
        uint32_t segment_offset = 0;
        if (query_id) {
            offset = results_index[query_id - 1];
        }
        if (segment_id) {
            segment_offset = results_segment_offset[query_id * (1U << nr_segments_log2) + segment_id - 1];
        }
        uint32_t mram_index = (offset + segment_offset + buffer_id * (DPU_RESULTS_CACHE_SIZE - 1)) * sizeof(query_buffer_elem_t);

        assert(buffer_size > 0);

        mram_write(&results_cache[me()][1], &results_batch_sorted[mram_index], buffer_size * sizeof(query_buffer_elem_t));
    }
}

static void
lookup_postings_info_for_query(mram_ptr_t index, uint32_t query_id)
{

    // if the number of terms is still at zero at the end of this function
    // this means that there are no results for the query (e.g., field or term not found)
    queries_nb_terms[query_id] = 0;

    // initialize a query parser
    query_parser_t query_parser;
    init_query_parser(&query_parser, query_batch + query_offset_in_batch[query_id]);

    // read query type
    uint8_t query_type = 0;
    read_query_type(&query_parser, &query_type);
    assert(query_type == PIM_PHRASE_QUERY_TYPE); // only PIM PHRASE QUERY TYPE supported

    // lookup the field norms and block table addresses, if not found return
    // do it only once for all the terms
    mram_ptr_t field_norms_address = 0;
    mram_ptr_t field_bt_address = 0;
    term_t term;
    read_field(&query_parser, &term);

    if (!get_field_addresses(index, &term, &field_norms_address, &field_bt_address)) {
        goto end;
    }

    // printf("query_id= %d norms_addr=%p block_addr=%p\n", query_id, field_norms_address, field_bt_address);

    if (field_bt_address == field_norms_address) {
        // this field has no norm
        set_query_no_norms(query_id);
    } else {
        // register where the norms are to be read for this query
        set_query_doc_norms_addr(query_id, field_norms_address);
    }

    uint8_t nr_terms = ({
        uint32_t nr_terms_long = 0;
        read_nr_terms(&query_parser, &nr_terms_long);
        if (nr_terms_long > NB_DECODERS_FOR_POSTINGS) {
            // it is not possible to handle the query as it requires
            // a larger number of decoders than the total in the pool
            // TODO(jlegriel): error handling back to the host
            goto end;
        }
        (uint8_t) nr_terms_long;
    });

    for (uint8_t each_term = 0; each_term < nr_terms; each_term++) {
        read_term(&query_parser, &term);
        if (!get_term_postings(field_bt_address, &term, postings_cache_wram[me()])) {
            goto end;
        }
        assert(1U << nr_segments_log2 <= UINT8_MAX);
        set_postings_in_cache(query_id, each_term, (uint8_t)(1U << nr_segments_log2), postings_cache_wram[me()]);
    }

    // at this point, all the postings for the terms of the query have been found
    // so the query will have to be handled
    queries_nb_terms[query_id] = nr_terms;

end:
    release_query_parser(&query_parser);
}

static void
get_segments_info(mram_ptr_t index)
{

    // get a decoder from the pool
    decoder_t *decoder = decoder_pool_get_one();
    initialize_decoder(decoder, index);

    // read the total number of DPUs
    nr_dpus = ({
        uint32_t nr_dpus_long = decode_short_from(decoder);
        assert(nr_dpus_long <= UINT16_MAX);
        (uint16_t) nr_dpus_long;
    });
    // read the dpu index
    dpu_id = ({
        uint32_t dpu_id_long = decode_short_from(decoder);
        assert(dpu_id_long <= UINT16_MAX);
        (uint16_t) dpu_id_long;
    });
    // read the number of segments (log2 encoding)
    nr_segments_log2 = ({
        uint32_t nr_segments_log2_long = decode_byte_from(decoder);
        assert(nr_segments_log2_long <= 8);
        (uint8_t) nr_segments_log2_long;
    });
    // read number of lucene segments
    nr_lucene_segments = ({
        uint32_t nr_lucene_segments_long = decode_byte_from(decoder);
        assert(nr_lucene_segments_long <= DPU_MAX_NR_LUCENE_SEGMENTS);
        (uint8_t) nr_lucene_segments_long;
    });
    decode_vint_from(decoder); // number of bytes, used to skip
    // read lucene segments max doc info
    for (int i = 0; i < nr_lucene_segments; ++i) {
        lucene_segment_maxdoc[i] = decode_vint_from(decoder);
    }

    decoder_pool_release_one(decoder);
}

// returns the absolute doc id from the relative doc id
static uint32_t
get_abs_doc_id(int rel_doc)
{
    assert(rel_doc >= 0);
    return (uint32_t)rel_doc * nr_dpus + dpu_id;
}

static void
prefix_sum_each_query(__mram_ptr uint32_t *array, uint32_t sz)
{
    for (uint32_t i = me(); i < nb_queries_in_batch; i += NR_TASKLETS) {
        // values are loaded 8 by 8 for more efficient MRAM access
        uint32_t curr = 0;
        for (uint32_t j = 0; j < (sz + NB_ELEM_TRANSFER - 1) / NB_ELEM_TRANSFER; ++j) {
            uint32_t nb_elem = NB_ELEM_TRANSFER;
            if (sz - (j * NB_ELEM_TRANSFER) < NB_ELEM_TRANSFER) {
                nb_elem = sz - (j * NB_ELEM_TRANSFER);
            }

            uint32_t *cache = mram_read_unaligned(
                &array[i * sz + j * NB_ELEM_TRANSFER], segment_offset_cache[me()], nb_elem * sizeof(uint32_t));

            cache[0] += curr;
            for (uint32_t k = 1; k < nb_elem; ++k) {
                cache[k] += cache[k - 1];
            }
            curr = segment_offset_cache[me()][NB_ELEM_TRANSFER - 1];

            mram_write_unaligned(cache, &array[i * sz + j * NB_ELEM_TRANSFER], nb_elem * sizeof(uint32_t));
        }
    }
}

void
early_exit(uint32_t query_id, uint32_t segment_id, uint32_t nr_terms, did_matcher_t *matchers)
{
    // mark the search as unfinished
    mutex_lock(results_mutex);
    search_done = 0;
    mutex_unlock(results_mutex);

    // update the current state in the postings cache for this query and DPU segment
    postings_info_t *cache = postings_cache_wram[me()];
    uint32_t curr_did = matcher_get_curr_did(matchers, 0);
    for (uint32_t i = 0; i < nr_terms; ++i) {
        mram_ptr_t curr_addr = matcher_get_curr_address(matchers, i);
        assert(curr_did == matcher_get_curr_did(matchers, i));
        assert(curr_addr >= cache[i].addr);
        uint32_t curr_size = (uint32_t)(curr_addr - cache[i].addr);
        assert(cache[i].size >= curr_size);
        cache[i].size -= curr_size;
        cache[i].addr = curr_addr;
    }
    assert(nr_terms <= UINT8_MAX);
    assert(segment_id <= UINT8_MAX);
    update_postings_in_cache(query_id, (uint8_t)nr_terms, (uint8_t)segment_id, cache);

    flush_query_buffer();

    // save information on the current partial buffer of results
    assert(query_id <= UINT16_MAX);
    save_context((uint16_t)query_id, (uint8_t)segment_id, results_cache[me()], curr_did);
}

void
normal_exit(uint32_t query_id, uint32_t segment_id, uint32_t nr_terms)
{

    postings_info_t *cache = postings_cache_wram[me()];
    for (uint32_t i = 0; i < nr_terms; ++i) {
        cache[i].size = 0;
    }
    assert(nr_terms <= UINT8_MAX);
    assert(segment_id <= UINT8_MAX);
    update_postings_in_cache(query_id, (uint8_t)nr_terms, (uint8_t)segment_id, cache);
    flush_query_buffer();
}
