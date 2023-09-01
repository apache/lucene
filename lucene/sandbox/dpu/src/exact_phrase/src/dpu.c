#include <stdint.h>
#include <defs.h>
#include <mram.h>
#include <barrier.h>
#include <mutex.h>
#include <assert.h>
#include <alloc.h>
#include <string.h>
#include "common.h"
#include "matcher.h"
#include "decoder.h"
#include "term_lookup.h"
#include "query_result.h"
#include "postings_util.h"

//#define PERF_MESURE
#ifdef PERF_MESURE
#include <perfcounter.h>
#include <stdio.h>
uint64_t total_cycles = 0;
#endif

__host uint32_t index_loaded = 0;

/**
  Input queries
  */
__host uint32_t nb_queries_in_batch;
__host uint32_t nb_bytes_in_batch;
__mram_noinit uint8_t query_batch[DPU_QUERY_BATCH_BYTE_SIZE];
__host uint32_t query_offset_in_batch[DPU_MAX_BATCH_SIZE];

/**
  Output results
  */
__mram_noinit uint8_t results_batch[DPU_RESULTS_MAX_BYTE_SIZE];
__mram_noinit uint8_t results_batch_sorted[DPU_RESULTS_MAX_BYTE_SIZE];
__host uint32_t results_index[DPU_MAX_BATCH_SIZE] = {0};
__mram_noinit uint64_t results_segment_offset[DPU_MAX_BATCH_SIZE][MAX_NR_SEGMENTS];
__dma_aligned uint64_t segment_offset_cache[NR_TASKLETS][8];

/* Results WRAM caches */
__dma_aligned query_buffer_elem_t results_cache[NR_TASKLETS][DPU_RESULTS_CACHE_SIZE];
MUTEX_INIT(results_mutex);
uint32_t results_buffer_index = 0;

/* Number of terms for each query */
uint8_t queries_nb_terms[DPU_MAX_BATCH_SIZE];

/* WRAM cache for postings info */
#define POSTINGS_CACHE_SIZE (MAX_NR_TERMS > MAX_NR_SEGMENTS ? MAX_NR_TERMS : MAX_NR_SEGMENTS)
__dma_aligned postings_info_t postings_cache_wram[NR_TASKLETS][POSTINGS_CACHE_SIZE];

uint8_t nr_segments_log2 = 0;
uintptr_t dpu_index = 0;

uint32_t batch_num = 0;
MUTEX_INIT(batch_mutex);
BARRIER_INIT(barrier, NR_TASKLETS);

#ifdef TEST1
#define TEST
#define DEBUG
#include "../test/test1.h"
#endif

//#define DEBUG
#ifdef DEBUG
#include <stdio.h>
#endif

static uint32_t perform_did_and_pos_matching(uint32_t query_id, uint16_t segment_id, did_matcher_t *matchers, uint32_t nr_terms);
static void init_results_cache(uint32_t query_id, uint32_t buffer_id, uint8_t segment_id);
static void lookup_postings_info_for_query(uintptr_t index, uint32_t query_id);
static void sort_query_results();
static void flush_query_buffer();
static uint8_t get_nr_segments_log2(uintptr_t);

int main() {

#ifndef TEST
    if(!index_loaded) {
#ifdef DEBUG
        printf("No index loaded\n");
#endif
        return 0;
    }
#endif
    if(me() == 0) {

        mem_reset();

#ifdef PERF_MESURE
        perfcounter_config(COUNT_CYCLES, true);
        printf("Number of queries: %d\n", nb_queries_in_batch);
#endif
        batch_num = 0;
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
        dpu_index = (uintptr_t)DPU_MRAM_HEAP_POINTER;
#endif
        nr_segments_log2 = get_nr_segments_log2(dpu_index);
        assert(nr_segments_log2 >=0 && nr_segments_log2 < 8);
    }
    barrier_wait(&barrier);

    // first lookup the postings addresses for each query/term/segment
    // store them in MRAM for later use by the tasklets to find matching document/positions
    for(uint32_t i = me(); i < nb_queries_in_batch; i += NR_TASKLETS) {
            lookup_postings_info_for_query(dpu_index, i);
            results_index[i] = 0;
    }
    //TODO avoid a barrier here ? Load balancing of lookup postings operation is not very good
    barrier_wait(&barrier);

    // each tasklet loops and take the next pair (query/segment) in the batch, until no more queries
    uint32_t batch_num_tasklet;
    while(1) {
        mutex_lock(batch_mutex);
        batch_num_tasklet = batch_num++;
        mutex_unlock(batch_mutex);
        if(batch_num_tasklet >= nb_queries_in_batch << nr_segments_log2)
            break;

        uint32_t query_id = batch_num_tasklet >> nr_segments_log2;
        uint32_t segment_id = batch_num_tasklet - (query_id << nr_segments_log2);
        uint8_t nr_terms = queries_nb_terms[query_id];
        uint64_t nr_results = 0;

        /*printf("tid:%d start query %d segment %d nr_terms %d\n", me(), query_id, segment_id, nr_terms);*/

        // nr_terms is set to zero if the field was not found, or no postings were found for some of the terms
        if(nr_terms != 0) {

            init_results_cache(query_id, 0, segment_id);

            get_postings_from_cache(query_id, nr_terms, segment_id, postings_cache_wram[me()]);
            did_matcher_t *matchers = setup_matchers(nr_terms, postings_cache_wram[me()]);

    #ifdef DEBUG
            printf("Query %d: %d terms matchers %x\n", query_id, nr_terms, (uintptr_t)matchers);
    #endif
            // a null matchers means one of the term of the query does not have postings in this segment
            // we can skip it
            if(matchers != 0) {
                nr_results = perform_did_and_pos_matching(query_id, segment_id, matchers, nr_terms);
            }

            release_matchers(matchers, nr_terms);
            flush_query_buffer();

            //TODO could use a mutex pool here, but is it worth ?
            if(nr_results != 0) {
                mutex_lock(results_mutex);
                results_index[query_id] += nr_results;
                mutex_unlock(results_mutex);
            }
        }
#ifdef DEBUG
        printf("tid %d nr_results for query %d:%d = %lu\n", me(), query_id, segment_id, nr_results);
#endif
        mram_write(&nr_results, &results_segment_offset[query_id][segment_id], 8);
    }

    barrier_wait(&barrier);

    // prefix sum of the query index values
    if(me() == 0) {
        for(int i = 1; i < nb_queries_in_batch; ++i) {
            results_index[i] += results_index[i-1];
        }
    }
    for(int i = me(); i < nb_queries_in_batch; i += NR_TASKLETS) {
        // read the segment offsets for this query and prefix sum the values and write it back
        // values are loaded 8 by 8 for more efficient MRAM access
        uint32_t curr = 0;
        for(int j = 0; j < ((1 << nr_segments_log2) + 7) >> 3; ++j) {
            int nbElem = 8;
            if((1 << nr_segments_log2) - (j * 8) < 8)
              nbElem = (1 << nr_segments_log2) - (j * 8);
            mram_read(results_segment_offset[i] + j * 8, segment_offset_cache[me()], nbElem * 8);
            segment_offset_cache[me()][0] += curr;
            for(int k = 1; k < nbElem; ++k) {
                segment_offset_cache[me()][k] += segment_offset_cache[me()][k-1];
            }
            curr = segment_offset_cache[me()][7];
            mram_write(segment_offset_cache[me()], results_segment_offset[i] + j * 8, nbElem * 8);
        }
    }

    barrier_wait(&barrier);

    sort_query_results();

#ifdef DEBUG
    barrier_wait(&barrier);
    if(me() == 0) {
       printf("\nQUERIES RESULTS:\n");
       for(int i = 0; i < nb_queries_in_batch; ++i) {
        printf("Query %d results:\n", i);
        int start = 0;
        if(i) start = results_index[i-1];
        for(int j=start; j < results_index[i]; j++) {
            uint64_t res;
            mram_read(&results_batch_sorted[j * 8], &res, 8);
            printf("doc:%u freq:%u\n", *((uint32_t*)&res), *((uint32_t*)(&res) + 1));
        }
       }
    }
#endif

#ifdef PERF_MESURE
    barrier_wait(&barrier);
    if(me() == 0) {
      total_cycles += perfcounter_get();
      printf("Nb cycles=%lu total=%lu\n", perfcounter_get(), total_cycles);
    }
#endif
    return 0;
}

static void store_query_result(uint16_t query_id, uint32_t did, __attribute((unused)) uint32_t pos);

static bool perform_pos_matching_for_did(uint32_t query_id, did_matcher_t *matchers,
                                                unsigned int nr_terms, uint32_t did)
{
    start_pos_matching(matchers, nr_terms);

    if (!matchers_has_next_pos(matchers, nr_terms))
        goto end;

    bool result_found = false;
    while (true) {
        uint32_t max_pos, index;

        get_max_pos_and_index(matchers, nr_terms, &index, &max_pos);

        switch (seek_pos(matchers, nr_terms, max_pos, index)) {
        case POSITIONS_FOUND: {
            store_query_result(query_id, did, max_pos - index);
            result_found = true;
#ifdef DEBUG
            printf("Found a result for query %d: did=%d, pos=%d\n", query_id, did, max_pos - index);
#endif
            // switch to next position
            if (!matchers_has_next_pos(matchers, nr_terms))
                    goto end;
        }
        case POSITIONS_NOT_FOUND:
            break;
        case END_OF_POSITIONS:
            goto end;
        }
    }
end:
    stop_pos_matching(matchers, nr_terms);
    return result_found;
}

static uint32_t perform_did_and_pos_matching(uint32_t query_id, uint16_t segment_id, did_matcher_t *matchers, uint32_t nr_terms)
{
    uint32_t nr_results = 0;
    while (true) {
        // This is either the initial loop, or we come back from a
        // set of matching DIDs. Whatever the case is, need to
        // warm up the iterator again by fetching next DIDs.
        if (!matchers_has_next_did(matchers, nr_terms))
            return nr_results;

        seek_did_t did_status;
        do {
            uint32_t did = get_max_did(matchers, nr_terms);
            did_status = seek_did(matchers, nr_terms, did);
            switch (did_status) {
            case END_OF_INDEX_TABLE:
                return nr_results;
            case DID_FOUND: {
#ifdef DEBUG
                printf("Found did %d query %d segment_id %d\n", did, query_id, segment_id);
#endif
                if(perform_pos_matching_for_did(query_id, matchers, nr_terms, did))
                    nr_results++;
#ifdef DEBUG
                printf("nr_results after pos match did=%d %d\n", did, nr_results);
#endif
            } break;
            case DID_NOT_FOUND:
                break;
            }
        } while (did_status == DID_NOT_FOUND);
    }
    return nr_results;
}

static void flush_query_buffer() {

    if(results_cache[me()][0].info.buffer_size == 0)
        return;

    uint32_t mram_buffer_id = 0;
    mutex_lock(results_mutex);
    mram_buffer_id = results_buffer_index++;
    mutex_unlock(results_mutex);
    if(mram_buffer_id * DPU_RESULTS_CACHE_SIZE * sizeof(query_buffer_elem_t) >= DPU_RESULTS_MAX_BYTE_SIZE) {
        // the size of results is exceeded, we need to send a corresponding status to the host
        //TODO
        return;
    }
    // TODO possibly avoid writting the cache fully for the last buffer where it is not full
    mram_write(results_cache[me()],
                &results_batch[mram_buffer_id * DPU_RESULTS_CACHE_SIZE * sizeof(query_buffer_elem_t)],
                DPU_RESULTS_CACHE_SIZE * sizeof(query_buffer_elem_t));
}

static void init_results_cache(uint32_t query_id, uint32_t buffer_id, uint8_t segment_id) {

    results_cache[me()][0].info.buffer_id = buffer_id;
    results_cache[me()][0].info.buffer_size = 0;
    results_cache[me()][0].info.segment_id = segment_id;
    results_cache[me()][0].info.query_id = query_id;
}

static void store_query_result(uint16_t query_id, uint32_t did, __attribute((unused)) uint32_t pos) {

    // there are different cases:
    // 1) doc id is the same as last result => increase the frequency of last result
    // 2) the results cache is full => flush the buffer
    // 3) the buffer has space available => insert the result in the current buffer

    //TODO query_id is not necessary as a parameter
    assert(query_id == results_cache[me()][0].info.query_id);

    uint16_t buffer_size = results_cache[me()][0].info.buffer_size;
    if(buffer_size > 0) {
        // the buffer contains a result, check if the did is the same
        if(results_cache[me()][buffer_size].result.doc_id == did) {
            // same did, increase the frequency
            results_cache[me()][buffer_size].result.freq++;
            return;
        }
    }

    // first check if the buffer is full, in which case
    // we need to flush the buffer to MRAM
    if(buffer_size >= DPU_RESULTS_CACHE_SIZE - 1) {
        // write the buffer to MRAM
        flush_query_buffer();
        init_results_cache(query_id, results_cache[me()][0].info.buffer_id + 1, results_cache[me()][0].info.segment_id);
    }

    // insert the new result in the WRAM cache
    buffer_size = ++results_cache[me()][0].info.buffer_size;
    results_cache[me()][buffer_size].result.doc_id = did;
    results_cache[me()][buffer_size].result.freq = 1;
}

static void sort_query_results() {

    for(int buffer_id = me(); buffer_id < results_buffer_index; buffer_id += NR_TASKLETS) {

        mram_read(&results_batch[buffer_id * DPU_RESULTS_CACHE_SIZE * sizeof(query_buffer_elem_t)],
                    results_cache[me()], DPU_RESULTS_CACHE_SIZE * sizeof(query_buffer_elem_t));

        uint32_t buffer_id = results_cache[me()][0].info.buffer_id;
        uint8_t buffer_size = results_cache[me()][0].info.buffer_size;
        uint8_t segment_id = results_cache[me()][0].info.segment_id;
        uint16_t query_id = results_cache[me()][0].info.query_id;
        uint32_t offset = 0;
        uint32_t segment_offset = 0;
        if(query_id) offset = results_index[query_id - 1];
        if(segment_id) segment_offset = results_segment_offset[query_id][segment_id - 1];
        uint32_t mram_index = (offset + segment_offset + buffer_id * (DPU_RESULTS_CACHE_SIZE - 1))
                                * sizeof(query_buffer_elem_t);

        assert(buffer_size > 0);

        mram_write(&results_cache[me()][1], &results_batch_sorted[mram_index], buffer_size * sizeof(query_buffer_elem_t));
    }
}

static void lookup_postings_info_for_query(uintptr_t index, uint32_t query_id) {

    // if the number of terms is still at zero at the end of this function
    // this means that there are no results for the query (e.g., field or term not found)
    queries_nb_terms[query_id] = 0;

    // initialize a query parser
    query_parser_t query_parser;
    init_query_parser(&query_parser, query_batch + query_offset_in_batch[query_id]);

    // read segment id and query type
    uint32_t segment_id;
    uint8_t query_type;
    read_segment_id(&query_parser, &segment_id);
    read_query_type(&query_parser, &query_type);
    assert(query_type == PIM_PHRASE_QUERY_TYPE); // only PIM PHRASE QUERY TYPE supported

    // lookup the field block table address, if not found return
    // do it only once for all the terms
    uintptr_t field_address;
    term_t term;
    read_field(&query_parser, &term);

    if(!get_field_address(index, &term, &field_address))
        goto end;

    uint32_t nr_terms;
    read_nr_terms(&query_parser, &nr_terms);
    if(nr_terms > NB_DECODERS_FOR_POSTINGS) {
        // it is not possible to handle the query as it requires
        // a larger number of decoders than the total in the pool
        // TODO error handling back to the host
        goto end;
    }

    for (int each_term = 0; each_term < nr_terms; each_term++) {
        read_term(&query_parser, &term);
        if(!get_term_postings(field_address, &term, postings_cache_wram[me()]))
                goto end;
        set_postings_in_cache(query_id, each_term, 1 << nr_segments_log2, postings_cache_wram[me()]);
    }

    // at this point, all the postings for the terms of the query have been found
    // so the query will have to be handled
    queries_nb_terms[query_id] = nr_terms;

end:
    release_query_parser(&query_parser);
}

static uint8_t get_nr_segments_log2(uintptr_t index) {

    // get a decoder from the pool
    decoder_t* decoder = decoder_pool_get_one();
    initialize_decoder(decoder, index);

    // read the number of segments (log2 encoding)
    uint8_t val = decode_byte_from(decoder);
    decoder_pool_release_one(decoder);
    return val;
}
