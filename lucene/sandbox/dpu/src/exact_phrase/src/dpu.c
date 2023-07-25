#include <stdint.h>
#include <defs.h>
#include <mram.h>
#include <barrier.h>
#include <mutex.h>
#include <assert.h>
#include <alloc.h>
#include <string.h>
#include <mram_unaligned.h>
#include "common.h"
#include "matcher.h"
#include "decoder.h"
#include "query_result.h"

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

/* Results WRAM caches */
__dma_aligned query_buffer_elem_t results_cache[NR_TASKLETS][DPU_RESULTS_CACHE_SIZE];
uint8_t results_cache_index[NR_TASKLETS] = {0};
MUTEX_INIT(results_mutex);
uint32_t results_buffer_index = 0;

uint32_t batch_num = 0;
MUTEX_INIT(batch_mutex);
BARRIER_INIT(barrier, NR_TASKLETS);

#ifdef TEST1
#define TEST
#define DEBUG
#include "../test/test1.h"
#endif

#define DEBUG
#ifdef DEBUG
#include <stdio.h>
#endif

static void perform_did_and_pos_matching(uint32_t query_id, did_matcher_t *matchers, uint32_t nr_terms);
static void init_results_cache(uint32_t query_id, uint32_t buffer_id);
static void sort_query_results();
static void flush_query_buffer();

int main() {

    if(!index_loaded) {
#ifdef DEBUG
        printf("No index loaded\n");
#endif
        return 0;
    }
    if(me() == 0) {
        mem_reset();
        batch_num = 0;
        results_buffer_index = 0;
        initialize_decoder_pool();
#ifdef TEST
        // in test mode set the queries inputs correctly
        nb_queries_in_batch = test_nb_queries_in_batch;
        nb_bytes_in_batch = test_nb_bytes_in_batch;
        mram_write(test_query_batch, query_batch, ((test_nb_bytes_in_batch + 7) >> 3) << 3);
        memcpy(query_offset_in_batch, test_query_offset_in_batch, test_nb_queries_in_batch * sizeof(uint32_t));
#endif
    }
    barrier_wait(&barrier);
    results_cache_index[me()] = 0;

    // each tasklet loops and take the next query in the batch, until no more queries
    uint32_t batch_num_tasklet;
    while(1) {
        mutex_lock(batch_mutex);
        batch_num_tasklet = batch_num++;
        mutex_unlock(batch_mutex);
        if(batch_num_tasklet >= nb_queries_in_batch)
            break;

        results_index[batch_num_tasklet] = 0;
        init_results_cache(batch_num_tasklet, 0);

        // initialize a query parser
        query_parser_t query_parser;
        init_query_parser(&query_parser, query_batch + query_offset_in_batch[batch_num_tasklet]);

        // read segment id and query type
        uint32_t segment_id;
        uint8_t query_type;
        read_segment_id(&query_parser, &segment_id);
        read_query_type(&query_parser, &query_type);
        assert(query_type == PIM_PHRASE_QUERY_TYPE); // only PIM PHRASE QUERY TYPE supported

#ifdef TEST
        did_matcher_t *matchers = setup_matchers(&query_parser, (uintptr_t)(&index_mram[0]));
#else
        did_matcher_t *matchers = setup_matchers(&query_parser, (uintptr_t)DPU_MRAM_HEAP_POINTER);
#endif

        uint32_t nr_terms = query_parser.nr_terms;
        release_query_parser(&query_parser);

#ifdef DEBUG
        printf("Query %d: %d terms matchers %x\n", batch_num_tasklet, nr_terms, (uintptr_t)matchers);
#endif
        // a null matchers means one of the term of the query is not present in the index and we can skip it
        if(matchers != 0)
            perform_did_and_pos_matching(batch_num_tasklet, matchers, nr_terms);

        release_matchers(matchers, nr_terms);
        flush_query_buffer();
    }

    barrier_wait(&barrier);

    // sum the query index values
    if(me() == 0) {
        for(int i = 1; i < nb_queries_in_batch; ++i) {
            results_index[i] += results_index[i-1];
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

    return 0;
}

static void store_query_result(uint16_t query_id, uint32_t did, __attribute((unused)) uint32_t pos);

static void perform_pos_matching_for_did(uint32_t query_id, did_matcher_t *matchers,
                                                unsigned int nr_terms, uint32_t did)
{
    start_pos_matching(matchers, nr_terms);

    if (!matchers_has_next_pos(matchers, nr_terms))
        goto end;

    while (true) {
        uint32_t max_pos, index;

        get_max_pos_and_index(matchers, nr_terms, &index, &max_pos);

        switch (seek_pos(matchers, nr_terms, max_pos, index)) {
        case POSITIONS_FOUND: {
            store_query_result(query_id, did, max_pos - index);
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
}

static void perform_did_and_pos_matching(uint32_t query_id, did_matcher_t *matchers, uint32_t nr_terms)
{
    while (true) {
        // This is either the initial loop, or we come back from a
        // set of matching DIDs. Whatever the case is, need to
        // warm up the iterator again by fetching next DIDs.
        if (!matchers_has_next_did(matchers, nr_terms))
            return;

        seek_did_t did_status;
        do {
            uint32_t did = get_max_did(matchers, nr_terms);
            did_status = seek_did(matchers, nr_terms, did);
            switch (did_status) {
            case END_OF_INDEX_TABLE:
                return;
            case DID_FOUND: {
#ifdef DEBUG
                printf("Found did %d\n", did);
#endif
                perform_pos_matching_for_did(query_id, matchers, nr_terms, did);
            } break;
            case DID_NOT_FOUND:
                break;
            }
        } while (did_status == DID_NOT_FOUND);
    }
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

    results_index[results_cache[me()][0].info.query_id] += results_cache[me()][0].info.buffer_size;
}

static void init_results_cache(uint32_t query_id, uint32_t buffer_id) {

    results_cache_index[me()] = 1;
    results_cache[me()][0].info.buffer_id = buffer_id;
    results_cache[me()][0].info.buffer_size = 0;
    results_cache[me()][0].info.query_id = query_id;
}

static void store_query_result(uint16_t query_id, uint32_t did, __attribute((unused)) uint32_t pos) {

    // there are different cases:
    // 1) doc id is the same as last result => increase the frequency of last result
    // 2) the results cache is full => flush the buffer
    // 3) the buffer has space available => insert the result in the current buffer

    assert(query_id == results_cache[me()][0].info.query_id);

    if(results_cache_index[me()] > 1) {
        // the buffer contains a result, check if the did is the same
        if(results_cache[me()][results_cache_index[me()] - 1].result.doc_id == did) {
            // same did, increase the frequency
            results_cache[me()][results_cache_index[me()] - 1].result.freq++;
            return;
        }
    }

    // first check if the buffer is full, in which case
    // we need to flush the buffer to MRAM
    if(results_cache_index[me()] >= DPU_RESULTS_CACHE_SIZE) {
        // write the buffer to MRAM
        flush_query_buffer();
        init_results_cache(query_id, results_cache[me()][0].info.buffer_id + 1);
    }

    // insert the new result in the WRAM cache
    results_cache[me()][results_cache_index[me()]].result.doc_id = did;
    results_cache[me()][results_cache_index[me()]].result.freq = 1;
    results_cache[me()][0].info.buffer_size++;
    results_cache_index[me()]++;
}

static void sort_query_results() {

    for(int buffer_id = me(); buffer_id < results_buffer_index; buffer_id += NR_TASKLETS) {

        mram_read(&results_batch[buffer_id * DPU_RESULTS_CACHE_SIZE * sizeof(query_buffer_elem_t)],
                    results_cache[me()], DPU_RESULTS_CACHE_SIZE * sizeof(query_buffer_elem_t));

        uint32_t buffer_id = results_cache[me()][0].info.buffer_id;
        uint16_t buffer_size = results_cache[me()][0].info.buffer_size;
        uint16_t query_id = results_cache[me()][0].info.query_id;
        uint32_t offset = 0;
        if(query_id) offset = results_index[query_id - 1];
        uint32_t mram_index = (offset + buffer_id * (DPU_RESULTS_CACHE_SIZE - 1))
                                * sizeof(query_buffer_elem_t);

        assert(buffer_size > 0);

        mram_write(&results_cache[me()][1], &results_batch_sorted[mram_index], buffer_size * sizeof(query_buffer_elem_t));
    }
}
