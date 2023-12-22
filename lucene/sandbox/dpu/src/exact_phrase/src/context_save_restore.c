#include "context_save_restore.h"
#include "common.h"
#include <assert.h>

struct results_mram_buffer_info {
    uint16_t buffer_id;
    uint16_t nb_results;
    uint32_t start_did;
};
__mram_noinit struct results_mram_buffer_info results_buffer_info[DPU_MAX_BATCH_SIZE][MAX_NR_SEGMENTS];

void save_context(uint16_t query_id, uint8_t segment_id, query_buffer_elem_t *results_cache, uint32_t curr_did) {

    // save information on the current partial buffer of results
    // we need to save only if the buffer was not empty or full, in which
    // case we can start over with a fresh buffer. In any case we need to save the
    // buffer id and the start did
    struct results_mram_buffer_info binfo;
    binfo.nb_results = results_cache[0].info.buffer_size;
    if(binfo.nb_results == 0) {
      // empty buffer, buffer id stays the same
      binfo.buffer_id = results_cache[0].info.buffer_id;
    }
    else if(binfo.nb_results >= DPU_RESULTS_CACHE_SIZE - 1) {
      // buffer is full and will be fully written
      // put nb_results to 0 so that nothing will be restored
      binfo.nb_results = 0;
      // the buffer id should be incremented
      binfo.buffer_id = results_cache[0].info.buffer_id + 1;
    }
    else {
      // this is a partial buffer, the buffer id stores the mram_id from
      // where this buffer will be restored
      binfo.buffer_id = results_cache[0].info.mram_id;
    }
    binfo.start_did = curr_did;
    assert(query_id == results_cache[0].info.query_id);
    mram_write(&binfo, &results_buffer_info[query_id][segment_id], 8);
}

void restore_context(uint16_t query_id, uint8_t segment_id, uint32_t *start_did,
                        query_buffer_elem_t *results_cache, __mram_ptr uint8_t *results_batch) {

    struct results_mram_buffer_info binfo;
    mram_read(&results_buffer_info[query_id][segment_id], &binfo, 8);
    if(binfo.nb_results) {
          mram_read(&results_batch[binfo.buffer_id * DPU_RESULTS_CACHE_SIZE * sizeof(query_buffer_elem_t)],
                        results_cache,
                        (binfo.nb_results + 1) * sizeof(query_buffer_elem_t));
    }
    else {
          // no partial buffer of result to restore
          // still read the buffer id
          results_cache[0].info.buffer_id = binfo.buffer_id;
    }

    *start_did = binfo.start_did;
    assert(results_cache[0].info.query_id == query_id);
}