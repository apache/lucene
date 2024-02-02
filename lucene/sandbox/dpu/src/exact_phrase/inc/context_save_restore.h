#ifndef CONTEXT_SAVE_RESTORE_H_
#define CONTEXT_SAVE_RESTORE_H_

#include <mram.h>
#include <stdint.h>

#include "common.h"
#include "query_result.h"

void
save_context(uint16_t query_id, uint8_t segment_id, query_buffer_elem_t *results_cache, uint32_t curr_did);

void
restore_context(uint16_t query_id,
    uint8_t segment_id,
    uint32_t *start_did,
    query_buffer_elem_t *results_cache,
    mram_ptr_t results_batch);

#endif