#ifndef QUERY_RESULT_H_
#define QUERY_RESULT_H_

#include <stdint.h>

/**
 * structure used to store the query results
 * A result contains the document id, the document norm and frequency.
 */
typedef struct __attribute__((packed, aligned(8))) _query_result {
    uint32_t doc_id;
    uint32_t norm : 8;
    uint32_t freq : 24;
} query_result_t;

/**
 * Results are stored in several buffers of fixed size.
 * The first element of the buffer contains information
 * stored in the following structure.
 * Stores the buffer id, the number of results in the buffer,
 * the dpu segment id and the query id.
 * Optionally stores a mram_id if this buffer is already partially written
 * in MRAM and has already an id where to be written in MRAM (UINT16_MAX if not defined)
 */
typedef struct _query_buffer_info {
    uint16_t buffer_id;
    uint8_t buffer_size;
    uint8_t segment_id;
    uint16_t query_id;
    uint16_t mram_id;
} query_buffer_info_t;

/**
 * Elements stored in a results buffer
 * are of this type. This is a union
 * of buffer information (first element of the buffer)
 * and query result (rest of the elements)
 */
typedef union _query_buffer_elem {
    query_result_t result;
    query_buffer_info_t info;
} query_buffer_elem_t;

#endif
