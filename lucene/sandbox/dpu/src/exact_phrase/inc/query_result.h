#ifndef QUERY_RESULT_H_
#define QUERY_RESULT_H_

/**
 * structure used to store the query results
 * A result is a pair of document id and frequency.
 */
typedef struct _query_result {
    uint32_t doc_id;
    uint32_t freq;
} query_result_t;

/**
 * Results are stored in several buffers of fixed size.
 * The first element of the buffer contains information
 * stored in the following structure.
 * Stores the buffer id, the number of results in the buffer,
 * the dpu segment id and the query id.
 */
typedef struct _query_buffer_info {
    uint32_t buffer_id;
    uint8_t buffer_size;
    uint8_t segment_id;
    uint16_t query_id;
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
