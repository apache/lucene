#ifndef QUERY_RESULT_H_
#define QUERY_RESULT_H_

typedef struct _query_result {
    uint32_t doc_id;
    uint32_t freq;
} query_result_t;

typedef struct _query_buffer_info {
    uint32_t buffer_id;
    uint16_t buffer_size;
    uint16_t query_id;
} query_buffer_info_t;

typedef union _query_buffer_elem {
    query_result_t result;
    query_buffer_info_t info;
} query_buffer_elem_t;

#endif