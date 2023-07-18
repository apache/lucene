#ifndef QUERY_PARSER_H_
#define QUERY_PARSER_H_

#include <stdbool.h>
#include <stdint.h>
#include <mram.h>
#include "common.h"
#include "term.h"
#include "decoder.h"

/**
 * Structure used to parse the query
 */
typedef struct _query_parser {
    decoder_t* decoder;
    uint32_t nr_terms;
    uintptr_t curr_ptr;
} query_parser_t;

void init_query_parser(query_parser_t* parser, __mram_ptr const uint8_t* query);
void read_segment_id(query_parser_t* parser, uint32_t* segment_id);
void read_query_type(query_parser_t* parser, uint8_t* query_type);
void read_field(query_parser_t* parser, term_t* field);
void read_nr_terms(query_parser_t* parser, uint32_t* nr_terms);
void read_term(query_parser_t* parser, term_t* term);
void release_query_parser(query_parser_t* parser);

#endif /* QUERY_PARSER_H_ */
