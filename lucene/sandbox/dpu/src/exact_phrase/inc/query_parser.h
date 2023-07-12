#ifndef QUERY_PARSER_H_
#define QUERY_PARSER_H_

#include <stdbool.h>
#include <stdint.h>
#include "common.h"
#include "term.h"

typedef struct _query_parser {
    uint32_t segment;
    term_t field;
    term_t terms[MAX_NR_TERMS];
    uint32_t nr_terms;
} query_parser_t;

void parse_query(query_parser_t* parser, const uint8_t* query);

#endif /* QUERY_PARSER_H_ */
