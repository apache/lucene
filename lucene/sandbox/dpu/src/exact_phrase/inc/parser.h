#ifndef PARSER_H_
#define PARSER_H_

#include <stdbool.h>
#include <stdint.h>
#include "term.h"

typedef enum {
    DOC_INFO, // Description of a new document/positions
    SKIP_INFO, // Starting a new segment, get the next 100th DID and the length of this segment
    END_OF_FRAGMENT,
} parse_did_t;

typedef struct _parser parser_t;

parse_did_t parse_did(parser_t *parser, uint32_t *did, uint32_t *freq, uint32_t* len);
void abort_parse_did(parser_t *parser);

void prepare_to_parse_pos_list(parser_t *parser, uint32_t freq, uint32_t len);
bool parse_pos(parser_t *parser, uint32_t *pos);
void abort_parse_pos(parser_t *parser);

void allocate_parsers(uint32_t nr_terms);
parser_t *setup_parser(uintptr_t field_block_address, const term_t* term, uint32_t id);
void release_parsers(uint32_t nr_terms);

#endif /* PARSER_H_ */
