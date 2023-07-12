#ifndef MATCHER_H_
#define MATCHER_H_

#include <stdbool.h>
#include <stdint.h>
#include "query_parser.h"

typedef struct _did_matcher did_matcher_t;

did_matcher_t *setup_matchers(query_parser_t query_parser, uintptr_t index);

typedef enum {
    DID_NOT_FOUND = 0,
    DID_FOUND = 1,
    END_OF_INDEX_TABLE = -1,
} seek_did_t;

bool matchers_has_next_did(did_matcher_t *matchers, uint32_t nr_terms);
seek_did_t seek_did(did_matcher_t *matchers, uint32_t nr_terms, uint32_t pivot);
uint32_t get_max_did(did_matcher_t *matchers, uint32_t nr_terms);

typedef enum {
    POSITIONS_NOT_FOUND = 0,
    POSITIONS_FOUND = 1,
    END_OF_POSITIONS = -1,
} seek_pos_t;

bool matchers_has_next_pos(did_matcher_t *matchers, uint32_t nr_terms);
seek_pos_t seek_pos(did_matcher_t *matchers, uint32_t nr_terms, uint32_t max_pos, uint32_t ref_index);
void get_max_pos_and_index(did_matcher_t *matchers, uint32_t nr_terms, uint32_t *index, uint32_t *max_pos);

void start_pos_matching(did_matcher_t *matchers, uint32_t nr_terms);
void stop_pos_matching(did_matcher_t *matchers, uint32_t nr_terms);

#endif /* MATCHER_H_ */
