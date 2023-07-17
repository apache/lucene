#ifndef MATCHER_H_
#define MATCHER_H_

#include <stdbool.h>
#include <stdint.h>
#include "query_parser.h"

/**
 * structure used to match document id and positions of different terms in the phrase
 */
typedef struct _did_matcher did_matcher_t;

/**
 * Setup matchers for a given query. Internally lookup the posting addresses
 * and initialize one decoder per term at the posting address
 */
did_matcher_t *setup_matchers(query_parser_t* query_parser, uintptr_t index);
/**
 * Releases the matchers. Internally releases the decoders used by the matchers
 */
void release_matchers(did_matcher_t *matchers, uint32_t nr_terms);

/**
 * type returned by the function seeking the next document ID
 */
typedef enum {
    DID_NOT_FOUND = 0,
    DID_FOUND = 1,
    END_OF_INDEX_TABLE = -1,
} seek_did_t;

/**
 * Returns true if all matchers (for each term) have a next document id
 */
bool matchers_has_next_did(did_matcher_t *matchers, uint32_t nr_terms);
/**
 * Seek a new document id common to all matchers
 */
seek_did_t seek_did(did_matcher_t *matchers, uint32_t nr_terms, uint32_t pivot);
/**
 * Get the maximum document id among all matchers
 */
uint32_t get_max_did(did_matcher_t *matchers, uint32_t nr_terms);

/**
 * type returned by the function seeking the next position
 */
typedef enum {
    POSITIONS_NOT_FOUND = 0,
    POSITIONS_FOUND = 1,
    END_OF_POSITIONS = -1,
} seek_pos_t;

/**
 * Returns true if all matchers (for each term) have a next position for a document id
 */
bool matchers_has_next_pos(did_matcher_t *matchers, uint32_t nr_terms);
/**
 * Seek a new position common to all terms for a given document id
 */
seek_pos_t seek_pos(did_matcher_t *matchers, uint32_t nr_terms, uint32_t max_pos, uint32_t ref_index);
/**
 * Get the maximum position id among all matchers
 */
void get_max_pos_and_index(did_matcher_t *matchers, uint32_t nr_terms, uint32_t *index, uint32_t *max_pos);

void start_pos_matching(did_matcher_t *matchers, uint32_t nr_terms);
void stop_pos_matching(did_matcher_t *matchers, uint32_t nr_terms);

#endif /* MATCHER_H_ */
