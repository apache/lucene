#ifndef MATCHER_H_
#define MATCHER_H_

#include <stdbool.h>
#include <stdint.h>

#include "query_parser.h"
#include "postings_util.h"

/**
 * structure used to match document id and positions of different terms in the phrase
 */
typedef struct did_matcher_s did_matcher_t;

/**
 * Setup matchers to decode the posting addresses and sizes passed as input for each term
 * initialize one decoder per term at the posting address
 */
did_matcher_t *
setup_matchers(uint32_t nr_terms, postings_info_t *postings, uint32_t start_did);
/**
 * Releases the matchers. Internally releases the decoders used by the matchers
 */
void
release_matchers(did_matcher_t *matchers, uint32_t nr_terms);

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
bool
matchers_has_next_did(did_matcher_t *matchers, uint32_t nr_terms);
/**
 * Seek a new document id common to all matchers
 */
seek_did_t
seek_did(did_matcher_t *matchers, uint32_t nr_terms, uint32_t pivot);
/**
 * Get the maximum document id among all matchers
 */
uint32_t
get_max_did(did_matcher_t *matchers, uint32_t nr_terms);

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
bool
matchers_has_next_pos(did_matcher_t *matchers, uint32_t nr_terms);
/**
 * Seek a new position common to all terms for a given document id
 */
seek_pos_t
seek_pos(did_matcher_t *matchers, uint32_t nr_terms, uint32_t max_pos, uint32_t ref_index);
/**
 * Get the maximum position id among all matchers
 */
void
get_max_pos_and_index(did_matcher_t *matchers, uint32_t nr_terms, uint32_t *index, uint32_t *max_pos);
/**
 * Returns the current address of the matcher for the given term, where the next did or position would be searched
 */
mram_ptr_t
matcher_get_curr_address(did_matcher_t *matchers, uint32_t term_id);
/**
 * Returns the current frequency for the given term and for the doc at which the matcher is positioned
 */
uint32_t
matcher_get_curr_freq(did_matcher_t *matchers, uint32_t term_id);
/**
 * Returns the current doc id for the given term
 */
uint32_t
matcher_get_curr_did(did_matcher_t *matchers, uint32_t term_id);

void
start_pos_matching(did_matcher_t *matchers, uint32_t nr_terms);
void
stop_pos_matching(did_matcher_t *matchers, uint32_t nr_terms);
void
abort_did(did_matcher_t *matchers, uint32_t nr_terms);

#endif /* MATCHER_H_ */
