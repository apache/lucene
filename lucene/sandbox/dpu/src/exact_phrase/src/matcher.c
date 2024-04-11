#include <defs.h>

#include "parser.h"

typedef struct _did_matcher {
    parser_t *parser;
    uint32_t current_did;
    uint32_t current_pos_freq;
    uint32_t current_pos_len;
    uint32_t current_pos;
} did_matcher_t;

#include "matcher.h"

static did_matcher_t matchers[NR_TASKLETS][MAX_NR_TERMS];

// =============================================================================
// INIT MATCHERS FUNCTIONS
// =============================================================================
did_matcher_t *
setup_matchers(uint32_t nr_terms, postings_info_t *postings, uint32_t start_did)
{
    allocate_parsers(nr_terms);
    did_matcher_t *tasklet_matchers = matchers[me()];
    for (int each_term = 0; each_term < nr_terms; each_term++) {
        did_matcher_t *matcher = &tasklet_matchers[each_term];
        if (postings[each_term].size == 0) {
            // this means there are no postings for this term for this segment
            release_parsers(nr_terms);
            return 0;
        }
        matcher->parser = setup_parser(each_term, postings[each_term].addr, postings[each_term].size, start_did);
    }
    return tasklet_matchers;
}

void
release_matchers(did_matcher_t *matchers, uint32_t nr_terms)
{
    if (matchers == 0)
        return;
    release_parsers(nr_terms);
}

// =============================================================================
// DID MATCHING FUNCTIONS
// =============================================================================
static bool
matcher_has_next_did(did_matcher_t *matcher)
{
    uint32_t did;
    uint32_t freq;
    uint32_t len;
    parse_did_t next_item = parse_did(matcher->parser, &did, &freq, &len);

    if (next_item == END_OF_FRAGMENT)
        return false;

    matcher->current_did = did;
    matcher->current_pos_freq = freq;
    matcher->current_pos_len = len;
    return true;
}

bool
matchers_has_next_did(did_matcher_t *matchers, uint32_t nr_terms)
{
    for (uint32_t i = 0; i < nr_terms; i++) {
        if (!matcher_has_next_did(&matchers[i]))
            return false;
    }
    return true;
}

seek_did_t
seek_did(did_matcher_t *matchers, uint32_t nr_terms, uint32_t pivot)
{
    uint32_t nb_matches = 0;
    for (uint32_t i = 0; i < nr_terms; i++) {
        while (true) {
            uint32_t did = matchers[i].current_did;
            if (did == pivot) {
                nb_matches++;
                break;
            } else if (did > pivot) {
                break;
            }
            abort_parse_did(matchers[i].parser, matchers[i].current_pos_len);
            if (!matcher_has_next_did(&matchers[i]))
                return END_OF_INDEX_TABLE;
        }
    }
    if (nb_matches == nr_terms) {
        return DID_FOUND;
    }
    return DID_NOT_FOUND;
}

uint32_t
get_max_did(did_matcher_t *matchers, uint32_t nr_terms)
{
    uint32_t max = 0;
    for (uint32_t i = 0; i < nr_terms; i++) {
        uint32_t this_did = matchers[i].current_did;
        if (this_did > max)
            max = this_did;
    }
    return max;
}

void
abort_did(did_matcher_t *matchers, uint32_t nr_terms)
{

    for (uint32_t i = 0; i < nr_terms; i++) {
        abort_parse_did(matchers[i].parser, matchers[i].current_pos_len);
    }
}

// ============================================================================
// POS MATCHING FUNCTIONS
// ============================================================================
bool
matchers_has_next_pos(did_matcher_t *matchers, uint32_t nr_terms)
{
    bool parsers_has_next = true;
    for (uint32_t i = 0; i < nr_terms; i++) {
        parsers_has_next &= parse_pos(matchers[i].parser, &(matchers[i].current_pos));
    }
    return parsers_has_next;
}

seek_pos_t
seek_pos(did_matcher_t *matchers, uint32_t nr_terms, uint32_t max_pos, uint32_t ref_index)
{
    uint32_t nb_matches = 0;
    bool no_pos_inc = true;
    for (uint32_t i = 0; i < nr_terms; i++) {
        uint32_t pivot = max_pos + i - ref_index;
        while (true) {
            if (matchers[i].current_pos == pivot) {
                nb_matches++;
                break;
            } else if (matchers[i].current_pos > pivot) {
                break;
            }
            no_pos_inc = false;
            if (!parse_pos(matchers[i].parser, &(matchers[i].current_pos)))
                return END_OF_POSITIONS;
        }
    }
    if (nb_matches == nr_terms) {
        return POSITIONS_FOUND;
    }
    if (no_pos_inc) {
        // corner case where every word either match or is at a position
        // higher than the pivot. This may happen with doubled words in the search sentence
        // In this case we must look for the next word
        // otherwise we end up in an infinite loop.
        if (!parse_pos(matchers[nr_terms - 1].parser, &(matchers[nr_terms - 1].current_pos)))
            return END_OF_POSITIONS;
    }
    return POSITIONS_NOT_FOUND;
}

void
get_max_pos_and_index(did_matcher_t *matchers, uint32_t nr_terms, uint32_t *index, uint32_t *max_pos)
{
    uint32_t _max_pos = 0;
    uint32_t _index;
    for (uint32_t i = 0; i < nr_terms; i++) {
        uint32_t pos = matchers[i].current_pos;
        if (pos > _max_pos) {
            _max_pos = pos;
            _index = i;
        }
    }
    *max_pos = _max_pos;
    *index = _index;
}

void
start_pos_matching(did_matcher_t *matchers, uint32_t nr_terms)
{
    for (uint32_t i = 0; i < nr_terms; i++) {
        prepare_to_parse_pos_list(matchers[i].parser, matchers[i].current_pos_freq, matchers[i].current_pos_len);
    }
}

void
stop_pos_matching(did_matcher_t *matchers, uint32_t nr_terms)
{
    for (uint32_t i = 0; i < nr_terms; i++) {
        abort_parse_pos(matchers[i].parser);
    }
}

uintptr_t
matcher_get_curr_address(did_matcher_t *matchers, uint32_t term_id)
{

    return parser_get_curr_address(matchers[term_id].parser);
}

uint32_t
matcher_get_curr_freq(did_matcher_t *matchers, uint32_t term_id)
{

    return matchers[term_id].current_pos_freq;
}

uint32_t
matcher_get_curr_did(did_matcher_t *matchers, uint32_t term_id)
{

    return matchers[term_id].current_did;
}
