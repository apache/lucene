#include <defs.h>

#include "parser.h"
#include "term_lookup.h"

typedef struct _did_matcher {
    parser_t *parser;
    uint32_t current_did;
    uint32_t current_pos_freq;
    uint32_t current_pos;
} did_matcher_t;

#include "matcher.h"

static did_matcher_t matchers[NR_TASKLETS][MAX_NR_TERMS];


// =============================================================================
// INIT MATCHERS FUNCTIONS
// =============================================================================
did_matcher_t *setup_matchers(query_parser_t query_parser)
{
    // lookup the field block table address, if not found return 0
    // do it only once for all the terms
    uintptr_t field_address;
    if(!get_field_address((uintptr_t)DPU_MRAM_HEAP_POINTER, &(query_parser.field), &field_address))
        return 0;

    // setup the postings parser for each term
    did_matcher_t *tasklet_matchers = matchers[me()];
    for (int each_term = 0; each_term < query_parser.nr_terms; each_term++) {
        did_matcher_t *matcher = &tasklet_matchers[each_term];
        matcher->parser = setup_parser(field_address, &(query_parser.terms[each_term]));
        if(matcher->parser == 0) {
            // if the parser is null, this means the term was not found in the index
            return 0;
        }
    }
    return tasklet_matchers;
}

// =============================================================================
// DID MATCHING FUNCTIONS
// =============================================================================
static bool matcher_has_next_did(did_matcher_t *matcher)
{
    uint32_t did;
    uint32_t freq;
    parse_did_t next_item = parse_did(matcher->parser, &did, &freq);

    if (next_item == END_OF_FRAGMENT)
        return false;

    matcher->current_did = did;
    matcher->current_pos_freq = freq;
    return true;
}

bool matchers_has_next_did(did_matcher_t *matchers, uint32_t nr_terms)
{
    for (uint32_t i = 0; i < nr_terms; i++) {
        if (!matcher_has_next_did(&matchers[i]))
            return false;
    }
    return true;
}

seek_did_t seek_did(did_matcher_t *matchers, uint32_t nr_terms, uint32_t pivot)
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
            abort_parse_did(matchers[i].parser);
            if (!matcher_has_next_did(&matchers[i]))
                return END_OF_INDEX_TABLE;
        }
    }
    if (nb_matches == nr_terms) {
        return DID_FOUND;
    }
    return DID_NOT_FOUND;
}

uint32_t get_max_did(did_matcher_t *matchers, uint32_t nr_terms)
{
    uint32_t max = 0;
    for (uint32_t i = 0; i < nr_terms; i++) {
        uint32_t this_did = matchers[i].current_did;
        if (this_did > max)
            max = this_did;
    }
    return max;
}

// ============================================================================
// POS MATCHING FUNCTIONS
// ============================================================================
bool matchers_has_next_pos(did_matcher_t *matchers, uint32_t nr_terms)
{
    bool parsers_has_next = true;
    for (uint32_t i = 0; i < nr_terms; i++) {
        parsers_has_next &= parse_pos(matchers[i].parser, &(matchers[i].current_pos));
    }
    return parsers_has_next;
}

seek_pos_t seek_pos(did_matcher_t *matchers, uint32_t nr_terms, uint32_t max_pos, uint32_t ref_index)
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

void get_max_pos_and_index(did_matcher_t *matchers, uint32_t nr_terms, uint32_t *index, uint32_t *max_pos)
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

void start_pos_matching(did_matcher_t *matchers, uint32_t nr_terms)
{
    for (uint32_t i = 0; i < nr_terms; i++) {
        prepare_to_parse_pos_list(matchers[i].parser, matchers[i].current_pos_freq);
    }
}

void stop_pos_matching(did_matcher_t *matchers, uint32_t nr_words)
{
    for (uint32_t i = 0; i < nr_words; i++) {
        abort_parse_pos(matchers[i].parser);
    }
}
