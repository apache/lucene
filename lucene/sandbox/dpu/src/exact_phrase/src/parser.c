#include <defs.h>
#include <mram.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "common.h"
#include "decoder.h"
#include "term_lookup.h"

typedef struct _parser {
    decoder_t *decoder;
    struct {
        int32_t nr_pos_left; // How many positions still to be read
        uint32_t current_pos; // Last position recorded during the parsing
    } pos_parser;
    struct {
        uint32_t current_did; // Either the first DID of a new segment, or the last DID read in the current segment.
        uintptr_t did_end_addr;
    } did_parser;
} parser_t;

#include "parser.h"

static parser_t parsers[NR_TASKLETS][MAX_NR_TERMS];

// ============================================================================
// INIT PARSERS FUNCTIONS
// ============================================================================
static parser_t *initialize_parser(uintptr_t mram_addr, uint32_t term_id, uint32_t byte_size)
{
    parser_t *parser = &parsers[me()][term_id];

    parser->decoder = initialize_decoder(mram_addr, term_id);
    parser->did_parser.current_did = 0;
    parser->did_parser.did_end_addr = mram_addr + byte_size;

    return parser;
}

parser_t *setup_parser(uintptr_t field_block_address, const term_t* term)
{
    parser_t parser = parsers[me()][term->id];
    uint32_t byte_size = 0;
    uintptr_t postings_address = 0;

    if(!get_term_postings(field_block_address, term, &postings_address, &byte_size))
        return 0;

    return initialize_parser(postings_address, term->id, byte_size);
}

// ============================================================================
// PARSER DID FUNCTIONS
// ============================================================================

static void parse_length_and_freq(parser_t *parser, uint32_t *freq, unsigned int *len)
{
    int f = decode_zigzag_from(parser->decoder);
    if(f > 0) {
        *freq = f;
        *len = decode_byte_from(parser->decoder);
    }
    else if(f == 0) {
        *freq = decode_vint_from(parser->decoder);
        *len = decode_vint_from(parser->decoder);
    }
    else {
        *freq = -f;
        *len = decode_short_from(parser->decoder);
    }
}

parse_did_t parse_did(parser_t *parser, uint32_t *did, unsigned int *freq)
{
    if(get_absolute_address_from(parser->decoder) >= parser->did_parser.did_end_addr)
        return END_OF_FRAGMENT;

    uint32_t delta_did = decode_vint_from(parser->decoder);
    uint32_t len;
    parse_length_and_freq(parser, freq, &len);
    uint32_t next_did = parser->did_parser.current_did + delta_did;

    *did = parser->did_parser.current_did = next_did;

    return DOC_INFO;
}

void abort_parse_did(parser_t *parser)
{
    skip_bytes_to_jump(parser->decoder, parser->did_parser.did_end_addr);
}

// ============================================================================
// PARSER POS FUNCTIONS
// ============================================================================

// Sets up the parser to decode a series of positions. The parameter specifies
// the size of the position list, in bytes
void prepare_to_parse_pos_list(parser_t *parser, uint32_t freq)
{
    parser->pos_parser.nr_pos_left = (int32_t)freq;
    parser->pos_parser.current_pos = 0;
}

// Gets the next position. Returns true if more positions are expected.
bool parse_pos(parser_t *parser, uint32_t *pos)
{
    if (parser->pos_parser.nr_pos_left <= 0)
        return false;

    uint32_t this_pos = decode_vint_from(parser->decoder);

    parser->pos_parser.nr_pos_left--;
    parser->pos_parser.current_pos += this_pos;

    *pos = parser->pos_parser.current_pos;
    return true;
}

// Aborts the parsing of positions, moving the cursor to the next
// document or segment descriptor.
void abort_parse_pos(parser_t *parser) { skip_bytes_to_jump(parser->decoder, parser->did_parser.did_end_addr); }
