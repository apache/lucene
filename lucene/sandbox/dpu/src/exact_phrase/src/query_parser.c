#include <assert.h>
#include "query_parser.h"

void init_query_parser(query_parser_t* parser, __mram_ptr const uint8_t* query) {
    parser->nr_terms = 0;
    parser->decoder = decoder_pool_get_one();
    initialize_decoder(parser->decoder, (uintptr_t)query);
    parser->curr_ptr = (uintptr_t)query;
}

void read_segment_id(query_parser_t* parser, uint32_t* segment_id) {
    *segment_id = decode_vint_from(parser->decoder);
    parser->curr_ptr = get_absolute_address_from(parser->decoder);
}

void read_query_type(query_parser_t* parser, uint8_t* query_type) {
    *query_type = decode_byte_from(parser->decoder);
    parser->curr_ptr = get_absolute_address_from(parser->decoder);
}

void read_field(query_parser_t* parser, term_t* field) {
    read_term(parser, field);
}

void read_nr_terms(query_parser_t* parser, uint32_t* nr_terms) {

    skip_bytes_to_jump(parser->decoder, parser->curr_ptr);
    *nr_terms = decode_vint_from(parser->decoder);
    parser->nr_terms = *nr_terms;
    parser->curr_ptr = get_absolute_address_from(parser->decoder);
}

void read_term(query_parser_t* parser, term_t* term) {

    skip_bytes_to_jump(parser->decoder, parser->curr_ptr);
    term->size = decode_vint_from(parser->decoder);
    parser->curr_ptr = get_absolute_address_from(parser->decoder) + term->size;
    term->term_decoder = parser->decoder;
}

/*
static void parser_query_term(query_parser_t* parser, const uint8_t** query, uint32_t id) {

    term_t* term = &(parser->terms[id]);
    term->size = read_vint(query);
    term->term = *query;

    *query += term->size;
}

void parse_query(query_parser_t* parser, const uint8_t* query) {

    parser->segment = read_vint(&query);
    // for now support only the phrase query
    assert(*query == PIM_PHRASE_QUERY_TYPE);
    query++;
    parser->field.size = read_vint(&query);
    parser->field.term = query;
    query += parser->field.size;
    parser->nr_terms = read_vint(&query);
    for(int i = 0; i < parser->nr_terms; i++) {
        parser_query_term(parser, &query, i);
    }
}
*/
