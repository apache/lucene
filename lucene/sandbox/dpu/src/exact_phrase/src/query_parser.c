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

    seek_decoder(parser->decoder, parser->curr_ptr);
    *nr_terms = decode_vint_from(parser->decoder);
    parser->nr_terms = *nr_terms;
    parser->curr_ptr = get_absolute_address_from(parser->decoder);
}

void read_term(query_parser_t* parser, term_t* term) {

    seek_decoder(parser->decoder, parser->curr_ptr);
    term->size = decode_vint_from(parser->decoder);
    parser->curr_ptr = get_absolute_address_from(parser->decoder) + term->size;
    term->term_decoder = parser->decoder;
}
