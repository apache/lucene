#include <assert.h>
#include "query_parser.h"

#define PIM_PHRASE_QUERY_TYPE 1

static int read_vint(const uint8_t** data) {
    int i = **data & 0x7F;
    for (int shift = 7; (**data & 0x80) != 0; shift += 7) {
        *data+=1;
        i |= ((**data) & 0x7F) << shift;
    }
    *data+=1;
    return i;
}

static void parser_query_term(query_parser_t* parser, const uint8_t** query, uint32_t id) {

    term_t* term = &(parser->terms[id]);
    term->id = id;
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
