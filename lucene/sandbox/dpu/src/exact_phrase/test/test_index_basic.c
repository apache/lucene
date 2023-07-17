#include <stdio.h>
#include <seqread.h>
#include "../inc/term_lookup.h"
#include "../inc/decoder.h"

/**
* Test for the function get_block_from_table
* A small index is stored in MRAM
* Test to find two fields field1 and field2 in the block table
* Test to find two terms green and orange in the block table of each field
*/

__mram uint8_t index_mram[217] = {
   0x18, 0x31, 0x87, 0x1, 0x6, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x32, 0x25, 0x10, 0x6, 0x66, 0x69, 0x65, 0x6c,
   0x64, 0x31, 0x0, 0x7, 0x2, 0x69, 0x64, 0x0, 0x0, 0x19, 0x3, 0x41, 0x41, 0x41, 0x0, 0x0, 0xe, 0x5, 0x62, 0x6c,
   0x61, 0x63, 0x6b, 0x0, 0xe, 0x35, 0x5, 0x62, 0x72, 0x6f, 0x77, 0x6e, 0x0, 0x35, 0x56, 0x0, 0x4, 0x3, 0x41, 0x41,
   0x42, 0x4, 0x4, 0x3, 0x41, 0x41, 0x43, 0x8, 0x4, 0xc, 0x8, 0x4, 0x62, 0x6c, 0x75, 0x65, 0x14, 0x8, 0x5, 0x67,
   0x72, 0x65, 0x65, 0x6e, 0x1c, 0x4, 0x4, 0x70, 0x69, 0x6e, 0x6b, 0x20, 0x4, 0x3, 0x72, 0x65, 0x64, 0x24, 0x4, 0x6,
   0x79, 0x65, 0x6c, 0x6c, 0x6f, 0x77, 0x28, 0x8, 0x30, 0x4, 0x5, 0x67, 0x72, 0x65, 0x65, 0x6e, 0x34, 0x4, 0x6, 0x6f,
   0x72, 0x61, 0x6e, 0x67, 0x65, 0x38, 0x5, 0x3, 0x72, 0x65, 0x64, 0x3d, 0x9, 0x5, 0x77, 0x68, 0x69, 0x74, 0x65, 0x46,
   0x8, 0x0, 0x2, 0x1, 0x0, 0x1, 0x2, 0x1, 0x0, 0x2, 0x2, 0x1, 0x0, 0x0, 0x2, 0x1, 0x1, 0x2, 0x2, 0x1,
   0x0, 0x1, 0x2, 0x1, 0x2, 0x1, 0x2, 0x1, 0x1, 0x1, 0x2, 0x1, 0x1, 0x2, 0x2, 0x1, 0x2, 0x0, 0x2, 0x1,
   0x0, 0x0, 0x2, 0x1, 0x2, 0x1, 0x2, 0x1, 0x0, 0x2, 0x2, 0x1, 0x1, 0x1, 0x2, 0x1, 0x0, 0x0, 0x4, 0x2,
   0x1, 0x2, 0x0, 0x4, 0x2, 0x0, 0x4, 0x1, 0x2, 0x1, 0x1, 0x0, 0x2, 0x1, 0x2, 0x2, 0x2, 0x1,
   0x0};

__mram uint8_t field1_arr[6] = {0x66, 0x69, 0x65, 0x6c, 0x64, 0x31};
term_t field1 = {0, 6};
uint32_t field1_addr = 7;

__mram uint8_t field2_arr[6] = {0x66, 0x69, 0x65, 0x6c, 0x64, 0x32};
term_t field2 = {0, 6};
uint32_t field2_addr = 16;

__mram uint8_t field3_arr[6] = {0x66, 0x69, 0x65, 0x6c, 0x32, 0x64};
term_t field3 = {0, 6};

__mram uint8_t green_arr[5] = {0x67, 0x72, 0x65, 0x65, 0x6e};
term_t green = {0, 5};
uint32_t green_addr = 14;

__mram uint8_t orange_arr[6] = {0x6f, 0x72, 0x61, 0x6e, 0x67, 0x65};
term_t orange = {0, 6};
uint32_t orange_addr = 53;

// read a variable length integer in MRAM
int readVInt_mram(const uint8_t** data, seqreader_t* reader) {
    int i = **data & 0x7F;
    for (int shift = 7; (**data & 0x80) != 0; shift += 7) {
        *data = seqread_get((void*)*data, 1, reader);
        i |= ((**data) & 0x7F) << shift;
    }
    *data = seqread_get((void*)*data, 1, reader);
    return i;
}

int readByte_mram(const uint8_t** data, seqreader_t* reader) {
    int i = **data & 0x7F;
    *data = seqread_get((void*)*data, 1, reader);
    return i;
}

int zigZagDecode(int i) {
    return ((((uint32_t)i) >> 1) ^ -(i & 1));
}

int main() {

    initialize_decoder_pool();

    // init sequential reader to read the index
    seqreader_buffer_t buffer = seqread_alloc();
    seqreader_t seqread;
    const uint8_t* index_ptr = seqread_init(buffer, index_mram, &seqread);
    int block_offset = readVInt_mram(&index_ptr, &seqread);
    int block_list_offset = readVInt_mram(&index_ptr, &seqread);
    int postings_offset = readVInt_mram(&index_ptr, &seqread);
    uintptr_t index_begin_addr = (uintptr_t)seqread_tell((void*)index_ptr, &seqread);

    // look for field1
    decoder_t* decoder = decoder_pool_get_one();
    initialize_decoder(decoder, (uintptr_t)field1_arr);
    field1.term_decoder = decoder;
    uintptr_t field_address;
    if(get_field_address((uintptr_t)index_mram, &field1, &field_address)
            && (field_address == (index_begin_addr + block_offset + field1_addr))) {
        printf("field1 OK\n");
    } else {
        printf("field1 KO: %d\n", field_address);
    }
    
    // lookup for the green term postings
    uintptr_t postings_address;
    uint32_t postings_byte_size;
    initialize_decoder(decoder, (uintptr_t)&green_arr[0]);
    green.term_decoder = decoder;
    get_term_postings(field_address, &green, &postings_address, &postings_byte_size);

    // read postings
    index_ptr = seqread_seek((__mram_ptr void*)postings_address, &seqread);
    uint32_t doc_id = readVInt_mram(&index_ptr, &seqread);
    uint32_t freq = zigZagDecode(readVInt_mram(&index_ptr, &seqread));
    uint32_t length = readByte_mram(&index_ptr, &seqread);
    uint32_t pos = readVInt_mram(&index_ptr, &seqread);
    if(doc_id == 1 && freq == 1 && pos == 1) {
        printf("green postings OK: doc:%d freq:%d pos:%d\n", doc_id, freq, pos);
    } else {
        printf("green postings KO: doc:%d freq:%d pos:%d\n", doc_id, freq, pos);
    }


    // look for field "field2"
    initialize_decoder(decoder, (uintptr_t)&field2_arr[0]);
    field2.term_decoder = decoder;
    if(get_field_address((uintptr_t)index_mram, &field2, &field_address)
                && (field_address == (index_begin_addr + block_offset + field2_addr))) {
        printf("field2 OK\n");
    } else {
        printf("field2 KO: %d\n", field_address);
    }

    // lookup for the green term postings
    initialize_decoder(decoder, (uintptr_t)&green_arr[0]);
    green.term_decoder = decoder;
    get_term_postings(field_address, &green, &postings_address, &postings_byte_size);

    // read postings
    index_ptr = seqread_seek((__mram_ptr void*)postings_address, &seqread);
    doc_id = readVInt_mram(&index_ptr, &seqread);
    freq = zigZagDecode(readVInt_mram(&index_ptr, &seqread));
    length = readByte_mram(&index_ptr, &seqread);
    pos = readVInt_mram(&index_ptr, &seqread);
    if(doc_id == 1 && freq == 1 && pos == 0) {
        printf("green postings OK: doc:%d freq:%d pos:%d\n", doc_id, freq, pos);
    } else {
        printf("green postings KO: doc:%d freq:%d pos:%d\n", doc_id, freq, pos);
    }

    // lookup for the orange term postings
    initialize_decoder(decoder, (uintptr_t)orange_arr);
    orange.term_decoder = decoder;
    get_term_postings(field_address, &orange, &postings_address, &postings_byte_size);

    // read postings
    index_ptr = seqread_seek((__mram_ptr void*)postings_address, &seqread);
    doc_id = readVInt_mram(&index_ptr, &seqread);
    freq = zigZagDecode(readVInt_mram(&index_ptr, &seqread));
    length = readByte_mram(&index_ptr, &seqread);
    pos = readVInt_mram(&index_ptr, &seqread);
    uint32_t pos2 = readVInt_mram(&index_ptr, &seqread) + pos;
    if(doc_id == 0 && freq == 2 && pos == 1 && pos2 == 3) {
        printf("orange postings OK: doc:%d freq:%d pos:%d pos:%d\n", doc_id, freq, pos, pos2);
    } else {
        printf("orange postings KO: doc:%d freq:%d pos:%d pos:%d\n", doc_id, freq, pos, pos2);
    }

    // look for field "field3"
    initialize_decoder(decoder, (uintptr_t)field3_arr);
    field3.term_decoder = decoder;
    if(!get_field_address((uintptr_t)index_mram, &field3, &field_address)) {
        printf("field3 OK\n");
    }
    else {
        printf("field3 KO: %d\n", field_address);
    }

    decoder_pool_release_one(decoder);

    return 0;
}
