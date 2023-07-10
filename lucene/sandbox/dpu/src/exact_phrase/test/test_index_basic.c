#include <stdio.h>
#include <seqread.h>
#include "../read_index.h"

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

uint8_t field1_arr[6] = {0x66, 0x69, 0x65, 0x6c, 0x64, 0x31};
struct Term field1 = {field1_arr, 6};
uint32_t field1_addr = 7;

uint8_t field2_arr[6] = {0x66, 0x69, 0x65, 0x6c, 0x64, 0x32};
struct Term field2 = {field2_arr, 6};
uint32_t field2_addr = 16;

uint8_t field3_arr[6] = {0x66, 0x69, 0x65, 0x6c, 0x32, 0x64};
struct Term field3 = {field3_arr, 6};

uint8_t green_arr[5] = {0x67, 0x72, 0x65, 0x65, 0x6e};
struct Term green = {green_arr, 5};
uint32_t green_addr = 14;

uint8_t orange_arr[6] = {0x6f, 0x72, 0x61, 0x6e, 0x67, 0x65};
struct Term orange = {orange_arr, 6};
uint32_t orange_addr = 53;

struct Block term_block;

int zigZagDecode(int i) {
    return ((((uint32_t)i) >> 1) ^ -(i & 1));
}

int main() {

    // init sequential reader to read the index
    seqreader_buffer_t buffer = seqread_alloc();
    seqreader_t seqread;

    // look for the field "field1"
    const uint8_t* index_ptr = seqread_init(buffer, index_mram, &seqread);
    //skip offsets first
    int block_offset = readVInt_mram(&index_ptr, &seqread);
    int block_list_offset = readVInt_mram(&index_ptr, &seqread);
    int postings_offset = readVInt_mram(&index_ptr, &seqread);
    __mram_ptr uint8_t* index_begin_addr = seqread_tell((void*)index_ptr, &seqread);

    get_block_from_table(index_ptr, &seqread, &field1, &term_block);
    if(term_block.block_address == field1_addr) {
        printf("field1 OK\n");
    } else {
        printf("field1 KO: %d\n", (int)term_block.term);
    }
    // search for the term "green"
    __mram_ptr uint8_t* block_address = index_begin_addr + block_offset + term_block.block_address;
    index_ptr = seqread_seek(block_address, &seqread);
    get_block_from_table(index_ptr, &seqread, &green, &term_block);
    if(term_block.block_address == green_addr) {
        printf("green OK\n");
    } else {
        printf("green KO: %d\n", (int)term_block.term);
    }
    // lookup for the green term postings
    index_ptr = seqread_seek(block_address, &seqread);
    __mram_ptr const uint8_t* postings_addr =
                    get_term_postings_from_index(index_ptr, &seqread,
                            &green, index_begin_addr + block_list_offset,
                            index_begin_addr + postings_offset, &term_block);
    // read postings
    index_ptr = seqread_seek((__mram_ptr void*)postings_addr, &seqread);
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
    index_ptr = seqread_init(buffer, index_mram, &seqread);
    //skip offsets first
    block_offset = readVInt_mram(&index_ptr, &seqread);
    block_list_offset = readVInt_mram(&index_ptr, &seqread);
    postings_offset = readVInt_mram(&index_ptr, &seqread);
    get_block_from_table(index_ptr, &seqread, &field2, &term_block);
    if(term_block.block_address == field2_addr) {
        printf("field2 OK\n");
    } else {
        printf("field2 KO: %d\n", (int)term_block.term);
    }
    //searching for the term orange
    block_address = index_begin_addr + block_offset + term_block.block_address;
    index_ptr = seqread_seek(block_address, &seqread);
    get_block_from_table(index_ptr, &seqread, &orange, &term_block);
    if(term_block.block_address == orange_addr) {
        printf("orange OK\n");
    } else {
        printf("orange KO: %d\n", (int)term_block.term);
    }

    // lookup for the green term postings
    index_ptr = seqread_seek(block_address, &seqread);
    postings_addr =
                    get_term_postings_from_index(index_ptr, &seqread,
                            &green, index_begin_addr + block_list_offset,
                            index_begin_addr + postings_offset, &term_block);
    // read postings
    index_ptr = seqread_seek((__mram_ptr void*)postings_addr, &seqread);
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
    index_ptr = seqread_seek(block_address, &seqread);
    postings_addr =
                    get_term_postings_from_index(index_ptr, &seqread,
                            &orange, index_begin_addr + block_list_offset,
                            index_begin_addr + postings_offset, &term_block);
    // read postings
    index_ptr = seqread_seek((__mram_ptr void*)postings_addr, &seqread);
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
    index_ptr = seqread_init(buffer, index_mram, &seqread);
    //skip offsets first
    block_offset = readVInt_mram(&index_ptr, &seqread);
    block_list_offset = readVInt_mram(&index_ptr, &seqread);
    postings_offset = readVInt_mram(&index_ptr, &seqread);
    get_block_from_table(index_ptr, &seqread, &field3, &term_block);
    if(term_block.term == 0) {
        printf("field3 OK\n");
    }
    else {
        printf("field3 KO: %d\n", (int)term_block.term);
    }

    return 0;
}
