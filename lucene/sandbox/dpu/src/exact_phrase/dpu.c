#include <stdint.h>
#include <defs.h>
#include <mram.h>
#include <barrier.h>
#include <mutex.h>
#include <assert.h>
#include "read_index.h"

#define PIM_PHRASE_QUERY_TYPE 1
#define DPU_QUERY_BATCH_BYTE_SIZE (1 << 18)
#define DPU_QUERY_MAX_BYTE_SIZE (1 << 8)
#define DPU_RESULTS_MAX_BYTE_SIZE (1 << 13)
#define DPU_MAX_BATCH_SIZE 256
#define MAX_NR_TERMS 32

/**
  Input queries
  */
__host uint32_t nb_queries_in_batch;
__host uint32_t nb_bytes_in_batch;
__mram_noinit char query_batch[DPU_QUERY_BATCH_BYTE_SIZE];
__host uint32_t query_offset_in_batch[DPU_MAX_BATCH_SIZE];

/**
  Output results
  */
__mram_noinit char results_batch[DPU_RESULTS_MAX_BYTE_SIZE];
__mram_noinit char results_index[DPU_MAX_BATCH_SIZE];

uint32_t batch_num = 0;
MUTEX_INIT(batch_mutex);
BARRIER_INIT(barrier, NR_TASKLETS);

/**
  Local variables for tasklets
  */
__dma_aligned uint8_t query_buffer[NR_TASKLETS][DPU_QUERY_MAX_BYTE_SIZE];

struct Block term_block[NR_TASKLETS];

seqreader_buffer_t index_seqread_buffer[NR_TASKLETS] = {0};
seqreader_t index_seqread[NR_TASKLETS];

seqreader_buffer_t postings_seqread_buffer[NR_TASKLETS][MAX_NR_TERMS] = {0};
seqreader_t postings_seqread[NR_TASKLETS][MAX_NR_TERMS];
const uint8_t* postings[NR_TASKLETS][MAX_NR_TERMS];

int main() {

    if(me() == 0) {
        batch_num = 0;
    }
    barrier_wait(&barrier);
    if(index_seqread_buffer[me()] == 0) {
        // initialize the seq reader buffer
        index_seqread_buffer[me()] = seqread_alloc();
        for(int i = 0; i < MAX_NR_TERMS; ++i) {
            postings_seqread_buffer[me()][i] = seqread_alloc();
        }
    }

    // each tasklet loops and take the next query in the batch, until no more queries
    uint32_t batch_num_tasklet;
    while(1) {
        mutex_lock(batch_mutex);
        batch_num_tasklet = batch_num++;
        mutex_unlock(batch_mutex);
        if(batch_num_tasklet >= nb_queries_in_batch)
            break;

        // start handling query
        // first read the query from MRAM using a seq reader
        // 1) get the type (for now expect it to be phrase query only)
        // 2) loop and read each word from the phrase query and call function that finds the posting address
        // from the word string (field table search, block table search)
        // 3) once we have all the posting addresses, we can start the phrase match algorithm. There may be an issue
        // if we have too many terms in the query. In that case, we cannot have one seq reader per word to read the
        // postings in parallel. At first we can put a limit on the number of words, and extend later to support less
        // than a sequential reader per word

        // load the query from MRAM
        int query_offset = query_offset_in_batch[batch_num_tasklet];
        int query_size;
        if(batch_num_tasklet + 1 < nb_queries_in_batch) {
            query_size = query_offset_in_batch[batch_num_tasklet + 1] - query_offset;
        } else {
            query_size = nb_bytes_in_batch - query_offset;
        }

        // the query size should not exceed the max size of the query buffer
        assert(query_size < DPU_QUERY_MAX_BYTE_SIZE);
        mram_read(query_batch + query_offset, query_buffer[me()], query_size);
        const uint8_t* query = query_buffer[me()];
        int segment = readVInt(&query);
        assert(*query == PIM_PHRASE_QUERY_TYPE);
        query++;
        int field_size = readVInt(&query);

        // initialize seq reader to point at the start of the index in MRAM
        const uint8_t* index = seqread_init(index_seqread_buffer[me()], DPU_MRAM_HEAP_POINTER, &index_seqread[me()]);
        // retrieve offsets to different sections of the index
        int block_offset = readVInt_mram(&index, &index_seqread[me()]);
        int block_list_offset = readVInt_mram(&index, &index_seqread[me()]);
        int postings_offset = readVInt_mram(&index, &index_seqread[me()]);
        __mram_ptr uint8_t* index_begin_addr = seqread_tell((void*)index, &index_seqread[me()]);

        // find the field block
        get_block_from_table(index, &index_seqread[me()], query, field_size, &term_block[me()]);

        // retrieve the block address for the field
        __mram_ptr uint8_t* field_block_address = index_begin_addr + block_offset + term_block[me()].block_address;

        // for each term in the phrase, look for its postings
        query += query_size;
        int nb_terms = readVInt(&query);
        for(int i = 0; i < nb_terms; ++i) {
            int term_size = readVInt(&query);
            // seek the seq reader to the field block address
            index = seqread_seek(field_block_address, &index_seqread[me()]);
            // get the postings address for this term
            __mram_ptr const uint8_t* postings_addr =
                    get_term_postings_from_index(index, &index_seqread[me()], query, term_size, &term_block[me()]);
            // seek the seq reader to the postings address
            postings[me()][i] = seqread_init(postings_seqread_buffer[me()][i], (__mram_ptr void*)postings_addr,
                                    &postings_seqread[me()][i]);
            query += term_size;
        }

        //TODO

    }

    return 0;
}
