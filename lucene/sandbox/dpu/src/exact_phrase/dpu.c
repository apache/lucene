#include <stdint.h>
#include <defs.h>
#include <mram.h>
#include <barrier.h>
#include <mutex.h>
#define SEQREAD_CACHE_SIZE 256
#include <seqread.h>
#include <built_ins.h>
#include <assert.h>

#define PIM_PHRASE_QUERY_TYPE 1
#define DPU_QUERY_BATCH_BYTE_SIZE (1 << 18)
#define DPU_QUERY_MAX_BYTE_SIZE (1 << 8)
#define DPU_RESULTS_MAX_BYTE_SIZE (1 << 13)
#define DPU_MAX_BATCH_SIZE 256

/**
* structure to hold the information relative to a block
* of the block table
*/
struct Block {
    __mram_ptr const uint8_t* term;
    uint32_t term_size;
    uint32_t block_address;
    uint32_t block_size;
};

// read a variable length integer in MRAM
int readVInt_mram(const uint8_t** data, seqreader_t* reader) {
    int i = **data & 0x7F;
    for (int shift = 7; (**data & 0x80) != 0; shift += 7) {
      *data = seqread_get((void*)*data, 1, reader);
      i |= ((**data) & 0x7F) << shift;
    }
    return i;
}

//TODO we never read a VLong because the address would be passed the MRAM size
// read a variable length long in MRAM
long readVLong_mram(const uint8_t** data, seqreader_t* reader) {
    long i = **data & 0x7F;
    for (int shift = 7; (**data & 0x80) != 0; shift += 7) {
      *data = seqread_get((void*)*data, 1, reader);
      i |= ((**data) & 0x7F) << shift;
    }
    return i;
}

// read a variable length integer in MRAM
int readVInt(const uint8_t** data) {
    int i = **data & 0x7F;
    for (int shift = 7; (**data & 0x80) != 0; shift += 7) {
      *data+=1;
      i |= ((**data) & 0x7F) << shift;
    }
    return i;
}

// compare terms, return 0 if equal, < 0 if term1 < term2, > 0 if term1 > term2
int compare_terms(const uint8_t* term1, int term1_length,
                    const uint8_t* term2, seqreader_t* term2_reader, int term2_length) {
    // terms are encoded as UTF-8, so we can compare the bytes directly
    // perform the comparison 4B per 4B
    // Need to be cautious of the endianness when loading as integer
    // Need to perform a load as big endian while the DPU is little-endian, hence using a builtin
    int term1_w, term2_w;
    int offset = 0;
    for(; (offset + 4) < term1_length && (offset + 4) < term2_length; offset += 4) {
        __builtin_lw_erri("!big", term1_w, term1 + offset, "0");
        __builtin_lw_erri("!big", term2_w, term2 + offset, "0");
        if(term1_w != term2_w) {
            return term1_w - term2_w;
        }
        term2 = seqread_get((void*)term2, sizeof(int), term2_reader);
    }
    for(; offset < term1_length && offset < term2_length; offset++) {
        if(term1[offset] != term2[offset]) {
            return term1[offset] - term2[offset];
        }
    }
    return term1_length - term2_length;
}

// search for a particular block in the block table in MRAM (floor operation in BST)
void get_block_from_table(const uint8_t* block_table, seqreader_t* block_table_reader,
                            const uint8_t* term, int term_length, struct Block* block) {

    block->term = 0;
    block->term_size = 0;
    block->block_address = 0;
    block->block_size = 0;
    __mram_ptr const uint8_t* succ_node = 0;
    __mram_ptr const uint8_t* succ_node_ancestor = 0;

    while (1) {
        __mram_ptr const uint8_t* curr_block = seqread_tell((void*)block_table, block_table_reader);
        int block_term_length = readVInt_mram(&block_table, block_table_reader);
        __mram_ptr const uint8_t* curr_term = seqread_tell((void*)block_table, block_table_reader);

        int cmp = compare_terms(term, term_length, block_table, block_table_reader, block_term_length);
        block_table = seqread_seek((__mram_ptr void*)(curr_term + block_term_length), block_table_reader);
        int childInfo = readVInt_mram(&block_table, block_table_reader);
        int address = readVLong_mram(&block_table, block_table_reader);
        if(cmp == 0) {
           block->term = curr_term;
           block->term_size = block_term_length;
           block->block_address = address;

           // update successor node
           // if there is a right child this is the successor
           // Otherwise this is the first ancestor for which the searched term is in the left subtree
           int right_child_offset = childInfo >> 2;
           if(right_child_offset) {
                succ_node = (__mram_ptr const uint8_t*)(seqread_tell((void*)block_table, block_table_reader))
                                + right_child_offset;
           }
           else {
                succ_node = succ_node_ancestor;
           }
           break;
        }
        else if(cmp < 0) {
             // searched term is smaller than current term, go to left child
             // the left child is simply the next node in the block table
             uint8_t hasLeftChild = (childInfo & 1) != 0;
             if(!hasLeftChild) {
                // the left child is the next node in the byte array
                // If no left child, we are done
                break;
             }
             succ_node_ancestor = curr_block;
        }
        else {
            // searched term is larger than current term, this is the new floor element
            // then go to right child if any
            block->term = curr_term;
            block->term_size = block_term_length;
            block->block_address = address;

            int right_child_offset = childInfo >> 2;
            if(right_child_offset) {
                block_table += right_child_offset;
                __mram_ptr uint8_t* right_child_mram_addr =
                                seqread_tell((void*)block_table, block_table_reader + right_child_offset);
                block_table = seqread_seek(right_child_mram_addr, block_table_reader);
                succ_node = right_child_mram_addr;
            }
            else {
                succ_node = succ_node_ancestor;
                // no right child, we are done
                break;
            }
        }
    }

    if(block->term) {
        // a term has been found, set the block size as the difference
        // between the next address and the block address
        int addr;
        if(succ_node == 0) {
            // special case when this node is the last
            // read the last address from the block table
            // As this node has no successor and the tree is written in pre-order,
            // the last address is the next element after this node
            succ_node = block->term + block->term_size;
            const uint8_t* last_node = seqread_seek((__mram_ptr void*)succ_node, block_table_reader);
            readVInt_mram(&last_node, block_table_reader);
            readVLong_mram(&last_node, block_table_reader);
            addr = readVLong_mram(&last_node, block_table_reader);
        }
        else {
            const uint8_t* last_node = seqread_seek((__mram_ptr void*)(succ_node), block_table_reader);
            int succ_term_length = readVInt_mram(&last_node, block_table_reader);
            last_node = seqread_seek((__mram_ptr void*)(succ_node + succ_term_length), block_table_reader);
            readVInt_mram(&last_node, block_table_reader);
            addr = readVLong_mram(&last_node, block_table_reader);
        }
        block->block_size = addr - block->block_address;
    }
}

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

int main() {

    if(me() == 0) {
        batch_num = 0;
    }
    barrier_wait(&barrier);
    if(index_seqread_buffer[me()] == 0) {
        // initialize the seq reader buffer
        index_seqread_buffer[me()] = seqread_alloc();
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
        assert(query_size < DPU_QUERY_MAX_BYTE_SIZE);
        mram_read(query_batch + query_offset, query_buffer[me()], query_size);
        const uint8_t* query = query_buffer[me()];
        int segment = readVInt(&query);
        assert(*query == PIM_PHRASE_QUERY_TYPE);
        query++;
        int field_size = readVInt(&query);

        // find the field block
        const uint8_t* index = seqread_init(index_seqread_buffer[me()], DPU_MRAM_HEAP_POINTER, &index_seqread[me()]);
        get_block_from_table(index, &index_seqread[me()], query, field_size, &term_block[me()]);

    }

    return 0;
}
