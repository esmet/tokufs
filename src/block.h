/**
 * Tokufs
 */

#ifndef TOKU_BLOCK_H
#define TOKU_BLOCK_H

#include "bstore.h"

/**
 * Get the block number and offset based on the given file position
 */
static inline uint64_t block_get_num_by_position(size_t position)
{
    return position / BSTORE_BLOCKSIZE;
}

static inline uint64_t block_get_offset_by_position(size_t position)
{
    return position % BSTORE_BLOCKSIZE;
}

static inline int block_get_count_by_size(size_t size)
{
    // this is how many whole blocks fit into size
    int blocks = size / BSTORE_BLOCKSIZE;
    // if there are extra bytes left over, there's one more block
    if (size % BSTORE_BLOCKSIZE != 0) {
        blocks++;
    }
    return blocks;
}

#endif /* TOKU_BLOCK_H */
