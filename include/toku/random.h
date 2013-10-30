/**
 * TokuFS
 */
 
#ifndef TOKU_RANDOM_H
#define TOKU_RANDOM_H

#include <stdio.h>
#include <stdlib.h>

#include "cc.h"

/**
 * Generate a random long. The generator seeds itself.
 */
UNUSED
static long toku_random_long(void)
{
    static int seeded;
    if (!seeded) {
        srand(toku_current_time_usec());
        seeded = 1;
    }

    long r1 = rand();
    long r2 = rand();
    return r1 | (r2 << 32);
}

/**
 * Generate an allocated, random permutation of uint64
 */
UNUSED
static uint64_t * toku_random_perm(uint64_t n)
{
    uint64_t i, j, tmp;
    uint64_t * perm;

    perm = malloc(sizeof(uint64_t) * n);
    for (i = 0; i < n; i++) {
        perm[i] = i;
    }
    for (i = n - 1; i > 0; i--) {
        j = toku_random_long();
        j %= (i + 1);
        tmp = perm[i];
        perm[i] = perm[j];
        perm[j] = tmp;
    }

    return perm;
}

#endif /* TOKU_RANDOM_H */
