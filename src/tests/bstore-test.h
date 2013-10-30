/**
 * Tokufs
 */

#ifndef BSTORE_TEST_H
#define BSTORE_TEST_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>

#include <unistd.h>

#include <bstore.h>
#include <debug/debug.h>

/**
 * Generate a random long
 */
static inline long random_long(void)
{
    long r1, r2;

    r1 = rand();
    r2 = rand();

    return r1 | (r2 << 32);
}

static inline void random_init(void)
{
   srand(time(NULL)); 
}

/**
 * Generate a random permutation of uint64s
 */
static inline uint64_t * random_perm(uint64_t n)
{
    uint64_t i, j, tmp;
    uint64_t * perm;

    perm = malloc(sizeof(uint64_t) * n);
    for (i = 0; i < n; i++) {
        perm[i] = i;
    }
    for (i = n - 1; i > 0; i--) {
        j = random_long() % (i + 1);
        tmp = perm[i];
        perm[i] = perm[j];
        perm[j] = tmp;
    }

    return perm;
}

#endif
