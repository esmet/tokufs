#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>
#include <time.h>

#define MIN(a,b) ((a) < (b) ? (a) : (b))

typedef struct dbt {
  void * data;
  uint32_t size;
  uint32_t ulen;
  uint32_t flags;
} DBT;

static int one_pass_keycmp(void * db, DBT const * a, DBT const * b)
{
    (void) db;

    // we'd like to compare two keys and do the least amount
    // of comparisons as possible. since meta keys are sorted
    // first by total number of slashes, every byte must be
    // read, everytime. there is no shortcut - you can't know
    // how many slashes there are without looking at every byte.
    // having said that, an optimal solution (potentially) only
    // looks at each byte in each key exactly once.
    //
    // idea: we count slashes and track the comparisons 
    // concurrently. after both keys are exhausted, we first
    // check the slash count, and, if equal, resort to the
    // comparison we kept.
    comparison = 0;
#if 0
    comparelen = MIN(a->size, b->size);
#define UNROLL 8
#define CMP_BYTE(i) v1 = k1[i]; v2 = k2[i]; if (v1 != v2) comparison = v1 - v2; if (v1 == '/') k1slash++; if (v2 == '/') k2slash++;
    for (; comparelen > UNROLL; 
            k1 += UNROLL, k2 += UNROLL, comparelen -= UNROLL) {
        CMP_BYTE(0);
        CMP_BYTE(1);
        CMP_BYTE(2);
        CMP_BYTE(3);
        CMP_BYTE(4);
        CMP_BYTE(5);
        CMP_BYTE(6);
        CMP_BYTE(7);
    }
#endif

    unsigned char * k1 = a->data;
    unsigned char * k2 = b->data;
    int comparison = 0, k1slash = 0, k2slash = 0;

    // may have to unroll alot since this loop overhead is big
#define UNROLL 8
    for (alen = a->size, blen = b->size, k1 = a->data, k2 = b->data; 
            alen > 0 && blen > 0; 
            alen -= unroll, blen -= UNROLL, k1 += UNROLL, k2 += UNROLL) {
        // check_one_byte algorithm:
        //
        // if the byte are equal, comparison is zero.
        // we know k1 == k2, so increment both slash 
        // counts if one of them is == '/'
        //
        // otherwise, we know k1 != k2, so store the
        // comparison and check if k1 is a slash. 
        // do an else for k2 == slash, since they're
        // mutually exclusive events
        //
        // if we macro this, unrolling the loop is
        // alot less painful
#define check_one_byte(i)                           \
        if (k1[i] == k2[i]) {                       \
            comparison = 0;                         \
            if (k1[i] == '/') {                     \
                k1slash++;                          \
                k2slash++;                          \
            }                                       \
        } else {                                    \
            comparison = (int) k1 - (int) k2;       \
            if (k1[0] == '/') {                     \
                k1slash++;                          \
            } else if (k2[0] == '/') {              \
                k2slash++;                          \
            }                                       \
        }
        check_one_byte(0);
        check_one_byte(1);
        check_one_byte(2);
        check_one_byte(3);
        check_one_byte(4);
        check_one_byte(5);
        check_one_byte(6);
        check_one_byte(7);
    }
    // check each of the remaining bytes
    for (int i = 0; i < alen && i < blen; i++, alen--, blen--, k1++, k2++) {
        check_one_byte(i);
    }
#undef check_one_byte
    // figure out how to finish up
    if (alen == 0 && blen == 0) {
        goto both_exhausted;
    }
    if (alen == 0) {
        // the rest of the slashes in a,
        // if the comparison == 0, set it
        // to -1, because a ran out of bytes
        if (comparison == 0) {
            comparison = -1;
        }
        // count the rest of the slashes in b.
        while (blen-- > 0) {
            if (*k2++ == '/') {
                k2slash++;
            }
        }
    } else {
        // if the comparison == 0, set it
        // to 1, because b ran out of bytes
        if (comparison == 0) {
            comparison = 1;
        }
        // count the rest of the slashes in a.
        while (alen-- > 0) {
            if (*k1++ == '/') {
                k1slash++;
            }
        }
    }

    // both keys have no more bytes. both have their slashes counted.
    // first compare by slash count. if they're the same, return
    // the comparison tracked above. 
    //
    // recall that the comparison is the result of comparing the last 
    // two bytes at equal index in both k1 and k2. if k1 was equal to 
    // k2 until k1 ran out of bytes, then the comparison switches to 
    // -1, and vice versa for k2.
    //
    // this is because when some key is a subkey of another, the longer
    // key is larger.
both_exhausted:
    if (k1slash > k2slash) {
        return 1;
    } else if (k1slash < k2slash) {
        return -1;
    } else {
        // same number of slashes, return the comparison
        return comparison;
    }
}

static int keycmp(void * db, DBT const * a, DBT const * b)
{
    (void) db;
    unsigned char *k1 = a->data;
    unsigned char *k2 = b->data;
    int v1, v2, asize, bsize, comparelen;

    asize = a->size;
    bsize = b->size;

    // taken from tokudb/newbrt/key.c and cleaned up
    // does a memcmp in a 4-unrolled loop
    //comparelen = MIN(a->size, b->size);
    comparelen = MIN(asize, bsize);
#define UNROLL 8
#define CMP_BYTE(i) v1 = k1[i]; v2 = k2[i]; if (v1 != v2) return v1 - v2;
    for (; comparelen > UNROLL; 
            k1 += UNROLL, k2 += UNROLL, comparelen -= UNROLL) {
        /*
        v1 = k1[0]; v2 = k2[0]; if (v1 != v2) return v1 - v2;
        v1 = k1[1]; v2 = k2[1]; if (v1 != v2) return v1 - v2;
        v1 = k1[2]; v2 = k2[2]; if (v1 != v2) return v1 - v2;
        v1 = k1[3]; v2 = k2[3]; if (v1 != v2) return v1 - v2;
        */
        // with high probability, keys are similar
        if (*(uint64_t *)k1 == *(uint64_t *)k2) {
            continue;
        }
        // check each individually
        CMP_BYTE(0);
        CMP_BYTE(1);
        CMP_BYTE(2);
        CMP_BYTE(3);
        CMP_BYTE(4);
        CMP_BYTE(5);
        CMP_BYTE(6);
        CMP_BYTE(7);
    }
#undef UNROLL
#undef CMP_BYTE
    for (; comparelen > 0; k1++, k2++, comparelen--) {
        if (*k1 != *k2) {
            return (int)*k1 - (int)*k2;
        }
    }
    if (a->size < b->size) {
        return -1;
    } else if (a->size > b->size) {
        return 1;
    } else {
        return 0;
    }
}
static void dbt_init(DBT * dbt, const void * data, size_t size)
{
    memset(dbt, 0, sizeof(DBT));
    dbt->data = (void *) data;
    dbt->size = size;
    dbt->ulen = size;
}
static int toku_keycompare (unsigned char * key1, int key1len, 
        unsigned char * key2, int key2len) {
    int comparelen = key1len<key2len ? key1len : key2len;
    const unsigned char *k1;
    const unsigned char *k2;
    int stride;

    stride = 8;
    for (k1=key1, k2=key2;
	 comparelen>stride;
	 k1+=stride, k2+=stride, comparelen-=stride) {
        //shortcut similar keys
        if (*(uint64_t *) k1 == *(uint64_t *) k2) {
            continue;
        }
        // check each byte
	{ int v1=k1[0], v2=k2[0]; if (v1!=v2) return v1-v2; }
	{ int v1=k1[1], v2=k2[1]; if (v1!=v2) return v1-v2; }
	{ int v1=k1[2], v2=k2[2]; if (v1!=v2) return v1-v2; }
	{ int v1=k1[3], v2=k2[3]; if (v1!=v2) return v1-v2; }
	{ int v1=k1[4], v2=k2[4]; if (v1!=v2) return v1-v2; }
	{ int v1=k1[5], v2=k2[5]; if (v1!=v2) return v1-v2; }
	{ int v1=k1[6], v2=k2[6]; if (v1!=v2) return v1-v2; }
	{ int v1=k1[7], v2=k2[7]; if (v1!=v2) return v1-v2; }
    }
    stride = 4;
    for (k1=key1, k2=key2;
	 comparelen>stride;
	 k1+=stride, k2+=stride, comparelen-=stride) {
        //shortcut similar keys
        if (*(uint64_t *) k1 == *(uint64_t *) k2) {
            continue;
        }
        // check each byte
	{ int v1=k1[0], v2=k2[0]; if (v1!=v2) return v1-v2; }
	{ int v1=k1[1], v2=k2[1]; if (v1!=v2) return v1-v2; }
	{ int v1=k1[2], v2=k2[2]; if (v1!=v2) return v1-v2; }
	{ int v1=k1[3], v2=k2[3]; if (v1!=v2) return v1-v2; }
    }
    for (;
	 comparelen>0;
	 k1++, k2++, comparelen--) {
	if (*k1 != *k2) {
	    return (int)*k1-(int)*k2;
	}
    }
    if (key1len<key2len) return -1;
    if (key1len>key2len) return 1;
    return 0;
}
static int doerror(int tokucmp, int mycmp, 
        unsigned char * a, int alen, unsigned char * b, int blen)
{
    printf("error, tokucmp %d mycmp %d\n", tokucmp, mycmp);
    printf("a [");
    for (int i = 0; i < alen; i++) {
        printf("%u ", a[i]);
    }
    printf("]\n");
    for (int i = 0; i < blen; i++) {
        printf("%u ", b[i]);
    }
    printf("]\n");
    return 0;
}
int main(void)
{
    int one = 1;
    if (*(unsigned char *) &one == 1) {
        printf("little endian\n");
    } else {
        printf("big endian\n");
    }
    const int len = 25;
    unsigned char a[len], b[len];
    DBT ax, bx;
    dbt_init(&ax, a, sizeof(a));
    dbt_init(&bx, b, sizeof(b));
    srand(time(NULL));
    for (int i = 0; i < len; i++) {
        a[i] = rand() & 0xFF;
        for (int j = 0; j < len; j++) {
            b[i] = rand() & 0xFF;
            int tokucmp = toku_keycompare(a, len, b, len);
            int mycmp = keycmp(NULL, &ax, &bx);
            if ((tokucmp < 0) != (mycmp < 0)) {
                doerror(tokucmp, mycmp, a, len, b, len);
            }
            if ((tokucmp == 0) != (mycmp == 0)) {
                doerror(tokucmp, mycmp, a, len, b, len);
            }
            if ((tokucmp > 0) != (mycmp > 0)) {
                doerror(tokucmp, mycmp, a, len, b, len);
            }
        }
    }
    return 0;
}
