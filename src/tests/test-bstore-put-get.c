#include "bstore-test.h"

#define NUM_PUTS 25
#define NUM_BSTORES 25
/**
 * Generate a random block id, populate the given buffer
 * with bytes that can later identify it, and return the id.
 */
static uint64_t generate_random_block_and_id(char ** buf, 
        size_t blocksize)
{
    size_t i;
    uint64_t k = random_long();

    for (i = 0; i < blocksize; i++) {
        char c = (int) (k + i) % 100;
        (*buf)[i] = c;
    }

    return k;
}

/**
 * Verify a block for a given id. The bytes should be the same
 * bytes generate random block and id would have written out
 * for id.
 */
static void verify_random_block_by_id(char * buf,
        uint64_t id, size_t blocksize)
{
    size_t i;

    for (i = 0; i < blocksize; i++) {
        char c = (int) (id + i) % 100;
        if (buf[i] != c) {
            fprintf(stderr, "%s: got %d, wanted %d at %lu for id %lu\n",
                    __FUNCTION__, buf[i], c, i, id);
        }
        assert(buf[i] == c);
    }
}

/**
 * Test random puts and gets into many bstores concurrently.
 */
static void test_multiple_bstore_random_put_get()
{
    int ret;
    size_t i, j;
    char name[30];
    struct bstore_s bstores[NUM_BSTORES];

    ret = system("rm -rf " ENV_PATH);
    assert(ret == 0);

    ret = bstore_env_open(ENV_PATH, NULL); 
    assert(ret == 0);


    // open up all the bstores
    memset(name, 0, 30);
    for (i = 0; i < NUM_BSTORES; i++) {
        snprintf(name, 30, "apple%lu", i);
        printf("opening bstore %s\n", name);
        ret = bstore_open(&bstores[i], name);
        assert(ret == 0);
    }

    // generate a random permutation of bstores to
    // do puts over, adding to the chaos factor.
    uint64_t * random_bstores = random_perm(NUM_PUTS);
    // record each put we do for each bstore
    char * blocks[NUM_PUTS][BSTORE_BLOCKSIZE];

    // for each put, iterate through the random bstores
    // permutation, doing a put on each. The put will be
    // a block of (i*bstore_index*2 % 100)'s, and we save
    // this block in the array, so we can verify later.
    for (i = 0; i < NUM_PUTS; i++) {
        for (j = 0; j < NUM_BSTORES; j++) {
            uint64_t random_index = random_bstores[i];
            char * block = malloc(BSTORE_BLOCKSIZE);
            memset(block, (i*random_index*2) % 100, BSTORE_BLOCKSIZE);
            printf("putting to bstore %s, id %lu\n", bstores[random_index].name, j);
            ret = bstore_put(&bstores[random_index], j, block);
            assert(ret == 0);
            blocks[random_index][j] = block;
        }
    }

    // for each put, iterate through the bstores
    // in order and verify a bstore_get for each
    // of its puts corresponds with the saved block
    // for that put. that is, assert bstore_get(bstores[i], j)
    // equals blocks[i][j]
    char readblock[BSTORE_BLOCKSIZE];
    for (i = 0; i < NUM_BSTORES; i++) {
        for (j = 0; j < NUM_PUTS; j++) {
            memset(readblock, 0, BSTORE_BLOCKSIZE);
            ret = bstore_get(&bstores[i], j, readblock);
            assert(ret == 0);
            int c = memcmp(blocks[i][j], readblock, BSTORE_BLOCKSIZE); 
            assert(c == 0);
        }
    }

    // close all of the bstores, then close the env
    for (i = 0; i < NUM_BSTORES; i++) {
        ret = bstore_close(&bstores[i]);
        assert(ret == 0);
    }
    ret = bstore_env_close();
    assert(ret == 0);
    
    // reopen the env, reopen the bstores
    ret = bstore_env_open(ENV_PATH, NULL); 
    assert(ret == 0);
    memset(name, 0, 30);
    for (i = 0; i < NUM_BSTORES; i++) {
        snprintf(name, 30, "apple%lu", i);
        ret = bstore_open(&bstores[i], name);
        assert(ret == 0);
    }

    // try to read again
    for (i = 0; i < NUM_BSTORES; i++) {
        for (j = 0; j < NUM_PUTS; j++) {
            memset(readblock, 0, BSTORE_BLOCKSIZE);
            ret = bstore_get(&bstores[i], j, readblock);
            assert(ret == 0);
            int c = memcmp(blocks[i][j], readblock, BSTORE_BLOCKSIZE); 
            assert(c == 0);
        }
    }

    // close all of the bstores, then close the env
    for (i = 0; i < NUM_BSTORES; i++) {
        printf("closing bstore %s\n", bstores[i].name);
        ret = bstore_close(&bstores[i]);
        assert(ret == 0);
    }
    ret = bstore_env_close();
    assert(ret == 0);

    // clean up
    for (i = 0; i < NUM_BSTORES; i++) {
        for (j = 0; j < NUM_PUTS; j++) {
            free(blocks[i][j]);
        }
    }
    free(random_bstores);
}

/**
 * Test that we can start with an empty environment, create
 * a bstore, put some blocks and get them back correctly.
 */
static void test_single_bstore_random_put_get(void)
{
    int ret;
    struct bstore_s bstore;
    size_t blocksize = BSTORE_BLOCKSIZE;

    ret = system("rm -rf " ENV_PATH);
    assert(ret == 0);

    ret = bstore_env_open(ENV_PATH, NULL); 
    assert(ret == 0);

    ret = bstore_open(&bstore, "apples");
    assert(ret == 0);

    // put a couple blocks with random id's,
    // the contents will be bytes whose values
    // cycle around i + id % 100
    uint64_t k;
    size_t i, num_puts = NUM_PUTS;
    uint64_t * ids = malloc(sizeof(uint64_t) * num_puts);
    char * buf = malloc(blocksize);
    for (i = 0; i < num_puts; i++) {
        k = generate_random_block_and_id(&buf, blocksize);
        ret = bstore_put(&bstore, k, buf);
        ids[i] = k;
        assert(ret == 0);
    }

    // read them back, clearing out the buffer before each read
    // then verifying the contents after a get
    for (i = 0; i < num_puts; i++) {
        memset(buf, 125, blocksize);
        k = ids[i];
        ret = bstore_get(&bstore, k, buf);
        assert(ret == 0);
        verify_random_block_by_id(buf, k, blocksize);
    }

    ret = bstore_close(&bstore);
    assert(ret == 0);

    ret = bstore_env_close();
    assert(ret == 0);

    // reopen the env, bstore, and try to read again.
    ret = bstore_env_open(ENV_PATH, NULL);
    assert(ret == 0);

    ret = bstore_open(&bstore, "apples");
    assert(ret == 0);

    for (i = 0; i < num_puts; i++) {
        memset(buf, 125, blocksize);
        k = ids[i];
        ret = bstore_get(&bstore, k, buf);
        assert(ret == 0);
        verify_random_block_by_id(buf, k, blocksize);
    }

    free(buf);
    free(ids);

    // close up for real this time
    ret = bstore_close(&bstore);
    assert(ret == 0);

    ret = bstore_env_close();
    assert(ret == 0);
}

/**
 * Put some blocks into an empty bstore, and assert
 * that we can read back what we put, and get
 * uninitialized read on what we didn't.
 */
static void test_uninitialized_get()
{
    int ret;
    struct bstore_s bstore;

    ret = system("rm -rf " ENV_PATH);
    assert(ret == 0);

    ret = bstore_env_open(ENV_PATH, NULL);
    assert(ret == 0);

    ret = bstore_open(&bstore, "apples");
    assert(ret == 0);

    // do puts on integers divisible by 7
    size_t i;
    char buf[BSTORE_BLOCKSIZE];
    for (i = 0; i < 100; i++) {
        if (i % 7 == 0) {
            memset(buf, i, BSTORE_BLOCKSIZE);
            ret = bstore_put(&bstore, i, buf);
            assert(ret == 0);
        }
    }

    // make sure the non powers of 2 give us uninitialized read
    // and the powers of 2 give us a block of all i's
    size_t j;
    for (i = 0; i < 100; i++) {
        memset(buf, 0, BSTORE_BLOCKSIZE);
        ret = bstore_get(&bstore, i, buf);
        if (i % 7 == 0) {
            assert(ret == 0);
            for (j = 0; j < BSTORE_BLOCKSIZE; j++) {
                char c = i;
                assert(buf[j] == c);
            }
        } else {
            assert(ret == BSTORE_UNINITIALIZED_GET);
        }
    }

    ret = bstore_close(&bstore);
    assert(ret == 0);

    ret = bstore_env_close();
    assert(ret == 0);
}

int main(void)
{
    random_init();

    test_single_bstore_random_put_get();

    test_multiple_bstore_random_put_get();

    test_uninitialized_get();

    return 0;
}

