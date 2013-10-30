#include "bstore-test.h"

static struct bstore_s bstore;

static char magic = 107;
static char wild = 37;
static char wacky = 59;

/**
 * Test a couple basic cases for updates on a single block.
 */
static void test_basic_update(void)
{
    int ret;
    size_t i;
    char buf[BSTORE_BLOCKSIZE];

    // update a block in the bstore, for a
    // random key - populate the block with magic.
    uint64_t k = random_long();
    memset(buf, magic, BSTORE_BLOCKSIZE);
    ret = bstore_update(&bstore, k, buf, BSTORE_BLOCKSIZE, 0);
    assert(ret == 0);
    
    // read it back, make sure we get all magic
    memset(buf, 0, BSTORE_BLOCKSIZE);
    ret = bstore_get(&bstore, k, buf);
    assert(ret == 0);
    for (i = 0; i < BSTORE_BLOCKSIZE; i++) {
        assert(buf[i] == magic);
    }

    // put wild at the first b/6 bytes
    size_t n = BSTORE_BLOCKSIZE / 6;
    memset(buf, wild, BSTORE_BLOCKSIZE);
    ret = bstore_update(&bstore, k, buf, n, 0);
    assert(ret == 0);

    // update the last n bytes to be wacky
    memset(buf, wacky, BSTORE_BLOCKSIZE);
    ret = bstore_update(&bstore, k, 
            buf, n, BSTORE_BLOCKSIZE - n);
    assert(ret == 0);

    // now, read the block back and make sure
    // everything is as we expect it
    memset(buf, 0, BSTORE_BLOCKSIZE);
    ret = bstore_get(&bstore, k, buf);
    assert(ret == 0);
    for (i = 0; i < BSTORE_BLOCKSIZE; i++) {
        if (i < n) {
            assert(buf[i] == wild);
        } else if (i < BSTORE_BLOCKSIZE - n) {
            assert(buf[i] == magic);
        } else {
            assert(buf[i] == wacky);
        }
    }
}

int main(void)
{
    int ret;

    // start it up
    ret = system("rm -rf " ENV_PATH);
    assert(ret == 0);

    ret = bstore_env_open(ENV_PATH, NULL); 
    assert(ret == 0);

    ret = bstore_open(&bstore, "apples");
    assert(ret == 0);

    random_init();

    // run the tests
    test_basic_update();

    // shut it down
    ret = bstore_close(&bstore);
    assert(ret == 0);

    ret = bstore_env_close();
    assert(ret == 0);

    return 0;
}

