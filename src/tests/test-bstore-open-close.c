#include "bstore-test.h"

/**
 * Make sure the internal structure of the bstore is what we
 * think it should be after an open.
 */
static void assert_initialized(struct bstore_s * bstore, char * name)
{
    assert(bstore->name != NULL);
    assert(strcmp(bstore->name, name) == 0);

    assert(bstore->data_key_buf != NULL);
    assert(bstore->data_key_buf_len == strlen(name) + 1 + sizeof(uint64_t));
}

static void test_nested_open(void)
{
    int ret;
    struct bstore_s bstore1, bstore2, bstore3;
    
    ret = system("rm -rf " ENV_PATH);
    assert(ret == 0);

    ret = bstore_env_open(ENV_PATH, NULL); 
    assert(ret == 0);

    // open three in a row with different names
    ret = bstore_open(&bstore1, "cats");
    assert(ret == 0);
    assert_initialized(&bstore1, "cats");
    ret = bstore_open(&bstore2, "dogs");
    assert(ret == 0);
    assert_initialized(&bstore2, "dogs");
    ret = bstore_open(&bstore3, "fish");
    assert(ret == 0);
    assert_initialized(&bstore3, "fish");

    // close them all out of order
    ret = bstore_close(&bstore2);
    assert(ret == 0);
    ret = bstore_close(&bstore3);
    assert(ret == 0);
    ret = bstore_close(&bstore1);
    assert(ret == 0);

    // open two in a row with the same name.
    // this should work fine.
    ret = bstore_open(&bstore1, "dogs");
    assert(ret == 0);
    assert_initialized(&bstore1, "dogs");
    ret = bstore_open(&bstore3, "dogs");
    assert(ret == 0);
    assert_initialized(&bstore3, "dogs");

    // close them
    ret = bstore_close(&bstore1);
    assert(ret == 0);
    ret = bstore_close(&bstore3);
    assert(ret == 0);

    ret = bstore_env_close();
    assert(ret == 0);
}

/**
 * Test that we can start with an empty environment, create
 * a bstore, and close it.
 */
static void test_basic(void)
{
    int ret;
    struct bstore_s bstore;

    ret = system("rm -rf " ENV_PATH);
    assert(ret == 0);

    ret = bstore_env_open(ENV_PATH, NULL); 
    assert(ret == 0);

    ret = bstore_open(&bstore, "apples");
    assert(ret == 0);
    assert_initialized(&bstore, "apples");

    ret = bstore_close(&bstore);
    assert(ret == 0);

    ret = bstore_env_close();
    assert(ret == 0);
}

int main(void)
{
    test_basic();

    test_nested_open();

    return 0;
}
