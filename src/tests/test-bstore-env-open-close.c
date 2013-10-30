#include "bstore-test.h"

/**
 * Test that we can start with no environment at some path,
 * create one, see that it exists, and then close it.
 */
static void test_basic(void)
{
    int ret;

    ret = system("rm -rf " ENV_PATH);
    assert(ret == 0);

    ret = bstore_env_open(ENV_PATH, NULL, NULL); 
    assert(ret == 0);

    ret = system("stat " ENV_PATH);
    assert(ret == 0);

    ret = bstore_env_close();
    assert(ret == 0);
}

int main(void)
{
    // do it twice in a row
    test_basic();
    test_basic();

    return 0;
}
