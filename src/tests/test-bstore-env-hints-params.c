#include "bstore-test.h"

/**
 * Make sure we can get the default cachesize, 512mb, and then
 * set it to some arbitrary values and get it back correctly.
 */
static void test_env_getset_cachesize(void)
{
    int ret;
    size_t cachesize = bstore_env_get_cachesize();

    // 512mb default, this test will fail once that changes
    assert(cachesize == 512 * 1024 * 1024L);

    // something wacky, but less than 1/2 phys mem size (probably >= 4g)
    size_t wacky = 247 * 881;
    ret = bstore_env_set_cachesize(wacky);
    assert(ret == 0);
    cachesize = bstore_env_get_cachesize();
    assert(cachesize == wacky);

    // and back to normal
    ret = bstore_env_set_cachesize(512 * 1024 * 1024L);
    assert(ret == 0);
    cachesize = bstore_env_get_cachesize();
    assert(cachesize == 512 * 1024 * 1024L);
}

int main(void)
{
    test_env_getset_cachesize();

    printf("BSTORE_BLOCKSIZE %d\n", BSTORE_BLOCKSIZE);

    return 0;
}
