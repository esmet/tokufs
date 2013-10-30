#include "bstore-test.h"

static void test_bstore_truncate_basic(void)
{
    int ret;
    struct bstore_s bstore;

    ret = bstore_open(&bstore, "store");
    assert(ret == 0);
  
    // put 10 blocks
    int i, n = 10;
    char * buf = malloc(BSTORE_BLOCKSIZE);
    for (i = 0; i < n; i++) {
        memset(buf, i % 100, BSTORE_BLOCKSIZE);
        ret = bstore_put(&bstore, i, buf);
        assert(ret == 0);
    }

    // chop off from k, asserting that we could read
    // before the truncate and that we couldn't after
    int k = 7;
    for (i = k; i < n; i++) {
        ret = bstore_get(&bstore, i, buf);
        assert(ret == 0);
    }
    ret = bstore_truncate(&bstore, k);
    assert(ret == 0);
    for (i = k; i < n; i++) {
        ret = bstore_get(&bstore, i, buf);
        assert(ret != 0);
    }
    for (i = 0; i < k; i++) {
        ret = bstore_get(&bstore, i, buf);
        assert(ret == 0);
    }

    //chop off one more. still works?
    k--;
    ret = bstore_get(&bstore, k, buf);
    assert(ret == 0);
    ret = bstore_truncate(&bstore, k);
    assert(ret == 0);
    ret = bstore_get(&bstore, k, buf);
    assert(ret != 0);

    //chop off everything. we can't read anything now.
    for (i = 0; i < k; i++) {
        ret = bstore_get(&bstore, i, buf);
        assert(ret == 0);
    }
    ret = bstore_truncate(&bstore, 0);
    assert(ret == 0);
    for (i = 0; i < n; i++) {
        ret = bstore_get(&bstore, i, buf);
        assert(ret != 0);
    }

    ret = bstore_close(&bstore);
    assert(ret == 0);
    free(buf);
}

int main(void)
{
    int ret;

    ret = bstore_env_open(ENV_PATH, NULL, NULL);
    assert(ret == 0);

    test_bstore_truncate_basic();

    ret = bstore_env_close();
    assert(ret == 0);

    return 0;
}

