#include "tokufs-test.h"

int main(void)
{
    size_t blocksize = toku_fs_get_blocksize();
    size_t cachesize = toku_fs_get_cachesize();

    printf("blocksize = %lu, cachesize = %lu\n",
            blocksize, cachesize);

    assert(blocksize > 0);
    assert(cachesize > 0);

    size_t new_cachesize = 768*1024*1024L;
    int ret = toku_fs_set_cachesize(new_cachesize);
    assert(ret == 0);
    cachesize = toku_fs_get_cachesize();
    printf("cachesize changed now to %lu\n", cachesize);
    assert(cachesize == new_cachesize);

    return 0;
}
