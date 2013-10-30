#include "tokufs-test.h"

static int all_zeroes(char * buf, size_t size)
{
    int ret = 1;
    size_t i;
    for (i = 0; i < size; i++) {
        if (buf[i] != 0) {
            printf("buf[%lu] was nonzero %d\n", i, buf[i]);
            ret = 0;
            break;
        }
    }
    return ret;
}

int main(void)
{   
    int ret;
    int fd; 
    ssize_t n;
    size_t blocksize = toku_fs_get_blocksize();
    char * buf = malloc(blocksize);

    ret = toku_fs_mount(MOUNT_PATH);
    assert(ret == 0);

    fd = toku_fs_open("/test-unitialized-read.file", O_CREAT, 0644);
    assert(fd >= 0);

    /* Write the magic number in the first and third block.
       Make sure we can read it back okay. */
    int magic = 87; 
    memset(buf, magic, blocksize);
    n = toku_fs_pwrite(fd, buf, blocksize, 0); 
    assert(n == (ssize_t) blocksize);
    n = toku_fs_pwrite(fd, buf, blocksize, blocksize*2);
    assert(n == (ssize_t) blocksize);
    n = toku_fs_pread(fd, buf, blocksize, 0); 
    assert(n == (ssize_t) blocksize);
    n = toku_fs_pread(fd, buf, blocksize, blocksize*2);
    assert(n == (ssize_t) blocksize);

    /* Try to read back the second, fourth, and fifth block
       make sure they all return 0 bytes read, because they
       were not initialized. */
    n = toku_fs_pread(fd, buf, blocksize, blocksize);
    assert(n == (ssize_t) blocksize);
    assert(all_zeroes(buf, blocksize));

    n = toku_fs_pread(fd, buf, blocksize, blocksize*3);
    assert(n == (ssize_t) blocksize);
    assert(all_zeroes(buf, blocksize));

    n = toku_fs_pread(fd, buf, blocksize, blocksize*4);
    assert(n == (ssize_t) blocksize);
    assert(all_zeroes(buf, blocksize));

    /* Try reading a region which straddles an initialized block
       and an uninitialized block. Make sure the valid portion
       is correct, and the rest is zero, not voodoo. */
    int off = blocksize / 2;
    int voodoo = 111;
    memset(buf, voodoo, blocksize);
    n = toku_fs_pread(fd, buf, blocksize, blocksize - off);
    assert(n == (ssize_t) blocksize);
    int i;
    for (i = 0; i < (int) blocksize; i++) {
        if (i < off) {
            assert(buf[i] == magic);
        } else {
            assert(buf[i] == 0);
        }
    }

    ret = toku_fs_close(fd);
    assert(ret == 0);

    ret = toku_fs_unmount();
    assert(ret == 0);

    free(buf);

    return 0;
}
