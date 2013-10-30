#include "tokufs-test.h"

static size_t blocksize;

static void test_middle_rw(void)
{
    int fd;

    fd = toku_fs_open("/test-middle-rw.file", O_CREAT, 0644);
    assert(fd >= 0);
    
    // populate it with some stuff, one block only
    size_t i;
    unsigned char * buf = malloc(blocksize);
    for (i = 0; i < blocksize; i++) {
        buf[i] = i % 100;
    }
    ssize_t bytes_written = toku_fs_pwrite(fd, buf, blocksize, 0);
    assert(bytes_written == (ssize_t) blocksize);

    // make some bytes in the middle the magic byte
    int magic = 107;
    for (i = blocksize/2; i < blocksize/2 + 10; i++) {
        buf[i] = magic;
    }
    bytes_written = toku_fs_pwrite(fd, buf + blocksize/2, 10, blocksize/2);
    assert(bytes_written == 10);

    // make sure everything is what we expect
    for (i = 0; i < blocksize; i++) {
        if (i < blocksize/2) {
            assert(buf[i] == i % 100);
        } else if (i < blocksize/2 + 10) {
            assert(buf[i] == magic);
        } else {
            assert(buf[i] == i % 100);
        }
    }
    free(buf);

    int ret = toku_fs_close(fd);
    assert(ret == 0);
}

static void test_corner_rw(void)
{
    int fd;
    size_t i;
    size_t size = blocksize * 2;
    ssize_t ret;
    unsigned char * buf = malloc(size);

    fd = toku_fs_open("/test-rw-unaligned.file", O_CREAT, 0644);
    assert(fd >= 0);

    for (i = 0; i < size; i++) {
        buf[i] = i % 100;
    }

    /* Set the offset 3 bytes under a block boundry. */
    off_t offset = blocksize - 3;
    /* Then, write 6 bytes, so that it runs over the boundry. */
    ret = toku_fs_pwrite(fd, buf + offset, 6, offset);
    assert(ret == 6);

    /* Read back 6 bytes from 3 bytes under the boundry. */
    unsigned char * rbuf = malloc(6);
    ret = toku_fs_pread(fd, rbuf, 6, offset);
    for (i = 0; i < 6; i++) {
        if (rbuf[i] != buf[offset + i]) {
            fprintf(stderr, "%s: i %u, wanted %u, got %u\n",
                    __FUNCTION__,
                    (unsigned) i, (unsigned) buf[offset + i],
                    (unsigned) rbuf[i]);
        }
        assert(rbuf[i] == buf[offset + i]);
    }

    free(rbuf);
    free(buf);

    ret = toku_fs_close(fd);
    assert(ret == 0);
}

static void test_block_corner_rw(void)
{
    int fd;
    ssize_t ret;
    size_t i;
    size_t size = blocksize * 3;
    unsigned char * rbuf;
    unsigned char * buf;
    
    fd = toku_fs_open("/test-rw-unaligned.file", O_CREAT, 0644);
    assert(fd >= 0);
    
    buf = malloc(size);
    for (i = 0; i < size; i++) {
        buf[i] = i % 100;
    }

    off_t offset = blocksize - 3;
    ret = toku_fs_pwrite(fd, buf + offset, blocksize + 6, offset);
    assert(ret == (ssize_t) blocksize + 6);

    rbuf = malloc(blocksize + 6);
    ret = toku_fs_pread(fd, rbuf, blocksize + 6, offset);
    for (i = 0; i < blocksize + 6; i++) {
        if (rbuf[i] != buf[offset + i]) {
            fprintf(stderr, "%s: i %u, wanted %u, got %u\n",
                    __FUNCTION__,
                    (unsigned) i, (unsigned) buf[offset + i],
                    (unsigned) rbuf[i]);
        }
        assert(rbuf[i] == buf[offset + i]);
    }

    free(rbuf);
    free(buf);

    ret = toku_fs_close(fd);
    assert(ret == 0);
}

int main(void)
{
    int ret;

    ret = toku_fs_mount(MOUNT_PATH);
    assert(ret == 0);

    blocksize = toku_fs_get_blocksize();

    test_corner_rw();

    test_block_corner_rw();

    test_middle_rw();

    ret = toku_fs_unmount();
    assert(ret == 0);

    return 0;
}
