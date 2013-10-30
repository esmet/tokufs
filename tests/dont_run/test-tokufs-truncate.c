#include "tokufs-test.h"

char * filename = "/test-truncate.file";

static char value_at(size_t i)
{
    return i % 100;
}

static void test_truncate(void)
{
    int ret;
    int fd;
    int blocksize = toku_fs_get_blocksize();
    size_t i;
    size_t filesize = 3 * toku_fs_get_blocksize() + 5;

    fd = toku_fs_open(filename, O_CREAT, 0644);
    assert(fd >= 0);

    /* generate a predictable buffer and write it. */
    char * buf = malloc(filesize);
    for (i = 0; i < filesize; i++) {
        buf[i] = value_at(i);
    }
    ssize_t n = toku_fs_pwrite(fd, buf, filesize, 0);
    assert(n == (ssize_t) filesize);
    printf("wrote %ld to the file\n", n);

    /* make sure we read zeros from the end of the
     * file or slightly passed it, for starters. */
    memset(buf, 0, filesize);
    for (i = filesize; i < filesize + 10; i++) {
        n = toku_fs_pread(fd, buf, blocksize, i);
        assert(n == blocksize);
        int j;
        for (j = 0; j < blocksize; j++) {
            assert(buf[j] == 0);
        }
    }
    /* we should be able to get the last byte, though.  */
    n = toku_fs_pread(fd, buf, 1, filesize - 1);
    assert(buf[0] == value_at(filesize - 1));
    assert(n == 1);
    memset(buf, 0, filesize);

    /* chop off the last 4 bytes and make sure they
     * are not readable anymore */
    ret = toku_fs_truncate(filename, filesize - 4);
    assert(ret == 0);

    for (i = 1; i <= 4; i++) {
        n = toku_fs_pread(fd, buf, blocksize, filesize - i);
        assert(n == blocksize);
        int j;
        for (j = 0; j < blocksize; j++) {
            assert(buf[j] == 0);
        }
    }
    n = toku_fs_pread(fd, buf, 5, filesize - 5);
    assert(n == 5);
    int j;
    for (j = 0; j < 5; j++) {
        assert(buf[j] == value_at((filesize - 5)+j));
    }
    memset(buf, 0, filesize);

    /* Make sure we can close, reopen, and see the truncate still. */
    ret = toku_fs_close(fd);
    assert(ret == 0);
    fd = toku_fs_open(filename, O_CREAT, 0644);
    assert(fd >= 0);
    n = toku_fs_pread(fd, buf, filesize, 0);
    assert(n == (ssize_t) filesize - 4);
    for (i = 0; i < filesize - 4; i++) {
        assert(buf[i] == value_at(i));
    }

    /* get rid of everything except the first 5 bytes, and verify. */
    memset(buf, 0, filesize);
    n = toku_fs_truncate(filename, 5);
    assert(n == 0);
    for (i = 0; i < filesize; i++) {
        n = toku_fs_pread(fd, buf, 1, i);
        if (i < 5) {
            assert(n == 1);
            assert(buf[0] == value_at(i));
        } else {
            assert(n == 0);
        }
    }

    ret = toku_fs_close(fd);
    assert(ret == 0);
    fd = toku_fs_open(filename, O_CREAT, 0644);
    assert(fd >= 0);

    memset(buf, 0, filesize);
    n = toku_fs_truncate(filename, 5);
    assert(n == 0);
    for (i = 0; i < filesize; i++) {
        n = toku_fs_pread(fd, buf, 1, i);
        if (i < 5) {
            assert(n == 1);
            assert(buf[0] == value_at(i));
        } else {
            assert(n == 0);
        }
    }

    ret = toku_fs_close(fd);
    assert(ret == 0);
    free(buf);
}

int main(void)
{
    int ret;

    ret = toku_fs_mount(MOUNT_PATH);
    assert(ret == 0);
    
    test_truncate();

    ret = toku_fs_unmount();
    assert(ret == 0);

    return 0;
}
