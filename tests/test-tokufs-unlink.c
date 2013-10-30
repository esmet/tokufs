#include "tokufs-test.h"

int main(void)
{
    int ret;
    int fd;

    ret = toku_fs_mount(MOUNT_PATH);
    assert(ret == 0);

    fd = toku_fs_open("/test-remove.file", O_CREAT, 0644);
    assert(fd >= 0);

    char buf[30];
    memset(buf, 'z', 30);
    ret = toku_fs_pwrite(fd, buf, 30, 0);
    assert(ret == 30);

    ret = toku_fs_close(fd);
    assert(ret == 0);

    printf("about to remove..\n");
    ret = toku_fs_unlink("/test-remove.file");
    assert(ret == 0);
    fd = -134;

    printf("about to re-open..\n");
    fd = toku_fs_open("/test-remove.file", O_CREAT, 0644);
    assert(fd >= 0);

    char rbuf[30];
    memset(buf, 200, 30);
    ret = toku_fs_pread(fd, rbuf, 30, 0);
    // shouldn't be able to read anything.
    assert(ret == 30);
    int j;
    for (j = 0; j < ret; j++) {
        assert(rbuf[j] == 0);
    }

    ret = toku_fs_close(fd);
    assert(ret == 0);

    ret = toku_fs_unmount();
    assert(ret == 0);

    return 0;
}
