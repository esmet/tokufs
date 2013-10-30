#include "tokufs-test.h"

#define BUF_SIZE (1024)

static void test_nested_open(void)
{
    int ret;
    int fd1, fd2, fd3;

    fd1 = toku_fs_open("/test-nested-open-1.file", O_CREAT, 0644);
    assert(fd1 >= 0);

    fd2 = toku_fs_open("/test-nested-open-2.file", O_CREAT, 0644);
    assert(fd2 >= 0);

    fd3 = toku_fs_open("/test-nested-open-3.file", O_CREAT, 0644);
    assert(fd3 >= 0);

    ret = toku_fs_close(fd1);
    assert(ret == 0);

    ret = toku_fs_close(fd3);
    assert(ret == 0);

    ret = toku_fs_close(fd2);
    assert(ret == 0);
}


static int test_open_close_basic(void)
{
    int ret;
    int fd;

    fd = toku_fs_open("/test-open-close.file", O_CREAT, 0644);
    assert(fd >= 0);
    
    ret = toku_fs_close(fd);
    assert(ret == 0);

    return 0;
}

static int test_open_close_multiple(void)
{
    int ret;
    int fd1, fd2, fd3;

    fd1 = toku_fs_open("/test-open-close-multiple.file", O_CREAT, 0644);
    assert(fd1 >= 0);

    fd2 = toku_fs_open("/test-open-close-multiple.file", O_CREAT, 0644);
    assert(fd1 != fd2);

    fd3 = toku_fs_open("/test-open-close-multiple.file", O_CREAT, 0644);
    assert(fd3 != fd1);
    assert(fd3 != fd2);

    ret = toku_fs_close(fd2);
    assert(ret == 0);

    ret = toku_fs_close(fd3);
    assert(ret == 0);

    ret = toku_fs_close(fd1);
    assert(ret == 0);

    return 0;
}

static int test_close_underflow(void)
{
    int ret;
    int fd;

    fd = toku_fs_open("/test-close-underflow.file", O_CREAT, 0644);
    assert(fd >= 0);

    ret = toku_fs_close(fd);
    assert(ret == 0);

    ret = toku_fs_close(fd);
    assert(ret != 0);

    ret = toku_fs_close(fd);
    assert(ret != 0);

    return 0;
}


static int test_open_close_persistency(void)
{
    int i;
    int ret;
    int fd;
    char buf[BUF_SIZE];

    fd = toku_fs_open("/test-open-close-persistency.file", 0, 0644);
    assert(fd < 0);
    fd = toku_fs_open("/test-open-close-persistency.file", O_CREAT, 0644);
    assert(fd >= 0);

    /* Wipe out whatever might have been there before. */
    memset(buf, 125, BUF_SIZE);
    ret = toku_fs_pwrite(fd, buf, BUF_SIZE, 0);
    assert(ret == BUF_SIZE);

    /* Generate 0..249 continously for this file */
    for (i = 0; i < BUF_SIZE; i++) {
        buf[i] = i % 100;
    }
    ret = toku_fs_pwrite(fd, buf, BUF_SIZE, 0);
    assert(ret == BUF_SIZE);

    /* Close the file, then re-open it. */
    ret = toku_fs_close(fd);
    assert(ret == 0);
    fd = toku_fs_open("/test-open-close-persistency.file", 0, 0644);
    assert(fd >= 0);

    /* Wipe out our buffer, read in the file, make sure it is legit. */
    memset(buf, 255, BUF_SIZE);
    ret = toku_fs_pread(fd, buf, BUF_SIZE, 0);
    assert(ret == BUF_SIZE);

    for (i = 0; i < BUF_SIZE; i++) {
        if (buf[i] != i % 100) {
            printf("expected %d got %d\n", i % 100, buf[i]);
        }
        assert(buf[i] == i % 100);
    }

    /* All good. close up. */
    ret = toku_fs_close(fd);
    assert(ret == 0);

    return 0;
}
            
static void test_abuse(void)
{
    int ret;
    int fd;

    //abuse open
    fd = toku_fs_open("NO!", 0, 0755);
    assert(fd < 0);
    fd = toku_fs_open("...nope", 0, 0711);
    assert(fd < 0);
    fd = toku_fs_open("...nope", 0, 0711);
    assert(fd < 0);
    fd = toku_fs_open("/a/b/c/d/e/f!!!!gg", 0, 0711);
    assert(fd < 0);
    fd = toku_fs_open("NO!", 0, 0755);
    assert(fd < 0);
    fd = toku_fs_open("NO!", 0, 0755);
    assert(fd < 0);
    fd = toku_fs_open("/a", O_CREAT, 0644);
    assert(fd >= 0);
    ret = toku_fs_close(fd);
    assert(ret == 0);
    // /a is not a directory!
    // TODO: 4168 this won't work until dirs exist/are recognized
    //fd = toku_fs_open("/a/b", O_CREAT, 0644);
    //assert(fd < 0);
    fd = toku_fs_open("/ab", 0, 0644);
    assert(fd < 0);

    //abuse close
    ret = toku_fs_close(fd);
    assert(ret != 0);
    ret = toku_fs_close(-113);
    assert(ret != 0);
    ret = toku_fs_close(-1354235234);
    assert(ret != 0);
    ret = toku_fs_close(50000000);
    assert(ret != 0);
}
    
int main(void)
{
    int ret;

    ret = toku_fs_mount(MOUNT_PATH);
    assert(ret == 0);

    ret = test_open_close_basic();
    assert(ret == 0);

    ret = test_open_close_multiple();
    assert(ret == 0);

    ret = test_close_underflow();
    assert(ret == 0);

    ret = test_open_close_persistency();
    assert(ret == 0);

    test_nested_open();
    test_abuse();

    ret = toku_fs_unmount();
    assert(ret == 0);

    return 0;
}
