#include "tokufs-test.h"
//getgid, time, getuid
#include <unistd.h>
#include <time.h>

static void test_file_op_stat(void)
{
    int fd;
    int ret;
    size_t blocksize = toku_fs_get_blocksize();

    fd = toku_fs_open("/opstat", O_CREAT, 0644);
    assert(fd >= 0);

    size_t magic = 1791;
    char * buf = malloc(magic);
    memset(buf, 100, magic);
    ssize_t n = toku_fs_pwrite(fd, buf, magic, 0);
    assert(n == (ssize_t) magic);
    struct stat st;
    ret = toku_fs_stat("/opstat", &st);
    assert(ret == 0);
    assert(st.st_size == (off_t) magic);
    assert((size_t)st.st_blocks == magic / blocksize + 1);
    time_t now = time(NULL);
    assert(st.st_mtime <= now && st.st_mtime >= now - 2);

    struct stat st2;
    n = toku_fs_pread(fd, buf, magic, 0);
    ret = toku_fs_stat("/opstat", &st2);
    assert(ret == 0);
    assert(st2.st_size == (off_t) magic);
    assert((size_t)st2.st_blocks == magic / blocksize + 1);
    now = time(NULL);
    assert(st2.st_atime <= now && st2.st_atime >= now - 2);

    ret = toku_fs_close(fd);
    assert(ret == 0);
    free(buf);
}

static void test_basic_stat(void)
{
    int fd;
    int ret;
    struct stat st;
    mode_t mode = 0613;

    fd = toku_fs_open("/basic-stat.file", O_CREAT, mode); 
    assert(fd >= 0);
    ret = toku_fs_close(fd);
    assert(ret == 0);

    ret = toku_fs_stat("/basic-stat.file", &st);
    assert(ret == 0);

    assert(st.st_blksize > 0);
    assert(st.st_nlink == 1);
    long now = time(NULL);
    assert(st.st_atime == st.st_mtime);
    assert(st.st_atime == st.st_ctime);
    assert(st.st_atime > 0 && st.st_atime <= now);
    printf("st_uid %d, uid %d\n", st.st_uid, getuid());
    assert(st.st_uid == getuid());
    assert(st.st_gid == getgid());
    assert(st.st_mode == mode);

    ret = toku_fs_stat("/this/does/not/exist", &st);
    assert(ret != 0);
    ret = toku_fs_stat("/this/def/does/not/exist", &st);
    assert(ret != 0);
    ret = toku_fs_stat("/nope...chucktesta", &st);
    assert(ret != 0);
    ret = toku_fs_stat("/basic-stat.file", &st);
    assert(ret == 0);

    struct stat st2;
    mode_t mode2 = 0724;
    fd = toku_fs_open("/clouds.file", O_CREAT, mode2);
    assert(fd >= 0);
    ret = toku_fs_close(fd);
    assert(ret == 0);
    ret = toku_fs_stat("/clouds.file", &st2);
    assert(ret == 0);

    now = time(NULL);
    assert(st2.st_atime == st2.st_mtime);
    assert(st2.st_atime == st2.st_ctime);
    assert(st2.st_atime > 0 && st.st_atime <= now);
    assert(st.st_uid == getuid());
    assert(st.st_gid == getgid());
    assert(st.st_mode == mode);
    assert(st2.st_uid == getuid());
    assert(st2.st_gid == getgid());
    printf("st2 mode %x vs mode2 %x\n", st2.st_mode, mode2);
    assert(st2.st_mode == mode2);
}

int main(void)
{
    int ret;

    ret = toku_fs_mount(MOUNT_PATH);
    assert(ret == 0);
    
    test_basic_stat();
    test_file_op_stat();

    ret = toku_fs_unmount();
    assert(ret == 0);

    return 0;
}
