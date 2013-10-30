#include <stdio.h>
#include <unistd.h>
#include <assert.h>
#include "../include/tokufs.h"

#define showval(v) printf(#v " %d\n", (int) st.v); 

static void usage(void)
{
    fprintf(stderr, "usage: tokufs_stat <mount_point> <filename>");
}

int main(int argc, char * argv[])
{
    int ret;
    struct stat st;

    if (argc != 3) {
        usage();
        return -1;
    }

    ret = toku_fs_mount(argv[1]);
    assert(ret == 0);
    ret = toku_fs_stat(argv[2], &st);
    printf("toku_fs_stat() ret %d\n", ret);
    if (ret == 0) {
        showval(st_dev);
        showval(st_ino);
        showval(st_mode);
        showval(st_nlink);
        showval(st_uid);
        showval(st_gid);
        showval(st_rdev);
        showval(st_size);
        showval(st_blksize);
        showval(st_blocks);
        showval(st_atime);
        showval(st_mtime);
        showval(st_ctime);
    }

    ret = toku_fs_unmount();
    assert(ret == 0);
    return 0;
}
