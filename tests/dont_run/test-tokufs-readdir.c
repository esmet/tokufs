/**
 * Test the basic functionality of readdir.
 * We should be able to create a bunch of directories with files
 * and other directories under it and get back what we put via
 * readdir.
 */

#include "tokufs-test.h"

static mode_t def_mode = 0755;

/**
 * Create a bunch of directories.
 */
static void make_dirs(void)
{
    int ret;

    ret = toku_fs_mkdir("/cat", def_mode);
    assert(ret == 0);
    ret = toku_fs_mkdir("/cat/fish", def_mode);
    assert(ret == 0);
    ret = toku_fs_mkdir("/cat/dog", def_mode);
    assert(ret == 0);
    ret = toku_fs_mkdir("/bears", def_mode);
    assert(ret == 0);
    ret = toku_fs_mkdir("/bears/deer", def_mode);
    assert(ret == 0);
    ret = toku_fs_mkdir("/bears/apples", def_mode);
    assert(ret == 0);
    ret = toku_fs_mkdir("/trees", def_mode);
    assert(ret == 0);
    ret = toku_fs_mkdir("/snakes", def_mode);
    assert(ret == 0);
    ret = toku_fs_mkdir("/ale", def_mode);
    assert(ret == 0);
    ret = toku_fs_mkdir("/ale/house", def_mode);
    assert(ret == 0);
}

int main(void)
{
    int ret;

    ret = toku_fs_mount(MOUNT_PATH);
    assert(ret == 0);

    make_dirs();
    test_open_close_dir();

    ret = toku_fs_unmount();
    assert(ret == 0);

    return 0;
}
