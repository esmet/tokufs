/**
 * Test the basic functionality of open and close. Only a few things
 * are asserted: you can't open a directory that doesn't exist,
 * you should be able to open an abritrary number of handles for
 * a valid directory, and each should be able to close successfully.
 */

#include "tokufs-test.h"

static mode_t def_mode = 0755;

/**
 * Remove empty directories, and fail to remove non empty ones.
 * At the end of the test, everything from test_mkdir is rmdir'd.
 */
static void test_open_close_dir(void)
{
    int ret;
    struct toku_dircursor c1, c2, c3, c4;

    d1 = toku_fs_opendir("/cat"); 
    assert(d1 != NULL);
    d2 = toku_fs_opendir("/cat/dog");
    assert(d2 != NULL);
    d3 = toku_fs_opendir("/cat/fish");
    assert(d3 != NULL);
    d4 = toku_fs_opendir("/cat/fish");
    assert(d4 != NULL);
    ret = toku_fs_closedir(d3);
    assert(ret == 0);
    ret = toku_fs_closedir(d4);
    assert(ret == 0);
    d3 = toku_fs_opendir("/cat");
    assert(d3 != NULL);
    ret = toku_fs_closedir(d3);
    assert(ret == 0);
    ret = toku_fs_closedir(d1);
    assert(ret == 0);
    d1 = toku_fs_opendir("/cat/dog");
    assert(d1 != NULL);
    ret = toku_fs_closedir(d1);
    assert(ret == 0);
    ret = toku_fs_closedir(d2);
    assert(ret == 0);

    // we never actually made these, right? so we can't open them.
    d1 = toku_fs_opendir("/cAt/dog/bad");
    assert(d1 == NULL);
    d1 = toku_fs_opendir("/cat/dog/bad");
    assert(d1 == NULL);
    d4 = toku_fs_opendir("cat/dog/fish");
    assert(d4 == NULL);

    d1 = toku_fs_opendir("/bears/deer");
    assert(d1 != NULL);
    d2 = toku_fs_opendir("/bears");
    assert(d2 != NULL);
    d3 = toku_fs_opendir("/bears/apples");
    assert(d3 != NULL);
    ret = toku_fs_closedir(d2);
    assert(ret == 0);
    d4 = toku_fs_opendir("/bears/apples");
    assert(d4 == NULL);
    ret = toku_fs_closedir(d3);
    assert(ret == 0);
    // nope, chuck testa.
    d3 = toku_fs_opendir("/bears/nope/chuck/testa");
    assert(d3 == NULL);
    ret = toku_fs_closedir(d1);
    assert(ret == 0);

    d1 = toku_fs_opendir("/bears");
    assert(d1 != NULL);
    d2 = toku_fs_opendir("/trees");
    assert(d2 != NULL);
    d3 = toku_fs_opendir("trees");
    assert(d3 == NULL);
    d4 = toku_fs_opendir("ale/house");
    assert(d4 == NULL);
    ret = toku_fs_closedir(d1);
    assert(ret == 0);
    ret = toku_fs_closedir(d2);
    assert(ret == 0);
    d3 = toku_fs_opendir("/ale/house");
    assert(d3 != NULL);
    d4 = toku_fs_opendir("/ale/house");
    assert(d4 != NULL);
    d1 = toku_fs_opendir("/ale");
    assert(d1 != NULL);
    d2 = toku_fs_opendir("/ale");
    assert(d2 != NULL);
    ret = toku_fs_closedir(d3);
    assert(ret == 0);
    ret = toku_fs_closedir(d1);
    assert(ret == 0);
    ret = toku_fs_closedir(d2);
    assert(ret == 0);
    ret = toku_fs_closedir(d4);
    assert(ret == 0);
}

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
