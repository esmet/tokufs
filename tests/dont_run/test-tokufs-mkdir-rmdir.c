/**
 * Test the basic functionality of mkdir and rmdir.
 */

#include "tokufs-test.h"

static mode_t def_mode = 0755;

/**
 * Remove empty directories, and fail to remove non empty ones.
 * At the end of the test, everything from test_mkdir is rmdir'd.
 */
static void test_rmdir(void)
{
    int ret;

    ret = toku_fs_rmdir("/cat"); 
    assert(ret != 0);
    ret = toku_fs_rmdir("/cat/dog");
    assert(ret == 0);
    ret = toku_fs_rmdir("/cat/fish");
    assert(ret == 0);
    // no ghost version of /cat/fish?
    ret = toku_fs_rmdir("/cat/fish");
    assert(ret != 0);
    ret = toku_fs_rmdir("/cat");
    assert(ret == 0);
    // we never actually made these, right?
    ret = toku_fs_rmdir("/cAt/dog/bad");
    assert(ret != 0);
    ret = toku_fs_rmdir("/cat/dog/bad");
    assert(ret != 0);
    ret = toku_fs_rmdir("cat/dog/fish");
    assert(ret != 0);
    ret = toku_fs_rmdir("/bears/deer");
    assert(ret == 0);
    ret = toku_fs_rmdir("/bears");
    assert(ret != 0);
    ret = toku_fs_rmdir("/bears/apples");
    assert(ret == 0);
    // nope, chuck testa.
    ret = toku_fs_rmdir("/bears/nope/chuck/testa");
    assert(ret != 0);
    ret = toku_fs_rmdir("/bears");
    assert(ret == 0);
    ret = toku_fs_rmdir("/trees");
    assert(ret == 0);
    ret = toku_fs_rmdir("trees");
    assert(ret != 0);
    ret = toku_fs_rmdir("ale/house");
    assert(ret != 0);
    ret = toku_fs_rmdir("/ale/house");
    assert(ret == 0);
    ret = toku_fs_rmdir("/ale/house");
    assert(ret != 0);
    ret = toku_fs_rmdir("/ale");
    assert(ret == 0);
}

/**
 * Create a bunch of directories, sometimes in bad order,
 * or with a bad path, or that cannot exist.
 */
static void test_mkdir(void)
{
    int ret;

    ret = toku_fs_mkdir("/cat/fish", def_mode);
    assert(ret != 0);
    ret = toku_fs_mkdir("/cat", def_mode);
    assert(ret == 0);
    ret = toku_fs_mkdir("/cat/fish", def_mode);
    assert(ret == 0);
    ret = toku_fs_mkdir("/cat/dog", def_mode);
    assert(ret == 0);
    // case sensitive
    ret = toku_fs_mkdir("/cAt/dog/bad", def_mode);
    assert(ret != 0);
    ret = toku_fs_mkdir("cat/dog/fish", def_mode);
    assert(ret != 0);
    ret = toku_fs_mkdir("/bears", def_mode);
    assert(ret == 0);
    ret = toku_fs_mkdir("/bears/deer", def_mode);
    assert(ret == 0);
    // ...nope
    ret = toku_fs_mkdir("/bears/nope/chuck/testa", def_mode);
    assert(ret != 0);
    ret = toku_fs_mkdir("/bears/apples", def_mode);
    assert(ret == 0);
    ret = toku_fs_mkdir("trees", def_mode);
    assert(ret != 0);
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

    test_mkdir();
    test_rmdir();

    ret = toku_fs_unmount();
    assert(ret == 0);

    return 0;
}
