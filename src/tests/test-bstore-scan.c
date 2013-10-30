#include "bstore-test.h"

/**
 * determines what the value should be at index
 * i for a block with id block_id for certain bstore.
 */
static char block_index_value(size_t i, int magic, uint64_t block_id)
{
    return (i + magic + block_id * i) % 100;
}

/**
 * verifies that all indicies in the given block
 * have the correct block index value for a certain bstore.
 */
static void verify_block(void * block, int magic, uint64_t block_id)
{
    int i;
    char * buf = block;

    for (i = 0; i < BSTORE_BLOCKSIZE; i++) {
        assert(buf[i] == block_index_value(i, block_id, magic));
    }
}

/**
 * initialize a block to have the block index value
 * for each index.
 */
static void init_block(void * block, int magic, uint64_t block_id)
{
    int i;
    char * buf = block;

    for (i = 0; i < BSTORE_BLOCKSIZE; i++) {
        buf[i] = block_index_value(i, block_id, magic);
    }
}

/**
 * Load blocks 0..n into a bstore, where each block
 * has the value given by init_block and is later
 * verifyed by verify_block
 */
static void load_some_blocks(struct bstore_s * bstore, 
        int magic, int num_blocks)
{
    int ret, i;
    char * buf = malloc(BSTORE_BLOCKSIZE);

    for (i = 0; i < num_blocks; i++) {
        init_block(buf, magic, i);
        ret = bstore_put(bstore, i, buf);
        assert(ret == 0);
    }
    free(buf);
}

static int basic_scan_num_blocks = 30;
static char * bstore1name = "pie.storezzz";
static char * bstore2name = "bears-trees-pigs.bstory";
static char * bstore3name = "bstore-test-scan.bwhat";

struct basic_scan_cb_info {
    char * name;
    int magic;
    int scan_iteration;
    uint64_t start_block_id;
};

#include <arpa/inet.h>
static int basic_scan_cb(DBT const * key, 
        DBT const * value, void * extra)
{
    int ret;
    struct basic_scan_cb_info * info = extra;

    // XXX hacks
    char * k = malloc(key->size - 8 + 1);
    strncpy(k, key->data, key->size - 8 + 1);
    uint64_t * x = key->data + key->size - 8;
    uint64_t num = (uint64_t) htonl(*x & 0xFFFFFFFF);
    num <<= 32L;
    num |= (uint64_t) htonl(*x >> 32);
    printf("key %s[%lu]\n", k, num);
    free(k);
    // XXX hacks
    
    // does the iteration make sense? we should be at least zero
    // and less than the number of blocks, otherwise we're scanning
    // more than we should.
    int i = info->scan_iteration;
    int block_id = info->start_block_id + i;
    assert(block_id >= 0 && block_id < basic_scan_num_blocks);
    // does the key have the expected bstore name prefix?
    size_t plen = key->size - sizeof(uint64_t);
    assert(plen == strlen(info->name)+ 1);
    assert(strncmp(key->data, info->name, key->size) == 0);
    // value should be a correct block
    assert(value->data != NULL && value->size == BSTORE_BLOCKSIZE);
    verify_block(value->data, info->magic, block_id);

    // increment the iterations, since we will pass CONTINUE
    // everytime expecting the implementation to stop us when
    // we've gone passed iteration num_blocks-1. the above assert
    // maintains this assumption.
    info->scan_iteration++;
    ret = BSTORE_SCAN_CONTINUE;

    return ret;
}

static void test_scans(struct bstore_s * bstore, int magic)
{
    int ret;

    //try scanning to the end from each starting block id
    struct basic_scan_cb_info info;
    info.name = bstore->name;
    info.magic = magic;
    int i;
    i = 0;
    for (i = 0; i < basic_scan_num_blocks; i++) {
        info.scan_iteration = 0;
        info.start_block_id = i;
        ret = bstore_scan(bstore, i, basic_scan_cb, &info);
        assert(ret == 0);
    }
    // choose a couple random ones, half of them in range,
    // half out of range.
    for (i = 0; i < 30; i++) {
        int k = random_long() % basic_scan_num_blocks;
        if (i % 2 == 0) {
            // every other run should test a k that is 
            // definitely invalid.
            k = k*basic_scan_num_blocks + basic_scan_num_blocks;
        }
        info.scan_iteration = 0;
        info.start_block_id = k;
        ret = bstore_scan(bstore, k, basic_scan_cb, &info);
        if (i % 2 == 0) {
            assert(ret == BSTORE_UNINITIALIZED_GET);
        } else {
            assert(ret == 0);
        }
    }

}

int main(void)
{
    int ret;
    int bstore1magic = 71;
    int bstore2magic = 13;
    int bstore3magic = 32;
    struct bstore_s bstore1, bstore2, bstore3;
    random_init();

    ret = bstore_env_open(ENV_PATH, NULL);
    assert(ret == 0);

    // open three different bstores
    ret = bstore_open(&bstore1, bstore1name);
    assert(ret == 0);
    ret = bstore_open(&bstore2, bstore2name);
    assert(ret == 0);
    ret = bstore_open(&bstore3, bstore3name);
    assert(ret == 0);

    // put some blocks in each, disambiguated by their magic number
    load_some_blocks(&bstore2, bstore2magic, basic_scan_num_blocks);
    load_some_blocks(&bstore1, bstore1magic, basic_scan_num_blocks);
    load_some_blocks(&bstore3, bstore3magic, basic_scan_num_blocks);

    // randomly choose a bstore and test scans 
    int i;
    for (i = 0; i < 10; i++) {
        struct bstore_s * b;
        long r = random_long() % 3;
        int m;
        if (r == 0) {
            b = &bstore1;
            m = bstore1magic;
        } else if (r == 1) {
            b = &bstore2;
            m = bstore2magic;
        } else if (r == 2) {
            b = &bstore3;
            m = bstore3magic;
        }
        test_scans(b, m);
    }

    // shut it down
    ret = bstore_close(&bstore2);
    assert(ret == 0);
    ret = bstore_close(&bstore1);
    assert(ret == 0);
    ret = bstore_close(&bstore3);
    assert(ret == 0);

    ret = bstore_env_close();
    assert(ret == 0);
    
    return 0;
}

