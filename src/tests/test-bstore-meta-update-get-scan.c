#include "bstore-test.h"

static struct bstore_s bstore;

static char magic = 107;
static size_t metadata_size = 25;

static int update_meta_ignore_cb(const DBT * oldval,
        DBT * newval, void * extra)
{
    (void) extra;
    (void) oldval;
    // these should be ignored. try to mess with it.
    newval->data = extra;
    newval->size = 10;
    return BSTORE_UPDATE_IGNORE;
}

static int update_meta_delete_cb(const DBT * oldval,
        DBT * newval, void * extra)
{
    (void) extra;
    (void) oldval;
    // these should be ignored. try to mess with it.
    newval->data = newval;
    newval->size = 21;
    return BSTORE_UPDATE_DELETE;
}

/**
 * This callback fills an initial meta block with what extra
 * points to.
 */
static int update_meta_initial_cb(const DBT * oldval,
        DBT * newval, void * extra)
{
    (void) extra;
    (void) oldval;
    
    char * value = extra;
    newval->data = malloc(metadata_size);
    newval->size = metadata_size;
    memset(newval->data, *value, metadata_size);

    return 0;
}

static int update_full_meta_cb(const DBT * oldval,
        DBT * newval, void * extra)
{
    (void) oldval;
    newval->data = malloc(metadata_size);
    newval->size = metadata_size;
    
    // the new value for metadata is given by extra
    memcpy(newval->data, extra, metadata_size);

    return 0;
}

static int update_stress_meta_cb(const DBT * oldval,
        DBT * newval, void * extra)
{
    // this should not be the first update to this value
    assert(oldval != NULL);

    size_t * i = extra;
    newval->data = malloc(metadata_size);
    newval->size = metadata_size;
    memcpy(newval->data, oldval->data, metadata_size);
    char c = (*i * *i) % 100;
    char * bytes = newval->data;
    bytes[*i] = c;
    //((char*)newval->data)[*i] = c;

    return 0;
}

/**
 * Test a couple basic cases for updates on a single block.
 */
static void test_basic_meta_update(void)
{
    int ret;
    size_t i;
    char buf[metadata_size];

    // update metadata for this bstore,
    // populates it with magic.
    ret = bstore_meta_update(bstore.name, update_meta_initial_cb,
            &magic, sizeof(char));
    assert(ret == 0);
    
    // read it back, make sure we get all magic
    memset(buf, 0, metadata_size);
    ret = bstore_meta_get(bstore.name, buf, metadata_size);
    assert(ret == 0);
    for (i = 0; i < metadata_size; i++) {
        assert(buf[i] == magic);
    }

    // choose two random metadata bytes to update,
    // such that the value at each index is the index.
    size_t x = random_long() % metadata_size;
    ret = bstore_meta_update(bstore.name,
            update_stress_meta_cb, &x, sizeof(size_t));
    size_t y = random_long() % metadata_size;
    ret = bstore_meta_update(bstore.name,
            update_stress_meta_cb, &y, sizeof(size_t));

    // now, read the block back and make sure
    // everything is as we expect it
    memset(buf, 0, metadata_size);
    ret = bstore_meta_get(bstore.name, buf, metadata_size);
    assert(ret == 0);
    for (i = 0; i < metadata_size; i++) {
        if (i == x) {
            char c = x*x % 100;
            assert(buf[i] == c);
        } else if (i == y) {
            char c = y*y % 100;
            assert(buf[i] == c);
        } else {
            assert(buf[i] == magic);
        }
    }

    // replace the whole block with a wacky block
    char wacky = 31;
    memset(buf, wacky, metadata_size);
    ret = bstore_meta_update(bstore.name,
            update_full_meta_cb, buf, metadata_size);
    assert(ret == 0);

    // read it back, make sure we get wacky
    memset(buf, 0, metadata_size);
    ret = bstore_meta_get(bstore.name, buf, metadata_size);
    for (i = 0; i < metadata_size; i++) {
        assert(buf[i] == wacky);
    }

    // do an update with the ignore callback, a few times.
    // make sure nothing happens
    for (i = 0; i < 15; i++) {
        ret = bstore_meta_update(bstore.name, 
                update_meta_ignore_cb, buf, metadata_size);
        assert(ret == 0);
    }
    
    // read it back, make sure we get wacky
    memset(buf, 0, metadata_size);
    ret = bstore_meta_get(bstore.name, buf, metadata_size);
    for (i = 0; i < metadata_size; i++) {
        assert(buf[i] == wacky);
    }
}

static void test_stress_update(void)
{
    int ret;
    size_t i;
    size_t * indicies = malloc(sizeof(size_t) * metadata_size); 

    // initial update the metadata to magic
    char buf[metadata_size];
    memset(buf, 0, metadata_size);
    ret = bstore_meta_update(bstore.name, 
            update_meta_initial_cb, &magic, sizeof(char));
    assert(ret == 0);

    // get a random permutation of indicies 0..metadata_size-1
    for (i = 0; i < metadata_size; i++) {
        indicies[i] = i;
    }
    for (i = metadata_size - 1; i > 0; i--) {
        size_t r = random_long() % (i + 1);
        size_t tmp = indicies[i];
        indicies[i] = indicies[r];
        indicies[r] = tmp;
    }

    // update each index in buffer with its new value,
    // which according to update_stress_meta_cb
    // is buf[i] = i*i % 100
    for (i = 0; i < metadata_size; i++) {
        ret = bstore_meta_update(bstore.name, update_stress_meta_cb, 
                &i, sizeof(size_t));
        assert(ret == 0);
        // every third one, do an ignore callback
        ret = bstore_meta_update(bstore.name, 
                update_meta_ignore_cb, buf, metadata_size);
        assert(ret == 0);
    }

    // read back the metadata
    memset(buf, 0, metadata_size);
    ret = bstore_meta_get(bstore.name, buf, metadata_size);
    assert(ret == 0);

    // make sure everything is correct
    for (i = 0; i < metadata_size; i++) {
        char c = (i * i) % 100;
        assert(buf[i] == c);
    }
    free(indicies);
}

static void test_misuse(void)
{
    int ret;
    char buf[10];
    struct bstore_s b;

    ret = bstore_open(&b, "nopechucktesta");
    assert(ret == 0);
    ret = bstore_meta_get(b.name, buf, 10);
    assert(ret != 0);
    ret = bstore_meta_get(b.name, buf, 10);
    assert(ret != 0);
    ret = bstore_meta_get(b.name, buf, 10);
    assert(ret != 0);
    ret = bstore_close(&b);
    assert(ret == 0);
    ret = bstore_open(&b, "nopechucktesta");
    assert(ret == 0);
    ret = bstore_meta_get(b.name, buf, 10);
    assert(ret != 0);
    ret = bstore_close(&b);
    assert(ret == 0);
}

#define NUM_BSTORES 25
static void test_update_delete(void)
{
    int ret, i;
    struct bstore_s bstores[NUM_BSTORES];
    char buf[metadata_size];
    char name[64];

    // open some bstores
    memset(name, 0, 64);
    for (i = 0; i < NUM_BSTORES; i++) {
        snprintf(name, 64, "bstore-delete-test.%d", i);
        ret = bstore_open(&bstores[i], name);
        assert(ret == 0);
    }

    // put metadata for each bstore
    for (i = 0; i < NUM_BSTORES; i++) {
        char v = i % 100;
        ret = bstore_meta_update(bstores[i].name,
                update_meta_initial_cb, &v, metadata_size);
        assert(ret == 0);
    }

    // generate 3 random indexes to bstores that
    // will be deleted
    int d[3];
    d[0] = random_long() % NUM_BSTORES;
    d[1] = random_long() % NUM_BSTORES;
    d[2] = random_long() % NUM_BSTORES;
    for (i = 0; i < 3; i++) {
        ret = bstore_meta_update(bstores[d[i]].name,
                update_meta_delete_cb, buf, metadata_size);
        assert(ret == 0);
    }

    // for each index in the del array, we should not succeed
    for (i = 0; i < NUM_BSTORES; i++) {
        ret = bstore_meta_get(bstores[i].name, buf, metadata_size);
        if (i == d[0] || i == d[1] || i == d[2]) {
            assert(ret != 0);
        } else 
            assert(ret == 0);
    }

    // clean up
    for (i = 0; i < NUM_BSTORES; i++) {
        ret = bstore_close(&bstores[i]);
        assert(ret == 0);
    }
}

/**
 * scans start at the start key and should go no further than
 * the end key. mark 'visited' when a letter is visited as a key.
 */
struct test_scan_cb_info {
    char * start;
    char * end;
    char visited[26];
};

/**
 * make sure that the key is in range, and the value is correct.
 */
static int test_scan_cb(DBT const * key,
        DBT const * value, void * extra)
{
    int k, i;
    struct test_scan_cb_info * info = extra;
    int ret;
    char * ckeybuf = (char *) key->data;

    // is the key in range?
    assert(key->size == 2); // "a", "b", etc.
    assert(info->start[0] <= ckeybuf[0]);
    assert(info->end[0] >= ckeybuf[0]);
    // are we done?
    if (info->end[0] == ckeybuf[0]) {
        ret = 0;
        goto out;
    }
    // mark this as visited. if its the first, assert that nothing
    // else has been visited, otherwise assert that everything before
    // this has been visited.
    k = ckeybuf[0] - 'a';
    if (info->start[0] == ckeybuf[0]) {
        for (i = 0; i < 26; i++) {
            assert(!info->visited[i]);
        }
    } else {
        int start = info->start[0] - 'a';
        for (i = 0; i < 26; i++) {
            // between start and current, should be visited,
            // not otherwise.
            if (i >= start && i < k) {
                assert(info->visited[i]);
            } else {
                assert(!info->visited[i]);
            }
        }
    }
    info->visited[k] = 1;

    // is the value okay?
    char v = ckeybuf[0];
    char * cbuf = value->data;
    for (i = 0; i < (int) metadata_size; i++) {
        assert(cbuf[i] == v);
    }
    ret = BSTORE_SCAN_CONTINUE;
out:
    return ret;
}

static void test_scan(void)
{
    int i;
    int ret;
    struct bstore_s bstores[26];
    
    // open some bstores, "a".."z"
    char * name = malloc(2);
    memset(name, 0, 2);
    snprintf(name, 2, "a");
    for (i = 0; i < 26; i++) {
        name[0] = 'a' + i;
        ret = bstore_open(&bstores[i], name);
        assert(ret == 0);
    }
    free(name);

    // put metadata for each bstore
    for (i = 0; i < 26; i++) {
        char v = bstores[i].name[0];
        ret = bstore_meta_update(bstores[i].name,
                update_meta_initial_cb, &v, metadata_size);
        assert(ret == 0);
    }

    // generate two bstore indicies, x and y,
    // and do a scan where start=x and end=y.
    // the callback will maintain the invariants.
    char startname[2];
    char endname[2];
    memset(startname, 0, 2);
    memset(endname, 0, 2);
    struct test_scan_cb_info info;
    info.start = startname;
    info.end = endname;
    for (i = 0; i < 100; i++) {
        long x = random_long() % 26;
        // make sure y is always >= x but < 26
        long y = x + (random_long() % (26 - x));
        x += 'a';
        y += 'a';
        //printf("testing scan between %c and %c\n",
        //        (char) x, (char) y);
        startname[0] = x;
        endname[0] = y;
        memset(&info.visited, 0, 26);
        ret = bstore_meta_scan(startname,
                test_scan_cb, &info);
        assert(ret == 0);
    }
    
    // clean up
    for (i = 0; i < 26; i++) {
        ret = bstore_close(&bstores[i]);
        assert(ret == 0);
    }
}

int main(void)
{
    int ret;

    // start it up
    ret = system("rm -rf " ENV_PATH);
    assert(ret == 0);

    ret = bstore_env_open(ENV_PATH, NULL); 
    assert(ret == 0);
    
    random_init();

    // this test depends on only its bstores existing
    // so it has to run first.
    test_scan();

    ret = bstore_open(&bstore, "apples");
    assert(ret == 0);

    // run the tests
    test_misuse();

    test_basic_meta_update();

    test_stress_update();

    test_update_delete();

    // shut it down
    ret = bstore_close(&bstore);
    assert(ret == 0);

    ret = bstore_env_close();
    assert(ret == 0);

    return 0;
}

