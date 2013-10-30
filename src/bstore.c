/**
 * TokuFS
 */

#define TOKUDB_CURSOR_CONTINUE 0
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>

#include <sys/stat.h>

#include <toku/str.h>
#include <toku/debug.h>

#include "bstore.h"
#include "byteorder.h"

// each db will have its own identifier which we
// can store in the app_private field.
#define DATA_DB_NAME "data"
#define META_DB_NAME "meta"

// There is exactly one db environment per process,
// plus one data and one meta database. the environment
// may have an associated key comparator, if keycmp != NULL
static DB_ENV * db_env;
static DB * data_db;
static DB * meta_db;
static bstore_env_keycmp_fn env_keycmp;
static bstore_update_callback_fn meta_update_cb;
static size_t db_cachesize = 1L * 1024L * 1024 * 1024;

/**
 * Initialize a DBT with the given data pointer and size.
 */
static void dbt_init(DBT * dbt, const void * data, size_t size)
{
    memset(dbt, 0, sizeof(DBT));
    dbt->data = (void *) data;
    dbt->size = size;
    dbt->ulen = size;
    dbt->flags = DB_DBT_USERMEM;
}

/**
 * Copy the buffers referred to by one dbt into another.
 */
static void dbt_copy_allocate(DBT * a, DBT * b)
{
    memset(b, 0, sizeof(DBT));
    b->data = malloc(a->size);
    memcpy(b->data, a->data, a->size);
    b->size = a->size;
    b->ulen = b->ulen;
    b->flags = a->flags;
}

/**
 * Get the block number from a data key dbt.
 */
static uint64_t get_data_key_block_num(DBT const * key)
{
    // HACK subtract one more to account for magic data key byte
    uint64_t * ptr = key->data + key->size - sizeof(uint64_t) - 1;
    uint64_t block_num = ntohl64(*ptr);
    return block_num;
}

/**
 * For a given bstore and block number, generate the data database key
 * database thing. the provided buffer must be large enough for the
 * bstore name, null byte, 8 byte block id. 
 *
 * ...and HACK 1 byte magic 
 */
static void generate_data_key_dbt(DBT * key_dbt, 
        char * key_buf, size_t key_buf_len, 
        const char * name, uint64_t block_num)
{
    uint64_t k = htonl64(block_num);

    // we'll pass the null terminated string to the key, 
    // because its worth one byte to save the headache of not 
    // being able to use string functions in key comparisons
    size_t key_name_prefix_len = key_buf_len - sizeof(uint64_t) - 1;
    memcpy(key_buf, name, key_name_prefix_len);
    memcpy(key_buf + key_name_prefix_len, &k, sizeof(uint64_t));
    // HACK need one more byte for the magic data key byte
    key_buf[key_buf_len - 1] = DATA_DB_KEY_MAGIC;
    dbt_init(key_dbt, key_buf, key_buf_len);
}

/**
 * For a given bstore, generate the meta db key database thing,
 * using the bstore's name as the key's value. For convenience,
 * store the null terminated string, so we can use string functions
 * when we read keys back.
 */
static void generate_meta_key_dbt(DBT * key_dbt, const char * name)
{
    dbt_init(key_dbt, name, strlen(name) + 1);    
}

/**
 * Info passed to the block update callback. It says that
 * size bytes should be applied to the block, starting at
 * offset, with the given buf. 
 */
struct block_update_cb_info
{
    size_t size;
    uint64_t offset;
    char buf[];
};

/**
 * Update a block by copying bytes from the update op's buf 
 * into the new buff starting at offset, for size bytes. 
 */
static int block_update_cb(const DBT * oldval,
        DBT * newval, void * extra)
{
    assert(newval != NULL);
    assert(extra != NULL);

    struct block_update_cb_info * info = extra;
    debug_echo("called with info->offset %lu, info->size %lu\n",
            info->offset, info->size);

    // XXX if we do truncates via callbacks eventually, then
    // it probably makes sense to pass info->size == 0
    // and then return BSTORE_UPDATE_DELETE, before malloc.
    
    if (newval->size == 0) {
        newval->data = malloc(BSTORE_BLOCKSIZE);
        newval->size = BSTORE_BLOCKSIZE;
    }
    assert(newval->size == BSTORE_BLOCKSIZE);
    if (oldval != NULL) {
        memcpy(newval->data, oldval->data, BSTORE_BLOCKSIZE);
    } else {
        memset(newval->data, 0, BSTORE_BLOCKSIZE);
    }
    memcpy(newval->data + info->offset, info->buf, info->size);

    return 0;
}

struct set_val_emulator_info {
    DB * db;
    DBT * key;
};

/**
 * emulate the set val function so bdb can reuse the upsert
 * logic, albeit as a rmw.
 */
static void set_val_emulator(const DBT * new_val, void * set_extra)
{
    int ret;
    struct set_val_emulator_info * info = set_extra;
    DB * db = info->db;
    DBT * key = info->key;
    assert(db != NULL);
    assert(key != NULL);

    // the set val function deletes the key if new val is NULL,
    // replaces it otherwise.
    if (new_val != NULL) {
        // get rid of new_val's constness via cast
        ret = db->put(db, NULL, key, (DBT *) new_val, 0);
    } else {
        ret = db->del(db, NULL, key, 0);
    }
    assert(ret == 0);
}

/**
 * The update callback will come back with a key and old value.
 * The extra DBT will have a update op pointer in it's data.
 * 
 * If the callback passes a null old_val, that means there was
 * no old value for the given key and we must create one.
 *
 * Use the callback in the update op structure to update the
 * old value into a new value, then set the new value and
 * free up the bytes allocated by the callback. 
 */
static int env_update_cb(DB * db, 
        const DBT * key, const DBT * old_val, const DBT * extra, 
        void (*set_val)(const DBT * new_val, void * set_extra),
        void * set_extra)
{
    assert(db != NULL && key != NULL);
    assert(extra != NULL && extra->data != NULL);

    int ret;
    DBT val, newval;

    // HACK bad hack to identify meta vs data db.
    // the idea is that metadata keys are null terminated
    // strings, so they end in zero. data db keys were
    // hacked to have a magic byte at the end, so that
    // is how we identify them.
    char * k = key->data;
    int is_data_db = k[key->size - 1] == DATA_DB_KEY_MAGIC;
    int is_meta_db = k[key->size - 1] == 0;
    assert(is_data_db ^ is_meta_db);

    // provide the callback with a buffer equal in size
    // to the old value's. if there was no old value,
    // then give a 0 byte buffer. if the callback needs
    // more buffer space than we provide here, it will 
    // allocate the space and put a pointer to it
    // in newval.data and the size in newval.size. 
    size_t newval_buf_size = old_val != NULL ? old_val->size : 0;
    char newval_buf[newval_buf_size];
    newval.data = newval_buf;
    newval.size = newval_buf_size;
    ret = 0;
    if (is_data_db) {
        debug_echo("called for data db\n");
        ret = block_update_cb(old_val, &newval, extra->data);
    } else if (is_meta_db) {
        debug_echo("called for meta db\n");
        ret = meta_update_cb(old_val, &newval, extra->data);
    }

    switch (ret) {
        case BSTORE_UPDATE_DELETE:
            set_val(NULL, set_extra);
        case BSTORE_UPDATE_IGNORE:
            assert(newval.data == newval_buf);
            ret = 0;
            break;
        case 0:
            dbt_init(&val, newval.data, newval.size);
            set_val(&val, set_extra);
            // we are responsible for freeing newval.data if
            // its not the same buffer we passed 
            if (newval.data != newval_buf) {
                free(newval.data);
            }
    }

    return ret;
}

//
// Bstore environment operations
//

/**
 * Open the database environment at the given path.
 */
static int env_open(const char * path)
{
    int ret;
    uint32_t flags = 0;
    uint32_t gb, bytes;

    // Set up the environment and set default parameters.
    assert(db_env == NULL);   
    ret = db_env_create(&db_env, 0);
    assert(ret == 0);
    gb = db_cachesize / (1L << 30);
    bytes = db_cachesize % (1L << 30);
    assert(gb > 0 || bytes > 0);
    ret = db_env->set_cachesize(db_env, gb, bytes, 1);
    assert(ret == 0);
#ifndef USE_BDB
    db_env->set_update(db_env, env_update_cb);
    if (env_keycmp != NULL) {
        ret = db_env->set_default_bt_compare(db_env, env_keycmp);
        assert(ret == 0);
    }
#else
    (void) env_update_cb;
#endif
    flags = DB_CREATE | DB_PRIVATE | DB_THREAD | DB_INIT_MPOOL;
    ret = db_env->open(db_env, path, flags, 0755);
    assert(ret == 0);

    return ret;
}

#undef SET_READ_PARAMS

static void db_set_read_params(DB * db)
{
#ifdef SET_READ_PARAMS
    const long num_basement_nodes = 1;
    const long basement_node_size = 4L * 1024*1024;
    const long leaf_node_size = num_basement_nodes * basement_node_size;
    int ret;
    ret = db->set_readpagesize(db, basement_node_size);
    assert(ret == 0);
    ret = db->set_pagesize(db, leaf_node_size);
    assert(ret == 0);
    printf("set basement node size to %ld, leaf node size to %ld\n",
            basement_node_size, leaf_node_size);
#else
    (void) db;
#endif
}

/**
 * Open the meta and data databases.
 */
static int env_open_databases(void)
{
    int ret;
    int flags = DB_CREATE | DB_THREAD;
    assert(db_env != NULL);

    // open the data db
    assert(data_db == NULL);
    ret = db_create(&data_db, db_env, 0);
    assert(ret == 0);
    db_set_read_params(data_db);
    ret = data_db->open(data_db, NULL, DATA_DB_NAME, NULL,
            DB_BTREE, flags, 0644);
    assert(ret == 0);

    // open the meta db
    assert(meta_db == NULL);
    ret = db_create(&meta_db, db_env, 0);
    assert(ret == 0);
    db_set_read_params(meta_db);
    ret = meta_db->open(meta_db, NULL, META_DB_NAME, NULL,
            DB_BTREE, flags, 0644);
    assert(ret == 0);

    return ret;
}

/**
 * Possibly create a directory at the given path, fail only if it
 * exists already as a regular file.
 */
static int os_maybe_mkdir(const char * path)
{
    int ret;
    struct stat st;

    ret = mkdir(path, 0755);
    if (ret != 0 && errno == EEXIST) {
        stat(path, &st);
        if (S_ISDIR(st.st_mode)) {
            ret = 0;
        }
    }

    return ret;
}

/**
 * Open a bstore environment at the given path. One will be created
 * if it does not already exist. If keycmp is nonnull, use it to
 * compare keys in both the meta and block databases.
 */
int toku_bstore_env_open(const char * path, bstore_env_keycmp_fn keycmp,
        bstore_update_callback_fn meta_update)
{
    int ret;

    assert(env_keycmp == NULL);
    env_keycmp = keycmp;
    assert(meta_update_cb == NULL);
    meta_update_cb = meta_update;

    ret = os_maybe_mkdir(path);
    assert(ret == 0);
    ret = env_open(path);
    assert(ret == 0);
    ret = env_open_databases();
    assert(ret == 0);

    return ret;
}

/**
 * Close the bstore environment.
 * via bstore_env_open(). 
 */
int toku_bstore_env_close(void)
{
    int ret;

#undef OPTIMIZE_ON_CLOSE
    // close the data db.
    assert(data_db != NULL);
#ifdef OPTIMIZE_ON_CLOSE
    ret = data_db->optimize(data_db);
    assert(ret == 0);
    ret = data_db->hot_optimize(data_db, NULL, NULL);
    assert(ret == 0);
#endif
    ret = data_db->close(data_db, 0);
    assert(ret == 0);
    data_db = NULL;

    // close the meta db
    assert(meta_db != NULL);
#ifdef OPTIMIZE_ON_CLOSE
    ret = meta_db->optimize(meta_db);
    assert(ret == 0);
    ret = meta_db->hot_optimize(meta_db, NULL, NULL);
    assert(ret == 0);
#endif
    ret = meta_db->close(meta_db, 0);
    assert(ret == 0);
    meta_db = NULL;

    // close the environment
    assert(db_env != NULL);
    ret = db_env->close(db_env, 0);
    assert(ret == 0);
    db_env = NULL;

    // forget the key comparator and update functions
    env_keycmp = NULL;
    meta_update_cb = NULL;

    return ret;
}

//
// Bstore operations
//

/**
 * Open a bstore handle with the given name.
 */
int toku_bstore_open(struct bstore_s * bstore, const char * name)
{    
    assert(db_env != NULL);

    // We use the null terminated name string as a key into the 
    // data db, plus sizeof(uint64_t) more for a block id
    bstore->name_len = strlen(name);
    bstore->name = toku_strdup(name);

    return 0;
}

/**
 * Close a bstore, cleaning up after a bstore_open()
 */
int toku_bstore_close(struct bstore_s * bstore)
{   
    free(bstore->name);

    return 0;
}

// tells the callback whether it should get the current
// value or check if we should name it, and set the flag
#define RENAME_PREFIX_CB_OP_GET 1
#define RENAME_PREFIX_CB_OP_CHECK 2

struct rename_prefix_cb_info {
    int op;
    union {
        struct {
            int should_rename;
            const char * oldprefix;
        } check;
        struct {
            DBT * key;
            DBT * value;
        } get;
    } u;
};

static int rename_prefix_cb(DBT const * key,
        DBT const * value, void * extra)
{
    struct rename_prefix_cb_info * info = extra;
    (void) value;

    if (info->op == RENAME_PREFIX_CB_OP_GET) {
        // fetch this pair by copying the given key/value
        // buffers into the callers' dbt, allocating buffers
        // and copying with dbt_copy_allocate
        //
        // need to cast away const'ness
        dbt_copy_allocate((DBT *) key, info->u.get.key);
        dbt_copy_allocate((DBT *) value, info->u.get.value);
    } else {
        assert(info->op == RENAME_PREFIX_CB_OP_CHECK);
        info->u.check.should_rename = 0;
        if (toku_strprefix(key->data, info->u.check.oldprefix)) {
            char * c = key->data + strlen(info->u.check.oldprefix);
            // this key should be renamed iff it is exactly
            // equal to the old prefix, or has the old prefix
            // followed by a forward slash. in the
            // first case, it's the file itself, in the latter 
            // latter case, it is some file in the subtree
            // rooted by the directory getting renamed.
            if (*c == '\0' || *c == '/') {
                info->u.check.should_rename = 1;
            }
        } 
        debug_echo("rename_cb: key %s and oldprefix %s, should_rename? %s\n",
                (char*)key->data, info->u.check.oldprefix, 
                info->u.check.should_rename ? "yes" : "no");
    }
    return 0;
}

static int rename_cursor_current_prefix(DB * db, DBC * cursor, 
        const char * oldprefix, const char * newprefix)
{
    int ret;
    DBT key, value;
    DBT newkey;

    debug_echo("rename current called on %s with old %s, new %s\n",
            db == data_db ? "data db" : "meta db", 
            oldprefix, newprefix);

    struct rename_prefix_cb_info info;
    info.u.get.key = &key;
    info.u.get.value = &value;
    info.op = RENAME_PREFIX_CB_OP_GET;
#ifndef USE_BDB
    ret = cursor->c_getf_current(cursor, 0, rename_prefix_cb, &info);
#else
    (void) rename_prefix_cb;
    (void) cursor;
    ret = ENOSYS;
#endif
    assert(ret == 0);

    // the new key is as big as the old one, minus the old
    // prefix, plus the new prefix.
    size_t oldprefix_len = strlen(oldprefix);
    size_t newprefix_len = strlen(newprefix);
    size_t newkey_size = info.u.get.key->size - oldprefix_len + newprefix_len;
    debug_echo("old key size %u, oldprefix len %lu, newkey_size %lu\n",
            info.u.get.key->size, oldprefix_len, newkey_size);
    char newkey_buf[newkey_size];
    // write the new prefix into the key
    memcpy(newkey_buf, newprefix, newprefix_len);
    // then write the rest of the old key without the old prefix
    memcpy(newkey_buf + newprefix_len, 
            info.u.get.key->data + oldprefix_len,
            info.u.get.key->size - oldprefix_len);
    dbt_init(&newkey, newkey_buf, newkey_size);

    // get rid of the old pair, then put the new one
    debug_echo("deleting cursor current, db is %s\n",
            db == data_db ? "data" : "meta");
    if (db == data_db) {
        uint64_t k = get_data_key_block_num(&key);
        (void)k;
        uint64_t newk = get_data_key_block_num(&newkey);
        (void)newk;
        debug_echo("oldkey is %u bytes, %s[%lu]\n", key.size, 
                (char*)key.data, k);
        debug_echo("newkey is %u bytes, %s[%lu]\n", newkey.size, 
                (char*)newkey.data, newk);
    } else {
        debug_echo("oldkey is %u bytes, %s\n", key.size, (char*)key.data);
        debug_echo("newkey is %u bytes, %s\n", newkey.size, (char*)newkey.data);
    }
    ret = ENOSYS;
    //ret = cursor->c_del(cursor, 0);
    assert(ret == 0);
    ret = db->put(db, NULL, &newkey, &value, 0);
    assert(ret == 0);

    // we are responsible for freeing the callback allocated
    // key and value buffers' data
    free(info.u.get.key->data);
    free(info.u.get.value->data);

    return 0;
}

static int rename_prefix(DB * db, const char * oldprefix, 
        const char * newprefix)
{
    int ret;
    DBC * cursor;
    DBT key;

    // get a cursor over the db
    if (db == data_db) {
        ret = data_db->cursor(data_db, NULL, &cursor, 0);
    } else {
        assert(db == meta_db);
        ret = meta_db->cursor(meta_db, NULL, &cursor, 0);
    }
    assert(ret == 0);

    size_t oldprefix_len = strlen(oldprefix);
    for (int i = 0, done = 0; !done; i++) {
        // we will use a prefix string with _i_ extra slashes
        // appended to the end. this means each set_range will
        // put us at the beginning of 1 or more contigous keys
        // that share a prefix, until there are no more, at
        // which point it returns DB_NOTFOUND
        // HACK i*2 because we need "i" slashes interleaved with "i" 0x1's
        size_t prefix_buf_len = oldprefix_len + 1 + i*2;
        if (db == data_db) {
            // XXX magic data db byte hack.
            prefix_buf_len += sizeof(uint64_t);
            prefix_buf_len++;
        }
        char prefix_buf[prefix_buf_len];
        strcpy(prefix_buf, oldprefix);
        // each iteration adds a slash followed by a !, a low valued ascii char
        for (int k = 0; k < i*2; k += 2) {
            char * c = prefix_buf + oldprefix_len + k;
            c[0] = '/';
            c[1] = 0x1;
        }
        if (db == data_db) {
            // XXX magic data db byte hack
            memset(prefix_buf + oldprefix_len + i, 0, sizeof(uint64_t) + 1);
            prefix_buf[prefix_buf_len - 1] = DATA_DB_KEY_MAGIC;
        } else { 
            prefix_buf[prefix_buf_len - 1] = 0;
        }
        dbt_init(&key, prefix_buf, prefix_buf_len);

        debug_echo("%s db : using key %s for rename...\n", 
                db == data_db ? "data" : "meta", prefix_buf);
        struct rename_prefix_cb_info info;
        info.op = RENAME_PREFIX_CB_OP_CHECK;
        info.u.check.oldprefix = oldprefix;
        info.u.check.should_rename = 0;
#ifndef USE_BDB
        ret = cursor->c_getf_set_range(cursor, 0, &key, 
                rename_prefix_cb, &info);
#else
        (void) cursor;
        ret = ENOSYS;
#endif
        assert(ret == 0 || ret == DB_NOTFOUND);
        debug_echo("initial getf_set_range ret %s\n", 
                ret == DB_NOTFOUND ? "DB_NOTFOUND" : "0");
        if (!info.u.check.should_rename) {
            // the callback looked at the first key and decided that
            // we should not rename it. there cannot exist any
            // keys with this prefix that are deeper, so we're done
            done = 1;
        } else {
            do {
                // rename the cursor's current element 
                // to have this prefix.
                ret = rename_cursor_current_prefix(db, 
                        cursor, oldprefix, newprefix);
                assert(ret == 0);
                info.u.check.should_rename = 0;
#ifndef USE_BDB
                ret = cursor->c_getf_next(cursor, 0, rename_prefix_cb, &info);
#else
                (void) cursor;
                ret = ENOSYS;
#endif
                assert(ret == 0 || ret == DB_NOTFOUND);
                // keep renaming while the callback says we should
            } while (info.u.check.should_rename);
        }
        // we only do one pass of the data_db, 
        // due to its depth first sort order
        if (db == data_db) {
            done = 1;
        } else {
            assert(db == meta_db);
        }
    }

    ret = cursor->c_close(cursor);
    assert(ret == 0);
    return ret;
}

/**
 * Rename all bstores whose name matches the given prefix
 * by replacing the original prefix with the new one.
 */
int toku_bstore_rename_prefix(const char * oldprefix, const char * newprefix)
{
    rename_prefix(data_db, oldprefix, newprefix);
    rename_prefix(meta_db, oldprefix, newprefix);

    return 0;
}

/**
 * Get a block from the store, writing its contents into buf, which
 * needs to be at least BSTORE_BLOCK_SIZE bytes.
 * 
 * If the block requested was not previously initialized by an update
 * or put, get will return BSTORE_UNITIALIZED_GET, and buf is unchanged
 */
int toku_bstore_get(struct bstore_s * bstore, uint64_t block_num, void * buf)
{
    int ret;
    DBT key, value;

    debug_echo("called, block_num %lu\n", block_num);
    size_t key_buf_len = strlen(bstore->name) + sizeof(uint64_t) + 1;
    char key_buf[key_buf_len];
    generate_data_key_dbt(&key, key_buf, key_buf_len, 
            bstore->name, block_num);
    dbt_init(&value, buf, BSTORE_BLOCKSIZE);
    ret = data_db->get(data_db, NULL, &key, &value, 0);
    assert(ret == 0 || ret == DB_NOTFOUND);
    if (ret == DB_NOTFOUND) {
        ret = BSTORE_NOTFOUND;
    }

    return ret;
}

/**
 * Put a block into the store, whose contents are the first
 * BSTORE_BLOCK_SIZE bytes from buf. 
 */
int toku_bstore_put(struct bstore_s * bstore, 
        uint64_t block_num, const void * buf)
{
    int ret;
    DBT key, value;

    debug_echo("called, block_num %lu\n", block_num);
    size_t key_buf_len = strlen(bstore->name) + sizeof(uint64_t) + 1;
    char key_buf[key_buf_len];
    generate_data_key_dbt(&key, key_buf, key_buf_len, 
            bstore->name, block_num);
    dbt_init(&value, buf, BSTORE_BLOCKSIZE);
    ret = data_db->put(data_db, NULL, &key, &value, 0);
    assert(ret == 0);

    return ret;
}

/**
 * Update a block using read-modify-write (slow)
 */
static int bstore_update_rmw(struct bstore_s * bstore, uint64_t block_num,
        const void * buf, size_t size, size_t offset)
{
    int ret;
    DBT key, value;
    DBT * oldval;

    // get the old block, if it exists.
    char block[BSTORE_BLOCKSIZE];
    size_t key_buf_len = strlen(bstore->name) + sizeof(uint64_t) + 1;
    char key_buf[key_buf_len];
    generate_data_key_dbt(&key, key_buf, key_buf_len,
            bstore->name, block_num);
    dbt_init(&value, block, BSTORE_BLOCKSIZE);
    ret = data_db->get(data_db, NULL, &key, &value, 0);
    assert(ret == 0 || ret == DB_NOTFOUND);
    oldval = ret == 0 ? &value : NULL;

    struct block_update_cb_info * info;
    size_t info_size = sizeof(struct block_update_cb_info) + size;
    char info_buf[info_size];
    // create a block update cb info structure out of the 
    // buf,size,offset triple and use it as the extra parameter
    // to an update 
    info = (struct block_update_cb_info *) info_buf;
    info->offset = offset;
    info->size = size;
    memcpy(info->buf, buf, size);
    // call the environment update callback immidiately. pass NULL
    // for oldval if it did not exist. we'll use a custom setval
    // function to emulate what tokudb does.
    struct set_val_emulator_info set_val_info = {
        .db = data_db,
        .key = &key,
    };
    DBT extra_dbt;
    dbt_init(&extra_dbt, info, info_size);
    ret = env_update_cb(meta_db, &key, oldval, &extra_dbt, 
            set_val_emulator, &set_val_info);
    assert(ret == 0);
    return 0;
}

/**
 * Update a block in the bstore. The update takes buf and copies size
 * bytes into the affected block, starting at the given offset. The
 * other bytes are unchanged.
 */
int toku_bstore_update(struct bstore_s * bstore, uint64_t block_num,
        const void * buf, size_t size, size_t offset)
{
#ifdef USE_BDB
    return bstore_update_rmw(bstore, block_num, buf, size, offset);
#else
    int ret;
    DBT key, extra_dbt;
    struct block_update_cb_info * info;
    size_t info_size = sizeof(struct block_update_cb_info) + size;
    char info_buf[info_size];

    debug_echo("called, block_num %lu\n", block_num);

    // create a block update cb info structure out of the 
    // buf,size,offset triple and use it as the extra parameter
    // to an update 
    info = (struct block_update_cb_info *) info_buf;
    info->offset = offset;
    info->size = size;
    memcpy(info->buf, buf, size);
    size_t key_buf_len = strlen(bstore->name) + sizeof(uint64_t) + 1;
    char key_buf[key_buf_len];
    generate_data_key_dbt(&key, key_buf, key_buf_len, 
            bstore->name, block_num);
    dbt_init(&extra_dbt, info, info_size);
    ret = data_db->update(data_db, NULL, &key, &extra_dbt, 0);
    assert(ret == 0);

    return ret;
#endif
}

/**
 * Test if two keys share the name prefix that bstore block
 * keys have. The name prefix is everything except the last
 * sizeof(uint64_t) (8) bytes.
 */
static int keys_share_name_prefix(DBT const * a, DBT const * b)
{
    int samesize = a->size == b->size;
    return samesize && toku_strprefix(a->data, b->data);
}

struct truncate_cursor_cb_info {
    DBT * key;
    uint64_t block_num;
    int should_delete;
};

/**
 * Examine the given key and mark should_delete
 * if the keys share a name prefix and the given
 * key's block id is greater than or equal to the
 * info's block id
 */
static int truncate_cursor_cb(DBT const * key,
        DBT const * value, void * extra)
{
    struct truncate_cursor_cb_info * info = extra;
    (void) value;

    info->should_delete = 0;
    if (keys_share_name_prefix(key, info->key)) {
        uint64_t block_num = get_data_key_block_num(key);
        if (block_num >= info->block_num) {
            info->should_delete = 1;
        }
    }

    return 0;
}

/**
 * Truncate a bstore, deleting any blocks greater than or 
 * equal to the given block number.
 */
int toku_bstore_truncate(struct bstore_s * bstore, uint64_t block_num)
{
    int ret;
    DBT key;
    DBC * cursor;

    size_t key_buf_len = strlen(bstore->name) + sizeof(uint64_t) + 1;
    char key_buf[key_buf_len];
    generate_data_key_dbt(&key, key_buf, key_buf_len, 
            bstore->name, block_num);
    struct truncate_cursor_cb_info info;
    info.key = &key;
    info.block_num = block_num;
    info.should_delete = 0;

    // put the cursor at the first key greater than
    // or equal to the first block number
    ret = data_db->cursor(data_db, NULL, &cursor, 0);
    assert(ret == 0);
#ifndef USE_BDB
    ret = cursor->c_getf_set_range(cursor, 0, &key,
            truncate_cursor_cb, &info);
#else
    (void) cursor;
    ret = ENOSYS;
#endif

    // the callback comes back with should_delete iff
    // the key it saw was within truncate range, so
    // we continue to getf_next and then delete while 
    // that is the case
    while (info.should_delete) {
        ret = ENOSYS;
        //ret = cursor->c_del(cursor, 0);
        assert(ret == 0);
#ifndef USE_BDB
        ret = cursor->c_getf_next(cursor, 0, truncate_cursor_cb, &info);
#else
        (void) truncate_cursor_cb;
        (void) cursor;
        ret = ENOSYS;
#endif
        assert(ret == 0 || ret == DB_NOTFOUND);
        // if we hit the end of the db, stop early
        if (ret == DB_NOTFOUND) {
            break;
        }
    }

    ret = cursor->c_close(cursor);
    assert(ret == 0);

    return ret;
}

/**
 * The block scan callback needs to know about the real
 * callback+extra to call when it gets pairs and the start_key,
 * so it can stop sending pairs once they bstore names are off.
 */
struct block_scan_cb_info {
    bstore_scan_callback_fn cb;
    DBT * start_key;
    void * extra;
    int do_continue;
};

/**
 * block bstore scans go here, where the pair is first checked
 * to see if it is corresponding to the caller's bstore and
 * passing it to the real callback if so. we then continue if
 * the callback so desires.
 */
static int block_scan_cb(DBT const * key, DBT const * val, void * extra)
{
    int ret;
    struct block_scan_cb_info * info = extra;

    // if they don't share the name prefix, then they're not
    // talking about the same bstore, so we must have reached
    // the end of the desired bstore's "block stream".
    // otherwise check the block number and give it to
    // the callback if it's before the end of the requested range
    ret = 0;
    info->do_continue = 0;
    if (keys_share_name_prefix(info->start_key, key)) {
        uint64_t block_num = get_data_key_block_num(key);
        ret = info->cb(key->data, block_num, val->data, info->extra);
        if (ret == BSTORE_SCAN_CONTINUE) {
            info->do_continue = 1;
            ret = TOKUDB_CURSOR_CONTINUE;
        }
    }

    return ret;
}

/**
 * Scan a bstore's blocks starting at first block greater than or
 * equal to block_num using the given callback and extra paramter.
 * The scan will attempt to prefetch blocks until the 
 * given prefetch block num 
 */
int toku_bstore_scan(struct bstore_s * bstore, 
        uint64_t block_num, uint64_t prefetch_block_num,
        bstore_scan_callback_fn cb, void * extra)
{
    int r, ret;
    DBT key, prefetch_key;
    DBC * cursor;

    // HACK aggresively fetch so much
    //block_num_end = UINT64_MAX;

    debug_echo("called, start %lu, prefetch until %lu\n", 
            block_num, prefetch_block_num);
    size_t key_buf_len = strlen(bstore->name) + sizeof(uint64_t) + 1;
    char key_buf[key_buf_len];
    char prefetch_key_buf[key_buf_len];
    generate_data_key_dbt(&key, key_buf, key_buf_len, 
            bstore->name, block_num);
    generate_data_key_dbt(&prefetch_key, prefetch_key_buf, key_buf_len, 
            bstore->name, prefetch_block_num);
    ret = data_db->cursor(data_db, NULL, &cursor, 0);
    assert(ret == 0);

    // acquire a range lock on the block range we want, so
    // that we maybe benefit from prefetching within the range
#ifndef USE_BDB
    ret = cursor->c_pre_acquire_range_lock(cursor, &key, &prefetch_key);
#else
    (void) cursor;
    ret = ENOSYS;
#endif
    assert(ret == 0);
    
    struct block_scan_cb_info info = {
        .cb = cb,
        .start_key = &key,
        .extra = extra,
        .do_continue = 0,
    };
    // set the cursor. if we succeed and the callback indicates
    // it wants more blocks, call it again with getf_next
#ifndef USE_BDB
    ret = cursor->c_getf_set_range(cursor, 0, 
            &key, block_scan_cb, &info);
#else
    (void) block_scan_cb;
    (void) cursor;
    ret = ENOSYS;
#endif
    if (ret == DB_NOTFOUND) {
        ret = BSTORE_NOTFOUND;
        goto out;
    } else {
        assert(ret == 0);
    }

    while (ret == 0 && info.do_continue) {
        info.do_continue = 0;
#ifndef USE_BDB
        ret = cursor->c_getf_next(cursor, 0, block_scan_cb, &info);
#else
        (void) cursor;
        ret = ENOSYS;
#endif
        assert(ret == 0 || ret == DB_NOTFOUND);
    }
    // even if we got db_notfound by continuing, the scan was successful
    ret = 0;

out:
    r = cursor->c_close(cursor);
    assert(r == 0);
    assert(ret == 0 || ret == BSTORE_NOTFOUND);
    return ret;
}

//
// Metadata operations
//

/**
 * Get the metadata block for a given bstore by name. 
 * There is exactly one metadata block per bstore. 
 * The provided buffer should have at least size bytes.
 */
int toku_bstore_meta_get(const char * name, void * buf, size_t size)
{
    int ret;
    DBT key, value;

    generate_meta_key_dbt(&key, name);
    dbt_init(&value, buf, size);
    ret = meta_db->get(meta_db, NULL, &key, &value, 0);
    assert(ret == 0 || ret == DB_NOTFOUND);
    if (ret == DB_NOTFOUND) {
        ret = BSTORE_NOTFOUND;
    }

    return ret;
}

/**
 * Update the metadata using a read-modify-write (slow)
 */
static int bstore_meta_update_rmw(const char * name,
        const void * extra, size_t extra_size)
{
    int ret;
    DBT key, value;
    DBT * oldval;

    // get the old metadata, if it exists.
    // HACK HACK HACK we're not really supposed to know that
    // struct metadata is == struct stat
    char meta_buf[sizeof(struct stat)];
    generate_meta_key_dbt(&key, name);
    dbt_init(&value, &meta_buf, sizeof(meta_buf));
    ret = meta_db->get(meta_db, NULL, &key, &value, 0);
    assert(ret == 0 || ret == DB_NOTFOUND);
    oldval = ret == 0 ? &value : NULL;

    // call the environment update callback immidiately. pass NULL
    // for oldval if it did not exist. we'll use a custom setval
    // function to emulate what tokudb does.
    struct set_val_emulator_info set_val_info = {
        .db = meta_db,
        .key = &key,
    };
    DBT extra_dbt;
    dbt_init(&extra_dbt, extra, extra_size);
    ret = env_update_cb(meta_db, &key, oldval, &extra_dbt, 
            set_val_emulator, &set_val_info);
    assert(ret == 0);
    return ret;
}

/**
 * Update the metadata for a given bstore by name. 
 * Works the same way as bstore_update() except it updates 
 * the metadata and not an abitrary block.
 */
int toku_bstore_meta_update(const char * name,
        const void * extra, size_t extra_size)
{
#ifdef USE_BDB
    return bstore_meta_update_rmw(name, extra, extra_size);
#else
    int ret;
    DBT key, extra_dbt;

    generate_meta_key_dbt(&key, name);
    dbt_init(&extra_dbt, extra, extra_size);
    ret = meta_db->update(meta_db, NULL, &key, &extra_dbt, 0);
    assert(ret == 0);

    return ret;
#endif
}

struct meta_scan_cb_info {
    bstore_meta_scan_callback_fn cb;
    void * extra;
    int do_continue;
};

/**
 * Like the block scan cb, but always passes pairs to the real
 * callback since meta scans have full jursidiction over the
 * metadata space.
 */
static int meta_scan_cb(DBT const * key, DBT const * val, void * extra)
{
    int ret;
    struct meta_scan_cb_info * info = extra;

    info->do_continue = 0;
    ret = info->cb(key->data, val->data, info->extra);
    if (ret == BSTORE_SCAN_CONTINUE) {
        ret = TOKUDB_CURSOR_CONTINUE;
        info->do_continue = 1;
    }

    return ret;
}

/**
 * Scan the metadata, starting with the metadata whose
 * key is greater than or equal to the given name.
 * Works like bstore_scan otherwise.
 */
int toku_bstore_meta_scan(const char * name, 
        bstore_meta_scan_callback_fn cb, void * extra)
{
    int r, ret;
    DBT key;
    DBC * cursor;

    generate_meta_key_dbt(&key, name);
    ret = meta_db->cursor(meta_db, NULL, &cursor, 0);
    assert(ret == 0);
    
    struct meta_scan_cb_info info;
    info.cb = cb;
    info.extra = extra;
    info.do_continue = 0;
#ifndef USE_BDB
    ret = cursor->c_getf_set_range(cursor, 0, &key, meta_scan_cb, &info);
#else
    (void) meta_scan_cb;
    (void) cursor;
    ret = ENOSYS;
#endif
    if (ret == DB_NOTFOUND) {
        ret = BSTORE_NOTFOUND;
        goto out;
    } else {
        assert(ret == 0);
    }

    // continue only if the callback wants to continue and getf_set_range
    // succeeded in finding a matching pair
    while (ret == 0 && info.do_continue) {
#ifndef USE_BDB
        ret = cursor->c_getf_next(cursor, 0, meta_scan_cb, &info);
#else
        (void) meta_scan_cb;
        (void) cursor;
        ret = ENOSYS;
#endif
        assert(ret == 0 || ret == DB_NOTFOUND);
    }
    // even if we got db_notfound by continuing, the scan was successful
    ret = 0;

out:
    r = cursor->c_close(cursor);
    assert(r == 0);
    return ret;
}

static int meta_dump_cb(DBT const * key, DBT const * value, void * extra)
{
    (void) extra; (void) value;
    const char * name = key->data;
    printf("metadump: %s size %u\n", name, key->size);
    return 0;
}

/**
 * Debugging function to dump the meta dictionary.
 */
int toku_bstore_meta_dump(void)
{
    int ret;
    DBC * cursor;

    ret = meta_db->cursor(meta_db, NULL, &cursor, 0);
    assert(ret == 0);

    do {
#ifndef USE_BDB
        ret = cursor->c_getf_next(cursor, 0, meta_dump_cb, NULL);
#else
        (void) meta_dump_cb;
        (void) cursor;
        ret = ENOSYS;
#endif
    } while (ret == 0);

    return 0;
}

//
// Hints and parameters.
//

/**
 * Get or set the cachesize used by the bstore env
 */
size_t toku_bstore_env_get_cachesize(void)
{
    return db_cachesize;
}

/**
 * Set the cache size. Must be set before the env is open.
 */
int toku_bstore_env_set_cachesize(size_t size)
{
    assert(db_env == NULL);

    db_cachesize = size;

    return 0;
}

