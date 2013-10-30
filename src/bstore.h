/**
 * TokuFS
 */

#ifndef TOKU_BSTORE_H
#define TOKU_BSTORE_H

#include <stdint.h>

// support both BDB and TokuDB
#ifdef USE_BDB
#include <db.h>
#else
#include <tokudb.h>
#endif

#ifndef BSTORE_BLOCKSIZE
#define BSTORE_BLOCKSIZE 512 //1024
#endif

#define BSTORE_NOTFOUND -1

// HACK magic key descriptor hack byte
#define DATA_DB_KEY_MAGIC 115

/**
 * Block storage abstraction 
 */
struct bstore_s
{
    char * name;
    size_t name_len;
};

/**
 * Comparison type for database keys. Both metadata and file block
 * keys are compared this way.
 *
 * returns: ret > 0 iff a > b, ret < 0 iff a < b, ret == 0 iff a == b
 */
typedef int (*bstore_env_keycmp_fn)(DB * db, 
        DBT const * a, DBT const * b);

/**
 * _asynchronous_ update callback type,
 * called once a bstore update needs to change the old value.
 *
 * given:
 *  oldval - read only DBT for the old value
 *  newval - writable DBT for the new value, 
 *  extra  - extra parameter passed at the time of toku_bstore_update
 * return:
 *  newval - new value's bytes/size go here
 *  BSTORE_UPDATE_IGNORE if oldval should stay unchanged. newval ignored
 *  BSTORE_UPDATE_DELETE if the value should be deleted. newval ignored
 *  0 if oldval should be replaced by newval
 *  < 0 on error
 */
typedef int (*bstore_update_callback_fn)(const DBT * oldval,
        DBT * newval, void * extra);

#define BSTORE_UPDATE_IGNORE 1
#define BSTORE_UPDATE_DELETE 2

/**
 * scan callback type, called once a bstore has a key/val pair to give
 * to the caller of some scan function. this is not asynchronous.
 *
 * given:
 *  name - the name of the bstore
 *  block_num - the block number if scanning blocks
 *  block/meta - the block or metadata buffer
 *  extra - extra parameter passed to the scan function
 * return:
 *  BSTORE_SCAN_CONTINUE if this function should be called again with the
 *                       next key/value pair from the store.
 *  0 on success, the callback will not be called again
 *  < 0 on error
 */
typedef int (*bstore_scan_callback_fn)(const char * name,
        uint64_t block_num, void * block, void * extra);
typedef int (*bstore_meta_scan_callback_fn)(const char * name,
        void * meta, void * extra);

#define BSTORE_SCAN_CONTINUE 1

//
// Bstore environment operations
//

/**
 * Open a bstore environment at the given path. One will be created
 * if it does not already exist. If keycmp is nonnull, use it to
 * compare keys in both the meta and block databases.
 */
int toku_bstore_env_open(const char * path, bstore_env_keycmp_fn keycmp,
        bstore_update_callback_fn meta_callback_fn);

/**
 * Close the bstore environment.
 */
int toku_bstore_env_close(void);

//
// Bstore operations
//

/**
 * Open a bstore handle with the given name.
 */
int toku_bstore_open(struct bstore_s * bstore, const char * name);

/**
 * Close a bstore, cleaning up after a bstore_open()
 */
int toku_bstore_close(struct bstore_s * bstore);

/**
 * Rename all bstores whose name matches the given prefix
 * by replacing the original prefix with the new one.
 */
int toku_bstore_rename_prefix(const char * oldprefix, const char * newprefix);

/**
 * Get a block from the store, writing its contents into buf, which
 * needs to be at least BSTORE_BLOCKSIZE bytes.
 * 
 * If the block requested was not previously initialized by an update
 * or put, get will return BSTORE_UNITIALIZED_GET, and buf is unchanged
 */
int toku_bstore_get(struct bstore_s * bstore, uint64_t block_num, void * buf);

/**
 * Put a block into the store, whose contents are the first
 * BSTORE_BLOCKSIZE bytes from buf. 
 */
int toku_bstore_put(struct bstore_s * bstore, 
        uint64_t block_num, const void * buf);

/**
 * Update a block in the bstore. The update takes buf and copies size
 * bytes into the affected block, starting at the given offset. The
 * other bytes are unchanged.
 */
int toku_bstore_update(struct bstore_s * bstore, uint64_t block_num, 
        const void * buf, size_t size, size_t offset);

/**
 * Truncate a bstore, deleting any blocks greater than 
 * or equal to the given block number
 */
int toku_bstore_truncate(struct bstore_s * bstore, uint64_t block_num);

/**
 * Scan a bstore's blocks starting at first block greater than or
 * equal to block_num using the given callback and extra paramter.
 * The scan will attempt to prefetch blocks until the 
 * given prefetch block num 
 */
int toku_bstore_scan(struct bstore_s * bstore, 
        uint64_t block_num, uint64_t prefetch_block_num,
        bstore_scan_callback_fn cb, void * extra);

//
// Metadata operations
//

/**
 * Get the metadata block for a given bstore by name. 
 * There is exactly one metadata block per bstore. 
 * The provided buffer should have at least size bytes.
 */
int toku_bstore_meta_get(const char * name, void * buf, size_t size);

/**
 * Update the metadata for a given bstore by name. 
 * Works the same way as bstore_update() except it updates 
 * the metadata and not an abitrary block.
 */
int toku_bstore_meta_update(const char * name,
        const void * extra, size_t extra_size);

/**
 * Scan the metadata, starting with the given name.
 * Works just like bstore_scan, except over the space of
 * all metadata.
 */
int toku_bstore_meta_scan(const char * name, 
        bstore_meta_scan_callback_fn cb, void * extra);

/**
 * Dump out the metadata dictionary to stdout (debugging)
 */
int toku_bstore_meta_dump(void);

//
// Hints and parameters.
//

/**
 * Get or set the cache size used by the bstore environment
 */
size_t toku_bstore_env_get_cachesize(void);

int toku_bstore_env_set_cachesize(size_t cachesize);

#endif /* TOKU_BSTORE_H */
