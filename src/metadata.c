/**
 * TokuFS
 */

#define _XOPEN_SOURCE 500

#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <time.h>

#include <toku/debug.h>

#include "metadata.h"
#include "block.h"

#define MIN(A, B)           ((A) < (B) ? (A) : (B))
#define MAX(A, B)           ((A) > (B) ? (A) : (B))

/**
 * Meta callbacks do different things
 * based on the type of operation.
 * This enumerates those types.
 */
enum meta_cb_info_type {
    CREATE,
    PREAD,
    PWRITE,
    TRUNCATE,
    SYMLINK,
    DELETE,
    RENAME,
    UTIME,
    CHMOD,
    CHOWN,
};

/**
 * All metadata callback infos will have this
 * header, which has a type indentifier to let
 * the central metadata callback function
 * know which real update callback to use.
 *
 * requires that all meta callback info types
 * have this header as their first field
 */
struct meta_cb_info_header {
    enum meta_cb_info_type type;
};

/**
 * Various metadata callbacks for each type.
 */
static int create_meta_cb(const DBT * oldval,
        DBT * newval, void * extra);
static int pread_meta_cb(const DBT * oldval,
        DBT * newval, void * extra);
static int pwrite_meta_cb(const DBT * oldval,
        DBT * newval, void * extra);
static int truncate_meta_cb(const DBT * oldval,
        DBT * newval, void * extra);
static int symlink_meta_cb(const DBT * oldval,
        DBT * newval, void * extra);
static int delete_meta_cb(const DBT * oldval,
        DBT * newval, void * extra);
static int rename_meta_cb(const DBT * oldval,
        DBT * newval, void * extra);
static int utime_meta_cb(const DBT * oldval,
        DBT * newval, void * extra);
static int chmod_meta_cb(const DBT * oldval,
        DBT * newval, void * extra);
static int chown_meta_cb(const DBT * oldval,
        DBT * newval, void * extra);

/**
 * All metadata updates will go through this callback
 * via the bstore.
 */
int toku_metadata_update_callback(const DBT * oldval,
        DBT * newval, void * extra)
{
    bstore_update_callback_fn cb;
    struct meta_cb_info_header * h = extra;
    assert(h != NULL);

    // choose the real callback based on the type
    switch (h->type) {
        case CREATE:
            cb = create_meta_cb;
            break;
        case PREAD:
            cb = pread_meta_cb;
            break;
        case PWRITE:
            cb = pwrite_meta_cb;
            break;
        case TRUNCATE:
            cb = truncate_meta_cb;
            break;
        case SYMLINK:
            cb = symlink_meta_cb;
            break;
        case DELETE:
            cb = delete_meta_cb;
            break;
        case RENAME:
            cb = rename_meta_cb;
            break;
        case UTIME:
            cb = utime_meta_cb;
            break;
        case CHMOD:
            cb = chmod_meta_cb;
            break;
        case CHOWN:
            cb = chown_meta_cb;
            break;
        default:
            assert(0);
    }

    int ret = cb(oldval, newval, extra);
    return ret;
}

struct create_meta_cb_info {
    struct meta_cb_info_header h;
    time_t ctime;
    mode_t mode;
};

/**
 * This callback serves to update the metadata after a
 * successful tokufs_open.
 *
 * XXX: this should change alot once tokufs_opens force
 * a meta_get due to mode bit checks, because then this should
 * only happen if the metadata doesn't already exist.
 */
static int create_meta_cb(const DBT * oldval,
        DBT * newval, void * extra)
{
    int ret;
    struct create_meta_cb_info * info = extra;
    assert(info->h.type == CREATE);

    if (oldval != NULL) {
        ret = BSTORE_UPDATE_IGNORE;
        goto out;
    }

    // no meta data existed. create it.
    // we have to provide our own buffer space
    newval->data = malloc(METADATA_SIZE);
    newval->size = METADATA_SIZE;
    struct metadata * meta = newval->data;
    memset(meta, 0, METADATA_SIZE);
    meta->st.st_mode = info->mode;
    meta->st.st_nlink = 1;
    meta->st.st_uid = getuid();
    meta->st.st_gid = getgid();
    meta->st.st_blksize = BSTORE_BLOCKSIZE;
    meta->st.st_atime = info->ctime;
    meta->st.st_mtime = info->ctime;
    meta->st.st_ctime = info->ctime;
    ret = 0;

out:
    return ret;
}

/**
 * Update metadata for a create operation, which gives the
 * metadata some default data and does nothing if it's
 * already there.
 */
int toku_metadata_update_for_create(const char * name, 
        time_t create_time, mode_t mode)
{
    int ret;

    struct create_meta_cb_info info;
    info.h.type = CREATE;
    info.ctime = create_time;
    info.mode = mode;
    ret = toku_bstore_meta_update(name, &info, sizeof(info));
    assert(ret == 0);

    return ret;
}

struct pread_meta_cb_info {
    struct meta_cb_info_header h;
    time_t atime;
};

/**
 * Update the access time due to a pread
 */
static int pread_meta_cb(const DBT * oldval,
        DBT * newval, void * extra)
{
    int ret;
    struct pread_meta_cb_info * info = extra;
    assert(info->h.type == PREAD);
    assert(oldval != NULL);

    // if the new time is the same, do nothing
    struct metadata * meta = oldval->data;
    if (meta->st.st_atime == info->atime) {
        ret = BSTORE_UPDATE_IGNORE;
    } else {
        assert(newval->size == METADATA_SIZE);
        memcpy(newval->data, oldval->data, METADATA_SIZE);
        meta = newval->data;
        meta->st.st_atime = info->atime;
        ret = 0;
    }

    return ret;
}

/**
 * Update access time after a pread
 */
int toku_metadata_update_for_pread(const char * name, time_t atime)
{
    int ret;

    struct pread_meta_cb_info info;
    info.h.type = PREAD;
    info.atime = atime;
    ret = toku_bstore_meta_update(name, &info, sizeof(info));
    assert(ret == 0);

    return ret;
}

/**
 * meta callbacks for pwrite set the modified time and possibly
 * set the file size if the last_offset is bigger than the existing
 */
struct pwrite_meta_cb_info {
    struct meta_cb_info_header h;
    time_t mtime;
    off_t last_offset;
};

/**
 * callback to update the metadata after a pwrite
 */
static int pwrite_meta_cb(const DBT * oldval, 
        DBT * newval, void * extra)
{
    assert(oldval != NULL);

    struct pwrite_meta_cb_info * info = extra;
    assert(info->h.type == PWRITE);
    assert(newval->size == METADATA_SIZE);
    memcpy(newval->data, oldval->data, METADATA_SIZE);

    struct metadata * meta = newval->data;
    meta->st.st_mtime = info->mtime;
    meta->st.st_size = MAX(meta->st.st_size, info->last_offset);
    //XXX this should really be the number of blocks allocated,
    //not how many logical blocks fill the potentially sparse file
    //possible solution: have tokufs_write count the number of
    //bstore puts it does and pass that with the cb info
    meta->st.st_blocks = block_get_count_by_size(meta->st.st_size);
    return 0;
}

/**
 * Update modification time and maybe 
 * set highest offset after a pwrite
 */
int toku_metadata_update_for_pwrite(const char * name, 
        time_t mtime, off_t last_offset)
{
    int ret;

    struct pwrite_meta_cb_info info;
    info.h.type = PWRITE;
    info.mtime = mtime;
    info.last_offset = last_offset;
    ret = toku_bstore_meta_update(name, &info, sizeof(info));
    assert(ret == 0);

    return ret;
}

struct truncate_meta_cb_info {
    struct meta_cb_info_header h;
    off_t size;
};

/**
 * The truncate file sized is passed as extra info.
 * The word truncate is deceiving - we're supposed
 * to set the file to exactly the new size
 * even if it's bigger than the old. Blame POSIX.
 */
static int truncate_meta_cb(const DBT * oldval,
        DBT * newval, void * extra)
{
    assert(oldval != NULL);
    struct truncate_meta_cb_info * info = extra;
    assert(info->h.type == TRUNCATE);

    debug_echo("called with size %ld\n", info->size);

    assert(newval->size == METADATA_SIZE);
    memcpy(newval->data, oldval->data, METADATA_SIZE);
    struct metadata * meta = newval->data;
    meta->st.st_size = info->size;
    meta->st.st_blocks = block_get_count_by_size(meta->st.st_size);

    debug_echo("finished. st.st_size %ld\n", meta->st.st_size);

    return 0;
}

/**
 * Update the size and block count of a file after truncate
 */
int toku_metadata_update_for_truncate(const char * name, off_t size)
{
    int ret;

    struct truncate_meta_cb_info info;
    info.h.type = TRUNCATE;
    info.size = size;
    ret = toku_bstore_meta_update(name, &info, sizeof(info));
    assert(ret == 0);

    return ret;
}

struct symlink_meta_cb_info {
    struct meta_cb_info_header h;
    time_t ctime;
    size_t link_size;
};

/**
 * callback to create the metadata for a symlink
 */
static int symlink_meta_cb(const DBT * oldval,
        DBT * newval, void * extra)
{
    assert(oldval == NULL);
    struct symlink_meta_cb_info * info = extra;
    assert(info->h.type == SYMLINK);

    // it is an error for symlink if newpath already exists.
    // this condition should have been checked by the caller.
    assert(newval->size == 0);
    newval->data = malloc(METADATA_SIZE);
    newval->size = METADATA_SIZE;
    struct metadata * meta = newval->data;
    memset(meta, 0, METADATA_SIZE);
    // mark it as a symbolic link
    meta->st.st_mode = S_IFLNK;
    meta->st.st_atime = info->ctime;
    meta->st.st_mtime = info->ctime;
    meta->st.st_ctime = info->ctime;
    meta->st.st_size = info->link_size;
    meta->st.st_blocks = 1;

    return 0;
}

/**
 * Update metadata after a symlink. Requires that the metadata
 * does not already exist.
 */
int toku_metadata_update_for_symlink(const char * name,
        time_t create_time, size_t link_size)
{
    int ret;

    struct symlink_meta_cb_info info;
    info.h.type = SYMLINK;
    info.ctime = create_time;
    info.link_size = link_size;
    ret = toku_bstore_meta_update(name, &info, sizeof(info));
    assert(ret == 0);

    return ret;
}

struct delete_meta_cb_info {
    struct meta_cb_info_header h;
};

/**
 * No hard links supported, so nlinks is always one,
 * and the meta callback always just does a delete.
 */
static int delete_meta_cb(const DBT * oldval,
        DBT * newval, void * extra)
{
    struct delete_meta_cb_info * info = extra;
    assert(info->h.type == DELETE);

    (void) oldval;
    (void) newval;
    debug_echo("deleting metadata\n");
    return BSTORE_UPDATE_DELETE; 
}

/**
 * Delete metadata for the given bstore name
 */
int toku_metadata_delete(const char * name)
{
    int ret;

    struct delete_meta_cb_info info;
    info.h.type = DELETE;
    ret = toku_bstore_meta_update(name, &info, sizeof(info));
    assert(ret == 0);

    return ret;
}

struct rename_meta_cb_info {
    struct meta_cb_info_header h;
    struct metadata meta;
};

/**
 * callback that copies old metadata to a new name
 */
static int rename_meta_cb(const DBT * oldval,
        DBT * newval, void * extra)
{
    // there shouldn't have been something here beforehand
    assert(oldval == NULL);
    // copy over the metadata from info into newval
    struct rename_meta_cb_info * info = extra;
    assert(info->h.type == RENAME);
    if (newval->size == 0) {
        // there was no metadata with this name previously.
        // so we need to give space.
        newval->data = malloc(METADATA_SIZE);
        newval->size = METADATA_SIZE;
    }
    assert(newval->size == METADATA_SIZE);
    memcpy(newval->data, &info->meta, METADATA_SIZE);

    return 0;
}

/**
 * Update metadata after a rename by copying existing metadata
 * into new metadata with the new name.
 */
int toku_metadata_update_for_rename(const char * name, 
        struct metadata * meta)
{
    int ret;

    struct rename_meta_cb_info info;
    info.h.type = RENAME;
    memcpy(&info.meta, meta, METADATA_SIZE);
    ret = toku_bstore_meta_update(name, &info, sizeof(info));

    return ret;
}

struct utime_meta_cb_info {
    struct meta_cb_info_header h;
    const struct utimbuf buf;
};

/**
 * callback to update access and modification times.
 * the utimebuf should not be NULL, that case should
 * be handled by the initial call, not this callback.
 */
static int utime_meta_cb(const DBT * oldval,
        DBT * newval, void * extra)
{
    struct utime_meta_cb_info * info = extra;
    assert(info->h.type == UTIME);

    assert(newval->size == METADATA_SIZE);
    memcpy(newval->data, oldval->data, METADATA_SIZE);
    struct metadata * meta = newval->data;
    meta->st.st_mtime = info->buf.modtime;
    meta->st.st_atime = info->buf.actime;

    return 0;
}

/**
* Update access and modification times for metadata after
* a utime operation.
*/
int toku_metadata_update_for_utime(const char * name,
    const struct utimbuf * buf)
{
    int ret;

    struct utime_meta_cb_info info = {
        .h.type = UTIME, 
        .buf = *buf 
    };
    ret = toku_bstore_meta_update(name, &info, sizeof(info));
    assert(ret == 0);

    return ret;
}

struct chmod_meta_cb_info {
    struct meta_cb_info_header h;
    mode_t mode;
};

/**
 * update callback for a chmod operation.
 * overwrites the old mode with the new one.
 */
static int chmod_meta_cb(const DBT * oldval,
        DBT * newval, void * extra)
{
    struct chmod_meta_cb_info * info = extra;
    assert(info->h.type == CHMOD);
    
    assert(newval->size == METADATA_SIZE);
    memcpy(newval->data, oldval->data, METADATA_SIZE);
    struct metadata * meta = newval->data;
    meta->st.st_mode = info->mode;

    return 0;
}

/**
* Update mode for metadata after a chmod
*/
int toku_metadata_update_for_chmod(const char * name, mode_t mode)
{
    int ret;

    struct chmod_meta_cb_info info;
    info.h.type = CHMOD;
    info.mode = mode;
    ret = toku_bstore_meta_update(name, &info, sizeof(info));
    assert(ret == 0);

    return ret;
}

struct chown_meta_cb_info {
    struct meta_cb_info_header h;
    uid_t owner;
    gid_t group;
};

/**
 * update callback for chown that sets new
 * group or owner
 */
static int chown_meta_cb(const DBT * oldval,
        DBT * newval, void * extra)
{
    struct chown_meta_cb_info * info = extra;
    assert(info->h.type == CHOWN);

    assert(newval->size == METADATA_SIZE);
    memcpy(newval->data, oldval->data, METADATA_SIZE);
    struct metadata * meta = newval->data;
    // the api is stupid and says to ignore if these
    // _unsigned_ values (uid_t, gid_t) are -1
    if (info->owner != (uid_t) -1) {
        meta->st.st_uid = info->owner;
    }
    if (info->group != (gid_t) -1) {
        meta->st.st_gid = info->group;
    }

    return 0;
}

/**
 * Update owner and group for metadata after a chown.
 */
int toku_metadata_update_for_chown(const char * name, 
        uid_t owner, gid_t group)
{
    int ret;

    struct chown_meta_cb_info info;
    info.h.type = CHOWN;
    info.owner = owner;
    info.group = group;
    ret = toku_bstore_meta_update(name, &info, sizeof(info));
    assert(ret == 0);

    return ret;
}
