/**
 * TokuFS
 */

#define _XOPEN_SOURCE 600

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <pthread.h>

#include <tokufs.h>
#include <toku/str.h>
#include <toku/debug.h>

// this will the one and only place the global
// symbol toku_debug will be defined, as prototyped
// in the toku/debug.h header
#ifdef DEBUG
UNUSED
int toku_debug = 1;
#else
UNUSED
int toku_debug = 0;
#endif

#include "metadata.h"
#include "block.h"
#include "bstore.h"

#define MAX_OPEN_FILES      1024
#define MIN(A, B)           ((A) < (B) ? (A) : (B))
#define MAX(A, B)           ((A) > (B) ? (A) : (B))

enum open_file_status {
    FREE,
    VALID,
    PENDING
};

/**
 * An open file is represented by an open bstore.
 */
struct open_file
{
    int last_pread_offset;
    int last_pread_size;
    enum open_file_status status;
    struct bstore_s bstore;
};

/**
 * Path where TokuFS is mounted locally.
 */
static char * mount_path;

/**
 * Table of open files and a lock to protect it.
 */
static struct open_file fd_table[MAX_OPEN_FILES];
static pthread_rwlock_t fd_table_lock = PTHREAD_RWLOCK_INITIALIZER;

/**
 * Synchronize reading and writing to the file descriptor table
 */
static void fd_table_read_lock(void)
{
    debug_echo("called\n");
    pthread_rwlock_rdlock(&fd_table_lock);
}
static void fd_table_write_lock(void)
{
    debug_echo("called\n");
    pthread_rwlock_wrlock(&fd_table_lock);
}
static void fd_table_unlock(void)
{
    debug_echo("called\n");
    pthread_rwlock_unlock(&fd_table_lock);
}

static void invalidate_open_file(struct open_file * file)
{
    file->last_pread_offset = -1;
    file->last_pread_size = -1;
    file->status = FREE;
    memset(&file->bstore, 0, sizeof(struct bstore_s));
}

/**
 * Find a free table index and return it, or -1 if there is none.
 */
static int get_free_fd(void)
{
    int ret;

    ret = -1;
    for (int i = 0; i < MAX_OPEN_FILES; i++) {
        if (fd_table[i].status == FREE) {
            ret = i;
            break;
        }
    }

    return ret;
}

/**
 * Get an open file from the table while holding the lock.
 */
static struct open_file * locked_get_open_file(int fd)
{
    struct open_file * file;

    fd_table_read_lock();
    file = NULL;
    if (fd >= 0 && fd < MAX_OPEN_FILES) {
        file = &fd_table[fd];
        if (file->status != VALID) {
            file = NULL;
        }
    }
    fd_table_unlock();

    return file;
}

//
// File system operations
//

/**
 * Our metadata and blocks will be compared using this function.
 *
 * Metadata keys are ordered differently than data db keys.
 * - We want metadata to have _level_ locality, so we sort them
 *   level order. This makes all the metadata for a directory
 *   contiguous and readable in a single scan.
 * - We want data blocks to have _subtree_ locality, so we sort
 *   them depth first order. This makes all directories and
 *   their children share a common prefix and thus be part
 *   of the same subtree, allowing us to later implement renames
 *   with an efficient tree 'clip and move' operation.
 *
 * Metadata level order is accomplished as follows
 * - the number of slashes (/) comes first. 
 *   a > b if num_slashes(a) > num_slashes(b)
 *   a < b if num_slashes(a) < num_slashes(b)
 *   
 *   if they have the same number of slashes, the result is
 *   equivalent to memcmp
 *
 *     so /apples/bears/years vs /apples/bears/snares becomes a
 *         memcmp(/years, /snares)
 *
 * Data blocks depth first order is accomplished with just a memcmp
 */
static int keycmp(DB * db, DBT const * a, DBT const * b)
{
    (void) db;
    unsigned char *k1 = a->data;
    unsigned char *k2 = b->data;
    int v1, v2, comparelen;
    int num_slashes_k1, num_slashes_k2;

    // HACK check the magic byte to see if this is a data db key
    int magic = *(k1 + a->size - 1);
    if (magic == DATA_DB_KEY_MAGIC) {
        goto just_memcmp;
    }
    assert(magic == 0 || magic == DATA_DB_KEY_MAGIC);

    // count slashes first. winner is greater, tie goes to memcmp
    num_slashes_k1 = toku_strcount(a->data, '/');
    num_slashes_k2 = toku_strcount(b->data, '/');
    if (num_slashes_k1 > num_slashes_k2) {
        return 1;
    } else if (num_slashes_k1 < num_slashes_k2) {
        return -1;
    }

just_memcmp:
    // taken from tokudb/newbrt/key.c and cleaned up to do
    // a memcmp in an 8-unrolled loop
    comparelen = MIN(a->size, b->size);
#undef UNROLL
#define UNROLL 8
#define CMP_BYTE(i) v1 = k1[i]; v2 = k2[i]; if (v1 != v2) return v1 - v2;
    for (; comparelen > UNROLL; 
            k1 += UNROLL, k2 += UNROLL, comparelen -= UNROLL) {
        // maybe this make similar key comparisons faster.
        // maybe the unalignment devastates performance.
        // try only doing this if at least one of them is aligned
        if (*(uint64_t *) k1 == *(uint64_t *) k2) continue;
        CMP_BYTE(0);
        CMP_BYTE(1);
        CMP_BYTE(2);
        CMP_BYTE(3);
        CMP_BYTE(4);
        CMP_BYTE(5);
        CMP_BYTE(6);
        CMP_BYTE(7);
    }
#undef UNROLL
#undef CMP_BYTE
    for (; comparelen > 0; k1++, k2++, comparelen--) {
        if (*k1 != *k2) {
            return (int)*k1 - (int)*k2;
        }
    }
    if (a->size < b->size) {
        return -1;
    } else if (a->size > b->size) {
        return 1;
    } else {
        return 0;
    }
}

/**
 * Mount tokufs at the given path. If a tokufs mount point does not
 * exist at that path, one will be created.
 */
int toku_fs_mount(const char * path)
{
    int ret;

    debug_echo("mounting %s\n", path);
    assert(mount_path == NULL);
    mount_path = toku_strdup(path);
    ret = toku_bstore_env_open(mount_path, keycmp, 
            toku_metadata_update_callback);
    assert(ret == 0);
    // make sure the root directory exists
    ret = toku_fs_mkdir("/", 0755);
    assert(ret == 0);

    return ret;
}

/**
 * Unmount tokufs if there are no outstanding open files, 
 * error otherwise
 */
int toku_fs_unmount(void)
{
    int ret;

    debug_echo("unmounting %s\n", mount_path);
    assert(mount_path != NULL);

    ret = toku_bstore_env_close();
    assert(ret == 0);
    free(mount_path);
    mount_path = NULL;

    debug_echo("successfully unmounted\n");

    return 0;
}

//
// File data operations
//

/**
 * A file exists if and only if we can get its metadata.
 * XXX: this will go away once we force a meta_get in
 * toku_fs_open to check mode bits
 */
static int file_exists(const char * path)
{
    char buf[METADATA_SIZE];
    return toku_bstore_meta_get(path, buf, METADATA_SIZE) == 0;
}

/**
 * Open a tokufs file, returning a file descriptor on success.
 */
int toku_fs_open(const char * path, int flags, mode_t mode)
{
    int i;
    int ret;
    struct open_file * file;

    debug_echo("called path = %s, flags %d, mode %x (O_CREAT ? %d)\n", 
            path, flags, mode, flags & O_CREAT);

    assert(mount_path != NULL);

    // grab a free file descriptor
    fd_table_write_lock();
    i = get_free_fd();
    if (i < 0) {
        ret = -EMFILE;
        goto out;
    }

    file = &fd_table[i];
    invalidate_open_file(file);
    file->status = PENDING;
    fd_table_unlock();

    // if we're opening with O_CREAT, then possibly create this
    // file's metadata if it's new. otherwise, make sure the
    // file already exists.
    ret = 0;
    if (flags & O_CREAT) {
        time_t now = time(NULL);
        ret = toku_metadata_update_for_create(path, now, mode);
        assert(ret == 0);
    } else if (!file_exists(path)) {
        ret = -ENOENT;
    }

    // if there was no previous error, open the bstore
    fd_table_write_lock();
    if (ret == 0) {
        ret = toku_bstore_open(&file->bstore, path);
        assert(ret == 0);
        ret = i;
        file->status = VALID;
    } else {
        invalidate_open_file(file);
    }

out:
    fd_table_unlock();
    debug_echo("done, fd = %d\n", i);
    return ret;
}

/**
 * Close a toku_fs file by closing its underlying bstore
 */
int toku_fs_close(int fd)
{
    int ret;
    struct open_file * file;

    debug_echo("called, fd = %d\n", fd);

    assert(mount_path != NULL);
    if (fd < 0 || fd > MAX_OPEN_FILES) { 
        ret = -EBADF;
        goto out;
    }
    fd_table_write_lock();
    file = &fd_table[fd];
    if (file->status != VALID) {
        ret = -EBADF;
        goto out;
    }

    ret = toku_bstore_close(&file->bstore);
    assert(ret == 0);
    invalidate_open_file(file);

out:
    fd_table_unlock();
    return ret;
}

struct pread_scan_cb_info {
    void * buf;
    off_t offset;
    size_t count;
    size_t bytes_read;
};

/**
 * Update the scan cb info after a read of the given size
 */
static void update_pread_scan_cb_info(
        struct pread_scan_cb_info * info,
        size_t read_size)
{
    info->buf += read_size; 
    info->offset += read_size;
    info->count -= read_size;
    info->bytes_read += read_size;
}

static int pread_scan_cb(const char * name, 
        uint64_t current_block_num,
        void * block_buf, void * extra)
{
    struct pread_scan_cb_info * info = extra;
    uint64_t block_num;
    size_t block_offset;
    size_t read_size;
    (void) name;

    // block_num and block_offset are where we _should_ be
    // reading from. it's possible the current block we're scanning
    // at is further than this. if the current block num is greater
    // than what we want, fill up the buffer with zeros until our
    // offset gives a block number equal to the current one,
    // or we are out of bytes to write (count == 0)
    block_num = block_get_num_by_position(info->offset);
    block_offset = block_get_offset_by_position(info->offset);
    debug_echo("scan starting at current_block_num %lu, need block %lu "
            "at offset %ld\n", current_block_num, block_num,
            block_offset);
    while (info->count > 0 && block_num < current_block_num) {
        // The read size can be no more than the bytes count,
        // nor the bytes left in the block based on offset.
        read_size = MIN(info->count, BSTORE_BLOCKSIZE - block_offset);
        assert(read_size > 0);
        memset(info->buf, 0, read_size);
        debug_echo("block %lu missing, with %lu bytes to go, "
                "padding %lu zeros to buffer at %p\n",
                block_num, info->count, read_size, info->buf);
        // update the info struct, then calc the 
        // next block num and offset
        update_pread_scan_cb_info(info, read_size);
        block_num = block_get_num_by_position(info->offset);
        block_offset = block_get_offset_by_position(info->offset);
    }
    // we've padded up to the current block with zeroes,
    // and we still need more bytes, so we should need
    // the current block.
    if (info->count > 0) {
        assert(block_num == current_block_num);
        read_size = MIN(info->count, BSTORE_BLOCKSIZE - block_offset);
        assert(read_size > 0);
        // copy over the bytes required from the current
        // block and then update the info. if count is
        // still greater than 0 after the update, the
        // other bytes will come from a scan continue
        memcpy(info->buf, block_buf + block_offset, read_size);
        update_pread_scan_cb_info(info, read_size);
    }

    return info->count > 0 ? BSTORE_SCAN_CONTINUE : 0;
}

/**
 * Read count bytes from the file starting at offset into buf.
 */
ssize_t toku_fs_pread(int fd, void * buf,
        size_t count, off_t offset)
{
    int ret;
    ssize_t bytes_read;
    struct open_file * file;

    debug_echo("called with fd = %d, buf = %p, count = %lu, "
            "offset = %lu\n", fd, buf, count, offset);

    if (offset < 0) {
        bytes_read = -EINVAL;
        goto out;
    }
    file = locked_get_open_file(fd);
    if (file == NULL) {
        bytes_read = -EBADF;
        goto out;
    }

    struct metadata meta;
    ret = toku_bstore_meta_get(file->bstore.name, &meta, METADATA_SIZE);
    assert(ret == 0);

    struct pread_scan_cb_info info;
    info.buf = buf;
    info.offset = offset;
    info.count = count;
    info.bytes_read = 0;

    uint64_t block_num = block_get_num_by_position(offset);
    uint64_t end_block_num = block_get_num_by_position(offset + count);
    ret = toku_bstore_scan(&file->bstore, block_num, 
            end_block_num, pread_scan_cb, &info);
    assert(ret == 0 || ret == BSTORE_NOTFOUND);

    // this will fill out any extra bytes after the end
    // of the file as zeros.
    if (info.count > 0) {
        memset(info.buf, 0, info.count);
        info.bytes_read += info.count;
    }

    time_t now = time(NULL);
    ret = toku_metadata_update_for_pread(file->bstore.name, now);
    assert(ret == 0);

    debug_echo("done. offset = %lu, count = %lu, bytes_read = %ld\n", 
            info.offset, info.count, info.bytes_read);
    bytes_read = info.bytes_read;

out:
    return bytes_read;
}

/**
 * Write count bytes into the file starting at offset from buf.
 */
ssize_t toku_fs_pwrite(int fd, const void * buf,
        size_t count, off_t offset)
{
    int ret;
    ssize_t bytes_written;
    size_t write_size;
    struct open_file * file;
    
    debug_echo("called with fd = %d, buf = %p, count = %lu,"
            " offset = %lu\n", fd, buf, count, offset);

    if (offset < 0) {
        bytes_written = -EINVAL;
        goto out;
    }
    file = locked_get_open_file(fd);
    if (file == NULL) {
        bytes_written = -EBADF;
        goto out;
    }
    
    bytes_written = 0;
    while (count > 0) {
        uint64_t block_num = block_get_num_by_position(offset);
        size_t block_offset = block_get_offset_by_position(offset);
        // The write size can be no more than the bytes left to
        // write, nor the bytes left in the block based on offset.
        write_size = MIN(count, (BSTORE_BLOCKSIZE - block_offset));
        assert(write_size > 0);
        // If the write size is a full block, overwrite any old block
        // with the new block. Otherwise, update a subset of bytes 
        if (write_size == BSTORE_BLOCKSIZE) {
            ret = toku_bstore_put(&file->bstore, block_num, buf);
        } else {
            ret = toku_bstore_update(&file->bstore, block_num, 
                    buf, write_size, block_offset);
            assert(ret == 0);
        }

        buf += write_size;
        bytes_written += write_size;
        offset += write_size;
        count -= write_size;
    }

    time_t now = time(NULL);
    ret = toku_metadata_update_for_pwrite(file->bstore.name, now, offset);
    assert(ret == 0);

    debug_echo("done. offset = %lu, count = %lu,"
            " bytes_written = %lu\n", offset, count, bytes_written);
out:
    return bytes_written;
}

//
// Metadata operations
//

/**
 * Get metadata for a particular file. This
 * currently involves just getting the metadata
 * for its associated bstore and copying it over.
 */
int toku_fs_stat(const char * path, struct stat * st)
{
    int ret;
    struct metadata meta;

    ret = toku_bstore_meta_get(path, &meta, METADATA_SIZE);
    if (ret == 0) {
        memcpy(st, &meta.st, sizeof(struct stat));
    } else {
        ret = -ENOENT;
    }

    return ret;
}

/**
 * Truncate a block in a bstore to the given offset.
 */
static int truncate_block(struct bstore_s * bstore, 
        uint64_t block_num, off_t block_offset)
{
    int ret;
    char buf[BSTORE_BLOCKSIZE];

    assert(block_offset < BSTORE_BLOCKSIZE);
    memset(buf, 0, BSTORE_BLOCKSIZE);
    ret = toku_bstore_update(bstore, block_num, buf,
            BSTORE_BLOCKSIZE - block_offset, block_offset);
    assert(ret == 0);

    return 0;
}

/**
 * Truncate a file so that it is no more than length bytes.
 */
int toku_fs_truncate(const char * path, off_t length)
{
    int ret;
    struct metadata meta;

    debug_echo("called with path %s, length %ld\n", path, length);

    // Probably can't truncate below 0 bytes.
    if (length < 0) {
        ret = -EINVAL;
        goto out;
    }

    ret = toku_bstore_meta_get(path, &meta, METADATA_SIZE);
    if (ret == BSTORE_NOTFOUND) {
        ret = -ENOENT;
        goto out;
    }
    assert(ret == 0);
    if (meta.st.st_size < length) {
        // we're truncating up, or truncating to the
        // same size. either way, no blocks are 
        // going to get deleted, but the meta data
        // will be updated.
        goto not_deleting_blocks;
    } else if (meta.st.st_size == length) {
        // we don't need to do anything if the size
        // is not changing.
        goto out;
    }

    // if a file is length bytes, then its last byte resides
    // on block len / BLOCKSIZE, so we should remove every block
    // greater than that. 
    uint64_t new_max_block_num = block_get_num_by_position(length);
    uint64_t first_block_to_go = new_max_block_num + 1;
    debug_echo("max_block_num %lu, first to go %lu\n", 
            new_max_block_num, first_block_to_go);

    struct bstore_s bstore;
    ret = toku_bstore_open(&bstore, path);
    assert(ret == 0);

    ret = toku_bstore_truncate(&bstore, first_block_to_go);
    assert(ret == 0);

    size_t block_offset = block_get_offset_by_position(length); 
    ret = truncate_block(&bstore, new_max_block_num, block_offset); 
    assert(ret == 0);

    ret = toku_bstore_close(&bstore);
    assert(ret == 0);

not_deleting_blocks:
    // update the metadata to have the new file size.
    ret = toku_metadata_update_for_truncate(path, length);
    assert(ret == 0);
out:
    return ret;
}

/**
 * Symbolicly link old path to new path. This can be accomplished
 * by having newpath be a file whose contents are oldpath and
 * whose metadata indicates that it's a symlink.
 */
int toku_fs_symlink(const char * oldpath, const char * newpath)
{
    int ret;
    struct metadata meta;
    char buf[BSTORE_BLOCKSIZE];

    debug_echo("called with oldpath %s, newpath %s\n",
            oldpath, newpath);

    ret = toku_bstore_meta_get(newpath, &meta, METADATA_SIZE);
    if (ret == 0) {
        ret = -EEXIST;
        goto out;
    }
    // XXX here, being lazy, we restrict symlinks 
    // to only paths that fit into a block. 
    // should be good enough for a while.
    size_t oldpath_size = strlen(oldpath) + 1;
    if (oldpath_size > BSTORE_BLOCKSIZE) {
        ret = -ENAMETOOLONG;
        goto out;
    }

    struct bstore_s bstore;
    ret = toku_bstore_open(&bstore, newpath);
    assert(ret == 0);
    memset(buf, 0, BSTORE_BLOCKSIZE);
    memcpy(buf, oldpath, oldpath_size);
    ret = toku_bstore_put(&bstore, 0, buf);
    assert(ret == 0);
    ret = toku_bstore_close(&bstore);
    assert(ret == 0);

    time_t now = time(NULL);
    ret = toku_metadata_update_for_symlink(newpath, now, oldpath_size);
    assert(ret == 0);

out:
    return ret;
}

/**
 * Background task that does a full bstore truncate
 * on the bstore for the given name passed as arg
 */
static int do_bstore_truncate(const char * path)
{
    int ret;
    struct bstore_s bstore;

    ret = toku_bstore_open(&bstore, path);
    assert(ret == 0);
    ret = toku_bstore_truncate(&bstore, 0);
    assert(ret == 0);
    ret = toku_bstore_close(&bstore);
    assert(ret == 0);
    return ret;
}

/**
 * Unlink a file, decrementing its reference count. If the count
 * goes to zero, the files contents and metadata are removed.
 * XXX ref count is always 1, so decrementing is always 0
 */
int toku_fs_unlink(const char * path)
{
    int ret;
    struct metadata meta;

    debug_echo("called with path %s\n", path);

    ret = toku_bstore_meta_get(path, &meta, METADATA_SIZE);
    if (ret == BSTORE_NOTFOUND) {
        ret = -ENOENT;
        goto out;
    }
    assert(ret == 0);

    // delete the metadata
    ret = toku_metadata_delete(path);
    assert(ret == 0);
    // truncate away the blocks
    if (meta.st.st_blocks > 0) {
        ret = do_bstore_truncate(path);
        assert(ret == 0);
    }


out:
    return ret;
}

int toku_fs_readlink(const char * path, char * buf, size_t size)
{
    int ret;
    struct metadata meta;
    char linkbuf[BSTORE_BLOCKSIZE];

    ret = toku_bstore_meta_get(path, &meta, METADATA_SIZE);
    if (ret != 0) {
        ret = -ENOENT;
        goto out;
    }
    if (!S_ISLNK(meta.st.st_mode)) {
        ret = -EINVAL;
        goto out;
    }

    debug_echo("called with path %s, size %lu\n",
            path, size);

    struct bstore_s bstore;
    ret = toku_bstore_open(&bstore, path);
    assert(ret == 0);

    // get the first block and read its contents
    // as the path for the symlink. we should
    // read at most 'size' bytes into buf, minus one
    // so the null byte always fits
    size_t link_size = meta.st.st_size;
    size_t read_size = MIN(link_size, size);
    ret = toku_bstore_get(&bstore, 0, buf);
    assert(ret == 0);
    strncpy(linkbuf, buf, read_size);

    ret = toku_bstore_close(&bstore);
    assert(ret == 0);

out:
    return ret;
}


int toku_fs_rename(const char * oldpath, const char * newpath)
{
    int ret;
    struct metadata meta;

    // get the oldpath metadata and make sure 
    // the newpath does not exist
    ret = toku_bstore_meta_get(oldpath, &meta, METADATA_SIZE);
    if (ret == BSTORE_NOTFOUND) {
        debug_echo("cant rename %s to %s, source not found!\n", 
                oldpath, newpath);
        ret = -ENOENT;
    } else {
        if (file_exists(newpath)) {
            // XXX this will NOT work once hard links can make
            // nlinks > 1, and make an unlink not guaruntee 
            // to actually remove the file. 
            ret = toku_fs_unlink(newpath);
            assert(ret == 0);
        }

        ret = toku_bstore_rename_prefix(oldpath, newpath);
        assert(ret == 0);
    }

    return ret;
}

/**
 * Change the access and modification times of a file to
 * those in the utimbuf. If it's null, set them to the current time.
 */
int toku_fs_utime(const char * path, const struct utimbuf * buf)
{
    int ret;
    
    struct utimbuf tbuf;
    time_t now = time(NULL);
    tbuf.modtime = buf == NULL ? now : buf->modtime;
    tbuf.actime = buf == NULL ? now : buf->actime;
    ret = toku_metadata_update_for_utime(path, &tbuf);
    assert(ret == 0);

    return ret;
}

//XXX reuse code for this and toku_fs_open
int toku_fs_access(const char * path, int amode)
{
    (void) path;
    (void) amode;

    return -ENOSYS;
}

int toku_fs_chmod(const char * path, mode_t mode)
{
    int ret;

    ret = toku_metadata_update_for_chmod(path, mode);
    assert(ret == 0);

    return ret;
}

int toku_fs_chown(const char * path, uid_t owner, gid_t group)
{
    int ret;
    
    ret = toku_metadata_update_for_chown(path, owner, group);
    assert(ret == 0);

    return ret;
}

//
// Directory operations
//

/**
 * True if and only if the filename is a direct child
 * of the given directory. eg f(/a/b, /a) is true and
 * f(/a/b/c, /a) is false
 */
static int is_directly_under_dir(const char * filename, 
        const char * dirname)
{
    int same_prefix = strncmp(filename, dirname, strlen(dirname)) == 0;
    return same_prefix && 
        toku_strcount(filename, '/') == toku_strcount(dirname, '/');
}

int toku_fs_mkdir(const char * path, mode_t mode)
{
    int ret;

    //TODO check that it makes sense to mkdir at the
    //given path. major error checking needed.
    //XXX this check is not needed for fuse

    time_t now = time(NULL);
    ret = toku_metadata_update_for_create(path, now, mode | S_IFDIR);
    assert(ret == 0);

    return ret;
}

struct directory_is_empty_info {
    const char * dirname;
    int saw_child;
};

static int directory_is_empty_meta_scan(const char * name,
        void * meta, void * extra)
{
    struct directory_is_empty_info * info = extra;
    (void) meta;

    debug_echo("scanning to check for a child of %s\n", info->dirname);
    // we're supposed to scan with a starting key 
    // that has a slash after a real directory name,
    // which shouldn't directly exist, but rather
    // position us on the next thing alphabetically
    // (a child if one exists). unforunately,
    // the root path is an exception. 
    if (strcmp(info->dirname, "/") != 0) {
        assert(strcmp(name, info->dirname) != 0);
    }
    if (is_directly_under_dir(name, info->dirname)) {
        debug_echo("saw child %s\n", name);
        info->saw_child = 1;
    } else {
        debug_echo("did not see child, stopping at %s\n", name);
        info->saw_child = 0;
    }
    return 0;
}

/**
 * determine if the given directory is empty
 * by trying a metadata scan positioned at
 * the directory name. if we don't get anything, 
 * it's empty.
 */
static int directory_is_empty(const char * path)
{
    int ret;

    // XXX dirty copy/paste code from toku_fs_opendir
    size_t len = strlen(path) + 2;
    char path_with_slash[len];
    if (strcmp(path, "/") != 0) {
        strcpy(path_with_slash, path);
        path_with_slash[len - 2] = '/';
        path_with_slash[len - 1] = '\0';
    } else {
        strcpy(path_with_slash, path);
    }
    struct directory_is_empty_info info;
    info.dirname = path_with_slash;
    info.saw_child = 0;
    ret = toku_bstore_meta_scan(path_with_slash, directory_is_empty_meta_scan, &info);
    if (ret != 0) {
        // if we couldn't start a scan at path + '/', then there
        // are definitely no children, so it's empty.
        return 1;
    } else {
        // if we did scan, we know the directory is empty 
        // if we did not see a child
        return !info.saw_child;
    }
}

/**
 * Remove a directory only if it's empty.
 */
int toku_fs_rmdir(const char * path)
{
    int ret;
    struct metadata meta;

    debug_echo("called with path %s\n", path);

    // can't remove the root directory
    if (strcmp(path, "/") == 0) {
        ret = -EINVAL;
        goto out;
    }

    ret = toku_bstore_meta_get(path, &meta, METADATA_SIZE);
    if (ret != 0) {
        ret = -ENOENT;
    } else if (!S_ISDIR(meta.st.st_mode)) {
        ret = -ENOTDIR;
    } else if (!directory_is_empty(path)) {
        debug_echo("couldnt rmdir, wasn't empty!\n");
        ret = -ENOTEMPTY;
    } else {
        ret = toku_metadata_delete(path);
        assert(ret == 0);
    }

out:
    return ret;
}

/**
 * Open a directory at the given path, storing
 * cursor information to be used by a readdir
 */
int toku_fs_opendir(const char * path, struct toku_dircursor * cursor)
{
    int ret;
    struct metadata meta;

    // make sure a directory exists at path
    ret = toku_bstore_meta_get(path, &meta, METADATA_SIZE);
    if (ret == BSTORE_NOTFOUND) {
        ret = -ENOENT;
        goto out;
    }
    assert(ret == 0);
    if (!S_ISDIR(meta.st.st_mode)) {
        ret = -ENOTDIR;
        goto out;
    }

    // we don't need to add a '/' to the end of the root directory
    size_t len = strlen(path) + 2;
    char * path_with_slash = malloc(len);
    if (strcmp(path, "/") != 0) {
        strcpy(path_with_slash, path);
        path_with_slash[len - 2] = '/';
        path_with_slash[len - 1] = '\0';
    } else {
        strcpy(path_with_slash, path);
    }
    cursor->dirname = path_with_slash;
    cursor->current = toku_strdup(cursor->dirname);
    cursor->status = TOKU_DIRCURSOR_STATUS_FIRST;

out:
    return ret;
}

/**
 * Close a directory, cleaning up after toku_fs_opendir()
 */
int toku_fs_closedir(struct toku_dircursor * cursor)
{
    free(cursor->dirname);
    free(cursor->current);
    cursor->status = TOKU_DIRCURSOR_STATUS_DONE;
    return 0;
}

/**
 * The readdir scan cb can assume a few states:
 * skip_first - the scan should skip the first entry
 * reading - the scan can continue to read entries
 * more - the scan has filled up all available buffer
 *        space, and there could be more. should
 *        scan again with more space and updated cursor.
 * done - the scan is done. don't scan again.
 */

#define READDIR_SCAN_CB_STATUS_READING 0
#define READDIR_SCAN_CB_STATUS_MORE 1
#define READDIR_SCAN_CB_STATUS_SKIP_FIRST 2
#define READDIR_SCAN_CB_STATUS_DONE 3

/**
 * dirname is the directory name we're reading,
 * buf is the user's dirent buffer we'll write to,
 */ 
struct readdir_scan_cb_info {
    char * dirname;
    struct toku_dirent * buf;
    int num_entries_to_read;
    int entries_read;
    int status;
};

/**
 * While scanning the metadata, choose to copy over
 * only filenames that are directly under the directory
 * we're reading and only up to some user-defined limit
 * of entries. We track this in the info struct passed
 * as extra
 */
static int readdir_scan_cb(const char * name,
        void * meta, void * extra)
{
    int ret;
    struct readdir_scan_cb_info * info = extra;

    debug_echo("got key %s\n", name);

    // skip the first entry if requested
    if (info->status == READDIR_SCAN_CB_STATUS_SKIP_FIRST) {
        info->status = READDIR_SCAN_CB_STATUS_READING;
        ret = BSTORE_SCAN_CONTINUE;
        debug_echo("skipping first iteration by request\n");
        goto out;
    }
    // if we're not on the first iteration, the current
    // should never be the directory name itself. we should
    // have skipped the first iteration where key==dirname
    assert(info->status == READDIR_SCAN_CB_STATUS_READING);
    assert(strcmp(name, info->dirname) != 0);

    // pass back a directory entry if and only if the filename
    // is directly under the target directory and the number of
    // entries read is less than the total to read, finish
    // up otherwise.
    ret = 0;
    if (info->entries_read < info->num_entries_to_read) {
        if (is_directly_under_dir(name, info->dirname)) {
            // copy over key/val to info->buf
            struct toku_dirent * dirent = &info->buf[info->entries_read];
            dirent->filename = toku_strdup(name);
            memcpy(&dirent->st, meta, sizeof(struct stat));
            info->entries_read++;
            info->status = READDIR_SCAN_CB_STATUS_READING;
            ret = BSTORE_SCAN_CONTINUE;
        } else {
            // we've reached the end of the directory, so we're done
            // we know we've reached the end because the comparison
            // function is such that all files under a directory
            // are adjacent, so if we scan starting at some directory
            // then subsequent files are in that directory iff they
            // are directly under it
            debug_echo("end of the directory stream, "
                    " since %s is not directly under %s\n",
                    name, info->dirname);
            info->status = READDIR_SCAN_CB_STATUS_DONE;
        }
    } else {
        // we're out of buffer space, so we can't continue
        info->status = READDIR_SCAN_CB_STATUS_MORE;
    }

out:
    return ret;
}

/**
 * Read up to num_entries from the directory referred to
 * by the given cursor into buf. The cursor is incremented
 * by the number of entries actually read, and the number
 * of entries read is written in *entries_read.
 *
 * Returns 0 when there are no more dirents to read, > 0
 * when num_entries have been read and there could be more,
 * and < 0 on error.
 */
int toku_fs_readdir(struct toku_dircursor * cursor, 
        struct toku_dirent * buf, int num_entries,
        int * entries_read)
{
    int ret;

    debug_echo("called with dir %s, current %s\n",
            cursor->dirname, cursor->current);

    struct readdir_scan_cb_info info;
    info.dirname = cursor->dirname;
    info.buf = buf;
    info.num_entries_to_read = num_entries;
    info.entries_read = 0;
    switch (cursor->status) {
        case TOKU_DIRCURSOR_STATUS_FIRST:
            // if this is the first iteration, go straight
            // into reading
            // XXX hack so that reads on the root directory
            // skip the first entry instead of going straight
            // to reads. Maybe we should write a separate
            // function for handling readdir on /, so it's
            // so dirty getting it to work everywhere.
            if (strcmp(info.dirname, "/") == 0) {
                info.status = READDIR_SCAN_CB_STATUS_SKIP_FIRST;
            } else {
                info.status = READDIR_SCAN_CB_STATUS_READING;
            }
            break;
        case TOKU_DIRCURSOR_STATUS_READING:
            // if this cursor is in the middle of reading,
            // it needs to skip the first entry, since it will
            // be a repeat of a previous readdir
            info.status = READDIR_SCAN_CB_STATUS_SKIP_FIRST;
            break;
        case TOKU_DIRCURSOR_STATUS_DONE:
            // this cursor is exhausted
            debug_echo("tried to readdir on an exhausted cursor!\n");
            ret = 0;
            goto out;
    }

    ret = toku_bstore_meta_scan(cursor->current, readdir_scan_cb, &info);
    debug_echo("meta scan ret %d, info.status %d, "
            "info.entries_read %d\n", ret, 
            info.status, info.entries_read);
    if (ret == BSTORE_NOTFOUND) {
        // if we couldn't start the scan and this is the first
        // for this cursor, then the directory is simply empty.
        // otherwise, we're at the end of the stream. either way,
        // this cursor is done.
        cursor->status = TOKU_DIRCURSOR_STATUS_DONE;
        ret = 0;
        goto out;
    }
    assert(ret == 0);
    if (info.status == READDIR_SCAN_CB_STATUS_MORE) {
        debug_echo("readdir says more\n");
        cursor->status = TOKU_DIRCURSOR_STATUS_READING;
        ret = 1;
    } else {
        // if the cb didn't want more, it must have either explicitly
        // wanted to finish, or was in the middle of reading and ran
        // to the end of the directory stream. either way, the
        // cursor is done.
        debug_echo("readdir is done\n");
        cursor->status = TOKU_DIRCURSOR_STATUS_DONE;
        ret = 0;
    }

    // if any entries were ready by the callback, we need to
    // update the current position of the cursor. it should refer
    // to the last entry read by the scan.
    if (info.entries_read > 0) {
        // info.entries read is the first free index, minus one is the
        // last one we wrote to.
        int i = info.entries_read - 1;
        debug_echo("changing cursor from %s to %s\n", 
                cursor->current, info.buf[i].filename);
        free(cursor->current);
        cursor->current = toku_strdup(info.buf[i].filename);
    }

out:
    *entries_read = info.entries_read;
    return ret;
}

//
// Hints and parameters
//

size_t toku_fs_get_blocksize(void)
{
    size_t blocksize = BSTORE_BLOCKSIZE;

    return blocksize;
}

size_t toku_fs_get_cachesize(void)
{
    return toku_bstore_env_get_cachesize();
}

int toku_fs_set_cachesize(size_t cachesize)
{
    return toku_bstore_env_set_cachesize(cachesize);
}
