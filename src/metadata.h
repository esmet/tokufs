/**
 * TokuFS
 */

#ifndef TOKU_FS_META_H
#define TOKU_FS_META_H

#include <sys/stat.h>
#include <fcntl.h>
#include <utime.h>

#ifdef USE_BDB
#include <db.h>
#else
#include <tokudb.h>
#endif

/**
 * TokuFS file metadata is represented by a stat struct, and
 * perhaps more in the future.
 */
struct metadata {
    struct stat st;
};
#define METADATA_SIZE (sizeof(struct metadata))

/**
 * All metadata updates will go through this callback
 * via the bstore.
 */
int toku_metadata_update_callback(const DBT * oldval,
        DBT * newval, void * extra);

/**
 * Update metadata for a create operation, which gives the
 * metadata some default data and does nothing if it's
 * already there.
 */
int toku_metadata_update_for_create(const char * name, 
        time_t ctime, mode_t mode);

/**
 * Update access time after a pread
 */
int toku_metadata_update_for_pread(const char * name, time_t atime);

/**
 * Update modification time and maybe 
 * set highest offset after a pwrite
 */
int toku_metadata_update_for_pwrite(const char * name, 
        time_t mtime, off_t last_offset);

/**
 * Update the size and block count of a file after truncate
 */
int toku_metadata_update_for_truncate(const char * name, off_t size);

/**
 * Update metadata after a symlink. Requires that the metadata
 * does not already exist.
 */
int toku_metadata_update_for_symlink(const char * name,
        time_t ctime, size_t link_size);

/**
 * Delete metadata for the given bstore name
 */
int toku_metadata_delete(const char * name);

/**
 * Update metadata after a rename by copying existing metadata
 * into new metadata with the new name.
 */
int toku_metadata_update_for_rename(const char * name, 
        struct metadata * meta);

/**
 * Update access and modification times for metadata after
 * a utime operation. If buf is NULL, set to the current time.
 */
int toku_metadata_update_for_utime(const char * name,
        const struct utimbuf * buf);

/**
 * Update mode for metadata after a chmod
 */
int toku_metadata_update_for_chmod(const char * name, mode_t mode);

/**
 * Update owner and group for metadata after a chown.
 * Ignore either if they are -1.
 */
int toku_metadata_update_for_chown(const char * name, 
        uid_t owner, gid_t group);

#endif /* TOKU_FS_META_H */
