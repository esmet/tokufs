/**
 * TokuFS
 */

#ifndef TOKU_FS_H
#define TOKU_FS_H

#include <sys/stat.h>
#include <fcntl.h>
#include <utime.h>
#include <unistd.h>
#include <sys/types.h>
/* TokuFS functions return 0 on success and -ERRNO on error. */
#include <errno.h>

//
// File system operations
//

/**
 * Mount toku_fs at the given path. If a toku_fs mount point does not
 * exist at that path, one will be created. Mount points must be
 * unmounted before new ones can be mounted.
 */
int toku_fs_mount(const char * path);

int toku_fs_unmount(void);

//
// File data operations
//

int toku_fs_open(const char * path, int flags, mode_t mode);

int toku_fs_close(int fd);

ssize_t toku_fs_pread(int fd, void * buf, 
        size_t count, off_t offset);

ssize_t toku_fs_pwrite(int fd, const void * buf,
        size_t count, off_t offset);

//
// Metadata operations
//

int toku_fs_stat(const char * path, struct stat * st);

int toku_fs_truncate(const char * path, off_t length);

int toku_fs_symlink(const char * oldpath, const char * newpath);

int toku_fs_unlink(const char * path);

int toku_fs_readlink(const char * path, char * buf, size_t size);

int toku_fs_rename(const char * oldpath, const char * newpath);

int toku_fs_utime(const char * path, const struct utimbuf * buf);

int toku_fs_access(const char * path, int amode);

int toku_fs_chmod(const char * path, mode_t mode);

int toku_fs_chown(const char * path, uid_t owner, gid_t group);

//
// Directory operations
//

/**
 * Directory entries are filename/stat pairs
 */
struct toku_dirent
{
    char * filename;
    struct stat st;
};

/**
 * Remember the directory name and the current child the
 * cursor refers to. We know the cursor ends when the directory
 * name and the next metadata key do not share dirname as
 * a prefix.
 */
struct toku_dircursor
{
    char * dirname;
    char * current;
    int status;
};

#define TOKU_DIRCURSOR_STATUS_FIRST 0
#define TOKU_DIRCURSOR_STATUS_READING 1
#define TOKU_DIRCURSOR_STATUS_DONE 2

int toku_fs_mkdir(const char * path, mode_t mode);

int toku_fs_rmdir(const char * path);

int toku_fs_opendir(const char * path, struct toku_dircursor * cursor);

int toku_fs_closedir(struct toku_dircursor * cursor);

int toku_fs_readdir(struct toku_dircursor * cursor, 
        struct toku_dirent * buf, int num_entries,
        int * entries_read);

//
// Hints and parameters
//

size_t toku_fs_get_blocksize(void);

size_t toku_fs_get_cachesize(void);

int toku_fs_set_cachesize(size_t cachesize);

#endif /* TOKU_FS_H */
