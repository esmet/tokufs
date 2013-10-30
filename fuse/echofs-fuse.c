#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/statvfs.h>
#include <errno.h>
#include <time.h>

/* Fuse silliness that lets us use the 'new' api */
#define FUSE_USE_VERSION 26
#include <fuse/fuse.h>

/* Flag to say whether or not we should be quiet, meaning,
 * we should not echo the args of each call. */
static int quiet;

#define echo(...)                                       \
    do {                                                \
        if (1 /*!quiet*/) {                             \
            printf("%s:%d ", __FUNCTION__, __LINE__);   \
            printf(__VA_ARGS__);                        \
        }                                               \
    } while (0)

/* Fuse returns -errno on error, but the system calls,
 * return -1 on error and set errno accordingly. This
 * macro lets us turn an error code into the correct
 * fuse return code. */
#define system_ret_to_fuse_ret(r) ((r) < 0 ? (errno * -1) : (r))

static int echofs_fuse_getattr(const char * path, struct stat * buf)
{
    int ret;

    echo("path %s, stat %p\n", path, buf);
    errno = 0;
    ret = stat(path, buf);
    echo("ret %d, errno %d, strerror %s\n", 
            ret, errno, strerror(errno));

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_readlink(const char * path, 
        char * buf, size_t size)
{
    int ret;

    echo("path %s, buf %p, size %lu\n", path, buf, size);
    errno = 0;
    ret = readlink(path, buf, size);
    echo("ret %d, errno %d, strerror %s\n", 
            ret, errno, strerror(errno));

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_mkdir(const char * path, mode_t mode)
{
    int ret;

    echo("path %s, mode %o\n", path, mode);
    errno = 0;
    ret = mkdir(path, mode);
    echo("ret %d, errno %d, strerror %s\n", 
            ret, errno, strerror(errno));

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_unlink(const char * path)
{
    int ret;

    echo("path %s\n", path);
    errno = 0;
    ret = unlink(path);
    echo("ret %d, errno %d, strerror %s\n", 
            ret, errno, strerror(errno));

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_rmdir(const char * path)
{
    int ret;

    echo("path %s\n", path);
    errno = 0;
    ret = rmdir(path);
    echo("ret %d, errno %d, strerror %s\n", 
            ret, errno, strerror(errno));

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_symlink(const char * oldpath, 
        const char * newpath)
{
    int ret;

    echo("oldpath %s, newpath %s\n", oldpath, newpath); 
    errno = 0;
    ret = symlink(oldpath, newpath);
    echo("ret %d, errno %d, strerror %s\n", 
            ret, errno, strerror(errno));

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_rename(const char * oldpath, 
        const char * newpath)
{
    int ret;

    echo("oldpath %s, newpath %s\n", oldpath, newpath); 
    errno = 0;
    ret = rename(oldpath, newpath);
    echo("ret %d, errno %d, strerror %s\n", ret, errno, strerror(errno));

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_link(const char * oldpath, const char * newpath)
{
    int ret;

    echo("oldpath %s, newpath %s\n", oldpath, newpath); 
    errno = 0;
    ret = link(oldpath, newpath);
    echo("ret %d, errno %d, strerror %s\n", ret, errno, strerror(errno));

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_chmod(const char * path, mode_t mode)
{
    int ret;

    echo("path %s, mode %o\n", path, mode);
    errno = 0;
    ret = chmod(path, mode);
    echo("ret %d, errno %d, strerror %s\n",
            ret, errno, strerror(errno));

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_chown(const char * path, uid_t uid, gid_t gid)
{
    int ret;

    echo("path %s, uid %d, gid %d\n", path, uid, gid);
    errno = 0;
    ret = chown(path, uid, gid);
    echo("ret %d, errno %d, strerror %s\n", 
            ret, errno, strerror(errno));

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_truncate(const char * path, off_t offset)
{
    int ret;

    echo("path %s, offset %ld\n", path, offset);
    errno = 0;
    ret = truncate(path, offset);
    echo("ret %d, errno %d, strerror %s\n", 
            ret, errno, strerror(errno));

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_utime(const char * path, struct utimbuf * times)
{
    int ret;

    echo("path %s, times %p\n", path, times);
    errno = 0;
    ret = utime(path, times);
    echo("ret %d, errno %d, strerror %s\n", 
            ret, errno, strerror(errno));

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_open(const char * path,
        struct fuse_file_info * info)
{
    int fd;
    int ret;

    echo("path %s, info %p\n", path, info);
    errno = 0;
    fd = open(path, info->flags);
    echo("fd %d, errno %d, strerror %s\n", 
            fd, errno, strerror(errno));
    if (fd < 0) {
        ret = system_ret_to_fuse_ret(fd);
        goto out;
    }

    info->fh = fd;
    ret = 0;

out:
    return ret;
}

static int echofs_fuse_read(const char * path, char * buf, size_t size,
        off_t offset, struct fuse_file_info * info)
{
    ssize_t ret;
    (void) path;

    echo("path %s, buf %p, size %lu, offset %ld, info %p\n",
            path, buf, size, offset, info);
    errno = 0;
    ret = pread(info->fh, buf, size, offset);
    echo("ret %ld, errno %d\n", ret, errno);

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_write(const char * path, const char * buf, 
        size_t size, off_t offset, struct fuse_file_info * info)
{
    ssize_t ret;
    (void) path;

    echo("path %s, buf %p, size %lu, offset %ld, info %p\n",
            path, buf, size, offset, info);
    errno = 0;
    ret = pwrite(info->fh, buf, size, offset);
    echo("ret %ld, errno %d\n", ret, errno);

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_statfs(const char * path, struct statvfs * buf)
{
    int ret;

    echo("path %s, buf %p\n", path, buf);
    errno = 0;
    ret = statvfs(path, buf);
    echo("ret %d, errno %d, strerror %s\n", 
            ret, errno, strerror(errno));

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_release(const char * path, 
        struct fuse_file_info * info)
{
    int ret;
    (void) path;

    echo("path %s, info->fh %lu\n", path, info->fh);
    errno = 0;
    ret = close(info->fh);
    echo("ret %d, errno %d, strerror %s\n", 
            ret, errno, strerror(errno));

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_fsync(const char * path, int datasync, 
        struct fuse_file_info * info)
{
    int ret;
    (void) path;

    echo("path %s, datasync %d\n", path, datasync);
    errno = 0;
    ret = fsync(info->fh);
    echo("ret %d, errno %d, strerror %s\n", 
            ret, errno, strerror(errno));
    if (ret != 0) {
        goto out;
    }

    if (datasync) {
        errno = 0;
        ret = fdatasync(info->fh);
        echo("datasync ret %d, errno %d\n", ret, errno);
    }

out:
    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_readdir(const char * path, void * buf, 
        fuse_fill_dir_t filler, off_t offset, 
        struct fuse_file_info * info)
{
    int ret;
    DIR * dir;
    struct dirent * dirent;
    (void) buf;
    (void) info;
    (void) filler;

    /* Ignoring offset, doing mode "1" of operation in fuse.
       See fuse.h readdir() documentation. */
    (void) offset;

    echo("path %s\n", path);
    dir = opendir(path);
    echo("opendir ret %p, errno %d\n", dir, errno);
    if (dir == NULL) {
        ret = -1;
        goto out;
    }

    while ((dirent = readdir(dir)) != NULL) {
        ret = filler(buf, dirent->d_name, NULL, 0);
        if (ret != 0) {
            ret = -1;
            goto out;
        }
    }

    ret = closedir(dir);
    echo("closedir ret %d, errno %d\n", ret, errno);

out:
    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_access(const char * path, int mode)
{
    int ret;

    echo("path %s, mode %o\n", path, mode);
    ret = access(path, mode);
    echo("ret %d, errno %d, strerror %s\n", 
            ret, errno, strerror(errno));
    errno = 0;

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_create(const char * path, mode_t mode, 
        struct fuse_file_info * info)
{
    int ret;

    echo("path %s, mode %o\n", path, mode);
    errno = 0;
    ret = creat(path, mode);
    echo("ret %d, errno %d, strerror %s\n", 
            ret, errno, strerror(errno));
    if (ret < 0) {
        goto out;
    }

    info->fh = ret;

out:
    return ret >= 0 ? 0 : system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_ftruncate(const char * path, off_t offset,
        struct fuse_file_info * info)
{
    int ret;
    (void) path;

    echo("path %s, offset %ld\n", path, offset);
    errno = 0;
    ret = ftruncate(info->fh, offset);
    echo("ret %d, errno %d, strerror %s\n", 
            ret, errno, strerror(errno));

    return system_ret_to_fuse_ret(ret);
}

static int echofs_fuse_fgetattr(const char * path, struct stat * buf,
        struct fuse_file_info * info)
{
    int ret;
    (void) path;

    echo("path %s, buf %p\n", path, buf);
    errno = 0;
    ret = fstat(info->fh, buf);
    echo("ret %d, errno %d, strerror %s\n", 
            ret, errno, strerror(errno));

    return system_ret_to_fuse_ret(ret);
}

static struct fuse_operations echofs_fuse_ops =
{   
    .getattr = echofs_fuse_getattr,
    .readlink = echofs_fuse_readlink,
    .mkdir = echofs_fuse_mkdir,
    .unlink = echofs_fuse_unlink,
    .rmdir = echofs_fuse_rmdir,
    .symlink = echofs_fuse_symlink,
    .rename = echofs_fuse_rename,
    .link = echofs_fuse_link,
    .chmod = echofs_fuse_chmod,
    .chown = echofs_fuse_chown,
    .truncate = echofs_fuse_truncate,
    .utime = echofs_fuse_utime,
    .open = echofs_fuse_open,
    .read = echofs_fuse_read,
    .write = echofs_fuse_write,
    .statfs = echofs_fuse_statfs,
    .release = echofs_fuse_release,
    .fsync = echofs_fuse_fsync,
    .readdir = echofs_fuse_readdir,
    .access = echofs_fuse_access,
    .ftruncate = echofs_fuse_ftruncate,
    .fgetattr = echofs_fuse_fgetattr,
    .create = echofs_fuse_create,
};

int main(int argc, char * argv[])
{
    int ret;
    int i;

    //TODO: Too dirty
    for (i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--quiet") == 0) {
            quiet = 1;
        }
    }

    printf("%s: Calling fuse_main...\n", __FUNCTION__);

    ret = fuse_main(argc, argv, &echofs_fuse_ops, NULL);

    printf("%s: done.\n", __FUNCTION__);

    return ret;
}
