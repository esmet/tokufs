// need to define gnu source and include sys/syscall.h
// before including other headers, can't get it to
// correctly prototype syscall() otherwise
#define _GNU_SOURCE
#include <sys/syscall.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>
#include <time.h>

/* wkj: compiler error that this is already defined
static int gettid(void)
{
    int tid = (int) syscall(__NR_gettid);
    return tid;
}
*/

/* Fuse silliness that lets us use the 'new' api */
#define FUSE_USE_VERSION 26
#include <fuse/fuse.h>
#include <tokufs.h>

static size_t cachesize = 1L * 1024L * 1024 * 1024;
static char * env_path = "bstore-env.mount";
static int verbose;

#define verbose_echo(...)                                   \
    do {                                                    \
        if (verbose) {                                      \
            printf("[%d] %s(): ", gettid(), __FUNCTION__);  \
            printf(__VA_ARGS__);                            \
        }                                                   \
    } while (0)

static int tokufs_fuse_utimens(const char * path, 
        const struct timespec tv[2])
{
    int ret;

    verbose_echo("called with path %s\n", path);
    struct utimbuf buf = {
        .actime = tv[0].tv_sec,
        .modtime = tv[1].tv_sec,
    };
    ret = toku_fs_utime(path, &buf);

    return ret;
}

static int tokufs_fuse_getattr(const char * path, struct stat * st)
{
    int ret;

    verbose_echo("called with path %s\n", path);
    ret = toku_fs_stat(path, st);

    return ret;
}

static int tokufs_fuse_readdir(const char * path, void * buf,
        fuse_fill_dir_t filler, off_t offset,
        struct fuse_file_info * info)
{
    int r, ret;
    struct toku_dircursor cursor;

    verbose_echo("called with path %s, fd %lu, offset %ld\n",
            path, info->fh, offset);

    ret = toku_fs_opendir(path, &cursor);
    if (ret != 0) {
        goto out;
    }

    struct stat st;
    memset(&st, 0, sizeof(struct stat));
    ret = toku_fs_stat(path, &st);
    assert(ret == 0);
    filler(buf, ".", &st, 0);
    filler(buf, "..", NULL, 0);

    // TODO: better algorithm for how many pages we use
    // to buffer output from readdir? maybe an exponential
    // growth function makes the most sense. for now,
    // four pages gives approximately 60 dirents
    int bufsize = 4 * 4096;
    int num_entries = bufsize / sizeof(struct toku_dirent);
    struct toku_dirent * dirents = malloc(bufsize);

    // while readdir does not return end-of-dirent-stream
    int keep_reading = 1;
    while (keep_reading) {
        int entries_read = 0;
        ret = toku_fs_readdir(&cursor, dirents, 
                num_entries, &entries_read);
        if (ret == 0) {
            // we need to stop reading after this iteration
            keep_reading = 0;
        } else if (ret < 0) {
            break;
        }
        // copy each entry over using the filler
        // if we keep reading, we'll over write this
        // buffer with up to num_entries more entries,
        // until readdir returns 0 and we'll know to stop
        for (int i = 0; i < entries_read; i++) {
            struct toku_dirent * d = &dirents[i];
            // basename will modify d->filename, but that is okay,
            // we're going to just free it immidiately anyway
            r = filler(buf, basename(d->filename), &d->st, 0);
            assert(r == 0);
            free(d->filename);
        }
    }

    free(dirents);
    r = toku_fs_closedir(&cursor);
    assert(r == 0);

out:
    return ret;
}

static int tokufs_fuse_create(const char * path,
        mode_t mode, struct fuse_file_info * info)
{
    int fd;

    verbose_echo("called with path %s, mode %x\n", path, mode);

    fd = toku_fs_open(path, O_CREAT, mode);
    if (fd >= 0) {
        verbose_echo("fd %d\n", fd);
        info->fh = fd;
    }

    verbose_echo("returning fd %d\n", fd);

    return fd >= 0 ? 0 : -1;
}

static int tokufs_fuse_open(const char * path, 
        struct fuse_file_info * info)
{
    int fd;

    verbose_echo("called with path %s\n", path);

    fd = toku_fs_open(path, info->flags, 0);
    if (fd >= 0) {
        verbose_echo("fd %d\n", fd);
        info->fh = fd;
    }

    verbose_echo("returning fd %d\n", fd);

    return fd >= 0 ? 0 : -1;
}

static int tokufs_fuse_release(const char * path,
        struct fuse_file_info * info)
{
    int ret;

    verbose_echo("called with path %s, info->fh = %lu\n", 
            path, info->fh);

    ret = toku_fs_close(info->fh);

    return ret < 0 ? -1 * ret : ret;
}

static int tokufs_fuse_pread(const char * path, char * buf, 
        size_t size, off_t offset, struct fuse_file_info * info)
{
    int ret;

    verbose_echo("called with path %s, fd %lu, size %lu, offset %ld\n",
            path, info->fh, size, offset);

    ret = toku_fs_pread(info->fh, buf, size, offset);
    verbose_echo("read %d bytes\n", ret);

    return ret;
}

static int tokufs_fuse_pwrite(const char * path, const char * buf, 
        size_t size, off_t offset, struct fuse_file_info * info)
{
    int ret;

    verbose_echo("called with path %s, fd %lu, size %lu, offset %ld\n",
            path, info->fh, size, offset);
    ret = toku_fs_pwrite(info->fh, buf, size, offset);
    verbose_echo("wrote %d bytes\n", ret);

    return ret;
}

static int tokufs_fuse_truncate(const char * path, off_t offset)
{
    int ret;

    verbose_echo("called with path %s, offset %ld\n", path, offset);
    ret = toku_fs_truncate(path, offset);

    return ret;
}

static int tokufs_fuse_mkdir(const char * path, mode_t mode)
{
    int ret;

    verbose_echo("called with path %s, mode %d\n", path, mode);
    ret = toku_fs_mkdir(path, mode);

    return ret;
}

static int tokufs_fuse_rmdir(const char * path)
{
    int ret;

    verbose_echo("called with path %s\n", path);
    ret = toku_fs_rmdir(path);

    return ret;
}

static int tokufs_fuse_unlink(const char * path)
{
    int ret;

    verbose_echo("called with path %s\n", path);
    ret = toku_fs_unlink(path);

    return ret;
}

static int tokufs_fuse_chmod(const char * path, mode_t mode)
{
    int ret;

    verbose_echo("called with path %s, mode %d\n", path, mode);
    ret = toku_fs_chmod(path, mode);
    
    return ret;
}

static int tokufs_fuse_chown(const char * path, uid_t owner, gid_t group)
{
    int ret;
    
    verbose_echo("called with path %s, owner %d, group %d\n",
            path, owner, group);
    ret = toku_fs_chown(path, owner, group);

    return ret;
}

static int tokufs_fuse_rename(const char * oldpath, const char * newpath)
{
    int ret;

    verbose_echo("called with oldpath %s, newpath %s\n", oldpath, newpath);
    ret = toku_fs_rename(oldpath, newpath);

    return ret;
}

static int tokufs_fuse_access(const char * path, int amode)
{
    int ret;

    verbose_echo("called with path %s, mode %d\n", path, amode);
    //ret = toku_fs_access(path, amode);
    ret = 0;

    return ret;
}

static int tokufs_fuse_symlink(const char * oldpath, const char * newpath)
{
    int ret;

    verbose_echo("called with oldpath %s, newpath %s\n",
            oldpath, newpath);
    ret = toku_fs_symlink(oldpath, newpath);

    return ret;
}

static int tokufs_fuse_readlink(const char * path, char * buf, size_t size)
{
    int ret;

    verbose_echo("called with path %s, size %lu\n", path, size);
    ret = toku_fs_readlink(path, buf, size);

    return ret;
}

static struct fuse_operations tokufs_fuse_ops =
{   
    .utimens = tokufs_fuse_utimens,             /* tokufs_utime */
    .getattr = tokufs_fuse_getattr,             /* tokufs_stat */
    .readdir = tokufs_fuse_readdir,             /* tokufs_readdir */
    .create = tokufs_fuse_create,               /* tokufs_open + O_CREAT */
    .open = tokufs_fuse_open,                   /* tokufs_open */
    .release = tokufs_fuse_release,             /* tokufs_close */
    .read = tokufs_fuse_pread,                  /* tokufs_read_at */
    .write = tokufs_fuse_pwrite,                /* tokufs_write_at */
    .truncate = tokufs_fuse_truncate,           /* tokufs_truncate */ 
    .mkdir = tokufs_fuse_mkdir,                 /* tokufs_mkdir */
    .rmdir = tokufs_fuse_rmdir,                 /* tokufs_rmddir */
    .unlink = tokufs_fuse_unlink,               /* tokufs_unlink */
    .chmod = tokufs_fuse_chmod,                 /* tokufs_chmod */
    .chown = tokufs_fuse_chown,                 /* tokufs_chown */
    .rename = tokufs_fuse_rename,               /* tokufs_rename */
    .access = tokufs_fuse_access,               /* tokufs_access */
    .symlink = tokufs_fuse_symlink,             /* tokufs_symlink */
    .readlink = tokufs_fuse_readlink,           /* tokufs_symlink */
};

static void usage(void)
{
    printf(
    "usage:\n"
    "    --verbose\n"
    "        enable verbose output.\n"
    "    --env\n"
    "        local direcory where tokufs can find an environment, \n"
    "        or create one if it does not exist\n"
    "    --cachesize\n"
    "        cache size in mb\n"
    );
}

int main(int argc, char * argv[])
{
    int ret;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--verbose") == 0) {
            verbose = 1;
            argv[i] = NULL;
        } else if (strcmp(argv[i], "--help") == 0) {
            usage();
            return 0;
        } else if (strcmp(argv[i], "--env") == 0) {
            if (i + 1 == argc) {
                printf("invalid argument\n");
                return -1;
            } else {
                env_path = argv[i + 1];
            }
            argv[i] = NULL;
            argv[i + 1] = NULL;
            i++;
        } else if (strcmp(argv[i], "--cache") == 0) {
            if (i + 1 == argc || atol(argv[i + 1]) <= 0) {
                printf("invalid argument\n");
                return -1;
            } else {
                cachesize = atol(argv[i + 1]);
            }
            argv[i] = NULL;
            argv[i + 1] = NULL;
            i++;
        }
    }

    printf("Opening environment %s\n", env_path);
    ret = toku_fs_mount(env_path);
    if (ret != 0) {
        fprintf(stderr, "Failed to mount TokuFS, ret %d\n", ret);
        return ret;
    }

    int fuse_argc = 0;
    char * fuse_argv[argc];
    for (int i = 0; i < argc; i++) {
        if (argv[i] != NULL) {
            fuse_argv[fuse_argc++] = argv[i];
        }
    }

    printf("TokuFS fuse starting fuse main...\n");
    ret = fuse_main(fuse_argc, fuse_argv, &tokufs_fuse_ops, NULL);
    printf("fuse main finished, ret %d\n", ret);

    ret = toku_fs_unmount();
    if (ret != 0) {
        fprintf(stderr, "Failed to unmount TokuFS, ret %d\n", ret);
    }

    return ret;
}
