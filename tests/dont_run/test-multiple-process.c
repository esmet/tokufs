#include "test.h"

#define NUM_PROCS 5
#define WRITE_SIZE 2671

static void process_do_write(int id, char * path, void * buf,
        size_t off, size_t count)
{
    int ret;
    ssize_t n;
    toku_fs_fd fd;

    ret = toku_fs_open(&fd, path, id);
    assert(ret == 0);

    n = toku_fs_read_at(fd, buf, count, off);
    if (n != 0) {
        printf("proc %d read before write, got %ld bytes\n",
                id, n);
    }
    assert(n == 0);

    n = toku_fs_write_at(fd, buf, count, off);
    assert(n == (ssize_t) count);

    ret = toku_fs_close(fd);
    assert(ret == 0);
}

static void process_do_read(int id, char * path, void * buf,
        size_t off, size_t count)
{
    int ret;
    ssize_t n;
    toku_fs_fd fd;

    ret = toku_fs_open(&fd, path, id);
    assert(ret == 0);

    n = toku_fs_read_at(fd, buf, count, off);
    assert(n == (ssize_t) count);

    ret = toku_fs_close(fd);
    assert(ret == 0);
}

int main(void)
{
    size_t i;
    unsigned char buf[WRITE_SIZE];
    char * path = "test-multiple-process.file";
    int id;
    int ret;

    for (id = 0, i = 1; i < NUM_PROCS; i++) {
        if (fork() == 0) {
            /* child process, take the id to be i */
            id = i;         
            break;
        }
    }

    printf("process %d starting...\n", id);
    ret = toku_fs_mount("test-multiple-process.mount");
    assert(ret == 0);
    
    size_t magic = 29;
    memset(buf, magic, WRITE_SIZE);
    process_do_write(id, path, buf, i * WRITE_SIZE, WRITE_SIZE);
    memset(buf, !magic, WRITE_SIZE);
    process_do_read(id, path, buf, i * WRITE_SIZE, WRITE_SIZE);
    for (i = 0; i < WRITE_SIZE; i++) {
        assert(buf[i] == magic);
    }
    printf("process %d done!\n", id);

    ret = toku_fs_unmount();

    return 0;
}
