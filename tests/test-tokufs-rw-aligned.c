#include "tokufs-test.h"

static size_t blocksize;

/* Verify that buf is an array of all x's */
static int verify(void * buf, long x)
{
    size_t i;
    long * long_buf = buf;

    for (i = 0; i < blocksize / sizeof(long); i++) {
        if (long_buf[i] != x) {
            printf("index %ld wanted %ld got %ld\n",
                    i, x, long_buf[i]);
        }
        assert(long_buf[i] == x);
    }

    return 0;
}

static void memset_long(void * buf, long x, int size)
{
    size_t i;
    long * long_buf = buf;

    /* Need to be aligned for longs if we're going to memset longs. */
    assert(size % sizeof(long) == 0);

    for (i = 0; i < size / sizeof(long); i++) {
        long_buf[i] = x;
    }
}

static void * generate_buffer(int num_blocks)
{
    int i;
    char block[blocksize];
    void * buf = malloc(blocksize * num_blocks);

    for (i = 0; i < num_blocks; i++) {
        /* Have the i'th block be an array of i's */
        memset_long(block, i, blocksize);

        /* Copy this block into buf, an array of blocks. */
        memcpy(buf + blocksize * i, block, blocksize);
    }

    return buf;

}

static int write_single_aligned(int fd, void * buf, int num_blocks)
{
    ssize_t ret;
    size_t write_size = blocksize * num_blocks;

    /* Write the whole buffer starting at offset 0. */
    ret = toku_fs_pwrite(fd, buf, write_size, 0);
    assert(ret == (ssize_t) write_size);

    return 0;
}

static int read_single_aligned(int fd, void * buf, int num_blocks)
{
    int i;
    ssize_t ret;
    size_t read_size = blocksize * num_blocks;

    /* Let's make sure we clear out what was there first. */
    memset(buf, 255, read_size);

    /* Read in the whole file from offset 0 */
    ret = toku_fs_pread(fd, buf, read_size, 0);
    assert(ret == (ssize_t) read_size);
    
    for (i = 0; i < num_blocks; i++) {
        verify(buf + i * blocksize, i);
    }

    return 0;
}

static int write_multiple_aligned(int fd, 
        void * buf, int num_blocks, int blocks_per_write)
{
    int i;
    ssize_t ret;
    size_t write_size = blocksize * blocks_per_write;

    /* Nuke whatever is there. */
    char blanks[blocksize];
    for (i = 0; i < num_blocks; i++) {
        ret = toku_fs_pwrite(fd, blanks, 
                blocksize, i * blocksize);
        assert(ret == (ssize_t) blocksize);
    }

    for (i = 0; i < num_blocks / blocks_per_write; i++) {
        off_t offset = i * write_size;
        ret = toku_fs_pwrite(fd, buf + offset, write_size, offset);
        assert(ret == (ssize_t) write_size);
    }

    return 0;
}

static int read_multiple_aligned(int fd,
        void * buf, int num_blocks, int blocks_per_read)
{
    int i;
    ssize_t ret;
    size_t read_size = blocksize * blocks_per_read;

    memset(buf, 255, num_blocks * blocksize);

    for (i = 0; i < num_blocks / blocks_per_read; i++) {
        off_t offset = i * read_size;
        ret = toku_fs_pread(fd, buf + offset, read_size, offset);
        assert(ret == (ssize_t) read_size);
        int j;
        for (j = 0; j < blocks_per_read; j++) {
            verify(buf + offset + (j * blocksize), i * blocks_per_read + j);
        }
    }

    return 0;
}

int main(void)
{
    int ret;
    int fd;
    void * buf;
    int num_blocks = 128;

    ret = toku_fs_mount(MOUNT_PATH);
    assert(ret == 0);
    
    blocksize = toku_fs_get_blocksize();

    fd = toku_fs_open("/test-rw-aligned.file", O_CREAT, 0644);
    assert(fd >= 0);

    buf = generate_buffer(num_blocks);
    assert(buf != NULL);
    
    ret = write_single_aligned(fd, buf, num_blocks);
    assert(ret == 0);
    
    ret = read_single_aligned(fd, buf, num_blocks);
    assert(ret == 0);

    assert(num_blocks % 4 == 0);

    ret = write_multiple_aligned(fd, buf, num_blocks, num_blocks / 4);
    assert(ret == 0);

    ret = read_multiple_aligned(fd, buf, num_blocks, num_blocks / 4);
    assert(ret == 0);

    ret = toku_fs_close(fd);
    assert(ret == 0);

    free(buf);

    ret = toku_fs_unmount();
    assert(ret == 0);

    return 0;
}
