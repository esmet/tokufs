/*

int MPI_File_open(MPI_Comm, char *, int, MPI_Info, MPI_File *);

int MPI_File_close(MPI_File *);

int MPI_File_delete(char *, MPI_Info);

int MPI_File_read_at(MPI_File, MP2ddI_Offset, void *,
          int, MPI_Datatype, MPI_Status *);

int MPI_File_write_at(MPI_File, MPI_Offset, void *,
          int, MPI_Datatype, MPI_Status *);

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <assert.h>

#include <mpio.h>

#define BUF_SIZE 640

int main(int argc, char * argv[])
{
    int i;
    int ret;
    MPI_File file;
    MPI_Status status;
    char * filename = "tokufs:/hellobasicmpiio";

    MPI_Init(&argc, &argv);

    ret = MPI_File_open(MPI_COMM_WORLD, filename,
            MPI_MODE_CREATE | MPI_MODE_RDWR,
            MPI_INFO_NULL, &file);
    printf("open ret = %d\n", ret);
    if (ret != MPI_SUCCESS) {
        char * estr = malloc(1000);
        int l;
        MPI_Error_string(ret, estr, &l);
        printf("%s\n", estr);
        free(estr);
    }
    assert(ret == MPI_SUCCESS);

    /* Generate the usual 0-99 repeating buffer */
    char write_buf[BUF_SIZE];
    for (i = 0; i < BUF_SIZE; i++) {
        write_buf[i] = i % 100;
    }

    /* Write to the file as 10 seperate chunks with explicit offsets. */
    int num_chunks = 10;
    assert(BUF_SIZE % num_chunks == 0);
    int write_size = BUF_SIZE / num_chunks;
    for (i = 0; i < 10; i++) {
        ret = MPI_File_write_at(file, i * write_size,
                write_buf + i * write_size,
                write_size, MPI_CHAR, &status);
        assert(ret == MPI_SUCCESS);
    }

    /* Close the file and open it again */
    ret = MPI_File_close(&file);
    assert(ret == MPI_SUCCESS);
    ret = MPI_File_open(MPI_COMM_WORLD, filename, 
            MPI_MODE_EXCL | MPI_MODE_RDWR,
            MPI_INFO_NULL, &file);
    assert(ret == MPI_SUCCESS);

    /* Read back the file in a similar fashion, into a new buffer. */
    char read_buf[BUF_SIZE];
    int read_size = write_size;
    for (i = 0; i < 10; i++) {
        ret = MPI_File_read_at(file, i * read_size,
                read_buf + i * read_size,
                read_size, MPI_CHAR, &status);
        assert(ret == MPI_SUCCESS);
    }
    
    /* Verify the read buf is the same as the write buf */
    for (i = 0; i < BUF_SIZE; i++) {
        if (read_buf[i] != write_buf[i]) {
            printf("%s:%s: buf[%d]: expected %d, got %d\n",
                    __FILE__, __FUNCTION__,
                    i, write_buf[i], read_buf[i]);
        }
        assert(read_buf[i] == write_buf[i]);
    }

    ret = MPI_File_close(&file);
    assert(ret == 0);
    ret = MPI_Finalize();
    assert(ret == 0);

    return 0;
}
