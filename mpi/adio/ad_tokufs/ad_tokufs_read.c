/**
 * TokuFS
 */

#include "ad_tokufs.h"

void ADIOI_TOKUFS_ReadContig(ADIO_File fd, void *buf, int count, 
    MPI_Datatype datatype, int filetype, ADIO_Offset offset, 
    ADIO_Status * status, int * err)
{
    int ret;
    int rank;
    int datatype_size, read_size;
    ssize_t bytes_read;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* Get the size of the data type and determine the read size. */
    MPI_Type_size(datatype, &datatype_size);
    read_size = datatype_size * count;

    ad_tokufs_debug(rank, 
            "called. fd->fd_sys = %d, buf = %p, read_size = %d, offset = %d\n",
            fd->fd_sys, buf, read_size, (int) offset);

    /* For an explicit offset, do not move the ADIO file position,
     * and take the offset as a parameter. Otherwise, use the ADIO
     * file position as the offset, and adjust it after reading. */
    if (filetype == ADIO_EXPLICIT_OFFSET) {
        bytes_read = tokufs_read_at(fd->fd_sys, buf, read_size, offset);
        assert(bytes_read == read_size);
    } else {
        bytes_read = tokufs_read_at(fd->fd_sys, buf, read_size, fd->fp_ind);
        assert(bytes_read == read_size);
        fd->fp_ind += bytes_read;
    }
    
#ifdef HAVE_STATUS_SET_BYTES
    if (ret != -1) {
        MPIR_Status_set_bytes(status, datatype, ret);
    }
#endif

    *err = MPI_SUCCESS;
}

