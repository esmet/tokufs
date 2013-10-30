/**
 * TokuFS
 */

#include "ad_tokufs.h"

void ADIOI_TOKUFS_WriteContig(ADIO_File fd, void *buf, int count, 
    MPI_Datatype datatype, int filetype, ADIO_Offset offset, 
    ADIO_Status * status, int * err)
{

    int ret;
    int rank;
    int datatype_size, write_size;
    ssize_t bytes_written;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* The write size is the size of the data type times the count */
    MPI_Type_size(datatype, &datatype_size);
    write_size = datatype_size * count;

    ad_tokufs_debug(rank, 
            "called. fd->fd_sys = %d, buf = %p, size = %d, offset = %d\n", 
            fd->fd_sys, buf, write_size, (int) offset);

    /**
     * Use the offset parameter if we're doing an explicit offset write,
     * otherwise us the ADIO file position, and update it after writing.
     */
    if (filetype == ADIO_EXPLICIT_OFFSET) {
        bytes_written = tokufs_write_at(fd->fd_sys, buf, write_size, offset);
        assert(bytes_written == write_size);
    } else {
        bytes_written = tokufs_write_at(fd->fd_sys, buf, write_size, fd->fp_ind); 
        assert(bytes_written == write_size);
        fd->fp_ind += bytes_written;
    }

#ifdef HAVE_STATUS_SET_BYTES
    if (ret != -1) {
        MPIR_Status_set_bytes(status, datatype, ret);
    }
#endif

    *err = MPI_SUCCESS;
}
