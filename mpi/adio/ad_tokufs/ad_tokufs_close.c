/**
 * TokuFS
 */

#include "ad_tokufs.h"
#include "adio-tokufs.h"

void ADIOI_TOKUFS_Close(ADIO_File fd, int * err)
{
    int ret;
    int rank;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    ad_tokufs_debug(rank, "called with filename %s, fd->fd_sys = %u\n",
            fd->filename, (unsigned) fd->fd_sys);
    
    /* Pass the tokufs file desc down to close() */
    ret = tokufs_close(fd->fd_sys);
    assert(ret == 0);

    /* And invalidate it, I guess. */
    fd->fd_sys = -1;

    *err = MPI_SUCCESS;
}
