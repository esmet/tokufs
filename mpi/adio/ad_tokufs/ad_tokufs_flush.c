/**
 * TokuFS
 */

#include "ad_tokufs.h"
#include "adio-tokufs.h"

void ADIOI_TOKUFS_Flush(ADIO_File fd, int * err)
{
    int ret;
    int rank;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    ad_tokufs_debug(rank, "called with filename %s\n", fd->filename);

    /* Not implemented yet.
    ret = tokufs_flush(fd->fd_sys);
    assert(ret == 0);
    */
    ad_tokufs_debug(rank, "no-op!\n");
    ret = 0;

    *err = MPI_SUCCESS;
}
