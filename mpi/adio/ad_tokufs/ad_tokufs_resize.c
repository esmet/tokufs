/**
 * TokuFS
 */

#include "ad_tokufs.h"
#include "adio-tokufs.h"

void ADIOI_TOKUFS_Resize(ADIO_File fd, ADIO_Offset size, int * err)
{
    int ret;
    int rank;
    off_t new_size = size;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    ad_tokufs_debug(rank, "called with filename %s\n", fd->filename);

    /* Not implemented yet.
    ret = tokufs_truncate(fd->fd_sys, new_size);
    assert(ret == 0);
    */
    ret = 0;
    ad_tokufs_debug(rank, "no-op!\n");

    /* TODO: Looks like the generic code broadcasts the new
     * size to the other processes. Maybe this is a good idea. */

    *err = MPI_SUCCESS;
}
