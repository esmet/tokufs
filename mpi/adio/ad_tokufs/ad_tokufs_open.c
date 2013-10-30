/**
 * TokuFS
 */

#include "ad_tokufs.h"
#include "adio-tokufs.h"

static int tokufs_mounted;

void ADIOI_TOKUFS_Open(ADIO_File fd, int * err)
{
    int ret;
    int rank;
    tokufs_fd tokufd;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    ad_tokufs_debug(rank, "called with filename %s\n", fd->filename);

    if (!tokufs_mounted) {
        tokufs_mount("adio.mount");
        tokufs_mounted = 1;
    }

    ret = tokufs_open(&tokufd, fd->filename, rank);
    assert(ret == 0);

    fd->fd_sys = tokufd;
    fd->fp_ind = 0;
    fd->fp_sys_posn = -1;

    *err = MPI_SUCCESS;
}
