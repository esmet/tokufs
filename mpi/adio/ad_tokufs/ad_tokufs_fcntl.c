/**
 * TokuFS
 */

#include "ad_tokufs.h"
#include "adio-tokufs.h"

void ADIOI_TOKUFS_Fcntl(ADIO_File fd, int flag, 
        ADIO_Fcntl_t * fcntl, int * err)
{
    int ret;
    int rank;
    /*
    struct tokufs_stat_s st;
    */

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    ad_tokufs_debug(rank, "called with filename %s\n", fd->filename);

    /*
    ret = tokufs_stat(fd->fd_sys, &st);
    assert(ret == 0);
    */
    ret = 0;
    ad_tokufs_debug(rank, "no-op!\n");

    /*
    switch (flag) {
        case ADIO_FCNTL_SET_ATOMICITY:
            fd->atomicity = fcntl->atomicity;
            *err = MPI_SUCCESS;
            break;

        default:
            *err = MPIO_Err_create_code(MPI_SUCCESS,
                    MPIR_ERR_RECOVERABLE, __FUNCTION__,
                    __LINE__, MPI_ERR_ARG, "**flag",
                    "**flag %d", flag);
    }
    */
}
