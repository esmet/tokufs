/**
 * TokuFS
 */

#include "ad_tokufs.h"
#include "adio-tokufs.h"

void ADIOI_TOKUFS_Delete(char * filename, int * err)
{
    int ret;
    int rank;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    ad_tokufs_debug(rank, "called with filename %s\n", filename);

    /* Not implemented yet.
    ret = tokufs_remove(filename);
    assert(ret == 0);
    */
    ad_tokufs_debug(rank, "no-op!\n");
    ret = 0;

    *err = MPI_SUCCESS;
}
