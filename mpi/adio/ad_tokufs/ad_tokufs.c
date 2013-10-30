/**
 * TokuFS
 */

#include "ad_tokufs.h"
#include "adioi.h"

/**
 * Each instantiation of an ADIO layer is represented by a struct
 * of functions to call for things like open, read, write, etc...
 */
struct ADIOI_Fns_struct ADIO_TOKUFS_operations =
{
    ADIOI_TOKUFS_Open,              /* ad_tokufs_open.c */
    ADIOI_GEN_OpenColl,
    ADIOI_TOKUFS_ReadContig,        /* ad_tokufs_read.c */
    ADIOI_TOKUFS_WriteContig,       /* ad_tokufs_write.c */
    ADIOI_GEN_ReadStridedColl,
    ADIOI_GEN_WriteStridedColl,
    ADIOI_GEN_SeekIndividual,
    ADIOI_TOKUFS_Fcntl,             /* ad_tokufs_fcntl.c */
    ADIOI_GEN_SetInfo,
    ADIOI_GEN_ReadStrided,
    ADIOI_GEN_WriteStrided,
    ADIOI_TOKUFS_Close,             /* ad_tokufs_close.c */
#if 0
//#ifdef ROMIO_HAVE_WORKING_AIO
    ADIOI_GEN_IreadContig,
    ADIOI_GEN_IwriteContig,
#else
    ADIOI_FAKE_IreadContig,
    ADIOI_FAKE_IwriteContig,
#endif
    ADIOI_GEN_IODone,
    ADIOI_GEN_IODone,
    ADIOI_GEN_IOComplete,
    ADIOI_GEN_IOComplete,
    ADIOI_GEN_IreadStrided,
    ADIOI_GEN_IwriteStrided,
    ADIOI_TOKUFS_Flush,             /* ad_tokufs_flush.c */
    ADIOI_TOKUFS_Resize,            /* ad_tokufs_resize.c */
    ADIOI_TOKUFS_Delete,            /* ad_tokufs_delete.c */
    ADIOI_GEN_Feature,
};
