/**
 * TokuFS
 */
 
#ifndef AD_TOKUFS_H
#define AD_TOKUFS_H

#include <tokufs.h>
#include <assert.h>

#include "adio.h"
#include "debug.h"

void ADIOI_TOKUFS_Open(ADIO_File fd, int * err);

void ADIOI_TOKUFS_Close(ADIO_File fd, int * err);

void ADIOI_TOKUFS_WriteContig(ADIO_File fd, void * buf, int num_elements, 
    MPI_Datatype datatype, int filetype, ADIO_Offset offset, 
    ADIO_Status * status, int * err);
                
void ADIOI_TOKUFS_ReadContig(ADIO_File fd, void * buf, int num_elements, 
    MPI_Datatype datatype, int filetype, ADIO_Offset offset, 
    ADIO_Status * status, int * err);

void ADIOI_TOKUFS_Resize(ADIO_File fd, ADIO_Offset size, int * err);

void ADIOI_TOKUFS_Fcntl(ADIO_File fd, int flag, 
        ADIO_Fcntl_t * fcntl, int * err);

void ADIOI_TOKUFS_Delete(char * filename, int * err);

void ADIOI_TOKUFS_Flush(ADIO_File fd, int * err);

#endif /* AD_TOKUFS_H */
