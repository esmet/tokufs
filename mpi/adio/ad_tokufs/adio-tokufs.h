/**
 * TokuFS
 */

#ifndef ADIO_TOKUFS_H
#define ADIO_TOKUFS_H

/* Old code ideas for a distributed adio file over many bstores. */

#if 0
static inline size_t adio_tokufs_blocksize(void)
{
    return tokufs_blocksize() * 4;
}

/* For now, every participant will be a tokufs reader/writer */
#define is_tokufs_proc(__procnum, __nprocs)                 \
    (1)

#define adio_offset_to_blocknum(__offset, __blocksize)      \
    (__offset / __blocksize)

#define adio_blocknum_to_tokufs_proc(__blocknum, __nprocs)  \
    (__blocknum % __nprocs)
#endif

#endif /* ADIO_TOKUFS_H */
