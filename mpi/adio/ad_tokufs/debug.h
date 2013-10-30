/**
 * TokuFS
 */

#ifndef DEBUG_H
#define DEBUG_H

//#define DEBUG

#ifdef DEBUG
#define ad_tokufs_debug(rank, ...)                  \
    printf("process %d, %s: ", rank, __FUNCTION__); \
    printf(__VA_ARGS__)
#else
#define ad_tokufs_debug(...)   ((void) 0)
#endif

#endif /* DEBUG_H */
