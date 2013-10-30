/**
 * TokuFS
 */
 
#ifndef TOKU_DEBUG_H
#define TOKU_DEBUG_H

#define _GNU_SOURCE

#include <stdio.h>
#include <unistd.h>

#include <sys/syscall.h>
#include <sys/time.h>

#include "cc.h"

// globally defined in one place, typically tokufs.c
extern int toku_debug;

UNUSED
static int toku_gettid(void)
{
    // too much hassle getting syscall to prototype correctly
    long syscall(long n);
    return syscall(SYS_gettid);
}

UNUSED
static long toku_current_time_usec(void)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return t.tv_sec * 1000000L + t.tv_usec;
}

#define debug_echo(...)                                             \
    do {                                                            \
        if (toku_debug) {                                           \
            printf("[%d] %s(): ", toku_gettid(), __FUNCTION__);     \
            printf(__VA_ARGS__);                                    \
        }                                                           \
    } while (0)

#endif /* TOKU_DEBUG_H */
