/**
 * TokuFS
 */

#ifndef TOKU_BYTEORDER_H
#define TOKU_BYTEORDER_H

#include <assert.h>
#include <arpa/inet.h>

static int machine_is_little_endian = -1;

// the machine is little endian if the 64 bit integer
// representation for 1 starts with a one byte 1 value
__attribute__ ((constructor))
static inline void byteorder_check(void)
{
    static const int64_t one = 1;
    static const char * buf = (const char *) &one;
    if (buf[0] == 1) {
        machine_is_little_endian = 1;
    } else if (buf[0] == 0) {
        machine_is_little_endian = 0;
    } else {
        assert(0);
    }
}

/**
 * Get the network (big endian) byte ordering of a 64 bit integer.
 */
static inline uint64_t htonl64(uint64_t host)
{
    assert(machine_is_little_endian != -1);
    // if we're little endian, then we need to convert to 
    // the big endian net format
    if (machine_is_little_endian) {
        uint64_t left = htonl(host & 0xFFFFFFFF);
        uint64_t right = htonl(host >> 32);
        return (left << 32) | right;
    } else {
        return host;
    }
}

/**
 * Get the host byte ordering of a 64 bit integer.
 */
static inline uint64_t ntohl64(uint64_t net)
{
    assert(machine_is_little_endian != -1);
    // if we're little endian, then we need to convert the
    // big endian net format to our format
    if (machine_is_little_endian) {
        uint64_t left = ntohl(net & 0xFFFFFFFF);
        uint64_t right = ntohl(net >> 32);
        return (left << 32) | right;
    } else {
        return net;
    }
}

#endif /* TOKU_BYTEORDER_H */
