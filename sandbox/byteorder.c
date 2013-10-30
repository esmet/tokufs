#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>

static uint64_t htonl64(uint64_t x)
{
#ifdef LITTLE_ENDIAN
    uint64_t left = htonl(x & 0xFFFFFFFF);
    uint64_t right = htonl(x >> 32);
    return (left << 32) | right;
#else // BIG_ENDIAN, do nothing
    return x;
#endif
}

static inline size_t bigend(size_t x)
{
    return htonl64(x);
}

static void dump_bytes(void * buf, size_t size)
{   
    size_t i;
    unsigned char * cbuf = buf;

    for (i = 0; i < size; i++) {
        printf("%x ", cbuf[i]);
    }
}

int main(void)
{
    size_t n = 5;

    printf("before bigend()\n");
    dump_bytes(&n, sizeof(n));
    printf("\n");

    n = bigend(n);

    printf("after bigend()\n");
    dump_bytes(&n, sizeof(n));
    printf("\n");

    for (n = 0; n < 1000000; n++) {
        size_t x = bigend(n);
        dump_bytes(&n, sizeof(size_t));
        printf(" -> ");
        dump_bytes(&x, sizeof(size_t));
        printf("\n");
    }

    return 0;
}
