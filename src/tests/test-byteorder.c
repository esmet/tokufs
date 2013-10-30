#include <assert.h>
#include "../byteorder.h"

int main(void)
{
    uint64_t k;

    k = 0xDEADBEEF11223344;
    assert(ntohl64(htonl64(k)) == k);
    k = 1;
    assert(ntohl64(htonl64(k)) == k);
    k = (uint64_t) -1;
    assert(ntohl64(htonl64(k)) == k);
    k /= 2;
    assert(ntohl64(htonl64(k)) == k);
    k += 24152342;
    assert(ntohl64(htonl64(k)) == k);

    return 0;
}
