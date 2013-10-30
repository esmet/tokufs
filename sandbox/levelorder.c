#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int num_slashes(char * str)
{
    int count = 0;
    while (*str != '\0') {
        if (*str == '/') {
            count++;
        }
        str++;
    }
    return count;
}

static int keycmp(char * a, char * b)
{
    int ret;
    int slashes_a = num_slashes(a);
    int slashes_b = num_slashes(b);

    if (slashes_a < slashes_b) {
        ret = -1;
    } else if (slashes_a > slashes_b) {
        ret = 1;
    } else {
        printf("%d==%d, resorting to strcmp\n", slashes_a, slashes_b);
        ret = strcmp(a, b);
    }
    return ret;
}

int main(void)
{
    int c;

    c = keycmp("/apples", "/bears");
    printf("/apples,/bears %d\n", c);

    c = keycmp("/apples", "/apples/pears");
    printf("/apples,/apples/pears %d\n", c);

    c = keycmp("/zebras", "/apples/pears");
    printf("zebras,/apples/pears %d\n", c);

    c = keycmp("/apples/pears/bears", "/apples/cats");
    printf("/apples/pears/bears, /apples/cats %d\n", c);

    return 0;
}
