#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char * argv[])
{
    int i;

    for (i = 1; i < argc; i++) {
        int err = atoi(argv[i]);
        printf("%d %s\n", err, strerror(err));
    }
    
    return 0;
}
