#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include <bstore.h>

int main(int argc, char * argv[])
{
    int i, ret;
    struct bstore_s bstore;
    char * name;
    char * path;
    uint64_t key;

    if (argc == 6) {
        for (i = 1; i < argc - 1; i++) {
            if (strcmp(argv[i], "--env") == 0) {
                path = argv[++i];
            } else if (strcmp(argv[i], "--name") == 0) {
                name = argv[++i];
            }
        }
        long k = atol(argv[5]);
        assert(k >= 0);
        key = k;
    } else if (argc == 2) {
        name = "bstore";
        path = "bstore.env";
        long k = atol(argv[1]);
        assert(k >= 0);
        key = k;
    } else {
        fprintf(stderr, "usage: bstore-get [--env x --name y] key\n");
    }
        
    printf("opening env: %s\n", path);
    ret = bstore_env_open(path);
    assert(ret == 0);
    printf("opening bstore: %s\n", name);
    ret = bstore_open(&bstore, name);
    assert(ret == 0);

    char buf[BSTORE_BLOCKSIZE];
    ret = bstore_get(&bstore, key, buf);
    if (ret == 0) {
        size_t i;
        for (i = 0; i < BSTORE_BLOCKSIZE; i++) {
            printf("%x%s", buf[i], (i+1) % 80 == 0 ? "\n" : "");
        }
        printf("\n");
    } else if (ret == BSTORE_UNINITIALIZED_GET) {
        printf("key %lu not found\n", key);
    } else {
        printf("unknown return value %d for key %lu\n", ret, key);
    }

    return 0;
}
