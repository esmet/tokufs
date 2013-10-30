#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <pthread.h>
#include <assert.h>
#include <mpi.h>

#define MESSAGE_SIZE    1024
#define REQUEST_FOO     1
#define REQUEST_BAR     2

struct message_s
{
    char buf[MESSAGE_SIZE];
};

static void * recv_messages(void * arg)
{
    (void) arg;
    int ret;
    MPI_Status status;
    static struct message_s message;
    int rank;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /*
    int MPI_Irecv(void* buf, int count, 
        MPI_Datatype datatype, int source,
        int tag, MPI_Comm comm, MPI_Request, *request)
     */

    while (1) {
        printf("%s: proc %d waiting to recieve...\n", 
                __FUNCTION__, rank);
        
        ret = MPI_Recv(&message, sizeof(struct message_s),
                MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG,
                MPI_COMM_WORLD, &status);

        printf("%s: proc %d Got a message! It says \"%s\"\n",
                __FUNCTION__, rank, message.buf);
        printf("%s: proc %d The message is from %d\n",
                __FUNCTION__, rank, status.MPI_SOURCE);
        switch (status.MPI_TAG) {
        case REQUEST_FOO:
            printf("got request foo\n");
            break;

        case REQUEST_BAR:
            printf("got request bar\n");
            break;

        default:
            printf("unknown tag\n");
        }

        if (strcmp(message.buf, "stop") == 0) {
            printf("%s: proc %d got the stop message\n", 
                    __FUNCTION__, rank);
            break;
        }
    }

    pthread_exit(NULL);
}

static void send_message(int proc, struct message_s * message)
{
    //int MPI_Isend(void* buf, int count, MPI_Datatype datatype, 
    //      int dest, int tag, MPI_Comm, comm, MPI_Request *request)
    printf("%s: sending message to proc %d... \n",
            __FUNCTION__, proc);

    int ret;
    MPI_Request request;

    ret = MPI_Isend(message, sizeof(struct message_s), MPI_CHAR,
            proc, proc % 2 == 0 ? REQUEST_FOO : REQUEST_BAR,
            MPI_COMM_WORLD, &request);
    assert(ret == MPI_SUCCESS);

    printf("%s: message sent\n", __FUNCTION__);
}

int main(int argc, char * argv[])
{
    int proc;
    pthread_t thread;
    struct message_s message;
    char cmd[1024];
    char * proc_str;
    int rank;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    pthread_create(&thread, NULL, recv_messages, NULL);

    if (rank == 0) {
        while (printf("> ") && fgets(cmd, 1024, stdin) != NULL) {
            cmd[strlen(cmd) - 1] = '\0';
            proc_str = strtok(cmd, " \n");
            proc = atoi(proc_str);
            if (proc < 0) {
                printf("bad proc %s\n", proc_str);
                continue;
            }
            memset(&message, 0, sizeof(message));
            strcpy(message.buf, cmd + strlen(proc_str) + 1);
            send_message(proc, &message);
            printf("\n");
        }
    }

    pthread_join(thread, NULL);
    MPI_Finalize();
    printf("\n");

    return 0;
}
