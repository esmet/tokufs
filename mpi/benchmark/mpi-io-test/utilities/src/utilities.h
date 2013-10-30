/****************************************************************
* AUTHOR:    John Bent 
* DATE:      July 10, 2006
*
*      LOS ALAMOS NATIONAL LABORATORY
*      An Affirmative Action/Equal Opportunity Employer
*
* Copyright (c) 2005
* the Regents of the University of California.
*
* Unless otherwise indicated, this information has been authored by an
* employee or employees of the University of California, operator of the Los
* Alamos National Laboratory under Contract No. W-7405-ENG-36 with the U. S.
* Department of Energy. The U. S. Government has rights to use, reproduce, and
* distribute this information. The public may copy and use this information
* without charge, provided that this Notice and any statement of authorship
* are reproduced on all copies. Neither the Government nor the University
* makes any warranty, express or implied, or assumes any liability or
* responsibility for the use of this information.
******************************************************************/

#ifndef   __UTILITIES_H_INCLUDED
#define   __UTILITIES_H_INCLUDED

#define MPI_IO_TEST_VERSION "1.00.012"

#define O_CONCURRENT_WRITE 020000000000
#define TRUE 1
#define FALSE 0

#define WRITE_MODE 1
#define READ_MODE 0
#define INIT_MODE 2

#define BIG_FILE_OFFSET 8

#ifdef __cplusplus
extern "C" {
#endif


#include <getopt.h>
#include <stdio.h>
#include <stddef.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>


extern struct Parameters params;

struct myoption {
    const char *name;
    int  has_arg;
    int *flag;
    int val;
    char *help;
};

struct time_values {
    double file_open_wait_time; 
    double prealloc_wait_time; 
    double file_close_wait_time; 
    double barrier_wait_time; 
    double file_sync_wait_time; 
    double proc_send_wait_time; 
    double proc_recv_wait_time; 
    double file_op_wait_time;
    double total_op_time;
    double total_time;
    double stat_time;
};

struct read_error {
    off_t file_offset;
    off_t length;
    int   all_zeros;
    char  *expected;
    char  *received;
    struct read_error *next;
    struct read_error *prev;
    off_t str_length;
};

struct State {
    int fd;
    char *error_string;
    MPI_File mpi_fh;
    FILE *ofptr;
    FILE *efptr;
    int pagesize;
    int my_rank;
    unsigned objs_written;
    unsigned *objs_written_all;
    double total_mbs;
    unsigned min_objs;
    unsigned max_objs;
    unsigned ave_objs;
};

struct Parameters {
  int test_type;
  long   test_time; 
  size_t num_objs;
  size_t obj_size;
  size_t cbuf_size;
  int superblocks;
  int supersize;
  MPI_Offset prealloc_size;
  MPI_Info info;
  int    set_view_all; /* Flag denoting I/O procs need to set file view */
  int    info_allocated_flag;
  int    direct_io_flag;
  int    shift_flag;
  char   *mpiversion;
  char   *barriers;         /* User flag about when to barrier */
  char   *db_key;
  char   *datestr;
  char   *system;
  char   *hints;
  char   *tgtsfname;
  char   *efname;
  char   *ofname;
  char   *tfname;
  char   **host_io_list;
  char   *user;
  char   *experiment;       /* User flag to give name to DB to group runs*/
  char   *full_args;        /* complete set of command line args */
  char   *full_hints;       /* complete set of mpi-io hints */
  char   *host_list;
  float  procs_per_node;
  char   *panfs;
  char   *panfs_srv;
  char   *df_tot_before;
  int    df_perc_before;
  char   *jobid;
  char   *os_version;
  char   *error_message;    /* Wonder if we need multiple...? Just track 1st */
  int    panfs_threads;
  int    error_rank;
  int    *io_procs_list;
  int    prealloc_flag;
  int    ahead;
  int    touch;
  int    strided_flag;
  int    num_hosts;
  int    num_procs_io;
  int    num_procs_world;   /* how many procs are in this program? */
  int    init_flag;        /* initial flag to skip wait for writing    */
  int    write_only_flag;  /* Write target file only                */
  int    read_only_flag;   /* Read target file only                 */
  int    check_data_ndx;
  int    list_host_flag;   /* Flag denoting print all hosts         */
  int    aggregate_flag;   /* Flag denoting use of aggregate procs   */
  int    sync_flag;        /* Sync the data to file before close       */
  int    sleep_seconds;    /* Amount of time to slepp between write & read */
  int    delete_flag;          /* Flag denoting delete the target file     */
  int    collective_flag;      /* Use collective read/write calls */
  int    targets_flag;         /* Flag denoting a targets file exists     */
  int    write_file_flag;      /* Flag denoting messages written to file */
  int    io_proc_send_flag;    /* Flag denoting IO procs send data      */
  int    verbose_flag;         /* Print times for all processes           */
  int    trenddata;            /* Print output in Gazebo trend data format */
  int    time_limit;           /* whether to only run for a fixed time */

  int panfs_info_valid;
  int panfs_type;
  int panfs_stripe;
  int panfs_width;
  int panfs_depth;
  int panfs_comps;
  int panfs_visit;
  int panfs_concurrent_write;

  int posix_io;
  int    barrier_flag;     /* MPI_Barrier call before each write      */
  int    barrier_after_open_flag;
  int barrier_stat;
  int barrier_before_open;

  int random_io;
  int use_db;   // still only on if DEFINES set in Makefile, but this allows
                // the code to be there and selectively disabled at runtime
};

#define VALLOC(x) Valloc(x,efptr,my_rank)
#define MALLOC(x) Malloc(x,efptr,my_rank)
#define CALLOC(x,y) Calloc(x,y,efptr,my_rank)

void setTime( double *value, double initial, double end_time, 
        double start_time, char *name, char *file, int line, 
        FILE *efptr, int my_rank );

void *Calloc( size_t nmemb, size_t size, FILE *fp, int rank );
void *Malloc( size_t size, FILE *fp, int rank );
void *Valloc( size_t size, FILE *fp, int rank );

char printable_char( int index );
char * expand_path(char *str, int rank, long timestamp);
int parse_size(int my_rank, char *chbytes, long long int *out_value);

void fill_buf( char *check_buf, int blocksize, int rank, int i, int pagesize,
        int touch );
int  verify_buf( int rank, double *expected, double *received, char *filename, 
    long which_block, long blocksize, int touch );

int compare_double( char *file, int rank, long block, long block_off, long bs, 
	double actual, double expected );

void print_double( char *header, double one );

int get_min_sum_max(   int my_rank, 
                                double base_num, 
                                double *min, 
                                int *min_ndx,
                                double *sum, 
                                double *max, 
                                int *max_ndx, 
                                char *op,
                                FILE *ofptr, 
                                FILE *efptr);

ssize_t Pread64(int fildes, void *buf, size_t nbyte, off_t offset);
ssize_t Pwrite64(int fildes, const void *buf, size_t nbyte, off_t offset);
ssize_t Write(int fd, const void *vptr, size_t nbyte );
ssize_t Read(int fd, void *vptr, size_t nbyte );

int warning_msg( FILE *fp, int my_rank, int mpi_ret, MPI_Status *mstat,
        const char *format, ... );
int fatal_error(FILE *fp, int my_rank,  
			    int mpi_ret, MPI_Status *mstat, const char *fmt,...);
int debug_msg  ( FILE *fp, int my_rank,  const char *fmt, ... );

void get_panfs_file_info( struct Parameters *params );

void set_hostlist( struct Parameters *params, int rank );

void collect_additional_config( struct Parameters *params, int rank, int argc,
        char **argv );

void insert_panfs_info( char *key, char *value ); 

int db_insert(  int my_rank,
                int failure,
                char *error_msg,
                struct Parameters *params, 
                struct State *state,
                struct time_values *write_times,
                struct time_values *read_times );

#ifdef __cplusplus
}
#endif

#endif 
