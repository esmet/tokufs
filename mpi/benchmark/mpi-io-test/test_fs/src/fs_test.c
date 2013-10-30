/*************************************************************************
 * Copyright (c) 2005, The Regents of the University of California
 * All rights reserved.
 *
 * Copyright 2005. The Regents of the University of California. 
 * This software was produced under U.S. Government contract W-7405-ENG-36 for 
 * Los Alamos National Laboratory (LANL), which is operated by the University 
 * of California for the U.S. Department of Energy. The U.S. Government has 
 * rights to use, reproduce, and distribute this software.  
 * NEITHER THE GOVERNMENT NOR THE UNIVERSITY MAKES ANY WARRANTY, EXPRESS OR 
 * IMPLIED, OR ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If 
 * software is modified to produce derivative works, such modified software 
 * should be clearly marked, so as not to confuse it with the version available
 * from LANL.
 * Additionally, redistribution and use in source and binary forms, with or 
 * without modification, are permitted provided that the following conditions 
 * are met:
 *      Redistributions of source code must retain the above copyright notice, 
 *      this list of conditions and the following disclaimer. 
 *      Redistributions in binary form must reproduce the above copyright 
 *      notice, this list of conditions and the following disclaimer in the 
 *      documentation and/or other materials provided with the distribution. 
 *      Neither the name of the University of California, LANL, the U.S. 
 *      Government, nor the names of its contributors may be used to endorse 
 *      or promote products derived from this software without specific prior 
 *      written permission. 
 * THIS SOFTWARE IS PROVIDED BY THE UNIVERSITY AND CONTRIBUTORS "AS IS" AND 
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED. IN NO EVENT SHALL THE UNIVERSITY OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY 
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH 
 * DAMAGE.
*************************************************************************/

#include <math.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <assert.h>
#include <ctype.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <limits.h>
#include <getopt.h>
#include "mpi.h"
#include "utilities.h"
#include "print.h"


/*
   TODO:
    read should be affected by time limit also
    pass min and max blocks when time limit used
*/

int init( int argc, char **argv, struct Parameters *params,
        struct time_values *write_times,
        struct time_values *read_times,
        struct State *state );
int read_write_file( struct Parameters *params,
            struct time_values *times,
            struct State *state,
            int read_write );
void fini( struct Parameters *params, struct State *state );
void Usage( struct Parameters *params, struct State *state, char *field,
            char *missing );

// global so can be externed for more useful error messages outside this file
struct Parameters params;       /* Parameters passed from user   */ 

struct myoption mylongopts[] = {
    { "barriers",   required_argument,  NULL,                            'b',
    "When to barrier.  Comma seperated.\n"
    "\te.g. -barriers bopen,aopen,bwrite,aread,aclose"                      },
    { "dio",        no_argument,        NULL,                            'd',
        "Whether to tell MPI to use direct IO (for MPI-IO only)"            },
    { "errout",     required_argument,  NULL,                            'e',
        "The path to the error file"                                        },
    { "target",     required_argument,  NULL,                           'g',
        "The path to the data file"                                         },
    { "hints",      required_argument,  NULL,                           'h',
        "Any hints to pass to MPI such as panfs_concurrent_write (x=y[,a=b])" },
    { "time",       required_argument,  NULL,                           'l',
        "Whether to only run for a set time limit (in seconds)"             },
    { "op",         required_argument,  NULL,                           'o',
        "Whether to read only (read) or write only (write)"                 },
    { "output",     required_argument,  NULL,                           'O',
        "The path to the output file"                                       },
    { "nobj",       required_argument,  NULL,                           'n',
        "The number of objects written/read by each proc"                   },
    { "posix",      required_argument,  NULL,                           'P' ,
        "Whether to use posix I/O instead of MPI I/O routines"              },
    { "sleep",      required_argument,  NULL,                           'p',
        "Number of seconds to sleep between write and read phases"          },
    { "strided",    required_argument,  NULL,                           's',
        "Whether to use a strided pattern (for N-1 only)"                   },
    { "supersize",  required_argument,  NULL,                           'S',
        "When running N-1 non-strided with time limit, must specify how "
        "many objects per superblock"                                       },
    { "touch",      required_argument,  NULL,                           'T',
        "0 don't fill buffer nor verify, 1 touch first byte in each block,\n"
        "\t2 touch first byte in each page, 3 touch every byte"           },
    { "check",      required_argument,  NULL,                           'C',
        "0 don't fill buffer nor verify, 1 check first byte in each block,\n"
        "\t2 check first byte in each page, 3 check every byte"           },
    { "type",       required_argument,  NULL,                           't',
        "Whether to do N-N (1) or N-1 (2)"                                  },
    { "experiment", required_argument,  NULL,                           'x',
        "Assign an arbitrary label to this run in the database"             },
    { "size",       required_argument,  NULL,                           'z',
        "The size of each object"                                           },
    { "deletefile", no_argument,        &(params.delete_flag),           1,
        "Whether to delete the file before exiting"                         },
    { "sync",       no_argument,        &(params.sync_flag),             1,
        "Whether to sync the file after writing"                            },
    { "nodb",       no_argument,        &(params.use_db),                0,
        "Turn off the database code at runtime"                             },
    { "collective", no_argument,        &(params.collective_flag),       1,
        "Whether to use collective I/O (for N-1, mpi-io only)"              },
    { "shift",      no_argument,        &(params.shift_flag),            1,
        "Whether to shift ranks to avoid read caching"                      },
    { "help",       optional_argument,  NULL,                           'H',
        "General help or help about a specific argument"                    },
    { NULL,         0,                  NULL,                    0 ,
        NULL                                                        },
};

char *prog_name = NULL;

int 
main( int argc, char *argv[] )
{
    struct time_values write_times;         /* holder for all measured times */
    struct time_values read_times;         /* holder for all measured times */
    struct State       state;
    int rc;
    prog_name = strdup(argv[0]);

    if ( (rc = init( argc, argv, 
                        &params, &write_times, &read_times, &state )))
    {
        if ( state.my_rank == 0 ) {
            fprintf( stderr, "%d: Early exit due to init error: %d\n", 
                    state.my_rank,rc );
        }
        return -1;
    }
    if ( params.use_db ) {
        db_insert( state.my_rank, 0, "partial", &params, &state, NULL, NULL );
    }

    if(state.my_rank == 0) {
        time_t lt = time(NULL);
#ifdef MPI_IO_TEST_VERSION2
        fprintf(state.ofptr,"MPI-IO TEST v%s: %s\n",
            MPI_IO_TEST_VERSION2, asctime(localtime(&lt)));
#else
        fprintf(state.ofptr,"MPI-IO TEST v%s: %s\n",
            MPI_IO_TEST_VERSION, asctime(localtime(&lt)));
#endif
    }

    // write the file
    if ( !params.read_only_flag ) {
        read_write_file( &params, &write_times, &state, WRITE_MODE );
        if ( params.use_db ) {
            db_insert(state.my_rank, 0, NULL,&params,&state,&write_times,NULL);
            if ( params.write_only_flag ) { // clear the partial
              db_insert(state.my_rank, 0, "NULL", &params, &state, NULL, NULL );
            }
        }
    }

    // read the file
    if ( !params.write_only_flag ) {
        read_write_file( &params, &read_times, &state, READ_MODE );
      if ( params.use_db ) {
            // first push read times, then clear partial
          db_insert(state.my_rank, 0, NULL, &params, &state, NULL, &read_times);
          db_insert( state.my_rank, 0, "NULL", &params, &state, NULL, NULL ); 
      }
    }

    fini( &params, &state );
    return 0;
}

void
fini( struct Parameters *params, struct State *state ) {

    if (params->ofname != NULL && state->my_rank == 0)   fclose(state->ofptr);
    if (params->efname != NULL)   fclose(state->efptr);

    if(params->info && params->info != MPI_INFO_NULL){
        MPI_Info_free(&(params->info));
    }

    if (MPI_Finalize() != MPI_SUCCESS) {
        fprintf(stderr,"Rank %d ERROR: MPI_Finalize failed.\n", state->my_rank);
    }
    exit( 0 );
}


/*******************************************
 *   ROUTINE: parse_command_line
 *   PURPOSE: Parse the command line
 *   DATE:   January 3, 2005 
 *   LAST MODIFIED: February 14, 2006 [swh]
 *******************************************/
int parse_command_line(int my_rank, int argc, char *argv[], 
                struct Parameters *params, struct State *state )
{
  int i = 1;
  int index = 0;
  int mpi_ret;
  char ch;
  char strtok_buf[65536];
  char getopt_buf[65536];

  long long int temp_num;
  struct myoption *current = mylongopts;
  int    num_opts = 0;
  struct option *longopts;

  // copy our format of longopts to the gnu version (ours has extra fields
  // for help messages, etc)
  while( current->name ) {
      num_opts++;
      current++;
  }
  num_opts++;   // one extra space for the final NULL one 
  longopts = (struct option *)Malloc( sizeof(struct option) * num_opts,
                                      state->efptr, state->my_rank );
  getopt_buf[0] = (char)0;;
  for( i = 0; i < num_opts; i++ ) {
      if ( mylongopts[i].flag == NULL && mylongopts[i].name != NULL ) {
          int cur_len = strlen( getopt_buf );
          snprintf( &(getopt_buf[cur_len]), 65536 - cur_len, "%c%s",
              (char)mylongopts[i].val,
               ( mylongopts[i].has_arg == required_argument ? ":" : "" ) );
      }
      longopts[i].name    = mylongopts[i].name;
      longopts[i].has_arg = mylongopts[i].has_arg;
      longopts[i].flag    = mylongopts[i].flag;
      longopts[i].val     = mylongopts[i].val;
  }
  //if ( my_rank == 0 ) fprintf( stderr, "Built opt buf: %s\n", getopt_buf );

/******************************************************************
* allocate the MPI_Info structure. If no hints are set, the structure 
* will be freed at the end of this routine.
******************************************************************/
  if( (mpi_ret = MPI_Info_create(&params->info)) != MPI_SUCCESS){
    if(my_rank == 0)
        fatal_error( stderr, my_rank, mpi_ret, NULL,
				"Unable to create MPI_info structure.\n");
  }


/******************************************************************
* Now read and load all command line arguments. 
******************************************************************/
  while ((ch = getopt_long_only( argc, argv,
              getopt_buf, longopts, &index )) != -1 )
  {
      switch(ch) {
        case 'b':
            params->barriers = strdup( optarg );
            break;
        case 'e':   
            params->efname = strdup( optarg );
            break;
        case 'g':
            /*  // don't expand here, do it as needed
            if ((params->tfname = expand_path(optarg, state->my_rank)) 
                  == (char *)0) 
            {
                fatal_error( state->efptr, state->my_rank, 0, NULL,
				  "Problem expanding file name %s.\n", optarg );
            }
            */
            params->tfname = strdup( optarg );
            break;
        case 'd':
            params->direct_io_flag = TRUE;
            params->info_allocated_flag = TRUE;
            //MPIIO_set_hint(my_rank, &(params->info), "direct_read", "true");
            //MPIIO_set_hint(my_rank, &(params->info), "direct_write", "true");
            MPI_Info_set( params->info, "direct_read", "true" );
            MPI_Info_set( params->info, "direct_write", "true" );
            break;
        case 'h':
            {
            const char *outer_delim = ",";
            const char inner_delim = '=';
            char *hint; 
            //printf( "handling hints %s\n", optarg );
            params->info_allocated_flag = TRUE;
            params->hints = strdup( optarg );
            snprintf( strtok_buf, 65536, "%s", optarg );
            hint = strtok( strtok_buf, outer_delim );
            while( hint ) {
                char *equal = strchr( hint, (int)inner_delim );
                if ( ! equal ) {
                    fprintf( stderr, 
                            "No val passed to key %s for hints %s\n",
                            hint, optarg );
                    exit( 1 );
                }
                (*equal) = (char)NULL;
                equal++;
                if ( (mpi_ret = MPI_Info_set( params->info, hint, equal ))
                        != MPI_SUCCESS )
                {
                    printf( "Set hint %s -> %s: %d\n", hint, equal, mpi_ret );
                }
                hint = strtok( NULL, outer_delim );   // get next one
            }
            break;
            }
        case 'H':
            {
            char *flag = NULL;
            if ( argv[optind] && argv[optind][0] != '-' ) {
                flag = argv[optind];
            }
            Usage( params, state, flag, NULL );
            }
            break;
        case 'l':
            params->time_limit = atoi( optarg ); 
            break;
        case 'n':
            params->num_objs = (size_t)strtoul(optarg, NULL, 10);
            break;
        case 'o':
            if ( !strcmp( optarg, "write" ) ) {
                params->write_only_flag = TRUE;
            } else if (!strcmp(optarg, "read")){
                params->read_only_flag = TRUE;
            } else {
                if ( state->my_rank == 0 ) {
                    fprintf( state->efptr, 
                      "Operation %s is not supported. "
                      "Only recognized operation is read or write.\n", optarg);
                }
                Usage( params, state, NULL, NULL );
            }
            break;
        case 'O':
            params->ofname = strdup( optarg );
            break;
        case 'P':
            params->posix_io = atoi( optarg );
            break;
        case 'p':
            params->sleep_seconds = atoi(optarg);
            break;
        case 's':
            params->strided_flag  = atoi(optarg);
            break;
        case 'S':
            params->supersize = atoi(optarg);
            break;
        case 't':
            params->test_type   = atoi(optarg);
            break;
        case 'C':
            params->check_data_ndx   = atoi(optarg);
            break;
        case 'T':
            params->touch   = atoi(optarg);
            break;
        case 'x':
            params->experiment = strdup( optarg );
            break;
        case 'z':
            parse_size( state->my_rank, optarg, &temp_num );
            params->obj_size = temp_num;
            break;
        case ':':
            if ( state->my_rank == 0 ) {
                fprintf( state->efptr, 
                    "No arg passed for required arg (index %d)\n", index );
            }
            Usage( params, state, NULL, NULL );
            break;
        case '?':
            // think ones w/out args come here maybe ...
            if ( state->my_rank == 0 ) {
                //fprintf( state->efptr, 
                //    "Unknown arg %s (index %d) passed\n", 
                //    argv[optind-1], index );
            }
            //Usage( params, state, NULL, NULL );
            break;
        default:
            if ( state->my_rank == 0 ) {
                fprintf( state->efptr, 
                    "Unknown arg %s (index %d) passed\n", 
                    argv[optind-1], index );
            }
            // think ones w/out args come here maybe ....
            //fatal_error( state->efptr, state->my_rank, 0, NULL,
            //        "WTF: getopt_long\n" );
            //if ( state->my_rank == 0 ) {
            //    fprintf( stderr, "Bad args?\n" );
            //}
            break;
      }
  }

/********************************************************
* If no hints were set, free the MPI_Info structure.
**********************************************************/
  if(!(params->info_allocated_flag)){
    if( (mpi_ret = MPI_Info_free(&params->info)) != MPI_SUCCESS){
        fatal_error( state->efptr, state->my_rank, 0, NULL, 
                "MPI_Info_free failed\n" );
    }
    params->info = MPI_INFO_NULL;
  }

  if ( params->test_type == 1 && params->strided_flag == 1 ) {
    params->strided_flag = 0;
    if ( state->my_rank == 0 ) {
      fprintf( stderr, "Ignoring non-sensical strided arg for N-N\n" );
    }

  }
  
  return(1);
}

FILE *
set_output( struct Parameters *params, char *path, struct State *state ) {
    FILE *fp = fopen( path, "w" );
    if ( ! fp ) {
        fatal_error( state->efptr, state->my_rank, 0, NULL,
                "Unable to open output file %s: %s\n",
                path, strerror(errno) );
    }
    return fp;
}

int 
init( int argc, char **argv, struct Parameters *params,
        struct time_values *write_times,
        struct time_values *read_times,
        struct State *state ) 
{
    int mpi_ret;
    int myhost_num;
    char my_host[MPI_MAX_PROCESSOR_NAME];
    memset( params, 0, sizeof( *params ) );
    memset( write_times, 0, sizeof( *write_times ) );
    memset( read_times, 0, sizeof( *read_times ) );
    memset( state, 0, sizeof( *state ) );
    params->posix_io            = 0;
    params->superblocks         = 1;
    params->write_file_flag     = TRUE;
    params->io_proc_send_flag   = TRUE;
    params->use_db              = TRUE;

    state->efptr    = stderr;
    state->ofptr    = stdout;
    state->pagesize = getpagesize(); 

    if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
        fprintf(state->efptr, "ERROR: Unable to initialize MPI (MPI_Init).\n");
        return -1;
    }

    // tell MPI that we want to handle errors.  This should be good
    // because we always do our own error checking of MPI routines and
    // then pass any errors to fatal_error which in turn does MPI_Abort
    mpi_ret = MPI_Errhandler_set( MPI_COMM_WORLD, MPI_ERRORS_RETURN );
    if ( mpi_ret != MPI_SUCCESS ) {
        fprintf(state->efptr, "MPI_Errhandler set.\n");
        return -2;
    }

  
        // figure out rank and total procs
    if (MPI_Comm_rank(MPI_COMM_WORLD, &(state->my_rank) ) != MPI_SUCCESS) {
        fprintf(state->efptr, 
                "ERROR: Cant get processor rank (MPI_Comm_rank).\n");
        return -3;
    }
    if (MPI_Comm_size(MPI_COMM_WORLD, &(params->num_procs_world))!=MPI_SUCCESS){
        fprintf(state->efptr, "[RANK %d] ERROR: Problem getting number of "
                        "processors in MPI_COMM_WORLD.\n", state->my_rank);
        return -4;
    }


        // now parse the args
    if( !parse_command_line(state->my_rank, argc, argv, params, state ) ) {
        if ( state->my_rank == 0 ) {
            fprintf(state->efptr, 
                "ERROR [RANK %d]: Problem parsing the command line.\n",
                state->my_rank);
        }
        MPI_Finalize();
        return -5;
    }
    if ( params->tfname == NULL ) {
        Usage( params, state, NULL, "-target" );
    }
    if ( params->test_type == 0 ) {
        Usage( params, state, NULL, "-type" );

    }

        // set up the output files
    if ( params->efname ) {
        state->efptr = set_output( params, params->efname, state );
    }
    if ( params->ofname && state->my_rank == 0 ) {
        state->ofptr = set_output( params, params->ofname, state );
    }

        // get other info for the database
    collect_additional_config( params, state->my_rank, argc, argv );
    if(!print_input_environment(state->my_rank, 
                  myhost_num, 
                  my_host,
                  params,
			      state->ofptr, 
                  state->efptr))
    {
        fatal_error( state->efptr, state->my_rank, 0, NULL, 
              "Problem detected printing input and envirnoment variables.\n" );
    }
    return 0;
}

void
check_hints( struct Parameters *params,
             struct State *state ) 
{
    int nkeys, i, dummy_int;
    MPI_Info info_used = MPI_INFO_NULL;
    char key[MPI_MAX_INFO_KEY];
    char val[MPI_MAX_INFO_VAL];
    if ( state->my_rank != 0 || params->posix_io ) return;

    MPI_File_get_info( state->mpi_fh, &info_used ); 
    MPI_Info_get_nkeys( info_used, &nkeys );
    fprintf( state->ofptr, "\nMPI_info %d Hints Used (RANK 0):\n", nkeys );
    for( i = 0; i < nkeys; i++ ) {
        MPI_Info_get_nthkey( info_used, i, key );
        char *save_key = strdup( key );
        MPI_Info_get( info_used, key, MPI_MAX_INFO_VAL, val, &dummy_int );

	    fprintf(state->ofptr, "Key: %s = %s\n", save_key, val );
        if ( strstr( save_key, "panfs_" ) ) {
            //printf( "Inserting panfs info: %s %s\n", save_key, val );
            insert_panfs_info( save_key, val ); // inserting panfs_info
        }
        free( save_key );
    }
}


int
open_file(  struct Parameters *params,
            struct time_values *times,
            struct State *state,
            int read_write,
            double begin_time,
            char *target ) 
{
    int mpi_ret = MPI_SUCCESS;
    int mpi_mode;
    int posix_mode;
    MPI_Comm comm_file;

    if ( read_write == WRITE_MODE ) {
        mpi_mode   = MPI_MODE_WRONLY | MPI_MODE_CREATE;
        posix_mode = O_CREAT | O_RDWR;
    } else {
        mpi_mode   = MPI_MODE_RDONLY; 
        posix_mode = O_RDONLY; 
    }

    if ( params->test_type == 1 ) {
        comm_file = MPI_COMM_SELF;
        mpi_mode |= MPI_MODE_UNIQUE_OPEN;
    } else {
        if ( read_write == WRITE_MODE ) {
            posix_mode |= O_CONCURRENT_WRITE; 
        }
        comm_file = MPI_COMM_WORLD;
    }
    
    if ( params->posix_io ) {
        state->fd = open( target, posix_mode, 0666 );
    } else {
        mpi_ret = MPI_File_open( comm_file, target,
                mpi_mode, params->info, &(state->mpi_fh) );
    }

    if ( ( params->posix_io && state->fd < 0 ) || 
         ( ! params->posix_io && mpi_ret != MPI_SUCCESS ) ) 
    {
        fatal_error( state->efptr, state->my_rank, mpi_ret, NULL,
                "Unable to open file %s for %s.\n",
                target,
                read_write == WRITE_MODE ? "write" : "read" );
    }
    setTime( &(times->file_open_wait_time), 0, MPI_Wtime(), begin_time,
            "file_open_wait_time", __FILE__, __LINE__, 
            state->efptr, state->my_rank );
    return mpi_ret;
}

off_t
get_offset( struct Parameters *params, struct State *state, int write_num ) {
    off_t offset = 0;
    switch( params->test_type ) {
    case 1:
        offset = write_num * params->obj_size;
        break;
    case 2:
        if ( params->strided_flag ) {
            off_t stride_size   = params->num_procs_world * params->obj_size;
            off_t stride_offset = state->my_rank * params->obj_size;
            offset = write_num * stride_size + stride_offset;
        } else {
                // maybe if we add a time mode, it might go beyond the 
                // intended maximum size
            off_t num_objs, superblock_size, which_supersuper,
                  superblock_offset, offset_within_super, supersuper_size,
                  supersuper_offset; 
            if ( params->time_limit ) {
                if ( ! params->supersize && state->my_rank == 0 ) {
                    fatal_error( state->efptr, state->my_rank, 0, NULL,
                            "Bad args: must specify supersize "
                            "for N-1 non-strided with a time limit\n" );
                }
                num_objs = params->supersize;
            } else {
                num_objs = params->num_objs;
            }
            superblock_size       = params->obj_size * num_objs;
            superblock_offset     = superblock_size * state->my_rank;
            which_supersuper      = floor(write_num / num_objs);
            supersuper_size       = superblock_size * params->num_procs_world; 
            supersuper_offset     = supersuper_size * which_supersuper;
            offset_within_super   = write_num * params->obj_size;
            offset = supersuper_offset     // which supersuper
                   + superblock_offset     // which super
                   + offset_within_super;  // how far in
        }
        break;
    default:
        fatal_error( state->efptr, state->my_rank, 0, NULL, 
                "Unknown test_type %d\n", params->test_type );
        offset = -1;
        break;
    }
    //fprintf( stderr, "%d: offset %ld\n", state->my_rank, offset );
    return offset;
}

int
read_write_buf( struct Parameters *params,
           struct State *state,
           struct time_values *times,
           off_t offset,
           char *buffer,
           int read_write ) 
{
    int ret;
    MPI_Status io_stat;
    MPI_Status *status;
    char *op_name = (read_write == WRITE_MODE) ? "write" : "read";
    //fprintf( stderr, "%d: %s at %lld\n", state->my_rank, op_name, offset );
    if ( params->posix_io ) {
        status = NULL;
    } else {
        status = &io_stat;
    }

        // now do the actual data sending
    if ( params->posix_io ) {
        if ( params->test_type == 1 ) {
            if ( read_write == WRITE_MODE ) {
                ret = Write( state->fd, buffer, params->obj_size );
            } else {
                ret = Read( state->fd, buffer, params->obj_size );
            }
        } else { // N-N
            if ( read_write == WRITE_MODE ) {
                ret = Pwrite64( state->fd, buffer, params->obj_size, offset ); 
            } else {
                ret = Pread64( state->fd, buffer, params->obj_size, offset ); 
            }
        }
    } else {
        if ( params->collective_flag == 1 ) {
            if ( read_write == WRITE_MODE ) {
                ret = MPI_File_write_at_all( 
                    state->mpi_fh, 
                    offset, 
                    buffer,
                    params->obj_size,
                    MPI_CHAR,
                    &io_stat );
            } else {
                ret = MPI_File_read_at_all( 
                    state->mpi_fh, 
                    offset, 
                    buffer,
                    params->obj_size,
                    MPI_CHAR,
                    &io_stat );
            }
        } else {
#ifndef ROMIO
            MPI_Request request;
#else
            MPIO_Request request;
#endif
            double wait_begin;
                
                // seek
            ret = MPI_File_seek( state->mpi_fh, offset, MPI_SEEK_SET );
            if ( ret != MPI_SUCCESS ) {
                fatal_error( state->efptr, state->my_rank, ret, NULL,
                        "Unable to seek for %s\n", op_name );
            }

                // iwrite, then wait
            if ( read_write == WRITE_MODE ) {
                ret = MPI_File_iwrite( 
                    state->mpi_fh,
                    buffer,
                    params->obj_size,
                    MPI_CHAR,
                    &request );
            } else {
                ret = MPI_File_iread( 
                    state->mpi_fh,
                    buffer,
                    params->obj_size,
                    MPI_CHAR,
                    &request );
            }
            if ( ret != MPI_SUCCESS ) {
                fatal_error( state->efptr, state->my_rank, ret, NULL,
                        "MPI_File_i%s.", op_name );
            }
            wait_begin = MPI_Wtime();
            ret = MPIO_Wait( &request, &io_stat );
            setTime( &(times->file_op_wait_time ), 
                    times->file_op_wait_time,
                    MPI_Wtime(),  wait_begin,
                    "file_op_wait_time ", 
                    __FILE__, __LINE__ , 
                    state->efptr, state->my_rank );
        }
    }

        // check for any errors
    if ( ( params->posix_io && ret != params->obj_size ) ||
         (!params->posix_io && ret != MPI_SUCCESS ) )
    {
        fatal_error( state->efptr, state->my_rank, ret, status,
                "%s:%d %s posix is %d, ret %d, offset %lld, obj_size %lld\n",
                __FUNCTION__,
                __LINE__,
                op_name,
                params->posix_io,
                ret,
                offset,
                params->obj_size );
    }

    return ret;
}

void
Usage( struct Parameters *params, struct State *state, char *which_field,
       char *missing ) 
{
    if ( state->my_rank == 0 ) {
        fprintf( stderr, "Usage: %s\n", prog_name );
        struct myoption *opt = mylongopts;
        int field_found = 0;
        while( opt->name ) {
            if ( which_field ) {
                if ( ! strcmp( which_field, opt->name ) ) {
                    field_found = 1;
                    fprintf( stderr, "--%s\n" 
                                     "\t%s\n",
                                     opt->name,
                                     opt->help );
                    break;
                }
            } else {
                fprintf( stderr, "\t--%s", opt->name );
                if ( opt->has_arg == required_argument ) {
                    fprintf( stderr, " arg" );
                } else if ( opt->has_arg == optional_argument ) {
                    fprintf( stderr, " [arg]" );
                }
                fprintf( stderr, "\n" );
            }
            opt++;
        }
        if ( which_field && !field_found ) {
            fprintf( stderr, "%s is not a valid option to %s\n", 
                which_field, prog_name );
        }
        if ( missing ) {
            fprintf( stderr, "ERROR: Did not specify required field %s.\n",
                    missing );
        }
    }
    fini( params, state);
}

void
barrier( struct State *state, struct time_values *times ) 
{
    double barrier_start;
    if ( times ) {
        barrier_start = MPI_Wtime();
    }
    int mpi_ret;
    if ( (mpi_ret = MPI_Barrier(MPI_COMM_WORLD)) != MPI_SUCCESS ) {
        fatal_error( state->efptr, state->my_rank, mpi_ret, NULL,
                "Unable to call MPI_Barrier before write.\n" );
    }
    if ( times ) {
        setTime( &(times->barrier_wait_time), times->barrier_wait_time,
            MPI_Wtime(), barrier_start,
            "barrier_wait_time", __FILE__, __LINE__, 
            state->efptr, state->my_rank );
    }
}

void
conditional_barrier( struct State *state, struct time_values *times,
        char *full_barriers, char *this_barrier )
{
    if ( full_barriers && strstr( full_barriers, this_barrier ) ) {
        /*
        if ( state->my_rank==0 ) {
            fprintf( state->efptr, "Doing barrier for %s\n", this_barrier );
        }
        */
        barrier( state, times );
    }
}

int
close_file( struct Parameters *params,
            struct time_values *times,
            struct State *state,
            double begin_time,
            int read_write,
            char *target )
{
    double wait_start = MPI_Wtime(); 
    int mpi_ret;

        // sync the file
    if(read_write == WRITE_MODE && params->sync_flag){
        wait_start = MPI_Wtime();
        if ( params->posix_io ) {
            mpi_ret = fsync( state->fd );
        } else {
            mpi_ret = MPI_File_sync( state->mpi_fh );
        }
        if ( (params->posix_io && mpi_ret) || 
             (!params->posix_io && mpi_ret != MPI_SUCCESS ) ) 
        {
            fatal_error( state->efptr, state->my_rank, 
                    mpi_ret, NULL, 
                    "sync returned unsuccessfully.\n" );
        }
        setTime( &(times->file_sync_wait_time ), 0,   MPI_Wtime() ,  
                wait_start,
                "file_sync_wait_time ", __FILE__, __LINE__ , 
                state->efptr, state->my_rank );
    }

        // close the file
    wait_start = MPI_Wtime();
    if ( params->posix_io ) {
        mpi_ret = close( state->fd );
    } else {
        mpi_ret = MPI_File_close( &(state->mpi_fh) );
    }
    if ( (params->posix_io && mpi_ret) || 
         (!params->posix_io && mpi_ret != MPI_SUCCESS ) ) 
    {
        fatal_error( state->efptr, state->my_rank, mpi_ret, NULL, 
                     "file close failed.\n" );
    }
    setTime( &(times->file_close_wait_time), 0,   MPI_Wtime() ,  
            wait_start,
            "file_close_wait_time ", __FILE__, __LINE__ , 
            state->efptr, state->my_rank );

    //  set final time 
    setTime( &(times->total_time ), 0,  MPI_Wtime(),  begin_time,
            "total_time ", __FILE__, __LINE__ , 
            state->efptr, state->my_rank );

        // have we been asked to delete the file?
    if( params->delete_flag &&
            ( read_write == READ_MODE || params->write_only_flag ) ) 
    {
        //fprintf( stderr, "%d Trying to unlink file %s now (%d %d %d %d)\n", 
        //        state->my_rank, target,
        //        params->delete_flag, read_write, params->write_only_flag, 
        //        params->test_type );
        // who should delete?  only 0 for n-1, everybody for n-n.
        if ( (params->test_type == 2 || params->posix_io ) ) {
            // need to do a barrier for POSIX N-1 so that 0 doesn't delete
            // before everyone else done.  panfs doesn't preserve posix
            // unlink semantics across multiple clients
            // TODO: probably we need to add bclose to the barriers flag
            barrier( state, times );
        }
        if( (params->test_type == 1 || state->my_rank == 0) ){
            int error = 0;
            mpi_ret = MPI_SUCCESS;
            if ( params->posix_io ) { 
                while( 1 ) {
                    if ( ( mpi_ret = unlink( target ) ) != 0 ) {
                        perror( "perror unlink" );
                        if ( errno == EAGAIN ) {
                            fprintf(state->efptr, "Got EAGAIN for unlink...\n");
                            sleep( 2 );
                        } else {
                            error = 1;
                            break;
                        }
                    } else {
                        error = 0;
                        break;
                    }
                }
            } else {
                //fprintf( stderr, "Trying to delete file now\n" );
                mpi_ret = MPI_File_delete(target, params->info);
                if( mpi_ret != MPI_SUCCESS){
                    error = 1;
                }
            }
            if ( error ) {
                fatal_error( state->efptr, state->my_rank, mpi_ret, NULL, 
                    "Unable to remove file %s.\n", target );
            }
		}
    }

    return mpi_ret;
}

int 
shift_rank( int rank, int np ) {
    int shift = np / 2;
    int newrank = ( rank + shift ) % np; 
    //fprintf( stderr, "Shifting rank from %d to %d\n", rank, newrank );
    return newrank;
}

int
is_exit_condition(  int num_ios, 
                    struct Parameters *params, 
                    struct State *state,
                    double begin_time,
                    int read_write ) 
{
        // if it's read mode, we always just read the quantity of objs written
    if ( read_write == READ_MODE ) {
        if ( params->time_limit ) {
            if ( params->shift_flag ) {
                // here is how we know how much *this* rank wrote.  we
                // shifted our rank so we can't just use our own variable
                // for how much we wrote, instead we have to use what the
                // other one did
                return ( num_ios >= state->objs_written_all[state->my_rank] );
            } else {
                    // objs_written_all only populated when shifting
                return ( num_ios >= state->objs_written );
            }
        }
        return ( num_ios >= params->num_objs );
    }

        // if it's write mode, we write either a specified number of objects
        // or we write for a specified time limit
    if ( params->time_limit ) {
        return ( MPI_Wtime() - begin_time >= params->time_limit );
    } else {
        return ( num_ios >= params->num_objs );
    }
}

void
compute_amount_written( struct Parameters *params, struct State *state ) {
    double total_objs = 0.0;
    double max_d, min_d;
    int min_ndx = 0, max_ndx = 0;
    int mpi_ret;
    if( (mpi_ret = get_min_sum_max(state->my_rank, state->objs_written, &min_d, 
                    &min_ndx, &total_objs, &max_d, &max_ndx, 
                    "finding total_objs", state->ofptr, state->efptr)) 
            != MPI_SUCCESS)
    {
        fatal_error( state->efptr, state->my_rank, mpi_ret, NULL, 
                "problem finding total_objs" ); 
    }
    // now total_mbs = total number of objs
    state->min_objs  = (unsigned)min_d;
    state->max_objs  = (unsigned)max_d;
    state->ave_objs  = (unsigned)(total_objs / params->num_procs_world);
    state->total_mbs = (total_objs * params->obj_size)/(1024.0*1024.0);
    if ( state->my_rank == 0 ) {
        fprintf( state->efptr, "%d my objs %d, min objs %.0f, max objs %.0f, "
                         "total_mbs = %f\n", 
            state->my_rank, (int)state->objs_written, 
            min_d, max_d,
            state->total_mbs );
    }

    // everybody needs to know how much everybody else wrote if we do a 
    // shift so that everybody can read the correct amount.  actually,
    // we only *need* to know for whoever was our old rank but easier to
    // do an all-al, 
    if ( params->shift_flag ) {
        state->objs_written_all = (unsigned*)
                    Malloc(sizeof(unsigned)*params->num_procs_world,
                            state->efptr, state->my_rank);
        state->objs_written_all[0] = state->objs_written;
        mpi_ret = MPI_Allgather( (void*)&(state->objs_written), 1, MPI_UNSIGNED,
                (void*)state->objs_written_all, 
                1, MPI_UNSIGNED, MPI_COMM_WORLD );
        if ( state->my_rank == 0 ) {  // debugging in here
            int i;
            fprintf( stderr, "Array of objs written: " );
            for( i = 0; i < params->num_procs_world; i++ ) {
                fprintf( stderr, "%d ", state->objs_written_all[i] );
            }
            fprintf( stderr, "\n" );
        }
    }
}

char *
make_str_copy( char *orig, int length, struct State *state ) {
    char *new_str = (char *)Malloc( length, state->efptr, state->my_rank );
    strncpy( new_str, orig, length );
    return new_str;
}

void
catch_gdb() {}

int
test_set_char( char correct_char, char *location, int read_write, 
        off_t offset, struct State *state, struct read_error **err_tail ) 
{
    struct read_error *err = NULL;

    if ( read_write == WRITE_MODE ) {
        *location = correct_char;
        return 0;
    }

        // read_mode check for error, add error to struct
    if ( correct_char != *location ) {
        if ( *err_tail && 
                (*err_tail)->file_offset + (*err_tail)->length == offset ) 
        { 
            // just add this error to the previous one
            err = *err_tail;
            // don't grow, the error string is plenty long enough
            /*
            if ( err->length + 1 >= err->str_length ) { // grow strings
                char *tmp;
                err->str_length *= 2;
                tmp = make_str_copy( err->expected, err->str_length, state );
                free( err->expected );
                err->expected = tmp;
                tmp = make_str_copy( err->received, err->str_length, state );
                free( err->received );
                err->received = tmp;
            }
            */
        } else {
            // here's where we need to construct an error
            err = (struct read_error*)Malloc( sizeof( struct read_error ), 
                                            state->efptr, state->my_rank );
            err->next         = NULL;
            err->prev         = *err_tail;
            (*err_tail)->next = err;
            *err_tail         = err;    // move the tail
            err->expected     = (char*)Malloc(64,state->efptr,state->my_rank);
            err->received     = (char*)Malloc(64,state->efptr,state->my_rank);
            err->str_length   = 64;
            err->file_offset  = offset;
            err->length       = 0;
            err->all_zeros    = 1;
        }
            // now print expected and received into the strings
            // sprintf won't allow a char at the last position. 
            // only record the first so many bytes . . . no need for all of them
        if ( err->length + 1 < err->str_length ) {
            sprintf( &(err->expected[err->length]), "%c", correct_char );
            sprintf( &(err->received[err->length]), "%c", *location );
        }
        err->length++;
        if ( (int)*location != 0 ) {
            err->all_zeros = 0;
        }
        return 1;
    } else {
        return 0;
    }
}

int
test_set_buf( char *buffer, int blocksize, struct State *state,
        int which_block, int pagesize, int touch, 
        int check, int read_write, 
        off_t file_offset, struct read_error **err_tail ) 
{
    int j = 0, i = 0, errors = 0;
    char correct_char;
    off_t buf_offset; 
    int rank = state->my_rank;

    int descent_level;
    if ( read_write == INIT_MODE ) {
            // set it up to do the initial fill of nothing but rank
        descent_level = 3;
        read_write = WRITE_MODE;
    } else {
        descent_level = ( read_write == WRITE_MODE ? touch : check );
    }

    // touch == 0: do nothing
    // touch == 1: set the first char in each block
    // touch == 2: set the first char in each page
    // touch == 3: set every char in the buffer 
    // check == N; check correspondingly to touch option 
    if ( descent_level >= 1 ) { 
        if ( touch >= 1 ) {
            correct_char = printable_char( file_offset + rank ); 
        } else {
            correct_char = printable_char( rank );
        }
        errors += test_set_char(  correct_char, 
                                    &(buffer[0]), 
                                    read_write, 
                                    file_offset,
                                    state,
                                    err_tail );
        if ( descent_level >= 2 ) { 
            for( i = 0; i < blocksize / pagesize; i++ ) {
                buf_offset = pagesize*i;
                if ( touch >= 2 ) {
                    correct_char = printable_char( 
                            file_offset + buf_offset + rank);
                } else {
                    correct_char = printable_char( rank );
                }
                // don't check very first one, it was already check by touch==1
                if ( i > 0 ) {
                    errors += test_set_char(  correct_char, 
                                            &(buffer[buf_offset]), 
                                            read_write,
                                            file_offset + buf_offset,
                                            state,
                                            err_tail );
                }
                if ( descent_level == 3 ) { 
                    for( j = 1; j < pagesize; j++ ) { // j=0 already set/tested
                        buf_offset = pagesize*i + j;
                        if ( touch == 3 ) {
                            correct_char = printable_char( file_offset 
                                                        + buf_offset + rank );
                        } else {
                            correct_char = printable_char( rank );
                        }
                        errors += test_set_char(  correct_char,
                                                    &(buffer[buf_offset]), 
                                                    read_write, 
                                                    file_offset + buf_offset,
                                                    state,
                                                    err_tail );
                    }
                }
            }
        }
    }

    return errors;
}

void
report_errs( struct State *state, struct read_error *head ) {
    for( ; head != NULL; head = head->next ) {
        if ( head->all_zeros ) {
            warning_msg( state->efptr, state->my_rank, 0, NULL,
                "%ld bad bytes at file offset %ld.  Nothing but zeroes.\n",
                head->length,
                head->file_offset );
        } else  {
            warning_msg( state->efptr, state->my_rank, 0, NULL,
                "%ld bad bytes at file offset %ld.  Expected %40s, received %40s\n",
                head->length,
                head->file_offset,
                head->expected,
                head->received );
        }
    }
}

int
read_write_file( struct Parameters *params,
            struct time_values *times,
            struct State *state,
            int read_write ) 
{
    int mpi_ret;
    double begin_time, io_begin, check_buf_time, verify_begin;
    int i;
    int errs = 0;
    int orig_rank       = state->my_rank;  // save in case we shift
    char *buffer        = NULL;
    char *op_name = (read_write == WRITE_MODE) ? "Write" : "Read";
    char *target;
    check_buf_time = 0.0;
    struct read_error head;
    struct read_error *tail = &head;
    head.next = NULL;

        // shift the ranks to avoid read caching if specified
    if ( params->shift_flag && read_write == READ_MODE ) {
        orig_rank      = state->my_rank;
        state->my_rank = shift_rank( state->my_rank, params->num_procs_world );
    }

        // create the file path
    target = expand_path( params->tfname, state->my_rank, params->test_time );

        // allocate the buffer
    //fprintf( stderr, "%d in mode %s for %d %s of %s\n", 
    //      state->my_rank, op_name, params->num_objs, op_name, target );
    buffer        = Malloc( params->obj_size, state->efptr, state->my_rank );
    memset( buffer, 0, params->obj_size );
    test_set_buf( buffer, params->obj_size, state, i, state->pagesize, 
            params->touch, params->check_data_ndx, INIT_MODE, 0, NULL );

        // start the clock and open the file
    begin_time = MPI_Wtime();
    conditional_barrier( state, times, params->barriers, "bopen" );
    mpi_ret    = open_file(params, times, state, read_write, begin_time,target);
    conditional_barrier( state, times, params->barriers, "aopen" );

        // for each specified I/O, get offset and send/recv data
    io_begin   = MPI_Wtime();
    for( i = 0; !is_exit_condition(i,params,state,begin_time,read_write); i++ ){
        off_t offset = get_offset( params, state, i );    
        if ( read_write == WRITE_MODE ) {
            test_set_buf( buffer, params->obj_size, state, i, 
                    state->pagesize, params->touch, 
                    params->check_data_ndx, read_write, offset, NULL );
            conditional_barrier( state, times, params->barriers, "bwrite" );
        } else {
            conditional_barrier( state, times, params->barriers, "bread" );
        }
        read_write_buf( params, state, times, offset, buffer, read_write );
        if ( read_write == READ_MODE ) {
            verify_begin = MPI_Wtime();
            errs += test_set_buf( buffer, params->obj_size, state, i, 
                    state->pagesize, params->touch, 
                    params->check_data_ndx, read_write, offset, &tail );
            check_buf_time += (MPI_Wtime() - verify_begin);
            conditional_barrier( state, times, params->barriers, "aread" );
        } else {
            state->objs_written++;
            conditional_barrier( state, times, params->barriers, "awrite" );
        }
    }

    setTime( &(times->total_op_time ), 0,  MPI_Wtime(),  
            io_begin + check_buf_time,
            "total_op_time ", 
            __FILE__, __LINE__ , state->efptr, state->my_rank ); 

        
        // check hints, close the file, do barrier, print stats
    if ( read_write == WRITE_MODE ) {
        check_hints( params, state );
    }
    //begin_time = MPI_Wtime(); // don't reset this, this is for effective
    //fprintf( stderr, "closing file in %s mode\n", op_name );
    conditional_barrier( state, times, params->barriers, "bclose" );
    mpi_ret = close_file( params, times, state, begin_time, read_write, target);
    conditional_barrier( state, times, params->barriers, "aclose" );

        // if we're in time_limit mode, we need to compute explicitly how
        // much we wrote, so the read phase knows how much to read
    if ( read_write == WRITE_MODE && params->time_limit ) {
        compute_amount_written( params, state );
    }

        // unshift the rank
    state->my_rank = orig_rank;
    if(collect_and_print_time(state->my_rank, times, params, state, 
                op_name, state->ofptr, state->efptr) != MPI_SUCCESS)
    {
        fatal_error( state->efptr, state->my_rank, 0, NULL, 
                    "collect_and_print_time routine.\n" );
    }

        // now report any read errors
    if ( errs ) {
        assert( read_write == READ_MODE );
        assert( head.next  != NULL );
        report_errs( state, head.next );
    }
    
    return errs ? errs : mpi_ret;
}
