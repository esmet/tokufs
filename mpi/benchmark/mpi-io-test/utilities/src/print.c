 /******************************************************************
* Copyright (c) 2003
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

/******************************************************************
* PROGRAM:   print
*
* PURPOSE:   Routines that deal with program output, 
*
* AUTHOR:    James Nunez 
* DATE:      February 12, 2001
* LAST MODIFIED: September 7, 2005 [jnunez]
* VERSION:   1.00.011
*
*      LOS ALAMOS NATIONAL LABORATORY
*      An Affirmative Action/Equal Opportunity Employer
*
******************************************************************/

#include "print.h"
#include "utilities.h"

#include <string.h>
#include <stdlib.h>

/*******************************************
 *   ROUTINE: show_usage
 *   PURPOSE: Print input parameters for routine
 *   DATE:    February 19,2004
 *   LAST MODIFIED: September 7, 2005
 *******************************************/
void show_usage(int fullhelp){
  printf("mpi_io_test:\n");
  printf("Gather timing information for reading from and writing to\n"
	 "file using N processors to N files, one file, M processors to M\n"
	 "files, or M processors to one file.\n\n");
  if(fullhelp){
    printf("-type #             Type of data movement:\n"
	   "                     (1) N processors to N files\n"
	   "                     (2) N processors to 1 file\n"
	   "                     (3) N processors to M processors to M files\n"
	   "                     (4) N processors to M processors to 1 file\n"
	   "                     (5) N processors to M files\n");
    printf("-collective         Use collective read and write calls \n"
	   "                    (MPI_File_read/write_at_all) instead of \n"
	   "                    independent calls (MPI_File_iread/iwrite). \n"
	   "                    (Default: Use independent read/write calls)\n");
    printf("-num_io \"# # ... #\" Total number of I/O processors. If using the\n"
	   "                    -host flag, this is the number of I/O\n"
	   "                    processors on each host. For type 5, this is\n"
	   "                    the number of files to write to.\n"
	   "                    (Default: all N processors will do I/O)\n");
    printf("-nobj  #            Number of objects to write to file or to send\n"
	   "                    to each of the M I/O processors\n"
	   "                    (Default: 1)\n");
    printf("-strided #         All processors write in a strided, all \n"
	   "                   processors write to one region of the file and\n"
	   "                   then all seek to the next portion of the file,\n"
	   "                   or non-strided, all processors have a region \n"
	   "                   of the file that they exclusively write into,\n"
	   "                   pattern.\n"
	   "                        0. Non-strided (Default)\n"
	   "                        1. Strided - not implemented for use\n"
	   "                           with aggregation (-type 3 and 4)\n");
    printf(" -size  #           Number of bytes that each object will send\n"
	   "                       to the I/O processors or write to file.\n"
	   "                       Must be a multiple of sizeof(double).\n"
	   "                       Allows for (Linux) dd-like expressions; w 2, \n"
	   "                       b 512, KB 1000, K 1024, MB 1,000,000,  \n"
	   "                       M 1,048,576, GB 1,000,000,000, and \n"
	   "                       G 1,073,741,824.  A positive number followed by\n"
	   "                       one of the above letter will result in the two \n"
	   "                       multiplied, i.e. 2b => 1024.\n");
    printf("-csize #            Number of bytes in each I/O processors\n"
	   "                    circular buffers\n");
    printf("-target %%          Full path and file name to write and read test\n"
	   "                    data. The following in the file name or path\n"
	   "                    will resolve to:\n"
	   "                      %%r processor rank\n"
	   "                      %%h host name\n"
	   "                      %%p processor ID\n"
	   "                    (Default: ./test_file.out)\n");
    printf("-targets %%         File name of file containing one file name\n"
	   "                   for each processor. File should have one file\n"
	   "                   name on each line and an IO processor with IO\n"
	   "                   rank N will read the file name from line N+1.\n"
	   "                   The special characters for -target flag are \n"
	   "                   valid.\n"
	   "                   Test type 5 must use this flag and the first M\n"
	   "                   (-num_io) file names will be read from the \n"
	   "                   targets file.\n");
    printf("-deletefile        Causes the target file to be deleted at the \n"
	   "                    end of the program. \n"
	   "                    (Default: FALSE)\n");
    printf("-touch #           All data objects a processors sends/writes\n"
	   "                    will contain the (double) values processor \n"
	   "                    rank to size+processor rank. Some of these\n"
	   "                    values can be overwritten by the following: \n"
	   "                    the file offset will overwrite the rank \n"
	   "                    (first element):\n"
	   "                     (1) Never (default)\n"
	   "                     (2) Once per object\n"
	   "                     (3) Once per page per object\n");
    printf("-barrier            Call MPI_Barrier before each write\n"
	   "                    (Default: FALSE - do not call MPI_Barrier before each write)\n");
    printf("-sync              Sync the data before file close on writes\n"
	   "                    (Default: FALSE - do not sync data)\n");
    printf("-sleep #           Each processor will \"sleep\" for this number of\n"
	   "                    seconds between when the file is closed from \n"
	   "                    writing and opened to read.\n"
	   "                    (Default: 0 seconds)\n");
    printf("-chkdata #         When reading the data file, check that every \n"
	   "                    element equals the expected value, which \n"
	   "                    depends on the \"touch\" option. \n"
	   "                    Check the following data elements:\n"
	   "                    (0) None\n"
	   "                    (1) First data element of each object (Default)\n"
	   "                    (2) First two data elements of each object\n"
	   "                    (3) All data elements\n"
	   "                    Checking data will decrease the read bandwidth.\n");
    printf("-preallocate #      Preallocate this number of bytes before write\n");
    printf("-ahead #            Number of circular buffers for asynchronous I/O\n"
	   "                    (Default: 0)\n");
    printf("-host  \"%% %% ... %%\" List of host names to do I/O\n"
	   "                    (Default: all hosts)\n");
    printf("-lhosts             Print out all hosts running this program.\n");
    printf("-dio                (optional) Use direct read and write for all\n"
	   "                    read and write operations. This is a ROMIO\n"
	   "                    specific hint(\"direct_read\" and \"direct_write\")\n");
    printf("-hints %%s %%s ... %%s %%s Multiple MPI-IO key and value hint pairs. Any \n"
	   "                       MPI, ROMIO or file system specific hint can be \n"
	   "                       specified here.  \n");
    printf("-op %%              (Optional) Only read or write the target file.\n"
	   "                    1. \"read\" - only read the target files\n"
	   "                    2.\"write\" - only write out the target files\n");
    printf("-nofile             Only allocate memory and send message, do\n"
	   "                    not write the messages to file.\n");
    printf("-norsend            In normal operation, all processors send data\n"
	   "                    to an IO processor. Even IO processors send \n"
	   "                    data to themselves. This flag indicates that \n"
	   "                    IO processors do not send data to themselves.\n");
    printf("-output %%           File to write timing results, input parameters,\n"
	   "                    and MPI environment variables.\n"
	   "                    (Default: stdout)\n");
    printf("-errout %%           File to write all errors encountered while\n"
	   "                    running the program.\n"
	   "                    (Default: stderr)\n");
    printf("-verbose           Print all times for each processes\n"
	   "                           (Default: off)\n");
    printf("-help               (optional) Display the input options\n\n");
  }
}

/*******************************************
 *   ROUTINE: 
 *   PURPOSE: 
 *******************************************/
int print_parm(char *msg, int my_rank, int num_procs, MPI_Comm comm, 
	       FILE *output, FILE *error)
{
  int          i; 
  char        *new_msg;
  MPI_Status   recv_stat; 

  if(msg == NULL)
    return -1;

  if((new_msg = (char *) malloc((int)(strlen(msg) + 1)*sizeof(char))) == NULL){
    return -1;
  }

/***************************************************************************
* All processors send message to processor 0. Processor 0 will always print 
* its message and then print all other messages, with rank, that does not 
* match its own.
*****************************************************************************/
  if (my_rank != 0) {
    if(MPI_Send(msg, (int)(strlen(msg) + 1), MPI_CHAR, 0, my_rank, comm) != 
       MPI_SUCCESS) {
      fprintf(error, "[RANK: %d] ERROR: Unable to send message to processor 0.\n", my_rank);
      return -1;
    } 

  } 
  else {
    fprintf(output, "%s\n", msg);
    for (i = 1; i < num_procs; i++) {
      
      if(MPI_Recv(new_msg,  (int)(strlen(msg) + 1), MPI_CHAR, i, i, 
		  comm, &recv_stat) != MPI_SUCCESS) {
	fprintf(error, "%d: main (MPI_Recv) failed.", my_rank);
	return -1;
      }
      
      if (strcmp(msg, new_msg))
	fprintf(output, "RANK %d: %s.\n", i, new_msg);
    }
  }
  
  free(new_msg);
  return 1;
}

/*******************************************
 *   ROUTINE: print_input_environment
 *   PURPOSE: 
 *   LAST MODIFIED: August 20, 2005 [jnunez]
 *******************************************/
int 
print_input_environment(        int my_rank, 
                                int myhost_num, 
                                char *my_host, 
                                struct Parameters *params,
                                FILE *ofptr, 
                                FILE *efptr)
{
  int i = 0;
  int mpi_ret;

  extern char **environ;

/******************************************************************
* Print input parameters
******************************************************************/
  if(my_rank == 0){
    fprintf(ofptr, "Input Parameters:\n");
    if(params->test_type == 1){
      fprintf(ofptr, "I/O Testing type N -> N: %d\n", params->test_type);
    }
    else if(params->test_type == 2){
      fprintf(ofptr, "I/O Testing type N -> 1: %d\n", params->test_type);
    }
    else if(params->test_type == 3){
      fprintf(ofptr, "I/O Testing type N -> M -> M: %d\n", params->test_type);
    }
    else if(params->test_type == 4){
      fprintf(ofptr, "I/O Testing type N -> M -> 1: %d\n", params->test_type);
    }
    else if(params->test_type == 5){
      fprintf(ofptr, "I/O Testing type N -> M: %d\n", params->test_type);
    }
    fprintf(ofptr, "Total number of processors (N): %d\n", 
            params->num_procs_world);
    fprintf(ofptr, "Total number of I/O processors (M): %d\n", 
            params->num_procs_io);
    if(params->aggregate_flag){
      fprintf(ofptr, "Number of I/O processors per host: %d", 
              params->io_procs_list[0]);
      for(i=1; i < params->num_hosts; i++)  {
        fprintf(ofptr, " %d", params->io_procs_list[i]);
      }
      fprintf(ofptr, "\n");
    }
    
    if(params->num_hosts > 0){
      fprintf(ofptr, "Host Name List: %s", params->host_io_list[0]);
      for(i=1; i < params->num_hosts; i++) {
        fprintf(ofptr, " %s", params->host_io_list[i]);
      }
      fprintf(ofptr, "\n");
    } else{
      fprintf(ofptr, "Host Name List: None\n");
    }
    
    fprintf(ofptr, "Number of circular buffers (-ahead): %d\n", params->ahead);
    fprintf(ofptr, "Number of objects (-nobj): %d\n", (int)params->num_objs);
    fprintf(ofptr, "Number of bytes in each object (-size): %lld\n", 
	    (long long)params->obj_size);
    fprintf(ofptr, "Circular buffer size (-csize): %lld\n", 
            (long long)params->cbuf_size);
    fprintf(ofptr, "Preallocate file size (-preallocate): %lld\n", 
	    (long long)params->prealloc_size);
    fprintf(ofptr, "Check data read (-chkdata): %d\n", params->check_data_ndx);
    fprintf(ofptr, "Use collective read/write calls (-collective): %d\n", 
	    params->collective_flag);
    fprintf(ofptr, "Call MPI_Barrier before each write (-barrier): %d\n", 
            params->barrier_flag);
    fprintf(ofptr, "Flush data before file close (-sync): %d\n", 
            params->sync_flag);
    fprintf(ofptr, "Strided data layout (-strided): %d\n", 
            params->strided_flag);
    fprintf(ofptr, "Delete data file when done reading (-deletefile): %d\n", 
	        params->delete_flag);
    fprintf(ofptr, "Write output data to file (-nofile): %d\n",
	        params->write_file_flag);
    fprintf(ofptr, "Aggregators send data to themselves (-norsend): %d\n",
	        params->io_proc_send_flag);
    fflush(ofptr);
    if(params->targets_flag) {
        fprintf(ofptr, "File containing target files (-targets): %s\n",
              params->tgtsfname);
    } else {
        fprintf(ofptr, "Test data written to (-target): %s\n", params->tfname);
    }
    fprintf(ofptr, "Touch object option (-touch): %d\n", params->touch);
    fprintf(ofptr, "MPI_WTIME_IS_GLOBAL: %d\n", MPI_WTIME_IS_GLOBAL);
    fprintf(ofptr, "Timing results written to (-output): %s\n", 
              ( params->ofname ? params->ofname : "stdout" ) );
    fprintf(ofptr, "Errors and warnings written to (-errout): %s\n",
            (params->efname ? params->efname : "stderr" ) );

    fflush(ofptr);
  }
 
/******************************************************************
* Print out MPI environment variables
******************************************************************/
  if (my_rank == 0) {
    fflush(ofptr);
    fprintf(ofptr, "\nEnvironment Variables (MPI):\n");
    for (i = 0; environ[i] != NULL; i++){
      if (strstr(environ[i], "MPI") != NULL) {
	fprintf(ofptr, "%s\n", environ[i]);
      }
    }
    fprintf(ofptr, "\n");
    fflush(ofptr);
  }
  
/******************************************************************
* If necessary, print all hosts
******************************************************************/
  if(params->list_host_flag){
    if(!params->init_flag)
      if( (mpi_ret 
			  = MPI_Get_processor_name(my_host, &myhost_num) ) != MPI_SUCCESS){
	if(my_rank == 0)
          fatal_error( efptr, my_rank, mpi_ret, NULL,
                  "Unable to get proc name from MPI_Get_processor_name.\n" );
	MPI_Finalize();
	return -1;	      
      }
    
    if (my_rank == 0){
      fprintf(ofptr, "\nHosts List:\n");
      fprintf(ofptr, "RANK %d: %s\n", my_rank, my_host);
      fflush(ofptr);
      MPI_Barrier(MPI_COMM_WORLD);
    }      
    else{
      MPI_Barrier(MPI_COMM_WORLD);
      fprintf(ofptr, "RANK %d: %s\n", my_rank, my_host);
      fflush(ofptr);
    }
  }


  MPI_Barrier(MPI_COMM_WORLD);

  return 1;
}

int
collect_and_print_single_time( 
        int     my_rank,
        int     num_procs,
        double  total_mbs,
        double  time_value,
        char    *op,
        char    *bandwidth_type,    // NULL to not print bandwidth
        char    *time_type,         // NULL to not print time
        int     verbose_flag,
        int     trenddata,
        FILE    *ofptr, 
        FILE    *efptr)
{
    double min_time, sum_time, max_time;
    int    min_ndx,  max_ndx;
	int mpi_ret;

    if( (mpi_ret = get_min_sum_max(my_rank, time_value, &min_time, &min_ndx,
		      &sum_time, &max_time, &max_ndx, op, ofptr, efptr)) != MPI_SUCCESS)
    {
            fatal_error( efptr, my_rank, mpi_ret, NULL,
					"Problem computing min, max, and sum "
                       "of %s time.\n", op );
    }
  
    if ( my_rank == 0 ) {
        if ( bandwidth_type ) {
            fprintf(ofptr, "=== %s Bandwidth (min [rank] "
                           "avg max [rank]): "
                           "%e [%d] %e %e [%d] Mbytes/s.\n", 
                           bandwidth_type,
                           total_mbs/max_time, 
                           max_ndx,
                           (num_procs*total_mbs)/sum_time,
                           total_mbs/min_time, 
                           min_ndx);
            if(trenddata){

               if(!strcmp(bandwidth_type,op)){
                  fprintf(ofptr," <td>  %s_Bandwidth_min %e MB/s\n",bandwidth_type,            total_mbs/max_time);
                  fprintf(ofptr," <td>  %s_Bandwidth_ave %e MB/s\n",bandwidth_type,(num_procs*total_mbs)/sum_time);
                  fprintf(ofptr," <td>  %s_Bandwidth_max %e MB/s\n",bandwidth_type,            total_mbs/min_time);
               }
               else{
                  fprintf(ofptr," <td>  %s_%s_Bandwidth_min %e MB/s\n",bandwidth_type,op,            total_mbs/max_time);
                  fprintf(ofptr," <td>  %s_%s_Bandwidth_ave %e MB/s\n",bandwidth_type,op,(num_procs*total_mbs)/sum_time);
                  fprintf(ofptr," <td>  %s_%s_Bandwidth_max %e MB/s\n",bandwidth_type,op,            total_mbs/min_time);
               }
            }
        }
        if ( time_type ) {
            fprintf(ofptr, "=== %s Time (min [rank] avg max [rank]): "
                           "%e [%d] %e %e [%d] sec.\n", 
                           time_type,
                           min_time, 
                           min_ndx, 
                           sum_time/num_procs, 
                           max_time, 
                           max_ndx);

            if(trenddata){

               if(!strcmp(time_type,op)){
                  fprintf(ofptr," <td>  %s_Time_min %e s\n",time_type,min_time          );
                  fprintf(ofptr," <td>  %s_Time_ave %e s\n",time_type,sum_time/num_procs);
                  fprintf(ofptr," <td>  %s_Time_max %e s\n",time_type,max_time          );
               }
               else{
                  fprintf(ofptr," <td>  %s_%s_Time_min %e s\n",time_type,op,min_time          );
                  fprintf(ofptr," <td>  %s_%s_Time_ave %e s\n",time_type,op,sum_time/num_procs);
                  fprintf(ofptr," <td>  %s_%s_Time_max %e s\n",time_type,op,max_time          );
               }
            }
        }
    }
  
    if(verbose_flag){
        fflush(ofptr);
        MPI_Barrier(MPI_COMM_WORLD);
    
        if ( bandwidth_type ) {
            fprintf(ofptr, "[%d] %s Bandwidth = %e\n", 
                            my_rank, bandwidth_type, total_mbs/time_value);

            if(trenddata){
                if(!strcmp(bandwidth_type,op)){
                    fprintf(ofptr, "<td> Proc_%d_%s_Bandwidth = %e MB/s\n", 
                                my_rank, bandwidth_type, total_mbs/time_value);
                }
                else{
                    fprintf(ofptr, "<td> Proc_%d_%s_%s_Bandwidth = %e MB/s\n", 
                                my_rank, op, bandwidth_type, total_mbs/time_value);
                }

            }
        }
        if ( time_type ) {
            fprintf(ofptr, "[%d] %s Time = %e\n", 
                            my_rank, time_type, time_value );
            if(trenddata){
               if(!strcmp(time_type,op)){
                   fprintf(ofptr, "<td> Proc_%d_%s_Time = %e s\n", 
                               my_rank, time_type, time_value );
               }
               else{
                   fprintf(ofptr, "<td> Proc_%d__%s_%s_Time = %e s\n", 
                               my_rank, op, time_type, time_value );
               }
            }
        }
    
        fflush(ofptr);
        MPI_Barrier(MPI_COMM_WORLD);
    }  
  
	return 0;
}


/*******************************************
 *   ROUTINE: collect_and_print_time
 *   PURPOSE: Collect and print minimum, average, and maximum of input 
 *            processor wait and file open and close times across all procs
 *   LAST MODIFIED: July 24, 2005 [jnunez]
 *******************************************/
int 
collect_and_print_time( int my_rank, 
                        struct time_values *times,
                        struct Parameters *params,
                        struct State *state,
                        char *op, 
			            FILE *ofptr, 
                        FILE *efptr)
{
    double total_mbs = 0.0; 
    char fancy_description[1024];

    int num_procs = params->num_procs_world;
    int num_io_procs = params->num_procs_io;
  
    if(!params->io_proc_send_flag) num_procs -= num_io_procs;

        // need to compute total_size_mb differently if we used a time
        // limit
    if ( params->time_limit ) {
        //fprintf( stderr, "Using %.0f for total_mbs\n", state->total_mbs );
        total_mbs = state->total_mbs;
    } else {
        total_mbs = ((double)(num_procs*params->num_objs*params->superblocks) 
              * (params->obj_size))/(1024.0*1024.0);
    }
    //fprintf( stderr, "%d using %.2f as total_mbs\n", my_rank, total_mbs );

/*******************************************************************
* Find and print minimum and maximum total elapsed time; file open, 
* write/read data, and file close time.
*******************************************************************/
    collect_and_print_single_time( my_rank, 
                                   num_procs,
                                   total_mbs,
                                   times->total_time, 
                                   op,
                                   "Effective",     // yes, print "Effective" BW
                                   "Total",         // yes, print "Total" time
                                   params->verbose_flag,
                                   params->trenddata,
                                   ofptr,
                                   efptr );
  
/*******************************************************************
* Find and print minimum and maximum time to write/read the data to file; this 
* does not include file open and close times.
*******************************************************************/
    collect_and_print_single_time( my_rank,
                                   num_procs,
                                   total_mbs,
                                   times->total_op_time,
                                   op,
                                   op,  // print op Bandwidth
                                   op,  // print op Time
                                   params->verbose_flag,
                                   params->trenddata,
                                   ofptr,
                                   efptr );

/*******************************************************************
* Find and print minimum and maximum time any processor took to open the 
* output file
*******************************************************************/
    snprintf( fancy_description, 1024, "MPI_File_%s_Open", op );
    collect_and_print_single_time( my_rank,
                                   num_procs,
                                   total_mbs,
                                   times->file_open_wait_time,
                                   fancy_description,
                                   NULL,               // don't print the BW
                                   fancy_description,  // print the time
                                   params->verbose_flag,
                                   params->trenddata,
                                   ofptr,
                                   efptr );

/*******************************************************************
* Find minimum and maximum time any processor waited to read/write to file
*******************************************************************/
    snprintf( fancy_description, 1024, "MPI_File_%s_Wait", op );
    collect_and_print_single_time( my_rank,
                                   num_procs,
                                   total_mbs,
                                   times->file_op_wait_time,
                                   fancy_description,
                                   NULL,                // don't print the BW
                                   fancy_description,   // print the time
                                   params->verbose_flag,
                                   params->trenddata,
                                   ofptr,
                                   efptr );

/*******************************************************************
* If necessary, find and print minimum and maximum time any 
* processor took to preallocate storage space for the output file. 
*******************************************************************/
    snprintf( fancy_description, 1024, "MPI_File_%s_Preallocate_time", op );
    collect_and_print_single_time( my_rank,
                                   num_procs,
                                   total_mbs,
                                   times->prealloc_wait_time,
                                   fancy_description,
                                   NULL,                        // no BW
                                   fancy_description,           // print the time
                                   params->verbose_flag,
                                   params->trenddata,
                                   ofptr,
                                   efptr );

/*******************************************************************
* If specified, find and print minimum and maximum total time time any 
* processor took at the barrier before each write.
*******************************************************************/
    if( params->barrier_flag ) {
        snprintf( fancy_description, 1024, "MPI_%s_Barrier_wait", op );
        collect_and_print_single_time( my_rank,
                                       num_procs,
                                       total_mbs,
                                       times->barrier_wait_time,
                                       fancy_description,
                                       NULL,                        // no BW
                                       fancy_description,           // print the time
                                       params->verbose_flag,
                                       params->trenddata,
                                       ofptr,
                                       efptr );
  }


/*******************************************************************
* If necessary, find and print minimum and maximum time any 
* processor took to sync data to the output file. We don't sync the file for 
* reads.
*******************************************************************/
    if( params->sync_flag ){
        snprintf( fancy_description, 1024, "MPI_%s_File_Sync", op );
        collect_and_print_single_time( my_rank,
                                       num_procs,
                                       total_mbs,
                                       times->file_sync_wait_time,
                                       fancy_description,
                                       NULL,                        // no BW
                                       fancy_description,           // print the time
                                       params->verbose_flag,
                                       params->trenddata,
                                       ofptr,
                                       efptr );
    }

/*******************************************************************
* Find and print minimum and maximum time any processor took to close the 
* output file
*******************************************************************/
    snprintf( fancy_description, 1024, "MPI_%s_File_Close_Wait", op );
    collect_and_print_single_time( my_rank,
                                   num_procs,
                                   total_mbs,
                                   times->file_close_wait_time,
                                   fancy_description,
                                   NULL,                        // no BW
                                   fancy_description,           // print the time
                                   params->verbose_flag,
                                   params->trenddata,
                                   ofptr,
                                   efptr );

/*******************************************************************
* If using aggregation, find and print the minimum, sum, and maximum time any 
* processor waited to send and receive data to another processor and wait to 
* write to file time.
*******************************************************************/
    if(params->aggregate_flag){
        snprintf( fancy_description, 1024, "MPI_%s_Processor_Send_Wait", op );
        collect_and_print_single_time( my_rank,
                                       num_procs,
                                       total_mbs,
                                       times->proc_send_wait_time,
                                       fancy_description,
                                       NULL,                      // no BW
                                       fancy_description,         // print time
                                       params->verbose_flag,
                                       params->trenddata,
                                       ofptr,
                                       efptr );

        snprintf( fancy_description, 1024, "MPI_%s_Processor_Receive_Wait", op );
        collect_and_print_single_time( my_rank,
                                       num_procs,
                                       total_mbs,
                                       times->proc_recv_wait_time,
                                       fancy_description,
                                       NULL,                      // no BW
                                       fancy_description,         // print time
                                       params->verbose_flag,
                                       params->trenddata,
                                       ofptr,
                                       efptr );
    
    }

/*******************************************************************
* Flush the print statements.
*******************************************************************/
  if ( my_rank == 0 ) {
      fprintf( ofptr, "=== Completed MPI-IO %s.\n", op);
      fflush( ofptr );
  }
  
  return MPI_SUCCESS;
}
