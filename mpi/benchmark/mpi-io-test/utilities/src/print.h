/****************************************************************
* AUTHOR:    Brett Kettering
* DATE:      July 26, 2005
* LAST MODIFIED: September 7, 2005 [jnunez]
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

#ifndef   __PRINT_H_INCLUDED
#define   __PRINT_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stddef.h>

#include "mpi.h"
//#include "mpio.h"

#include "utilities.h"

  
  void show_usage(int fullhelp);
  
  int print_input_environment(  int my_rank, 
                                int myhost_num, 
                                char *my_host, 
                                struct Parameters *params,
                                FILE *ofptr, 
                                FILE *efptr);

  int collect_and_print_time(   int my_rank, 
                                struct time_values *times,
                                struct Parameters *params,
                                struct State *state,
                                char *op, 
			                    FILE *ofptr, 
                                FILE *efptr);
  
  int print_parm(   char *msg, 
                    int my_rank, 
                    int num_procs, 
                    MPI_Comm comm, 
                    FILE *output, 
                    FILE *error);     

#ifdef __cplusplus
}
#endif

#endif /* __PRINT_H_INCLUDED */
