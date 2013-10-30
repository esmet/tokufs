/****************************************************************
* AUTHOR:    Brett Kettering
* DATE:      July 26, 2005
* LAST MODIFIED: July 26, 2005
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

#ifndef   __BOOLEAN_H_INCLUDED
#define   __BOOLEAN_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

#define TRUE 1
#define FALSE 0
  

/****************************************************************
*
* This function returns text for a boolean value.
*
******************************************************************/
char *boolean_to_text( unsigned char the_boolean );

#ifdef __cplusplus
}
#endif

#endif /* __BOOLEAN_H_INCLUDED */
