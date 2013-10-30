#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "boolean.h"

/*
 * USAGE: read_binary fname [int | double] [1 | !1]
 * ARGUMENTS: fname Input file
 *            File Data type: int or double
 *            Output type: 1 = prints 12 values per line, !1 summary
 */

int main( int argc, char *argv[] ){

  FILE *fp = NULL;
  double double_num = 0.0;
  int int_num = -1;
  int old_num = -1;
  double double_array[10] = {0.0};
  int count = 0;
  int count_array[10] = {0};
  int i;
  int num_double;
  
  if( (fp = fopen(argv[1], "rb")) == NULL){
    fprintf(stderr,"READ_BINARY: Unable to open file %s\n", argv[1]);
    return(FALSE);
  }
  
  
  if(atoi(argv[3]) != 1){
    int_num = (int)double_num;
    old_num = int_num;
    count = 0;
    
    while( (num_double = fread(&double_num, sizeof(double), 1, fp)) == 1){
      int_num = (int)double_num;
      
      if(int_num != old_num){
	fprintf(stdout, "New value; old num %d %d times\n", 
		old_num, count);
	count = 1;
	old_num = int_num;
      }
      else{
	count++;
      }
    }
  
    fprintf(stdout, "Last double; old num %d %d times\n", old_num, count);
  }
  else{
    count = 1;
    
    if(!strcmp(argv[2], "int")){
      while( (num_double = fread(&int_num, sizeof(int), 1, fp)) == 1){
	fprintf(stdout, "%d ", int_num);
	count++;
	if(count == 12){
	  fprintf(stdout, "\n");
	  count = 1;
	}
      }
    }
    else{
      while( (num_double = fread(&double_num, sizeof(double), 1, fp)) == 1){
	fprintf(stdout, "%.2lf ", double_num);
	count++;
	if(count == 12){
	  fprintf(stdout, "\n");
	  count = 1;
	}
      }
    }

  }

  
  fclose(fp);
  return(TRUE);

}
