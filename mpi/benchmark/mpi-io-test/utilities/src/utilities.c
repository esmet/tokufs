#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <stdarg.h>
#include <stdlib.h>
#include <sys/file.h>
#include <limits.h>
#include <ctype.h>
#include <sys/statvfs.h>
#include <libgen.h>

#include <assert.h>
#include <time.h>
#include <malloc.h>
#define _XOPEN_SOURCE 500
#include <unistd.h>
#include "mpi.h"
#include "utilities.h"

#if defined(MYSQL_HOST) || defined(MYSQL_FILE)
#if defined(MYSQL_HOST)
#include <mysql.h>
#endif
#include <sys/utsname.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <pwd.h>
#include <string.h>
#endif

#define BIG_FILE_OFFSET 8

/*******************************************
 *   ROUTINE: parse_size
 *   PURPOSE: Convert character inputs to number bytes. Allow 
 *            for (Linux) dd-like expressions; w 2, b 512, KB 1000, K 1024, 
 *            MB 1,000,000,  M 1,048,576, GB 1,000,000,000, and G 1,073,741,824
 *            A positive number followed by one of the above letter will 
 *            result in the two multiplied, i.e. 2b => 1024.
 *
 *   DATE: February 19, 2004
 *   LAST MODIFIED: September 7, 2005 [jnunez]
 *******************************************/
int parse_size(int my_rank, char *chbytes, long long int *out_value){

  char last, next_last;
  unsigned long long int insize = 0;
  
  last = chbytes[strlen(chbytes) - 1];
  next_last = chbytes[strlen(chbytes) - 2];
  
  if( !isdigit(last))
    switch(last){
    case 'w':
      if(insize == 0) insize = 2;
    case 'b':
      if(insize == 0) insize = 512;
    case 'K':
      if(insize == 0) insize = 1024;
    case 'M':
      if(insize == 0) insize = 1048576;
    case 'G':
      if( !isdigit(next_last)){
	fprintf(stderr,"[RANK %d] ERROR: Unknown multiplicative suffix (%c%c) for input string %s.\n", 
		my_rank, next_last, last, chbytes);
	return 1;
      }
      chbytes[strlen(chbytes) - 1] = '\0';
      if(insize == 0) insize = 1073741824;
      break;
    case 'B':
      if( isdigit(next_last)){
	fprintf(stderr,"[RANK %d] ERROR: Unknown multiplicative suffix (%c) for input string %s.\n", 
		my_rank, last, chbytes);
	return 1;
      }
      
      chbytes[strlen(chbytes) - 1] = '\0';
      last = chbytes[strlen(chbytes) - 1];
      next_last = chbytes[strlen(chbytes) - 2];
      
      if( !isdigit(next_last)){
	fprintf(stderr,"[RANK %d] ERROR: Unknown multiplicative suffix (%cB) for input string %s.\n", 
		my_rank, last, chbytes);
	return 1;
      }
      
      switch(last){
      case 'K':
	if(insize == 0) insize = 1000;
      case 'M':
	if(insize == 0) insize = 1000000;
      case 'G':
	if(insize == 0) insize = 10000000000ULL;
	chbytes[strlen(chbytes) - 1] = '\0';
	break;
      default:
	fprintf(stderr,"[RANK %d] ERROR: Unknown multiplicative suffix (%cB) for input string %s.\n", 
		my_rank, last, chbytes);
	return 1;
	break;
      }
      
      break;
    default:
      fprintf(stderr,"[RANK %d] ERROR: Unknown multiplicative suffix (%c) for input string %s.\n", 
	      my_rank, last, chbytes);
      return 1;
    }
  else{
    insize = 1;
  }

  /*  XXXX Must find way to handle decimal points instead of atol
  fprintf(stderr,"chbytes = %s atol = %ld\n", chbytes, atol(chbytes));
  */
  *out_value = insize * (unsigned long long)atol(chbytes);

  return 0;
}

/***************************************************************************
*
*  FUNCTION:  string_to_int_array
*  PURPOSE:   Parse an input string, with user defined delimined substrings,
*             into an array of integers. The original input string is not
*             modified by this routine.
*
*  PARAMETERS:
*             char *input_string    - Input string to parse containing
*                                     deliminted substrings
*             char *delimiter       - String used to parse the input string.
*                                     May be multiple characters, ex " *:"
*             int *num_args         - Number of substrings contained in the
*                                     input string
*             int **output_ints     - Array of integers contained in the
*                                     input string
*             int *sum              - Sum of the integers in the string
*
*  RETURN:    TRUE(1) on successful exit; FALSE(0) otherwise
*  HISTORY:   03/05/01 [jnunez] Original version.
*
****************************************************************************/
int string_to_int_array(char *input_string, char *delimiter,
                           int *num_args, int **output_ints, int *sum){

  char *temp_string = NULL;
  char *str_ptr = NULL;
  int counter;


/*******************************************************************
* Check for missing input string.
******************************************************************/
  if(input_string == NULL){
    printf("Input string to parse does not exist (NULL).\n");
    return(FALSE);
  }

/*******************************************************************
* Allocate memory for a copy of the input string. This is used to count
* the number of and maximum length of the substrings and not destroy the
* input string.
******************************************************************/
  if( (temp_string = (char *)malloc(strlen(input_string) +1)) == NULL){
    printf("Unable to allocate memory for copy of the input string.\n");
    return(FALSE);
  }
  strcpy(temp_string, input_string);

/*******************************************************************
* Parse the input string to find out how many substrings it contains
* and the maximum length of substrings.
******************************************************************/
  str_ptr = strtok(temp_string, delimiter);
  counter = 1;

  while(str_ptr){
    str_ptr = strtok(NULL, delimiter);
    if(str_ptr)
      counter++;
  }

/*******************************************************************
* Allocate memory for the array of strings
******************************************************************/
  if( (*output_ints = (int *)calloc(counter, sizeof(int))) == NULL){
    printf("Unable to allocate memory for the output string.\n");
    return(FALSE);
  }

/*******************************************************************
* Allocate memory for a copy of the input string to parse and copy into
* the output array of strings.
******************************************************************/
  if( (temp_string = (char *)malloc(strlen(input_string) + 1)) == NULL){
    printf("Unable to allocate memory for a copy of the input string.\n");
    return(FALSE);
  }
  
  strcpy(temp_string, input_string);

/*******************************************************************
* Finally, copy the input substrings into the output array
******************************************************************/
  str_ptr = strtok(temp_string, delimiter);
  *sum = 0;
  counter = 0;
  while(str_ptr){
    if(str_ptr){
      (*output_ints)[counter] =  atoi(str_ptr);
      *sum += (*output_ints)[counter];
      counter++;
    }
    str_ptr = strtok(NULL, delimiter);
  }
  /*
  for(i=0; i < counter; i++)
    printf("output string %d = %d.\n", i, (*output_ints)[i]);
  printf("output string sum %d.\n", *sum);
  */
/*******************************************************************
* Copy the number of substrings into the output variable and return
******************************************************************/
  *num_args = counter;

  return(TRUE);
}

extern char *
expand_path(char *str, int rank, long timestamp)
{
  char tmp1[2048];
  char tmp2[1024];
  
  char *p;
  char *q;
  
  //int rank;
  int string_modified = FALSE;
  
  if (str == (char *)0) {
    return ((char *)0);
  }
  
  for (p = str, q = tmp1; *p != '\0';) {
    if (*p == '%') {
      string_modified = TRUE;
      p++;
      if (*p == '\0') {
	return ((char *)0);
      }

      switch (*p) {
      case 'h':
	if (gethostname(tmp2, sizeof(tmp2) - 1) != 0)
	  return ((char *)0);
	tmp2[sizeof(tmp2) - 1] = '\0';
	strncpy(q, tmp2, (sizeof(tmp1) - (q - tmp1) - 1));
	tmp1[sizeof(tmp1) - 1] = '\0';
	q += strlen(tmp2);
	break;

      case 's':
       snprintf(tmp2, (sizeof(tmp2) - 1), "%ld", timestamp);
       tmp2[sizeof(tmp2) - 1] = '\0';
       strncpy(q, tmp2, (sizeof(tmp1) - (q - tmp1) - 1));
       tmp1[sizeof(tmp1) - 1] = '\0';
       q += strlen(tmp2);
         break;


      case 'p':
	snprintf(tmp2, (sizeof(tmp2) - 1), "%d", getpid());
	tmp2[sizeof(tmp2) - 1] = '\0';
	strncpy(q, tmp2, (sizeof(tmp1) - (q - tmp1) - 1));
	tmp1[sizeof(tmp1) - 1] = '\0';
	q += strlen(tmp2);
	break;

      case 'r':
	//if (MPI_Comm_rank(comm, &rank) != MPI_SUCCESS) {
	//  return ((char *)0);
	//}
	snprintf(tmp2, (sizeof(tmp2) - 1), "%d", rank);
	tmp2[sizeof(tmp2) - 1] = '\0';
	strncpy(q, tmp2, (sizeof(tmp1) - (q - tmp1) - 1));
	tmp1[sizeof(tmp1) - 1] = '\0';
	q += strlen(tmp2);
	break;

      default:
	return ((char *)0);
      }

      p++;
    } 
    else {
      *q++ = *p++;
    }
  }

  //fprintf( stderr, "%s: %s -> %s\n", __FUNCTION__, str, tmp1 );
  if(!string_modified)
    strcpy(tmp1,str);

  return (strdup(tmp1));
}

/***************************************************************************
*
*  FUNCTION:  string_to_string_array
*  PURPOSE:   Parse an input string, with user defined delimined substrings,
*             into an array of strings. The original input string is not
*             modified by this routine.
*
*  PARAMETERS:
*             char *input_string    - Input string to parse containing
*                                     deliminted substrings
*             char *delimiter       - String used to parse the input string.
*                                     May be multiple characters, ex " *:"
*             int *num_args         - Number of substrings contained in the
*                                     input string
*             char ***output_string - Array of substrings contained in the
*                                     input string
*
*  RETURN:    TRUE(1) on successful exit; FALSE(0) otherwise
*  HISTORY:   03/05/01 [jnunez] Original version.
*
****************************************************************************/
int string_to_string_array(char *input_string, char *delimiter,
                           int *num_args, char ***output_string){

  char *temp_string = NULL;
  char *str_ptr = NULL;
  int max_len, counter;
  int i;                          /* General for loop index */


/*******************************************************************
* Check for missing input string.
******************************************************************/
  if(input_string == NULL){
    printf("Input string to parse does not exist (NULL).\n");
    return(FALSE);
  }

/*******************************************************************
* Allocate memory for a copy of the input string. This is used to count
* the number of and maximum length of the substrings and not destroy the
* input string.
******************************************************************/
  if( (temp_string = (char *)malloc(strlen(input_string) +1)) == NULL){
    printf("Unable to allocate memory for copy of the input string.\n");
    return(FALSE);
  }
  strcpy(temp_string, input_string);

/*******************************************************************
* Parse the input string to find out how many substrings it contains
* and the maximum length of substrings.
******************************************************************/
  str_ptr = strtok(temp_string, delimiter);
  counter = 1;
  max_len = (int)strlen(str_ptr);

  while(str_ptr){
    str_ptr = strtok(NULL, delimiter);
    if(str_ptr){
      counter++;
      if(strlen(str_ptr) > max_len) max_len = (int)strlen(str_ptr);
    }
  }

/*******************************************************************
* Allocate memory for the array of strings
******************************************************************/
  if( (*output_string = (char **)calloc(counter, sizeof(char *))) == NULL){
    printf("Unable to allocate memory for the output string.\n");
    return(FALSE);
  }
  for(i=0; i < counter; i++)
    if( ((*output_string)[i] = (char *)malloc(max_len + 1)) == NULL){
      printf("Unable to allocate memory for the output string.\n");
      return(FALSE);
    }

/*******************************************************************
* Allocate memory for a copy of the input string to parse and copy into
* the output array of strings.
******************************************************************/
  if( (temp_string = (char *)malloc(strlen(input_string) + 1)) == NULL){
    printf("Unable to allocate memory for a copy of the input string.\n");
    return(FALSE);
  }

  strcpy(temp_string, input_string);

/*******************************************************************
* Finally, copy the input substrings into the output array
******************************************************************/
  str_ptr = strtok(temp_string, delimiter);
  counter = 0;
  while(str_ptr){
    if(str_ptr){
      strcpy((*output_string)[counter], str_ptr);
      counter++;
    }
    str_ptr = strtok(NULL, delimiter);
  }
  /*
  for(i=0; i < counter; i++)
    printf("output string %d = %s.\n", i, (*output_string)[i]);
  */
/*******************************************************************
* Copy the number of substrings into the output variable and return
******************************************************************/
  *num_args = counter;

  return(TRUE);
}

/*******************************************
 *   ROUTINE: MPIIO_set_hint
 *   PURPOSE: Set the MPI hint in the MPI_Info structure.
 *   LAST MODIFIED: 
 *******************************************/
int MPIIO_set_hint(int my_rank, MPI_Info *info, char *key, char *value)
{

  if((MPI_Info_set(*info, key, value)) !=  MPI_SUCCESS){
    if(my_rank == 0)
      fprintf(stderr, "[RANK %d] WARNING: Unable to set the hint %s to value %s in the MPI_info structure.\n", my_rank, key, value);
  }
  
  return 1;
}

char
printable_char( int index ) {
    index %= 27;    // for an alphabetic numbers
    index += 96;    // for the lowercase
    return ( index == 96 ? '\n' : (char)index ); // throw some newlines in
    //return (char)index;
}

void
fill_buf( char *check_buf, int blocksize, int rank, int i, int pagesize,
          int touch ) 
{
    int j = 0;
    double value = i * blocksize;
    double *buffer = (double *)check_buf;
    int each_buffer_unique = 0;

    if ( touch == 0 ) {
        return;
    }

    if ( i == 0 || each_buffer_unique ) {
	// only fill it in fully for first block
	    memset( check_buf, 0, blocksize );
	    //snprintf( check_buf, blocksize, "Rank %d for object %d blocksize %d",
	    //        rank, i, blocksize );
	    for( j = 0; j < blocksize / sizeof(double); j++ ) {
		buffer[j] = value;
		value++;
	    }
        buffer[0] = (char)rank; // put the rank in there too
	} else {
        // for other blocks, just change one double per page
		for( j = 0; j < blocksize / sizeof(double); j += pagesize ) {
			//printf( "writing at %ld\n", j * pagesize );
			buffer[j] = value;
			value++;
		}
	}	
    /*
	// put a string in there 
    snprintf( check_buf, blocksize, "Rank %d for object %d blocksize %d\n",
            rank, i, blocksize );
    */
}

int
verify_buf( int rank, double *expected, double *received, char *filename, 
    long which_block, long blocksize, int touch ) 
{
    int errors_found = 0;

    if ( touch == 0 ) { // don't check
        return 0;
    }

    if ( memcmp( expected, received, blocksize ) != 0 ) {
        int j = 0;
        for( j = 0; j < blocksize / sizeof(double); j++ ) {
            errors_found += compare_double( filename, rank, 
                    which_block, j, blocksize, 
                    received[j], expected[j] );
            if ( errors_found >= 100 ) {
                fatal_error( stderr, rank, 0, NULL, "Too many errors\n" );
            }
        }
    }
    return errors_found;
}

void bin_prnt_byte(int x)
{
   int n;
   for(n=0; n<8; n++)
   {
      if((x & 0x80) !=0)
      {
         fprintf(stderr,"1");
      }
      else
      {
         fprintf(stderr,"0");
      }
      if (n==3)
      {
         fprintf(stderr," "); /* insert a space between nybbles */
      }
      x = x<<1;
   }
}

void bin_prnt_int(int x)
{
   int hi, lo;
   hi=(x>>8) & 0xff;
   lo=x&0xff;
   bin_prnt_byte(hi);
   printf(" ");
   bin_prnt_byte(lo);
}

char *itob(int x) {
	static char buff[sizeof(int) * CHAR_BIT + 1];
	int i;
	int j = sizeof(int) * CHAR_BIT - 1;

	buff[j] = 0;
	for(i=0;i<sizeof(int) * CHAR_BIT; i++) {
		if(x & (1 << i))
			buff[j] = '1';
		else
			buff[j] = '0';
		j--;
	}
	return buff;
}


void
print_double( char *header, double one ) {
    char *c_one = (char *)&one;
    int c;
	int *b = (int *)&one;
    fprintf( stderr, "%s %lf \n\tchars(", header, one );

    for( c = 0; c < sizeof(double) / sizeof(char); c++ ) {
	fprintf( stderr, "%c", c_one[c] );
    }
    fprintf( stderr, ")\n\tbinary(" );

	for( c = 0; c < sizeof(double) / sizeof(int); c++ ) {
		//bin_prnt_int( b[c] );
		//fprintf( stderr, " " );
		fprintf( stderr, "%s ", itob( b[c] ) );
	}	
    fprintf( stderr, ")\n" );
}

int
compare_double( char *file, int rank, long block, long block_off, long bs, 
	double received, double expected ) 
{
	if ( received != expected ) {
        warning_msg( stderr, rank, 0, NULL, 
			"read error in %s at block %lu, block off %lu, file off %llu: "
			"%lf != %lf\n",
			file, block, block_off,  (long long)block *  (long long)bs + (long long)block_off * sizeof(double), 
			received, expected );
		print_double( "Expected", expected );
		print_double( "Received", received );
		return 1;
	} else {
		return 0;
	}
}

void *
Calloc( size_t nmemb, size_t size, FILE *fp, int rank ) {
    void * buffer;
    buffer = calloc( nmemb, size );
    if ( ! buffer ) {
        fatal_error( fp, rank, 0, NULL, 
				"calloc %ld failed %s", size, strerror(errno) );
    }
    return buffer;
}

void *
Malloc( size_t size, FILE *fp, int rank ) {
    void * buffer;
    buffer = malloc( size );
    if ( ! buffer ) {
        fatal_error( fp, rank, 0, NULL, 
				"malloc %uld failed %s", size, strerror(errno) );
    }
    return buffer;
}

void *
Valloc( size_t size, FILE *fp, int rank ) {
    void * mybuf;
    mybuf = valloc( size ); // obsolete and warns . . . 
    //mybuf = memalign(sysconf(_SC_PAGE-SIZE),size);
    if ( ! mybuf ) {
        fatal_error( fp, rank, 0, NULL, 
				"valloc %ld failed %s", size, strerror(errno) );
    }
    return mybuf;
}

char * 
chomp( char * str ) {
    int len = strlen(str);
    while ( str[len - 1] == '\n' ) str[len - 1] = '\0';
    return str;
}

#if defined(MYSQL_HOST) || defined(MYSQL_FILE)

int
check_env( const char *param, const char *value ) {
    return ( getenv(param) && ! strcmp( getenv(param), value ) );
}

char *
get_output( const char *command ) {
    static char output[4096];
    static int uniquifier = 0;
    char full_command[4096];
    int input_fd;
    int len;
    char tmpfile[1024];
    /*
	fprintf( stderr, "# fork/exec not avail on roadrunner for %s\n",
                command );
    return NULL;    // broke in openmpi on infiniband . . . 
    */

    if ( check_env( "MY_MPI_HOST", "roadrunner" ) || check_env( "MY_MPI_MOST", "roadrunner_scc") ) {
    }
    snprintf( tmpfile, 1024, "/tmp/%d.%d",  (int)getpid(), uniquifier++ );
    snprintf( full_command, 4096, "%s > %s", command, tmpfile );

    if ( system( full_command ) == -1 ) {
        fprintf( stderr, "Exec %s failed.", command );
        return NULL;
    } else {
        fprintf( stderr, "# Ran %s\n", full_command );
    }

    input_fd = open( tmpfile, O_RDONLY );
    if ( input_fd <= 0 ) {
        fprintf( stderr, "Open %s failed: %s\n", tmpfile, strerror(errno) );
        return NULL;
    }
    if ( ( len = read( input_fd, output, 4096 ) ) <= 0 ) {
        fprintf( stderr, "read %s failed: %s\n", tmpfile, strerror(errno) );
        return NULL;
    }
    close( input_fd );
    unlink( tmpfile );
    output[len] = '\0';
    chomp( output );
//    fprintf( stderr, "# Rec'd %s\n", output );
    return output;
}

char *
read_version( char *ip_addr, char *web_page ) {
    struct sockaddr_in serv_addr;
    struct hostent *srvrptr;
    int fd;
    char request[4096];
    static char buffer[65536];
    static char version[128];
    int ret;
    memset( request, 0, 4096 );
    memset( buffer, 0, 65536 );
    memset( version, 0, 128 );

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port   = htons(80);
    
    if ((srvrptr = gethostbyname(ip_addr)) == NULL) {
        printf( "Couldn't gethostbyname(%s): %s\n", ip_addr,
                strerror(errno) );
        return NULL;
    }

    bcopy((char*)srvrptr->h_addr,(char*)&serv_addr.sin_addr,srvrptr->h_length);
    if ((fd=socket(AF_INET,SOCK_STREAM,0)) < 0 ) {
        perror( "socket" );
        return NULL;
    }
    
    if ( connect(fd, (struct sockaddr*)&serv_addr,sizeof(serv_addr)) < 0 ) {
        perror( "connect" );
        return NULL;
    }

    //printf( "#\n#\n#\n" );
    snprintf( request, 4096, "GET /%s HTTP/1.0\r\nHost: %s\r\n\r\n", 
            web_page, ip_addr );
    //printf( "# Will try request %s\n", request );
    if ( write( fd, request, strlen(request) ) != (int)strlen(request) ) {
        perror( "write" );
        return NULL;
    }

    while( ( ret = read( fd, buffer, 65536 ) ) > 0 ) {
        char *substr;
        if (( substr = strstr(buffer, "Release Notes for Version" ) ) != NULL ){
            //printf( "%s\n", substr );
            sscanf( substr, "Release Notes for Version %[^<]", version );
            while( version[strlen(version)-1] == '.' ) {
                version[strlen(version)-1] = '\0';
            }
            return version;
        } else {
            //printf( "Couldn't find version in %s\n", buffer );
            if (( substr = strstr(buffer, "URI: http://") ) != NULL ) {
                char *webpage = "?loginRequired=1&nextUrl=%2f&nextUrlData=";
                char ip[1024];
                // maybe they are sending a document has moved . . .
                //sscanf( substr, "http://%[^/]/%[^&\n]", ip, webpage );
                sscanf( substr, "URI: http://%[^/]", ip );
                // just hardcode stupid webpage
                //printf( "Will try again with %s and %s\n###\n", ip, webpage );
                return read_version( ip, webpage ); // recurse
            }
        }
    }
    //printf( "ret %d\n", ret );
    printf( "Couldn't read panfs version from %s %s\n", ip_addr, web_page );
    return NULL;
}

char *
find_server_version( char *mountpt) {
        //ugh, /bin/grep doesn't work on backend.
    //char command[512];
    //snprintf( command, 512, "/bin/mount | /bin/grep %s", "panfs" );

    char *mount = get_output( "/bin/mount" );
    if ( ! mount ) {
        fprintf( stderr, "Couldn't find panfs server version (%d).\n", 
                __LINE__ );
        return NULL;
    }
    mount = strstr( mount, "panfs://" );
    if ( ! mount ) {
        fprintf( stderr, "Couldn't find panfs server version (%d).\n", 
                __LINE__ );
        return NULL;
    }

    int ip1, ip2, ip3, ip4;
    char ip_addr[128];
    if ( sscanf( mount, "panfs://%d.%d.%d.%d", &ip1, &ip2, &ip3, &ip4 ) == 4 ) {
        //printf( "Mount is %s (%d.%d.%d.%d)\n", mount, ip1, ip2, ip3, ip4 );
        snprintf( ip_addr, 128, "%d.%d.%d.%d", ip1, ip2, ip3, ip4 );
        return read_version( ip_addr, "" );
    } else {
        fprintf( stderr, "Couldn't find panfs server version (%d).\n",
                __LINE__ );
        return NULL;
    }
}

int
mystr_count( char *needle, char *needle2, char *haystack ) {
    int count = 0;
    char *position = haystack;
    char full_needle[1024];
    snprintf( full_needle, 1024, "%s%s", needle, needle2 );
    //fprintf( stderr, "Looking for %s in %s\n", needle, haystack);
    while( ( position = strstr( position, full_needle ) ) != NULL ) {
        //fprintf( stderr, "Found %s at %s in %s\n", 
            //needle, position, haystack);
        count++;
        position++; // otherwise it will keep finding itself
    }
    return count;
}

void
set_hostlist( struct Parameters *params, int rank ) {
    char myname[256];
    char **hostnames;
    int i;
    int full_hostlist_length;
    char *full_hostlist;
	int mpi_ret;
    float average_procs_per_node = 0;

    int nproc = params->num_procs_world;

    memset( (void *)myname, 0, 256 );
    gethostname( myname, 256 );
    full_hostlist_length = strlen( myname ) + 10;   // give it slop

    //fprintf( stderr, "%d (%s): Entering set_hostlist\n", rank, myname );
    if ( rank == 0 ) {
        hostnames = (char **)Malloc( sizeof(char *) * nproc, stderr, rank);
        MPI_Status stat;
        for( i = 1; i < nproc; i++ ) {
            hostnames[i] = Malloc( 256, stderr, rank );
            memset( (void *)hostnames[i], 0, 256 );
            //fprintf( stderr, "0: recv'ing from %d\n", i );
            if ((mpi_ret = 
				  MPI_Recv( hostnames[i], 256, MPI_BYTE, i, 0, MPI_COMM_WORLD,
                    &stat ) ) != MPI_SUCCESS )
            {
                fatal_error( stderr, rank, mpi_ret, &stat, "MPI_Recv\n" );
            }
            //fprintf( stderr, "%s len %d, now full is %d", 
            //        hostnames[i],
            //        (int)strlen(hostnames[i]), 
            //        (int)(full_hostlist_length + strlen(hostnames[i] )) );
            full_hostlist_length += strlen( hostnames[i] ) + 2;
            //fprintf( stderr, "Recv'd %s (%d) from %d\n", 
            //        hostnames[i], (int)strlen(hostnames[i]), i );
        }
        full_hostlist = (char *)Malloc( full_hostlist_length, stderr, rank );
        snprintf( full_hostlist, full_hostlist_length, "%s,", myname );
        // iterate once to put them into a string
        for( i = 1; i < nproc; i++ ) {
            int cur_len = strlen(full_hostlist);
            snprintf( &(full_hostlist[cur_len]), 
                    full_hostlist_length - cur_len,
                    "%s,", hostnames[i] );
            //fprintf( stderr, "Writing %s at %d\n", hostnames[i], 
            //        full_hostlist_length - cur_len );
        }
        // iterate again to count them in the string and
        // to free the memory
        average_procs_per_node += mystr_count( myname, ",", full_hostlist );
        for( i = 1; i < nproc; i++ ) {
            average_procs_per_node 
                += mystr_count(hostnames[i],",",full_hostlist);
            free( hostnames[i] );
        }
        free( hostnames );
        params->host_list = strdup( full_hostlist );
        params->procs_per_node = average_procs_per_node / nproc;
        free( full_hostlist );
    } else {
        if ( (mpi_ret 
			= MPI_Send( myname, 256, MPI_BYTE, 0, 0, MPI_COMM_WORLD))
                != MPI_SUCCESS )
        {
            fatal_error( stderr, rank, mpi_ret, NULL, "MPI_Send\n" );
        }
        //fprintf( stderr, "%d: Sent %s to 0\n", rank, myname );
    }
}

char *
find_client_version( const char *package_name, const char *mountpt ) {
    char command[1024];
    //char *tracer = "panfs_trace"; 
    char *tracer = NULL;
    char *possibility_one = "/usr/sbin/panfs_trace";
    char *possibility_two = "/usr/local/sbin/panfs_trace";
    char *version = NULL;
    static char retbuf[1024];
    //char full_path[8192];

    if ( mountpt == NULL ) {
        return NULL;
    }

    //snprintf(command, 1024, "rpm -qa | grep %s | grep -v sdk", package_name );
    // won't work on bproc back-ends.
    // instead we need to call /usr/sbin/panfs_trace --version MNT_POINT
    // (or possibly /usr/local/sbin/panfs_trace)
    // add both to the path to ensure we find panfs_trace
    //char *path    = getenv("PATH");
    //snprintf( full_path, 8192, "/usr/sbin/:/usr/local/sbin%s%s",
    //        ( path ? ":" : "" ), path );
    //setenv( "PATH", full_path, 1 );
    //path = getenv( "PATH" );
    //printf( "path is %s\n", path );

    // figure out where panfs_trace is.  stat won't work on flash
    // so just hard code it . . . 
    // what the hell?  getenv returns some bogus address on bluesteel?
    //char *mpihost = getenv("MY_MPI_HOST");
    if ( check_env( "MY_MPI_HOST", "flash" ) ) {
        tracer = possibility_one;
    } else if(check_env( "MY_MPI_HOST", "bluesteel") ) {
        tracer = possibility_one;
    } else if(check_env( "MY_MPI_HOST", "coyote") ) {
        tracer = possibility_one;
    } else {
        // attempt to find it by hand
        struct stat statbuf;
        if ( stat( possibility_one, &statbuf ) == 0 ) {
            tracer = possibility_one;
        } else if ( stat( possibility_two, &statbuf ) == 0 ) {
            tracer = possibility_two;
        }
        if ( ! tracer ) {
            fprintf( stderr, "couldn't find %s or %s\n", possibility_one,
                possibility_two );
            return NULL;
        }
    }

    snprintf( command, 1024, "%s --version %s", tracer, mountpt );
    version = get_output( command );
        
    if ( version ) {
        char *header = "panfs_trace utility version: ";
        char *header_loc = NULL;
        if ( (header_loc = strstr( version, header )) != NULL ) {
            char *endline = index( version, '\n' );
            if ( endline ) {
                *endline = '\0';
            }
            snprintf( retbuf, 1024, "%s", &(header_loc[strlen(header)]) );
            return retbuf;
        }

        // remove the error string
        char *known_error = "error: panfs_trace utility must be run as root";
        char *err_msg = NULL;
        err_msg = strstr( version, known_error );
        if ( err_msg ) {
            *err_msg = '\0';
            chomp( version );
        }
    }
    return version;
}

char *
find_mountpoint( const char *path ) {
    static char *mntpt = NULL;
    char *second_slash = NULL;

        // clean up if this is the Nth time we're called (N > 1)
    if ( mntpt ) free( mntpt );
    if ( ! path ) {
        fprintf( stderr, "path is NULL at %s:%d\n", __FUNCTION__, __LINE__ );
        //fatal_error( stderr, 0, 0, NULL, "path is NULL" );
        return NULL;
    }
    mntpt = strdup( path );

    second_slash = index( mntpt, '/' );
    if ( ! second_slash ) return mntpt;
    second_slash++; // move past the first slash
    second_slash = index( second_slash, '/' );
    if ( ! second_slash ) return mntpt; 
    *second_slash = '\0';    // change to a NULL term so that only the
                            // first path component is visible
    return mntpt;
}

void
addDBColumnName( int string_len, char  *columns, char *name ) {
    snprintf( &(columns[strlen(columns)]), 
              string_len - strlen(columns),    
              "%s %s",
              strlen(columns) ? "," : "",
              name );
}

void
addDBInt( int string_len, char *columns, char *values, char *mixed,
        long long value, char *name ) 
{
    addDBColumnName( string_len, columns, name );
    snprintf( &(values[strlen(values)]),
              string_len - strlen(values),
              "%s \'%ld\'",
              strlen(values) ? "," : "",
              (long)value );
    snprintf( &(mixed[strlen(mixed)]),
            string_len - strlen(mixed),
            "%s%s=\'%ld\'",
            strlen(mixed) ? "," : "",
            name, (long)value ); 
}

void
addDBFloat( int string_len, char *columns, char *values, char *mixed,
        float value, char *name ) 
{
    addDBColumnName( string_len, columns, name );
    snprintf( &(values[strlen(values)]),
              string_len - strlen(values),
              "%s \'%e\'",
              strlen(values) ? "," : "",
              value );
    snprintf( &(mixed[strlen(mixed)]),
            string_len - strlen(mixed),
            "%s%s=\'%e\'",
            strlen(mixed) ? "," : "",
            name, value ); 

}

void
addDBStr( int string_len, char *columns, char *values, char *mixed,
        char *value, char *name ) 
{
        // if value == NULL, just don't add to query, db will set it NULL
    if ( value ) {
        addDBColumnName( string_len, columns, name );
        snprintf( &(values[strlen(values)]),
                  string_len - strlen(values),
                  "%s \'%s\'",
                  strlen(values) ? "," : "",
                  value ? value : "" );
    }
    if ( value ) {
        snprintf( &(mixed[strlen(mixed)]),
            string_len - strlen(mixed),
            "%s%s=\'%s\'",
            strlen(mixed) ? "," : "",
            name, value ? value : "" );
    }
}

void
addDBTimeComponent( float value, int string_len, char *values, char *columns,
        char *mixed, char *modifier, char *op, char *name ) 
{
    char fullname[1024];
    snprintf( fullname, 1024, "%s_%s%s", op, name, modifier );
    addDBFloat( string_len, columns, values, mixed, value, fullname );
}

void
addDBTime( int my_rank, int num_procs,
        int string_len, char *columns, char *values, char *mixed,
        float value, char *op, char *name ) 
{
    double min, max, sum;
    int min_ndx, max_ndx;
	int mpi_ret;

    if( (mpi_ret = get_min_sum_max(my_rank, value, &min, &min_ndx,
		      &sum, &max, &max_ndx, op, stdout, stderr) ) != MPI_SUCCESS)
    {
        fatal_error( stderr, my_rank, mpi_ret, NULL,
                "Problem computing min, max, and sum of %s %s time.\n", 
                       op, name );
    }

    addDBTimeComponent( min,             string_len, values, columns, mixed,
            "_min", op, name );
    addDBTimeComponent( max,             string_len, values, columns, mixed,
            "_max", op, name );
    addDBTimeComponent( sum / num_procs, string_len, values, columns, mixed,
            "",     op, name );
}

void
addDBTimes( int my_rank, 
            int num_procs, int string_len, char *columns, char *values,
            char *mixed, struct time_values *times, char *op ) 
{
    addDBTime( my_rank, num_procs, string_len, columns, values, mixed, 
            times->file_open_wait_time, op, "file_open_wait_time" );
    addDBTime( my_rank, num_procs, string_len, columns, values, mixed,
            times->prealloc_wait_time, op, "prealloc_wait_time" );
    addDBTime( my_rank, num_procs, string_len, columns, values, mixed,
            times->file_close_wait_time, op, "file_close_wait_time" );
    addDBTime( my_rank, num_procs, string_len, columns, values, mixed,
            times->barrier_wait_time, op, "barrier_wait_time" );
    addDBTime( my_rank, num_procs, string_len, columns, values, mixed,
            times->file_sync_wait_time, op, "file_sync_wait_time" );
    addDBTime( my_rank, num_procs, string_len, columns, values, mixed,
            times->proc_send_wait_time, op, "proc_send_wait_time" );
    addDBTime( my_rank, num_procs, string_len, columns, values, mixed,
            times->proc_recv_wait_time, op, "proc_recv_wait_time" );
    addDBTime( my_rank, num_procs, string_len, columns, values, mixed,
            times->file_op_wait_time, op, "file_op_wait_time" );
    addDBTime( my_rank, num_procs, string_len, columns, values, mixed,
            times->total_op_time, op, "total_op_time" );
    addDBTime( my_rank, num_procs, string_len, columns, values, mixed,
            times->total_time, op, "total_time" );
    addDBTime( my_rank, num_procs, string_len, columns, values, mixed,
            times->stat_time, op, "stat_time" );
}


/*
int
check_error_messages( struct Parameters *params ) {
    int count, source, avail = 0;
    char *buf; 
    MPI_Status status;
    MPI_Iprobe(MPI_ANY_SOURCE, ERROR_TAG, MPI_COMM_WORLD, &avail, &status);
    if ( avail && ! params->error_message ) {
        source = status.MPI_SOURCE;
        MPI_Get_count(&status, MPI_BYTE, &count);
        buf = malloc(count*sizeof(char));
        assert( buf );
        MPI_Recv(buf, count, MPI_BYTE, source,ERROR_TAG,MPI_COMM_WORLD,&status);
        params->error_message = strdup( buf );
        params->error_rank    = source;
        fprintf( stderr, "Received error %s from %d\n", buf, source );
    } else {
        fprintf( stderr, "No async error messages avail\n" );
    }
    return 1;
}
*/
#endif

void
get_panfs_file_info( struct Parameters *params ) {
#if defined(MYSQL_HOST) || defined(MYSQL_FILE)
    int mpi_ret;
    MPI_File wfh;
    MPI_Info info_used = MPI_INFO_NULL;
    int nkeys, i, dummy_int;
    char **keys;
    char **values;

	// info object not created for posix_io tests
    MPI_Info info = params->posix_io ? MPI_INFO_NULL : params->info;

    mpi_ret = MPI_File_open( MPI_COMM_SELF, params->tfname, 
            MPI_MODE_RDONLY | MPI_MODE_UNIQUE_OPEN, info, &wfh );
    if ( mpi_ret != MPI_SUCCESS ) {
        warning_msg( stderr, 0, mpi_ret, NULL, "Unable to open %s",
                params->tfname );
        return;
    }

    MPI_File_get_info( wfh, &info_used );
    MPI_Info_get_nkeys( info_used, &nkeys );
    keys   = (char **)Malloc(nkeys * sizeof(char *), stderr, 0 );
    values = (char **)Malloc(nkeys * sizeof(char *), stderr, 0 ); 
    for( i = 0; i < nkeys; i++ ) {
        keys[i]   = (char*)Malloc(200 * sizeof(char), stderr, 0);
        values[i] = (char*)Malloc(200 * sizeof(char), stderr, 0);
        assert( keys[i] && values[i] );
        MPI_Info_get_nthkey(info_used, i, keys[i] );
        MPI_Info_get( info_used, keys[i], 200, values[i], &dummy_int );
        printf( "Key: %s = %s\n", keys[i], values[i] );
        if ( strstr( keys[i], "panfs_" ) ) {
            insert_panfs_info( keys[i], values[i] );
        }
    }
    MPI_File_close( &wfh );
#endif
}

void
collect_additional_config( struct Parameters *params, int rank, int argc,
        char **argv ) 
{
    /*****************************************************************
    * Check large file capability
    ******************************************************************/
    if (rank == 0){
        if (sizeof(off_t) < BIG_FILE_OFFSET) {
            fprintf(stderr,
              "64 bit file size addressing not supported on this platform\n");
            MPI_Finalize();
        }
    }
#if defined(MYSQL_HOST) || defined(MYSQL_FILE)
    char *panfs_version;
    char *mount_pt;
    struct passwd *pw;
    struct tm *mytime;
    struct utsname utsbuf;
    time_t seconds;
    int resultlen  = 0;
    int mpi_version    = 0;
    int mpi_subversion = 0;
    int arg_len = 0;
    int i = 0;
    int mpi_ret;

    set_hostlist( params, rank );
    //return;   // if we return here, no bug
    //fprintf( stderr, "%d Received key %s\n", rank, params->db_key );

    params->db_key     = (char*)Malloc(512, stderr, rank);
    memset( params->db_key, 0, 512 );
    sprintf( params->db_key, "UNINITIALIZED\n" );
    if ( rank == 0 ) {
        char *panfs_threads;
        char *jobid;
        char *full_path;
        char *dir_name;
        int ret;
        struct statvfs statfs_buf;

        params->system     = (char*)Malloc(MPI_MAX_PROCESSOR_NAME, stderr,rank);
        params->datestr    = (char*)Malloc(16, stderr, rank);
        params->mpiversion = (char*)Malloc(16, stderr, rank);

        MPI_Get_processor_name( params->system, &resultlen );
        MPI_Get_version( &mpi_version, &mpi_subversion );
        snprintf( params->mpiversion, 16, "%d.%d", mpi_version,mpi_subversion);

        seconds = time(NULL);
        mytime = localtime( &seconds );
        snprintf( params->datestr, 16, "%d-%d-%d",
                mytime->tm_year+1900, mytime->tm_mon + 1, mytime->tm_mday );

        pw                = getpwuid(getuid());
        params->user      = strdup( pw->pw_name );
        params->test_time = time(NULL);

        mount_pt  = find_mountpoint( params->tfname );
        full_path = strdup( params->tfname ); 
        dir_name  = dirname( full_path );

        fprintf( stderr, "Finding panfs free space from %s\n", dir_name );
        ret = statvfs( dir_name, &statfs_buf );
        if ( ret ) {
            perror( "statvfs" );
        } else {
            params->df_perc_before = 
                (int)((statfs_buf.f_bfree * 100 )/ statfs_buf.f_blocks);
            fprintf( stderr, "%lld / %lld blocks free (%d%%)\n", 
                    statfs_buf.f_bfree, statfs_buf.f_blocks, 
                    params->df_perc_before );
        }

        fprintf( stderr, "Finding panfs client\n" );
        panfs_version = find_client_version( "panfs", mount_pt );
        if ( panfs_version ) {
            params->panfs = strdup( panfs_version );
        }
        //fprintf( stderr, "Finding panfs server\n" );
        panfs_version = find_server_version(mount_pt);
        if ( panfs_version ) {
            params->panfs_srv = strdup( panfs_version );
        }
        //fprintf( stderr, "Finding OS name\n" );
        if ( uname( &utsbuf ) == 0 ) {
            params->os_version = strdup( utsbuf.release );
        } else {
            perror( "uname" );
        }

        //fprintf( stderr, "looking for jobid\n" );
        jobid = getenv( "PBS_JOBID" );
        if ( ! jobid ) jobid = getenv( "LSF_JOBID" );
        if ( jobid ) params->jobid = strdup( jobid );

        panfs_threads = get_output( "ps auxw | grep pan | grep kpanfs_thpool "
                                    "| grep -v grep | wc -l" );
            // this can fail on pos bproc machines which don't have grep
        if ( panfs_threads != NULL ) {
            params->panfs_threads = atoi( panfs_threads );
        } else {
            params->panfs_threads = 0;  
        }

        printf( "Panfs Threads %d\n", params->panfs_threads ); 
        printf( "Panfs Client %s\n", params->panfs ? params->panfs : "NULL" ); 
        printf( "Panfs Server %s\n", params->panfs_srv ? params->panfs_srv :
                                     "NULL" );
        printf( "OS Version %s\n", params->os_version ? params->os_version :
                                     "NULL" );

        snprintf( params->db_key, 512, 
                "date_ts=%ld && user=\'%s\' && system=\'%s\'",
                params->test_time, params->user, params->system );
        printf( "DB_KEY %s\n", params->db_key );

      // create a string in the params to hold the full set of args
      //  fprintf( stderr, "Finding full args from %d args\n", argc );
      for( i = 0; i < argc; i++ ) {
          arg_len += 1 + strlen( argv[i] );
          //fprintf( stderr, "Arg %d: %s\n", i, argv[i] );
      } 
      params->full_args = (char *)Malloc( arg_len + 10, stderr, rank ); 
      params->full_args[0] = '\0';
      for( i = 0; i < argc; i++ ) {
          int cur_len = strlen(params->full_args);
          snprintf( &(params->full_args[cur_len]), 
                  arg_len - cur_len,
                  "%s ", argv[i] );
      }
    }


    if ( rank == 0 ) {
        //fprintf( stderr, "Bcasting %s\n", params->db_key );
        printf( "Full nodelist %s\n", params->host_list );
        printf( "Procs_per_node %.3f\n", params->procs_per_node );
        printf( "Full args %s\n", params->full_args );
    }
    if ( (mpi_ret = 
            MPI_Bcast( params->db_key, 512, MPI_BYTE, 0, MPI_COMM_WORLD ) )
            != MPI_SUCCESS ) 
    {
        fatal_error( stderr, rank, mpi_ret, NULL, "MPI_Bcast" );
    }
    if ( (mpi_ret = 
            MPI_Bcast( &(params->test_time), 1, MPI_LONG, 0, MPI_COMM_WORLD ) )
            != MPI_SUCCESS ) 
    {
        fatal_error( stderr, rank, mpi_ret, NULL, "MPI_Bcast" );
    }
    if ( rank != 0 ) {
        //fprintf( stderr, "%d Recv'd %s\n", rank, params->db_key );
    }

    //fprintf( stderr, "%d enter barrier\n", rank );
    if ( (mpi_ret = MPI_Barrier(MPI_COMM_WORLD)) != MPI_SUCCESS) {
        fatal_error( stderr, rank, mpi_ret, NULL, 
				"MPI_Barrier after writing data.\n" );
    }
    //fprintf( stderr, "%d exit barrier\n", rank );

#endif
}

// macros for turning macros into quoted strings
#define xstr(s) str(s)
#define str(s) #s

#define FULL_QUERY_SIZE 131072
#define QUERY_SIZE 65536
#define QUERY_PATH 512

// all procs need to make this call because they do allgathers to collect
// timing info in addDBTimes, also conclude with a Barrier to keep everybody
// sync'd
int
db_insert(  int my_rank,
            int last_errno,
            char *error_message,
            struct Parameters *params, 
            struct State *state,
            struct time_values *write_times,
            struct time_values *read_times )
{
#if defined(MYSQL_HOST) || defined(MYSQL_FILE)
    static int initial = 1;
    static int connected = 0;
    static char default_path[QUERY_PATH];
    char query[FULL_QUERY_SIZE];
    char values[QUERY_SIZE];
    char columns[QUERY_SIZE];
    char mixed[QUERY_SIZE];
    //char mpi_version[16];
    //char user[64];
#ifdef MYSQL_HOST
    MYSQL mysql;
    char *host  = xstr(MYSQL_HOST); 
    unsigned int connect_timeout = 15;
    char *query_file = NULL;
#else 
    char *host = NULL;
    char *query_file  = xstr(MYSQL_FILE);
#endif
    char *database = "mpi_io_test";
    char *table    = "experiment";
    char *mpihome;
    char *mpihost;
    char *segment;
    int  error_handler = 0;
    int successful_query_write = 0;
    int suppress_reminder = 0;
    //char *myversion = xstr( MPI_IO_TEST_VERSION );

    /* turn this off now bec we just allow whichever rank wants to push
       in the error message */
    /*
    // if we're rank 0 AND there is no error message or message is partial
    // check to see if anyone else has sent a message
    if (my_rank == 0 && (! error_message || ! strcmp(error_message, "partial")))
    {
        check_error_messages( params );
    }

    // if anyone sent an error message, use it
    if ( params->error_message ) {
        error_message = params->error_message;
    }
    */

    // are we in special mode where we just handle an error?
    if ( error_message ) {
        chomp(error_message);
        error_handler = 1;
        if ( ! strcmp( error_message, "partial" ) ) {
            error_handler = 0;
        }
            // only rank 0 should push the NULL at the end of the program
        if ( ! strcmp( error_message, "NULL" ) ) {
            if ( my_rank != 0 ) {
                return 1;
            }
        }
    }

    mpihome = getenv("MPIHOME");
    mpihost = getenv("MY_MPI_HOST");
    segment = getenv("HOSTNAME");
    values[0] = columns[0] = mixed[0] = '\0';  // null term

        // connect to db
    if ( my_rank == 0 || error_handler ) {
        if ( host == NULL ) {
            //assert( query_file );   // don't even try, just put in a file
            connected = 0;
        } else if ( mpihost && ! strcmp( mpihost, "flash" ) ) {
            // flash can't connect, don't even try
            connected = 0;
        } else

            // if connected == 1, this means we connected before, so try again
        if ( initial || connected ) {   // attempt to connect at least once
                                        // cant connect from bproc backend...
#ifdef MYSQL_HOST
            if ( my_rank != 0 ) {
                fprintf( stderr, "%d mysql_init with %s\n", my_rank,
                        error_message );
            }
            mysql_init(&mysql);
            mysql_options(&mysql, MYSQL_OPT_CONNECT_TIMEOUT,  
                    (char*) &connect_timeout);
            if (!mysql_real_connect(&mysql, 
                                    host,
                                    NULL,   // "mpi" for eureka
                                    NULL,   // "mpi" for eureka
                                    database,
                                    0,
                                    NULL,
                                    0 ) )
            {
                    // shoot, fatal_error would be recursive
                fprintf(stderr, "%d Failed to connect to database %s: %s (error %s)\n",
                    my_rank, host, mysql_error(&mysql), 
		    ( error_message ? error_message : "NULL" ) );
                connected = 0;  // only timeout on connect once
            } else {
                connected = 1;
                //debug_msg( stderr, 0, "Connected to db\n" );
            }
#else
            connected = 0;
#endif
        }
    }

        // create the query string
    if ( ! error_handler ) {
        if ( initial ) {
            addDBStr( QUERY_SIZE, columns, values, mixed, params->barriers,
                "barriers" );
            addDBStr( QUERY_SIZE, columns, values, mixed, params->user,         
                "user" );
            addDBStr( QUERY_SIZE, columns, values, mixed, params->mpiversion,   
                "mpi_version" );
#ifdef MPI_IO_TEST_VERSION2
            addDBStr( QUERY_SIZE, columns, values, mixed, MPI_IO_TEST_VERSION2,  
                "version" ); 
#else
            addDBStr( QUERY_SIZE, columns, values, mixed, MPI_IO_TEST_VERSION,  
                "version" ); 
#endif
            addDBStr( QUERY_SIZE, columns, values, mixed, params->system,       
                "system" );
            addDBStr( QUERY_SIZE, columns, values, mixed, params->hints,        
                "hints" );
            addDBStr( QUERY_SIZE, columns, values, mixed, params->tfname,       
                "target" );
            addDBStr( QUERY_SIZE, columns, values, mixed, params->ofname,       
                "output_file" );
            addDBStr( QUERY_SIZE, columns, values, mixed, params->experiment,   
                "description" );
            addDBStr( QUERY_SIZE, columns, values, mixed, mpihome,              
                "mpihome" );
            addDBStr( QUERY_SIZE, columns, values, mixed, segment,              
                "segment" );
            addDBStr( QUERY_SIZE, columns, values, mixed, mpihost,              
                "mpihost" );
            addDBStr( QUERY_SIZE, columns, values, mixed, params->datestr,      
                "yyyymmdd" );
            addDBStr( QUERY_SIZE, columns, values, mixed, params->full_args,    
                "full_args" );
            addDBStr( QUERY_SIZE, columns, values, mixed, params->full_hints,   
                "full_hints");
            addDBStr( QUERY_SIZE, columns, values, mixed, params->panfs,        
                "panfs" );
            addDBStr( QUERY_SIZE, columns, values, mixed, params->panfs_srv,    
                "panfs_srv" );
            addDBStr( QUERY_SIZE, columns, values, mixed, params->jobid,    
                "jobid" );
            addDBStr( QUERY_SIZE, columns, values, mixed, params->os_version,   
                "os_version" );
            addDBStr( QUERY_SIZE, columns, values, mixed, params->host_list,    
                "host_list" );
            addDBStr( QUERY_SIZE, columns, values, mixed, error_message,        
                "error" );
            addDBInt( QUERY_SIZE, columns, values, mixed, params->test_time,
                "date_ts"  );
            addDBInt( QUERY_SIZE, columns, values, mixed,params->df_perc_before,
                "df_perc_before"  );
            addDBInt( QUERY_SIZE, columns, values, mixed, params->panfs_threads,
                "panfs_threads");
            addDBInt( QUERY_SIZE, columns, values, mixed, params->test_type,   
                "test_type");
            addDBInt( QUERY_SIZE, columns, values, mixed, params->num_objs,  
                "num_objs"  );
            addDBInt( QUERY_SIZE, columns, values, mixed, params->obj_size,   
                "obj_size"  );
            addDBInt( QUERY_SIZE, columns, values, mixed, params->cbuf_size,  
                "cbuf_size" );
            addDBInt( QUERY_SIZE, columns, values,mixed,params->direct_io_flag, 
                "direct_io" );
            addDBInt( QUERY_SIZE, columns, values, mixed, params->ahead,       
                "ahead" );
            addDBInt( QUERY_SIZE, columns, values, mixed, params->touch, 
                "touch" );
            addDBInt( QUERY_SIZE, columns, values, mixed, params->strided_flag, 
                "strided" );
            addDBInt( QUERY_SIZE, columns, values, mixed, params->sync_flag,   
                "sync" );
            addDBInt( QUERY_SIZE, columns, values,mixed,params->write_only_flag,
                "write_only");
            addDBInt( QUERY_SIZE, columns, values, mixed,params->read_only_flag,
                "read_only" );
            addDBInt( QUERY_SIZE, columns, values, mixed, params->sleep_seconds,
                "sleep" );
            addDBInt( QUERY_SIZE, columns, values, mixed, last_errno,          
                "errno" );
            addDBInt( QUERY_SIZE, columns, values, mixed,params->aggregate_flag,
                "aggregate" );
            addDBInt( QUERY_SIZE, columns, values,mixed,params->collective_flag,
                "collective");
            addDBInt( QUERY_SIZE, columns, values,mixed,params->num_procs_world,
                "num_hosts" );
            addDBInt( QUERY_SIZE, columns, values, mixed, params->set_view_all, 
                "set_view_all");
            addDBInt( QUERY_SIZE, columns, values, mixed, params->num_procs_io, 
                "num_procs_io");
            addDBInt( QUERY_SIZE, columns, values, mixed, params->prealloc_size,
                "prealloc_size" );
            addDBInt( QUERY_SIZE, columns, values, mixed, params->prealloc_flag,
                "prealloc_flag" );
            addDBInt( QUERY_SIZE, columns, values, mixed, params->barrier_flag,
                "barrier" );
            addDBInt( QUERY_SIZE, columns, values, mixed, params->barrier_stat,
                "barrier_stat" );
            addDBInt( QUERY_SIZE, columns, values, mixed, 
                    params->barrier_before_open,   
                "barrier_before_open" );
            addDBInt( QUERY_SIZE, columns, values, mixed, 
                    params->shift_flag,   
                "read_shift" );
            addDBInt( QUERY_SIZE, columns, values, mixed, 
                    params->time_limit,   
                "time_limit" );
            addDBInt( QUERY_SIZE, columns, values, mixed,
                    params->barrier_after_open_flag,
                "barrier_open" );
            addDBInt( QUERY_SIZE, columns, values, mixed, params->posix_io,   
                "posix_io" );
            addDBInt( QUERY_SIZE, columns, values, mixed, params->random_io,   
                "random_io" );
            addDBInt( QUERY_SIZE, columns, values, mixed, 
                    params->check_data_ndx,  "check_data_ndx" );
            addDBInt( QUERY_SIZE, columns, values, mixed, params->superblocks,  
                "superblocks" );
            addDBFloat( QUERY_SIZE, columns, values, mixed, 
            ((float)params->num_objs* params->obj_size* params->num_procs_world
             * params->superblocks )
                / (1024.0 * 1024), "total_size_mb" );
            addDBFloat( QUERY_SIZE, columns, values, mixed, 
                    params->procs_per_node, "procs_per_node" );
        }

        if ( write_times ) {
            addDBTimes( my_rank, params->num_procs_world,
                QUERY_SIZE, columns, values, mixed, write_times, "write" );

            // need to add state->total_mbs here when time_limit
            if ( params->time_limit ) {
                addDBFloat( QUERY_SIZE, columns, values, mixed, 
                        (float)state->total_mbs,
                        "total_size_mb" );
                addDBInt( QUERY_SIZE, columns, values, mixed, state->ave_objs,  
                        "num_objs"  );
                addDBInt( QUERY_SIZE, columns, values, mixed, state->min_objs, 
                        "num_objs_min"  );
                addDBInt( QUERY_SIZE, columns, values, mixed, state->max_objs,
                        "num_objs_max"  );
                addDBInt( QUERY_SIZE, columns, values, mixed, params->supersize,
                        "supersize" );
            }
        }
        if ( read_times ) {
            addDBTimes( my_rank, params->num_procs_world,
                QUERY_SIZE, columns, values, mixed, read_times,  "read" );
        }

        if ( params->panfs_info_valid ) {
            addDBInt( QUERY_SIZE, columns, values, mixed,
                    params->panfs_type, "panfs_type" );
            addDBInt( QUERY_SIZE, columns, values, mixed,
                    params->panfs_stripe, "panfs_stripe" );
            addDBInt( QUERY_SIZE, columns, values, mixed,
                    params->panfs_width, "panfs_width" );
            addDBInt( QUERY_SIZE, columns, values, mixed,
                    params->panfs_depth, "panfs_depth" );
            addDBInt( QUERY_SIZE, columns, values, mixed,
                    params->panfs_comps, "panfs_comps" );
            addDBInt( QUERY_SIZE, columns, values, mixed,
                    params->panfs_visit, "panfs_visit" );
            addDBInt( QUERY_SIZE, columns, values, mixed,
                    params->panfs_concurrent_write, "panfs_cw" );
        }

        int ret = 0;
        if (strlen(columns)>=(QUERY_SIZE-1) || strlen(values)>=(QUERY_SIZE-1)) {
            fprintf( stderr, "Need to grow db strings\n" );
            MPI_Abort( MPI_COMM_WORLD, -1 );
        }
        if ( initial ) {
            ret = snprintf( query, FULL_QUERY_SIZE, 
                    "INSERT INTO %s (%s) VALUES (%s)", 
                table, columns, values ); 
        } else {
            ret = snprintf( query, FULL_QUERY_SIZE, 
                    "UPDATE %s set %s WHERE %s", 
                table, mixed, params->db_key );
        }
    } else {
        if ( ! strcmp( error_message, "NULL" ) ) {  // this means clear the
                                                    // partial
                // add additional info to only clear if set to partial
                // so if someone else (like in negative number) set a 
                // non-fatal error, we won't overwrite.
            int ret = snprintf( query, FULL_QUERY_SIZE, 
                "UPDATE %s set error = NULL WHERE %s && error like 'partial'",
                table, params->db_key ); 
            suppress_reminder = 1;  // a reminder was just set after the read
            error_handler = 1;  // only rank 0 should send this one
                                // everyone else already exited
            if ( ret >= FULL_QUERY_SIZE ) {
                fprintf( stderr, "query truncated!!! NEED TO FIX\n" );
                MPI_Abort( MPI_COMM_WORLD, -1 );
            }
        } else {
            // we could concatenate here to merge multiple error states
            // but this would also concatenate with the partial
            // so first we need to clear the partial and then we
            // need to concatenate the error string and use coalesce so
            // it gets rid of the NULL
            //UPDATE table SET fieldName = 
            // CONCAT(fieldName, 'New Data To Add Here') WHERE ...
            //UPDATE table SET field = 
            // CONCAT(COALESCE(field, ''), 'New Data') WHERE ...
            int ret = snprintf( query, FULL_QUERY_SIZE, 
                "UPDATE %s set error=CONCAT(COALESCE(error,''),\'%s\'),"
                "errno=%d WHERE %s",
                table, error_message, last_errno, params->db_key ); 
            if ( ret >= FULL_QUERY_SIZE ) {
                fprintf( stderr, "query truncated!!! NEED TO FIX\n" );
                MPI_Abort( MPI_COMM_WORLD, -1 );
            }
        }
    }

        // send the query string
    if ( my_rank == 0 || error_handler ) {
        if ( connected ) {
#ifdef MYSQL_HOST
            if ( mysql_query( &mysql, query ) != 0 ) {
                fprintf(stderr, "%d Failed to send query %s: %s\n",
                    my_rank, query, mysql_error(&mysql));
            } else {
                successful_query_write = 1;
            }
            mysql_close( &mysql );
#endif
        } else {
            if ( ! query_file ) {
                query_file = default_path; 
                snprintf( query_file, 512, "%s/db_up", getenv("HOME") );
            }
            assert( query_file );
            FILE *fp = fopen( query_file, "a" );
            if ( ! fp ) {
                fprintf(stderr, 
                        "Failed to open %s for append: %s\n", 
                        query_file, strerror(errno));
            } else {
                lockf( fileno( fp ), F_LOCK, 0 ); // lock
                fprintf( fp, "%s;\n", query );    // write
                rewind( fp ); // seek to 0, so unlock will work
                lockf( fileno( fp ), F_ULOCK, 0 ); // unlock
                fclose( fp );
                successful_query_write = 1;
            }
        }

        if ( successful_query_write ) {
            if ( connected ) {
                debug_msg( stdout, my_rank, "INSERT'd %ld -> mysql %s:%s:%s\n",
                    params->test_time, host, database, table );
            } else {
                if ( ! suppress_reminder ) {
                    debug_msg(stdout, my_rank, 
                        "Query in %s needs to be uploaded\n", query_file );
                }
            }
        } else {
            debug_msg( stdout, my_rank, "Was unable to write query\n" );
        }

        initial = 0;
    }

        // this is the normal case
        // do a barrier to get everybody sync'd for the next operation
        // if error_handler, means some individual rank by themselves
        // has failed and is doing this call individually, so a Barrier
        // would not be good.  But in the normal (i.e. non-error) case,
        // everybody comes here so do a barrier so they're all sync'd up
        // afterwards so any subsequent timed open's are not skewed 
        // because of rank 0 being the only one doing the db query
    if ( ! error_handler ) {
      MPI_Barrier(MPI_COMM_WORLD);
    }
    return 1;    
#else
    return 1;
#endif
}

ssize_t 
Write(int fd, const void *vptr, size_t nbyte ) {
    size_t      nleft;
    ssize_t     nwritten = 0;
    const void  *ptr;

    ptr = vptr;
    nleft = nbyte;
    while (nleft > 0) {
        ssize_t this_write = 0;
        if ( (this_write = write(fd, ptr, nleft ) ) <= 0) {
            if (errno == EINTR) {
                nwritten = 0;       /* and call write() again */
            } else {
                printf( "write to %d error: %s\n", fd, strerror(errno) );
                return(-1);         /* error */
            }
        }
        nleft    -= this_write;
        ptr      += this_write;
        nwritten += this_write;
    }
    assert( (size_t)nwritten == nbyte );
    return(nbyte);
}

ssize_t 
Read(int fd, void *vptr, size_t nbyte ) {
    size_t      nleft;
    ssize_t     nread = 0;
    void  *ptr;

    ptr = vptr;
    nleft = nbyte;
    while (nleft > 0) {
        ssize_t this_read = 0;
        if ( (this_read = read(fd, ptr, nleft ) ) <= 0) {
            if (errno == EINTR) {
                nread = 0;       /* and call write() again */
            } else {
                printf( "read from %d error: %s\n", fd, strerror(errno) );
                return(-1);         /* error */
            }
        }
        nleft    -= this_read;
        ptr      += this_read;
        nread    += this_read;
    }
    assert( (size_t)nread == nbyte );
    return(nbyte);
}

ssize_t 
Pwrite64(int fd, const void *vptr, size_t nbyte, off_t offset) {
    size_t      nleft;
    ssize_t     nwritten = 0;
    const void  *ptr;

    ptr = vptr;
    nleft = nbyte;
    while (nleft > 0) {
        //printf( STDERR, "pwrite64 %d %x %lld %lld
	ssize_t this_write = 0;
//        if ( (this_write = pwrite64(fd, ptr, nleft, offset + nwritten) ) <= 0) {
        if ( (this_write = pwrite(fd, ptr, nleft, offset + nwritten) ) <= 0) {
            if (errno == EINTR) {
                nwritten = 0;       /* and call write() again */
            } else {
                printf( "pwrite to %lld error: %s\n", 
                        (long long)offset,
                        strerror(errno) );
                return(-1);         /* error */
            }
        }
        nleft    -= this_write;
        ptr      += this_write;
	nwritten += this_write;
    }
    assert( (size_t)nwritten == nbyte );
    //printf( "pwrite64 successful!\n" );
    return(nbyte);
}

ssize_t 
Pread64(int fd, void *vptr, size_t nbyte, off_t offset) {
    size_t      nleft;
    ssize_t     nread = 0;
    const void  *ptr;

    ptr = vptr;
    nleft = nbyte;
    while (nleft > 0) {
//        if ( (nread = pread64(fd, ptr, nleft, offset + nread) ) <= 0) {
        if ( (nread = pread(fd, ptr, nleft, offset + nread) ) <= 0) {
            if (errno == EINTR)
                nread = 0;       /* and call write() again */
            else
                return(-1);         /* error */
        }
        nleft -= nread;
        ptr   += nread;
    }
    assert( (size_t)nread == nbyte );
    return(nbyte);
    /*
ssize_t 
Pread64(int fildes, void *buf, size_t nbyte, off_t offset) {
	int ret;
	do {
		ret = pread64( fildes, buf, nbyte, offset );
		if ( ret == EAGAIN ) {
			perror( "pwrite64" );
		}
	} while ( ret == EAGAIN ); 
    return nbyte;
    */
}

void
add_mpi_error_string( char *msg, int msg_len, int mpi_err ) {
    // stupid error strings are not valid in mpich and don't work in
    // lampi, because program is directly aborted by romio lib
    // just save the number . . . 
    snprintf( &msg[strlen(msg)], msg_len - strlen(msg),
                " (MPI_Error = %d)", mpi_err );
    return;
    /*
    char err_msg[MPI_MAX_ERROR_STRING];
    int err_len, error_class;
    MPI_Error_string( mpi_err, err_msg, &err_len );
    snprintf( &msg[strlen(msg)], msg_len - strlen(msg),
                "\nMPI_Error_string(%d)= %s", mpi_err, err_msg );
    fprintf( fp, "mpi error %d = %s\n", mpi_err, err_msg );
    MPI_Error_class(mpi_err, &error_class);
    MPI_Error_string( error_class, err_msg, &err_len );
    snprintf( &msg[strlen(msg)], msg_len - strlen(msg),
            "\nMPI_Error_string(class %d)=%s", error_class, err_msg );
    fprintf( fp, "mpi code %d = %s\n", error_class, err_msg );
    */
}
    

#define ERR_MSG_LEN 4096

int
make_error_messages( int my_rank, char *hdr, char *full, char *query, int len, 
        const char *format, va_list args, MPI_Status *mstat, int mpi_ret )
{
    char myname[256];
    char *newline  = NULL;
    memset( full, 0, len );
    memset( query, 0, len );
    gethostname( myname, 256 );
    vsnprintf( query, len, format, args );
    va_end( args );
    chomp( query );
    if ( errno ) {
        snprintf( &query[strlen(query)], 
                len - strlen(query),
                " (errno=%s)", strerror(errno) );
    }
    if ( mstat && mstat->MPI_ERROR != MPI_SUCCESS ) {
        add_mpi_error_string( query, len, mstat->MPI_ERROR );
    }
    if ( mpi_ret != MPI_SUCCESS ) {
        add_mpi_error_string( query, len, mpi_ret );
    }
    snprintf( full, len, "Rank %d Host %s %s %ld: %s", 
            my_rank, myname, hdr, time(NULL), query );
    strcpy( query, full );

    /*
       // was trying to send error message to rank 0, but problem is
       // that rank 0 can't catch the MPI_Abort . . . 
    if ( my_rank == 0 ) {
    } else {
        if ( MPI_Send( solo_error, strlen(solo_error), MPI_BYTE, 0, ERROR_TAG, 
                    MPI_COMM_WORLD) != MPI_SUCCESS ) 
        {
            fprintf(fp, "%d Couldn't send %s to rank 0\n", my_rank, solo_error);
        }
    }
    */

    while( ( newline = strstr( query, "\n" ) ) != NULL ) {
        *newline = '.';
    }
    if ( strstr( query, "\n" ) ) {
        fprintf( stderr, "Need to remove all newlines before sending to db\n" );
    }
        // append a comma
    snprintf( &query[strlen(query)], len - strlen(query), "," );

    /*
    MPI_Errhandler_set(MPI_COMM_WORLD, MPI_ERRORS_RETURN);
    error_code = MPI_Send(send_buffer, strlen(send_buffer) + 1, MPI_CHAR,
                              addressee, tag, MPI_COMM_WORLD);
    if (error_code != MPI_SUCCESS) {
        char error_string[BUFSIZ];
        int length_of_error_string, error_class;
        MPI_Error_class(error_code, &error_class);
        MPI_Error_string(error_class, error_string, &length_of_error_string);
        fprintf(stderr, "%3d: %s\n", my_rank, error_string);
        MPI_Error_string(error_code, error_string, &length_of_error_string);
        fprintf(stderr, "%3d: %s\n", my_rank, error_string);
        send_error = TRUE;
    }
    */
    return 0;
}

int
warning_msg( FILE *fp, int my_rank, int mpi_ret, MPI_Status *mstat,
        const char *format, ... ) 
{
    char message[ERR_MSG_LEN];
    char query[ERR_MSG_LEN];
    va_list args;   
    va_start( args, format );
    make_error_messages( my_rank, "WARNING ERROR", message, query, 
            ERR_MSG_LEN, format, args, mstat, mpi_ret );
    fprintf( fp, "%s\n", message );
    fflush( fp );
    if(params.trenddata){
       fprintf(stderr,"FAILED\n");
       exit(1);
    }
#if defined(MYSQL_HOST) || defined(MYSQL_FILE)
    // ugh, we need to get params->nodb in here ...e
    db_insert( my_rank, errno, query, &params, NULL, NULL, NULL );
#endif
    return 0;
}


void
setTime( double *value, double initial, double end_time, double start_time,
         char *name, char *file, int line, FILE *efptr, int my_rank ) 
{
    if (initial < 0 || start_time < 0 || end_time < 0 || end_time < start_time){
            // do a warning msg and then allow to continue
        warning_msg( efptr, my_rank, 0, NULL,
                "Negative value passed from %s:%d for %s.  "
                "Init %f, end %f, start %f. "
                "MPI_WTIME_IS_GLOBAL %d", 
                file, line, name, initial, end_time, start_time,
                MPI_WTIME_IS_GLOBAL );
        *value = 0;
    } else {
        *value = initial + ( end_time - start_time );
    }
}

void
file_system_check( int my_rank ) {
    char *new_file;
    char myname[256];
    int fd;
    int my_errno = 0;
    char *my_err_msg = "";
    gethostname( myname, 256 );
    new_file = (char*)Malloc(strlen(params.tfname) + 10  + strlen(myname), 
            stderr, my_rank );
    memset( new_file, 0, strlen(params.tfname + 10 + strlen(myname)) );
    strcpy( new_file, params.tfname );
    sprintf( new_file, params.tfname );
    sprintf( &(new_file[strlen(new_file)]), ".%d.%s", my_rank, myname );

    fd = open( new_file, O_WRONLY | O_CREAT | O_TRUNC, 0x666 );
    if ( fd > 0 ) {
        unlink( new_file );
    } else {
        my_errno = errno;
        my_err_msg = strerror(errno);
    }
    fprintf( stderr, "FNF: Rank %d Host %s.  Open %s: %d %s\n",
            my_rank, myname,
            new_file, my_errno, my_err_msg );

        // enter infinite loop to try to preserve system state
    /*
    fprintf( stderr, "FNF: Rank %d Host %s.  Entering infinite loop.\n",
            my_rank, myname );
    while( 1 ) { }
    */
}

void 
insert_panfs_info( char *key, char *value ) {
    if ( ! strcmp( key, "panfs_layout_type" ) ) {
        params.panfs_type = atoi(value);
    } else if ( ! strcmp( key, "panfs_layout_stripe_unit" ) ) {
        params.panfs_stripe = atoi(value);
    } else if ( ! strcmp( key, "panfs_layout_parity_stripe_width" ) ) {
        params.panfs_width = atoi(value);
    } else if ( ! strcmp( key, "panfs_layout_parity_stripe_depth" ) ) {
        params.panfs_depth = atoi(value);
    } else if ( ! strcmp( key, "panfs_layout_total_num_comps" ) ) {
        params.panfs_comps = atoi(value);
    } else if ( ! strcmp( key, "panfs_layout_visit_policy" ) ) {
        params.panfs_visit = atoi(value);
    } else if ( ! strcmp( key, "panfs_concurrent_write" ) ) {
        params.panfs_concurrent_write = atoi(value);
    } else {
        fprintf( stderr, "Unknown panfs_key %s -> %s", key, value );
    }
    params.panfs_info_valid = 1;
}

int
fatal_error( FILE *fp, int my_rank, int mpi_ret, MPI_Status *mstat, 
		const char *format, ... ) 
{
    
    char message[ERR_MSG_LEN];
    char query[ERR_MSG_LEN];
    va_list args;   
    va_start( args, format );
    make_error_messages( my_rank, "FATAL ERROR", message, query, 
            ERR_MSG_LEN, format, args, mstat, mpi_ret );
    fprintf( fp, "%s\n", message );
    fflush( fp );
        // who cares, leave the damn partial in there....
        // doesn't work anyway maybe . . . makes it hang when it tries barrier
    //db_insert( my_rank, 0, "NULL", &params, NULL, NULL ); // clear the partial
    if ( ! params.db_key ) {
        fprintf( stderr, "Doing a fatal error before we set the db_key. Insert impossible\n" );
    } else {
        db_insert( my_rank, errno, query, &params, NULL, NULL, NULL );
    }
    if ( errno == ENOENT ) {
        file_system_check( my_rank );
    }
        // turn this off for speeding up debugging
    fprintf(fp,"[RANK %d] Waiting 60secs\n", my_rank);
    sleep( 60 );    // allow other nodes to fail or succeed first
    fflush(fp);

    if(!params.trenddata){
       fprintf(stderr, "FAILED\n");
    }

    MPI_Abort( MPI_COMM_WORLD, -1 );
    return 0;
}

int
debug_msg( FILE *fp, int my_rank, const char *format, ... ) {
    va_list args;   
    va_start( args, format );
    fprintf( fp, "Rank [%d] DEBUG: ", my_rank );
    vfprintf( fp, format, args );
    fflush( fp );
    va_end( args );
    return 1;
}

/*******************************************
 *   ROUTINE: get_min_sum_max
 *   PURPOSE: Collect minimum, sum, and maximum of input value across all 
 *            processors. Only processor with rank 0 will have the value and 
 *            rank of the processor with the maximum, minimum, and sum
 *******************************************/
int 
get_min_sum_max(int my_rank, 
            double base_num, double *min, int *min_ndx,
		    double *sum, double *max, int *max_ndx, char *op, 
		    FILE *ofptr, FILE *efptr)
{
  int mpi_ret;
  struct{
    double dval;
    int rank;
  } in, out;
  
  in.dval = base_num;
  in.rank = my_rank;
  
  if( (mpi_ret = MPI_Reduce(&in, &out, 1, MPI_DOUBLE_INT,
		MPI_MAXLOC, 0, MPI_COMM_WORLD)) != MPI_SUCCESS) 
  {
      fatal_error( efptr, my_rank, mpi_ret, NULL,
			  "Unable to find (reduce) maximum %s.\n", op);
  }
  
  *max = out.dval;
  *max_ndx = out.rank;
  MPI_Barrier(MPI_COMM_WORLD);

  if( (mpi_ret = MPI_Reduce(&base_num, sum, 1, MPI_DOUBLE,
		MPI_SUM, 0, MPI_COMM_WORLD)) != MPI_SUCCESS) 
  {
      fatal_error( efptr, my_rank, mpi_ret, NULL,
			  "Unable to find (reduce) sum of %s.\n", op);
  }
  
  MPI_Barrier(MPI_COMM_WORLD);

  if( (mpi_ret = MPI_Reduce(&in, &out, 1, MPI_DOUBLE_INT,
		MPI_MINLOC, 0, MPI_COMM_WORLD)) != MPI_SUCCESS) 
  {
      fatal_error( efptr, my_rank, mpi_ret, NULL,
			  "Unable to find (reduce) minimum %s.\n", op);
  }
  
  *min = out.dval;
  *min_ndx = out.rank;
  
  return MPI_SUCCESS;
}


