#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <limits.h>

void
Usage( char *prog, int line ) {
    fprintf( stderr, "Usage (line %d): %s -avqp\n", line, prog );
    exit( 0 );
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
    /*
    fprintf( stderr, "%s %lf \n\tchars(", header, one );

    for( c = 0; c < sizeof(double) / sizeof(char); c++ ) {
	fprintf( stderr, "%c", c_one[c] );
    }
    fprintf( stderr, ")\n\tbinary(" );
    */

	for( c = 0; c < sizeof(double) / sizeof(int); c++ ) {
		//bin_prnt_int( b[c] );
		//fprintf( stderr, " " );
		fprintf( stdout, "%s ", itob( b[c] ) );
	}	
    fprintf( stdout, "\n" );
    //fprintf( stderr, ")\n" );
}

void
print_char( char *header, char one ) {
    char *buf = itob( one );
    if ( header ) {
        fprintf( stdout, "%s ", header );
    }
    //buf += (sizeof(*buf)-8); // why doesn't this work?
    buf += 24;
    fprintf( stdout, "%s\n", buf );
}

int
main( int argc, char **argv ) {
    char *prog = strdup( argv[0] );

    int arg = 0;
    bool anon = 0, verbose = 0, quiet = 0;
    double d = 0;
    char *chars = NULL;
    char ch = 0;
    int c;
    while( ( c = getopt( argc, argv, "avqp:d:c:" )) != EOF ) {
        switch( c ) {
        case 'a':
            anon = 1;
            break;
        case 'v':
            verbose = 1;
            break;
        case 'q':
            quiet = 1;
            break;
        case 'c':
            chars = strdup( optarg );
            break;
        case 'p':
            ch = atoi( optarg );
            break;
        case 'd':
            d = atof( optarg );
            break;
        default:
            Usage( prog, __LINE__ );
        }
    }
    arg = optind;

    if ( chars ) {
        char *this_char = strtok( chars, "," );
        while( this_char ) {
            char character = atoi(this_char);
            print_char( NULL, character );
            this_char = strtok( NULL, "," );
        }
    } else if ( ch ) {
        print_char( NULL, ch );
    } else {
        print_double( "", d );
    }

    return 0;
}
