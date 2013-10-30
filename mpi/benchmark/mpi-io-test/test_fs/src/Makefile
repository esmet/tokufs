#
# Makefile for Linux machines 
#

LARGE_FILE  = -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE=1 -D__USE_LARGEFILE64=1
UTILS_DIR   = ../../utilities/src
#BENCH_COMM  =-DCRBENCH_COMM=MPI_COMM_WORLD


# get MPI_CC, MPI_INC, and MPI_LD from environment
ifdef MPI_CC 
	CC		= ${MPI_CC}	
else
	CC      = mpicc
endif
CFLAGS  = -g -I. ${MPI_INC} -DROMIO -I$(UTILS_DIR) ${LARGE_FILE} -DMPI_IO_TEST_VERSION2='"1.00.021"' -Wall
CLIBS   = ${MPI_LD} -lm 

MPIIOOBJS	=	
UTILS		=	$(UTILS_DIR)/print.o $(UTILS_DIR)/utilities.o

all:    fs_test.$(MY_MPI_HOST).x

fs_test.$(MY_MPI_HOST).x: fs_test.x
	cp $< $@

fs_test.x:	$(OBJS) $(UTILS) $(MPIIOOBJS) fs_test.c
	$(CC) $(CFLAGS) $(CLIBS) $(BENCH_COMM) \
		fs_test.c \
		$(MPIIOOBJS) $(UTILS) $(CLIBS) \
		-o $@

%.o : %.c
	$(CC) $(CFLAGS) -c $? -o $@

$(UTILS_DIR)/%.o: $(UTILS_DIR)/%.c
	$(CC) $(CFLAGS) -c $? -o $@

tidy: 
	- /bin/rm -rf *.mount

clean: tidy
	- /bin/rm -f *~
	- /bin/rm -f *.o $(UTILS_DIR)/*.o
	- /bin/rm -f fs_test.x
	- /bin/rm -f fs_test.$(MY_MPI_HOST).x
