#
# TokuFS
#

INC_DIR = ../../include/ 
LIB_DIR = ../../lib/

CC = mpicc

IFLAGS = -I$(INC_DIR)
CFLAGS = -W -Wall -Wextra -g $(IFLAGS)
LFLAGS = -L$(LIB_DIR) -Wl,--rpath,$(LIB_DIR) -ltokufs

OBJECTS = $(patsubst %.c, %.o, $(wildcard *.c))

.PHONY: all clean 

all: $(OBJECTS)

%.o: %.c
	$(CC) $(CFLAGS) $(LFLAGS) -o $* $^

tidy:
	rm -rf *.mount

clean: tidy
	rm -rf $(patsubst %.o, %, $(OBJECTS))

