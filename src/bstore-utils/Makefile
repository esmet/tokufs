#
# TokuFS
#

PREFIX = $(HOME)/local

INC_DIR = ../ 

CC = gcc

IFLAGS = -I$(INC_DIR)
CFLAGS = -W -Wall -Wextra -g $(IFLAGS)
LFLAGS += -Wl,-rpath,$(PREFIX)/lib
LFLAGS += -L$(PREFIX)/lib -lthread -ltokudb -ltokuportability

OBJECTS := $(patsubst %.c, %.o, $(wildcard *.c))

.PHONY: all tidy clean 

all: $(OBJECTS)

%.o: %.c
	$(CC) $(CFLAGS) $(LFLAGS) -o $* $^ ../bstore.o ../system.o

tidy:
	rm -rf *.env

clean: tidy
	rm -rf $(patsubst %.c, %, $(wildcard *.c))
	
