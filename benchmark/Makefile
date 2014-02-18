#
# TokuFS
#

include ../common/common.mk

INCDIR = ../include
LIBDIR = ../lib

CPPFLAGS += -I$(INCDIR) -D_XOPEN_SOURCE=500 -I/home/ftfs/local/include/
LDFLAGS += -L$(LIBDIR) -Wl,--rpath,$(LIBDIR) 
LDLIBS += -ltokufs -pthread -L/home/ftfs/local/lib/ -ltokufractaltree -ltokuportability

OBJECTS := $(patsubst %.c, %, $(wildcard *.c))
# threadpool is not going to be target
TARGETS := $(patsubst threadpool, , $(OBJECTS))

.PHONY: all clean

all: $(TARGETS)

# override the default so we can depend on threadpool.o
benchmark-fs-threaded: benchmark-fs-threaded.c threadpool.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) $^ $(LDLIBS)  -o $@


tidy:
	rm -rf *.mount

clean: tidy
	rm -rf $(TARGETS) benchmark-bucket
