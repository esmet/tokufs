#
# TokuFS
#

include ../common/common.mk

INCDIR = ../include
LIBDIR = ../lib

LIBTOKUFS = libtokufs.so
LIBTOKUFS_PATH := $(LIBDIR)/$(LIBTOKUFS)

CFLAGS += -fPIC
CPPFLAGS += -I$(INCDIR) -I$(PREFIX)/include
LDFLAGS += -shared -Wl,-soname,$(LIBTOKUFS) -fPIC
LDFLAGS += -Wl,-rpath,$(PREFIX)/lib -L$(PREFIX)/lib -pthread

ifeq ($(BDB), 1)
	LD_LIBS += -ldb
else
	LD_LIBS += -ltokudb -ltokuportability
endif

BLOCKSIZE = 0
ifneq ($(BLOCKSIZE), 0)
	CPPFLAGS += -DBSTORE_BLOCKSIZE=$(BLOCKSIZE)
endif

BDB = 0
ifeq ($(BDB), 1)
	CPPFLAGS += -DUSE_BDB
endif

OBJECTS = $(patsubst %.c, %.o, $(wildcard *.c))
TARGETS = $(LIBTOKUFS_PATH)

.PHONY: all check clean syntax

all: $(LIBTOKUFS_PATH)
syntax: CFLAGS+=--syntax-only
syntax: all

$(LIBTOKUFS_PATH): $(OBJECTS)
	$(LD) $(LDFLAGS) -o $(LIBTOKUFS_PATH) $(LD_LIBS) $^

check: $(OBJECTS)
	$(MAKE) -k -s -C tests check

clean: 
	rm -rf *.o $(LIBTOKUFS_PATH) $(OBJECTS)
	$(MAKE) -C tests clean

