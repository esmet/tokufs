#
# TokuFS
#

include ../common/common.mk

INCDIR = ../include
LIBDIR = ../lib

CFLAGS += -I$(INCDIR)
#LDFLAGS += -L$(LIBDIR) -Wl,--rpath,$(LIBDIR) -ltokufs

#SUBDIRS = mpi

OBJECTS := $(patsubst %.c, %, $(wildcard *.c))
TARGETS := $(OBJECTS) $(patsubst %, %.makesubdir, $(SUBDIRS))

.PHONY: all clean 

all: $(TARGETS)

%.makesubdir:
	@$(MAKE) -C $*

%.cleansubdir:
	@$(MAKE) -C $* clean

clean: $(patsubst %, %.cleansubdir, $(SUBDIRS))
	rm -f $(OBJECTS)
