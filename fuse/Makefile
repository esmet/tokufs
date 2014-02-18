#
# TokuFS
#

include ../common/common.mk

INCDIR = ../include
LIBDIR = ../lib

CFLAGS += -I$(INCDIR) -D_XOPEN_SOURCE=500 -Wno-unused-parameter
CFLAGS += $(shell pkg-config fuse --cflags)
LDFLAGS += -Wl,-no-as-needed
LDFLAGS += -L$(LIBDIR) -Wl,-rpath,$(LIBDIR) -ltokufs -L/home/ftfs/local/lib/ -ltokufractaltree -ltokuportability
LDFLAGS += $(shell pkg-config fuse --libs)

OBJECTS := $(patsubst %.c, %, $(wildcard *.c))
TARGETS := $(OBJECTS)

.PHONY: all tidy clean

all: $(TARGETS)

tidy:
	rm -rf *.mount

clean: tidy
	$(MAKE) -C fsx clean
	rm -f $(OBJECTS)

