#
# TokuFS
#

include ../common/common.mk

PREFIX = $(HOME)/local

LDFLAGS = -L$(PREFIX)/lib -Wl,-rpath,$(PREFIX)/lib
LDFLAGS += -L../lib -Wl,-rpath,../lib
LDFLAGS += -ltokudb -ltokuportability -ltokufs

TARGETS = $(patsubst %.c, %, $(wildcard *.c))

.PHONY: all clean 

all: $(TARGETS)

clean:
	rm -rf *.o $(TARGETS)
