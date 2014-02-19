#
# TokuFS
#

include ../common/common.mk

PREFIX = $(HOME)/local


CPPFLAGS = -I$(PREFIX)/include/
LDFLAGS = -L../lib -Wl,-rpath,../lib
LDFLAGS +=  -ltokufs
LDFLAGS += -L$(PREFIX)/lib -Wl,-rpath,$(PREFIX)/lib
LDFLAGS += -ltokufractaltree -ltokuportability
TARGETS = $(patsubst %.c, %, $(wildcard *.c))

.PHONY: all clean 

all: $(TARGETS)

$(TARGETS): %: %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -o $@ $< $(LDFLAGS)
clean:
	rm -rf *.o $(TARGETS)
