#
# TokuFS
#

DEBUG = 1
include ../common/common.mk

VALGRIND = 1
# if valgrind != 1, then let the valgrind string be empty
ifneq ($(VALGRIND), 1)
	VALGRIND_BIN =
else
	VALGRIND_BIN := $(VALGRIND_BIN) --suppressions=valgrind.suppressions
endif


ifeq ($(VERBOSE), 1)
	MAYBE_REDIRECT_OUTPUT =
else
	MAYBE_REDIRECT_OUTPUT = > /dev/null 2>&1 
endif

INCDIR = ../include
LIBDIR = ../lib

CPPFLAGS += -DMOUNT_PATH=\"$@.mount\" -I$(INCDIR)
LDFLAGS += -L$(LIBDIR) -Wl,-rpath,$(LIBDIR) -ltokufs

OBJECTS := $(patsubst %.c, %, $(wildcard *.c))
TARGETS = $(OBJECTS)

.PHONY: all check tidy clean 

all: $(OBJECTS)

check: tidy all $(patsubst %, %.check, $(TARGETS)) tidy

%.check: %
	@($(VALGRIND_BIN) ./$* $(MAYBE_REDIRECT_OUTPUT) && echo pass: $*) \
		|| echo fail: $*

tidy:
	rm -rf *.mount

clean: tidy
	rm -rf *.o $(OBJECTS)
	
