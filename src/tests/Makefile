#
# TokuFS
#

# Set debug before including common.mk, which
# decides if we should use debuggin symbols
# in CFLAGS or not
DEBUG = 1

include ../../common/common.mk

VALGRIND = 1
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

CPPFLAGS += -I../ -I../../include -DENV_PATH='"$*.env"'
LDFLAGS += -Wl,-rpath,$(PREFIX)/lib
LDFLAGS += -L$(PREFIX)/lib -pthread -ltokudb -ltokuportability
LDFLAGS += ../bstore.o

OBJECTS := $(patsubst %.c, %, $(wildcard *.c))
TARGETS = $(OBJECTS)

.PHONY: all tidy clean 

all: $(OBJECTS)

check: tidy all $(patsubst %, %.check, $(TARGETS)) tidy

%.check: %
	@($(VALGRIND_BIN) ./$* $(MAYBE_REDIRECT_OUTPUT) \
		&& echo pass: $*) || echo fail: $*

tidy:
	rm -rf *.env

clean: tidy
	rm -rf $(patsubst %.c, %, $(wildcard *.c))
	
