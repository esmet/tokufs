#
# TokuFS
#

PREFIX = $(HOME)/local

CC = gcc
LD = gcc

CFLAGS += -Wall -Wextra -Wshadow #-Werror -Wshadow
CFLAGS += -Wmissing-prototypes -Wmissing-declarations  
CFLAGS += -std=c99

ifeq ($(DEBUG), 1)
	CFLAGS += -g3 -O0 -DDEBUG
else
	CFLAGS += -g3 -O3
ifeq ($(FLTO), 1)
	CFLAGS += -flto
	LDFLAGS += -flto
endif
endif

VALGRIND_BIN = valgrind --leak-check=full --show-reachable=yes
VALGRIND_BIN += --error-exitcode=1
