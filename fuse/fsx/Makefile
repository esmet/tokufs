CC = gcc
LD = gcc

CFLAGS = -Wall -Wextra -O3 -Wno-sign-compare

# 8mb operations over a 1.25gb file
FSXFLAGS = -c 20 -o 8388608 -l 1342177280 -s 1 -W -R -P . 
TARGETFILE = ../mount/exercise.file

.PHONY: all check clean

all: fsx

check: all
	./fsx $(FSXFLAGS) $(TARGETFILE)

clean:
	rm -f fsx *.fsxlog *.fsxgood 
