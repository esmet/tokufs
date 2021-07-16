#
# TokuFS
#

DEFAULT_BUILD_SUBDIRS = src 
ALL_BUILD_SUBDIRS = src tests benchmark fuse utils
CLEAN_SUBDIRS = src tests benchmark sandbox fuse utils

.PHONY: all check install uninstall reinstall rebuild default tags clean

default: $(patsubst %, %.makesubdir, $(DEFAULT_BUILD_SUBDIRS));
bdb: 
	$(MAKE) default BDB=1

all: $(patsubst %, %.makesubdir, $(ALL_BUILD_SUBDIRS)) 
syntax: $(patsubst %, %.makesubdir.syntax, $(ALL_BUILD_SUBDIRS)) 

PREFIX=/home/jannen/local/

check:  default
	$(MAKE) -j8 -s -k -C src/tests check
	$(MAKE) -j8 -s -k -C tests check

install: default
	/bin/cp lib/libtokufs.so $(PREFIX)/lib
	/bin/cp include/tokufs.h $(PREFIX)/include

uninstall:
	rm -f $(PREFIX)/lib/libtokufs.so
	rm -f $(PREFIX)/include/tokufs.h

reinstall: uninstall rebuild install

rebuild: clean default

tags:
	ctags src/*.[ch] include/*.h include/*/*.h benchmark/*.[ch]


%.makesubdir:
	@$(MAKE) -C $*

%.makesubdir.syntax:
	@$(MAKE) -C $* syntax

clean: $(patsubst %, %.cleansubdir, $(CLEAN_SUBDIRS))
	rm -rf tags TAGS

%.cleansubdir:
	@$(MAKE) -C $* clean

