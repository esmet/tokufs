#!/bin/bash

sudo umount mount &> /dev/null
mkdir -p mount &> /dev/null
(make &> /dev/null && ./tokufs-fuse -f mount) || echo 'failed to build or run tokufs'
