#!/bin/bash
export USE_SDFS=1
export GO_SDFS_LIB="/opt/soloos/sdfs/bin/"
export GO_SDFS_INCLUDE="/opt/soloos/sdfs/bin/"

export GFLAGS_DIR="/opt/soloos/third_party/cpp/gflags/builds/"
export CPATH="$GFLAGS_DIR/include:$GO_SDFS_INCLUDE"
export LIBRARY_PATH="$GFLAGS_DIR/lib"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$GO_SDFS_LIB"
