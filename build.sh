#!/bin/bash
. env.sh
# touch ./dancestor/dancestor.cc
# DEBUG_LEVEL=2 make db_bench
# DEBUG_LEVEL=0 make db_bench -j8
DEBUG_LEVEL=0 make db_bench 
