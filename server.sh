#!/bin/bash
gdbserver 0.0.0.0:12505 ./db_bench ./sdfs.json -sdfs --env_uri localost:123 --benchmarks="fillseq,readrandom" --num=2000000 --compression_type=none
