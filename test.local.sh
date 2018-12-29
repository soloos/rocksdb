#!/bin/bash
# ./db_bench --benchmarks=fillseq --num=1000000 --compression_type=none
# ./db_bench -sdfs --env_uri localost:123 --benchmarks=fillseq --num=1000000 --compression_type=none
# ./db_bench -sdfs --env_uri localost:123 --benchmarks=fillseq --num=100 --compression_type=none
# ./db_bench --benchmarks=fillrandom,seekrandom --num=1000000 --compression_type=none
# ./db_bench -sdfs --env_uri localost:123 --benchmarks=fillrandom,seekrandom --num=1000000 --compression_type=none

#./db_bench --benchmarks="fillseq,overwrite,filluniquerandom,"\
# "readrandom,readwhilewriting,updaterandom" --num=200000 --compression_type=none

# ./db_bench --benchmarks="fillseq,overwrite,filluniquerandom,"\
# "readrandom,readwhilewriting,updaterandom" -sdfs --env_uri localost:123 --num=1000000 --compression_type=none

# ./db_bench --benchmarks="filluniquerandom,readrandom" -sdfs --env_uri localost:123 --num=10000000 --compression_type=none
# ./db_bench --benchmarks="filluniquerandom,readrandom" -sdfs --env_uri localost:123 --num=10000 --compression_type=none

./db_bench ./sdfs.json --benchmarks="fillseq,overwrite,filluniquerandom,readrandom,readwhilewriting,updaterandom" --num=60000 --compression_type=none

#./db_bench ./sdfs.json -sdfs --env_uri localost:123 --benchmarks="fillseq,readrandom" --num=200000 --compression_type=none

#r ./sdfs.json -sdfs --env_uri localost:123 --benchmarks="fillseq,readrandom" --num=20000 --compression_type=none

# ./db_bench --benchmarks="fillseq,overwrite,filluniquerandom,"\
# "readrandom,readwhilewriting,updaterandom" -sdfs --env_uri localost:123 --num=2000 --compression_type=none

# ./db_bench --benchmarks="fillseq" -sdfs --env_uri localost:123 --num=20000000 --compression_type=none
# ./db_bench -benchmarks="readwhilewriting" --duration=3600 --num=10000 --compaction_readahead_size=2097152 --level0_file_num_compaction_trigger=2 --max_background_compactions=32  -sdfs --env_uri localost:123
