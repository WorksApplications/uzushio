#!/bin/bash

# pathes on lserv65 server
nwjc_root="/data/works/nwjc"
nwjc_dir="${nwjc_root}/02rm_template_20220711"
out_dir="${nwjc_root}/minhash"
log_file="${out_dir}/minhash.txt"

mkdir -p ${out_dir}

nice -n 19 spark-submit \
    --master local[*] \
    --class org.sample.corpus.MinHashDeduplicator \
    ./target/scala-2.12/CorpusCleaning-assembly-0.1.jar \
    --input ${nwjc_dir}/ \
    --output ${out_dir} \
    --mode c \
    --ngram 5 \
    --minhash-b 15 \
    --minhash-r 150 \
    --jaccard-thr 0.8 \
    --skip-editsim \
    &> "${log_file}"
