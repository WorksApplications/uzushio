#!/bin/bash

#$ -j y
#$ -cwd

source $HOME/work/uzushio/.venv/bin/activate

python3 $HOME/work/uzushio/scripts/cal_overlap_ratio/cal_overlap.py \
    --dump_direc_path=/groups/gcf51199/cc/extracted \
    --output_path=$HOME/work/overlap-extracted.csv
