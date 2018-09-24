#!/bin/sh

h=$1
v=$2

hv=h"$(printf %02d $h)"v"$(printf %02d $v)"

echo $hv

# Ensure xgboost conda environment is activated.
source activate xgboost

# Scripts are hardcoded here.
cd /home/user/scripts

python build-datasets-3x3tile.py $h $v > "${hv}"-build.log && \
python xg-train-annualized.py $h $v > "${hv}"-annualized-train.log && \
python xg-classify-annualized.py models/"${hv}"-annualized.xgmodel "${hv}"/annualized-pickles $h $v > "${hv}"-annualized-classify.log
