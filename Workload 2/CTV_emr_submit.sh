#!/bin/bash

spark-submit \
    --master local[4] \
    ControversialTrendingVideoIdentification.py \
    --input file:///home/hadoop/ \
    --output file:///home/hadoop/workload2_score_out/