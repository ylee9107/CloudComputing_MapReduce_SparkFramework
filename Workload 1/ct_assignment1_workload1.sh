#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Invalid number of parameters!"
    echo "Usage: ./ct_assignment1_workload1.sh [input_location] [output_location]"
    exit 1
fi

hadoop jar /usr/lib/hadoop/hadoop-streaming-2.8.5-amzn-1.jar \
-D mapreduce.job.reduces=3 \
-D mapreduce.job.name='Category and Trending Correlation' \
-file ct_mapper.py \
-mapper ct_mapper.py \
-file ct_reducer.py \
-reducer ct_reducer.py \
-input $1 \
-output $2
