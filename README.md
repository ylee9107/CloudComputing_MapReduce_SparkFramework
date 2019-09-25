# YouTube Video satistics using MapReduce and Spark

## Dataset
This dataset includes several months of daily trending YouTube videos. Data is includes different regions for the US, GB, DE, CA, and FR (USA, Great Britain, Germany, Canada, and France, respectively), with up to 200 listed trending videos per day.

This data was obtained from (https://www.kaggle.com/datasnaek/youtube-new).

## Setting up AWS EMR
This code was run using a master node (m3.xlarge). Modules used in EMR are:

- Hadoop 2.8.5 
- Ganglia 3.7.2 
- Hive 2.3.5 
- Hue 4.4.0 
- Mahout 0.13.0 
- Pig 0.17.0
- Tez 0.9.2

## How to run Workload 1 – Category and Trending Correlation.
After setting up the EMR cluster on Amazon AWS and connect to it using the “Master Public DNS:” address.

- Step 1 - The files is then uploaded to the instance.

- Step 2 – The input file (AllVideos_short.csv) should be placed or be present in the </user/hadoop/assignment1_input> directory.

On local machine, this will require using the “hdfs dfs -put /home/hadoop/assignment1_workload1/AllVidoes_short.csv /user/hadoop/assignment1_input” command, to copy the csv file onto HDFS storage.

- Step 3 – As this assignment is done in Python, the following command in the EMR terminal can be used to excute and run the MapReduce script.
• Example command:
“sh ct_assignment1_workload1.sh [Input directory on HDFS] [create Output file, “OutputFolder]”
• Example on local machine:
“sh ct_assignment1_workload1.sh /user/hadoop/Assignment1_Input/AllVideos_short.csv OutputFolder”

- Step 4 - The output results can be checked using the following command. “hdfs dfs -cat /user/hadoop/OutputFolder/part-00000”

- Step 5 – Get the output to local file system from the HDFS:
“hdfs dfs -get /user/hadoop/OutputFolder”

## How to run Workload 2 - Controversial Trending Video Identification

- Step 1 – Run command in Terminal to navigate to where the .py script is located. Note that the input file should be present /home/hadoop/ directory.

- Step 2 – To run the py.script and generate the output files, a spark submit command is required and is shown as follows:
spark-submit --master yarn ControversialTrendingVideoIdentification.py /home/hadoop/assignment1_input /home/hadoop/workload2_score_out

- Step 3 – Do note that the output folder must not pre-exist. Grab the output to local file system from HDFS storage.

hadoop fs -get /home/hadoop/workload2_score_out
