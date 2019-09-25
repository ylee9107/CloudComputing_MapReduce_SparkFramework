# Calculate the average rating of each genre
# In order to run this, we use spark-submit, below is the
# spark-submit  \
#   --master local[2] \
    #   AverageRatingPerGenre.py
#   --input input-path
#   --output outputfile

from pyspark import SparkContext
from CTV_utils import *
import argparse
import datetime

if __name__ == "__main__":

  # Part 1 =========================================================================================================
  sc = SparkContext(appName="Controversial Trending Video Identification")
  parser = argparse.ArgumentParser()
  parser.add_argument("--input", help="the input path",
                      default='~/comp5349/lab_commons/week5/')
  parser.add_argument("--output", help="the output path",
                      default='workload2_results')
  args = parser.parse_args()
  input_path = args.input
  output_path = args.output

  # Part 2 =======================Read Data==========================================================================

  # Below Code: For EMR use:
  # dat_file = sc.textFile(input_path + "AllVideos_short.csv")

  # Below Code: For local Machine testing only:
  dat_file = sc.textFile("AllVideos_short.csv")

  # Extract Data:
  # Use pyspark map() func to apply pre-defined mapper func.
  dat_file_remapped = dat_file.map(extract_RelevantData)

            # Outputs -> (Key = Video_Id Country Category, Value = list(like, dislike)

  # Part 3 ============================Reduces and Calculates Data===================================================

  # Reduces the dat into Key-Value pairs:
  Vid_Likes_Dislike_reduced = dat_file_remapped.reduceByKey(Video_ID_Reducer)

  # Performs the calculations to find the Dislike scores:
  calculate_Disklike_Scores = Vid_Likes_Dislike_reduced.map(calculate_Disklike)

  # Sort/List the Top 10 videos with fastest growth of dislikes:
  sorted_Scores = calculate_Disklike_Scores.sortBy(lambda x: x[1],  False).collect()[0:10]

  # Splits the data to be processed by each node:
  results = sc.parallelize(sorted_Scores)

  # Part 4 ===============================Saves Outputs==============================================================

  # Ensures the results output are in a single file: part-00000, under workload2_results folder
  results.repartition(1).saveAsTextFile(output_path)

  # Stops the Spark session:
  sc.stop()




