#!/usr/bin/python3

import sys
import csv

def ct_mapper():
    """ This mapper select tags and return the =========== information.

    Input format: video_id \t trending_date \t category_id \t category \t publish_time \t views \t likes \t dislikes \t
    comment_count \t ratings_disabled \t video_error_or_removed \t country

    Output format: video_id,category \t country=count

    """

    # We want to map out Video_id, Category and Country = Keys
    # We want to map out

    for line in sys.stdin:
        # Clean input and split it
        line = line.strip()
        # Could be Comma Separated.
        # parts = line.split(",")
        parts = line.split(",")

        # Check that the line is of the correct format
        # CSV file check, shows 12 columns.
        # if len(parts) != 12:
        #   continue

        # Might need to include Video_id counter: (Or could be in Reducer part)
        # video_id = parts[0].strip()
        # categories = parts[3].strip()
        # country = parts[11].strip()

        video_id = parts[0]
        categories = parts[3]
        country = parts[11]

        # we want to out the mapping of the data with regards to the certain individual countries.
        # So set for-loop as country.
        # for category in categories:
        print("{},{}=1".format((categories + "," + video_id), country))


if __name__ == "__main__":
    ct_mapper()
