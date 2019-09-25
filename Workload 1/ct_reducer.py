#!/usr/bin/python3

import sys


def read_map_output(file):
    """ Return an iterator for key, value pair extracted from file (sys.stdin)
    Input format:  key \t value
    Output format: (key, value)
    """
    for line in file:
        yield line.strip("\n")
        # yield line.strip("\n").split(",", 1)

def calculate_average(dict):
    """ Takes in a dict parameter, and calculates the average value.
    """

    count_unique_countries = 0
    count_unique_videos_perCategory = 0

    for x in dict:
        count_unique_videos_perCategory += 1

        if isinstance(dict[x], list):
            count_unique_countries += len(set(dict[x]))

    average_value = (count_unique_countries /count_unique_videos_perCategory)

    return average_value


def ct_reducer():
    """ This reducer reads in Mapper output: category, video_id, country=count, as key \t pairs
    and returns the average country count for video in each category.

    Input format: category, video_id, country=count
    Output format: Category \t avg_value
    """

    # ===================================================================================#

    data = read_map_output(sys.stdin)

    # PART1: ==================Create Dictionary===============================================================

    # Dictionaries:
    category_dict = {} # {key is category: video is Value}

    # Skips headers in data file: mapper output
    next(data)

    for line in data:
        parts = line.split(",")
        line_tuple = (parts[0], parts[1], parts[2])

        # Create Category Dict:
        category_key = line_tuple[0]

        if category_key not in category_dict.keys():
            category_dict[category_key] = {}

        # Add in values into Category Dict: Values are Video_id, Country=1
        video_id_key = line_tuple[1]
        country_value = line_tuple[2]

        if video_id_key not in category_dict[category_key].keys():
            category_dict[category_key][video_id_key]=[]
        category_dict[category_key][video_id_key].append(country_value)


    # PART2: ==========================Calculation of Average====================================================

    for category_key in category_dict.keys():
        dict = category_dict[category_key]
        current_avgValue = calculate_average(dict)
        float_value = ("%.2f" % current_avgValue)

        print(category_key.strip(), float_value)


if __name__ == "__main__":
    ct_reducer()

