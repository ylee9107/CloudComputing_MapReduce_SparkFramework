import csv
import datetime

"""
This module includes a few functions used in ControversialTrendingVideoIdentification
"""
def extract_RelevantData(record):
    for row in csv.reader([record]):

        # (video_id[0],trending_date[1],category[3],likes[6],dislikes[7],country[11])
        # (video_id[0],category[3],likes[6],dislikes[7],country[11])

        video_ID, country, category, like, dislike = row[0],row[11],row[3],row[6],row[7]

        return (str(video_ID)+" "+country+" "+category,[like, dislike])
    # (Key = Video_Id Country Category, Value = list(like, dislike)

def Video_ID_Reducer(key, value):
    # (Key = Video_Id Country Category, Value = list(like, dislike)
    # Creates Key-value pair, by reducer method.
    return key + value


def calculate_Disklike(line):
    key, value = line

    # Checker:
    if len(value)<4:
        return (key,0)

    # Set values for Likes and Dislikes:
    like_value1 = int(value[0])
    dislike_value1 = int(value[1])
    like_value2 = int(value[2])
    dislike_value2 = int(value[3])

    # Perform Calc. to get Dislike scores between likes and dislikes.
    score_calc_dislike = (dislike_value2 - dislike_value1)
    score_calc_like = (like_value2 - like_value1)

    score_total = score_calc_dislike - score_calc_like
    return (key, score_total)

