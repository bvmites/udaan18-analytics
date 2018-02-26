import json
import pprint
from itertools import chain

import pandas as pd
from pymongo import MongoClient


def get_mapping(file_name):
    """
    Gets mapping file name and returns a JSON object
    :param file_name: name of the mapping file 
    :return: a python dict created from the JSON configuration
    """
    try:
        with open(file_name) as fields_mapping:
            mapping = json.load(fields_mapping)
        return mapping
    except Exception as ex:
        print(ex)


def generate_columns(max_participants, max_rounds, participants_list):
    """
    
    :param max_participants: 
    :param max_rounds: 
    :param participants_list: 
    :return: 
    """
    unique_years = set(chain.from_iterable([d[mapping['pYear']]] for d in participants_list))
    # convert dates to a legible format
    unique_years = sorted(unique_years)
    columns = [d for d in unique_years]
    unique_years = [d for d in unique_years]

    # insert EventName as first column
    columns.insert(0, 'EventName')
    return columns, unique_years


def populate_df(columns, participants_list, max_participants, unique_years, fee, name):
    empty_participant_list = [['' for i in range(0, len(columns))]]
    df = pd.DataFrame(empty_participant_list, columns=columns)

    years = [d[mapping['pYear']] for d in participants_list]
    total_entries_day = []
    for my_year in unique_years:
        total_entries_day.append(years.count(my_year))
        df.at[0, my_year] = total_entries_day[-1]

    df.at[0, 'EventName'] = name
    print(df)
    return df


client = MongoClient('mongodb://localhost:27017/')

db = client.test_db

mapping = get_mapping('fields_mapping.json')

event_collection = db.Events

df_list = []

data = db.Events.aggregate([{"$project": {"participants": 1, "name": 1}}, {"$unwind": "$participants"},
                            {"$group": {"_id": {"eventID": "$_id", "year": "$participants.pYear", "name": "$name"},
                                        "count": {"$sum": 1}}},
                            {"$group": {"_id": {"eventID": "$_id.eventID", "name": "$_id.name"},
                                        "entries": {"$push": {"year": "$_id.year", "count": "$count"}}}}])

columns = ['Event Name', 'Year 1', 'Year 2', 'Year 3', 'Year 4']

pp = pprint.PrettyPrinter(indent=4)
pp.pprint(list(data))

i = 0
for df in df_list:
    df.to_excel('df' + str(i) + '.xlsx')
    i += 1
