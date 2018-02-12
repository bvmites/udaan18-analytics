from itertools import chain

import pandas as pd
from pymongo import MongoClient


def generate_columns(max_participants, max_rounds, participants_list):
    unique_years = set(chain.from_iterable([d['year']] for d in participants_list))
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

    years = [d['year'] for d in participants_list]
    total_entries_day = []
    for my_year in unique_years:
        total_entries_day.append(years.count(my_year))
        df.at[0, my_year] = total_entries_day[-1]

    df.at[0, 'EventName'] = name
    print(df)
    return df


client = MongoClient('mongodb://localhost:27017/')

db = client.test_db

event_collection = db.Events

df_list = []

columns = pd.DataFrame(event_collection.find()[1]['Event']['participants'])
# print(df)

for post in event_collection.find():
    print(post)
    # Get the max number of participants and rounds
    max_participants = post['Event']['max_participants']
    max_rounds = len(post['Event']['rounds'])
    fee = post['Event']['fee']
    name = post['Event']['Name']

    # List of participants
    participants_list = post['Event']['participants']

    columns, unique_years = generate_columns(max_participants, max_rounds, participants_list)

    df = populate_df(columns, participants_list, max_participants, unique_years, fee, name)

    # print(df)
    df_list.append(df)

i = 0
for df in df_list:
    df.to_excel('df' + str(i) + '.xlsx')
    i += 1
