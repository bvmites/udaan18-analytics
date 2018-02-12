from itertools import chain

import pandas as pd
from pymongo import MongoClient


def generate_columns(max_participants, max_rounds, participants_list):
    unique_dates = set(chain.from_iterable([d['Date']] for d in participants_list))
    # convert dates to a legible format
    unique_dates = sorted(unique_dates)
    columns = [d.date().strftime('%d/%m/%Y') for d in unique_dates]
    unique_dates = [d.date().strftime('%d/%m/%Y') for d in unique_dates]

    # insert EventName as first column
    columns.insert(0, 'EventName')
    return columns, unique_dates


def populate_df(columns, participants_list, max_participants, unique_dates, fee, name):
    empty_participant_list = [['' for i in range(0, len(columns))]]
    df = pd.DataFrame(empty_participant_list, columns=columns)

    dates = [d['Date'].date().strftime('%d/%m/%Y') for d in participants_list]
    total_entries_day = []
    for my_date in unique_dates:
        total_entries_day.append(dates.count(my_date))
        total_entries_day[-1] *= fee
        df.at[0, my_date] = total_entries_day[-1]

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

    columns, unique_dates = generate_columns(max_participants, max_rounds, participants_list)

    df = populate_df(columns, participants_list, max_participants, unique_dates, fee, name)

    # print(df)
    df_list.append(df)

i = 0
for df in df_list:
    df.to_excel('df' + str(i) + '.xlsx')
    i += 1
