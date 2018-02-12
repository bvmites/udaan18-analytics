import pandas as pd
from pymongo import MongoClient


def generate_columns(max_participants, max_rounds, participants_list):
    # TODO - Add first column to be receipt no.
    # Columns for participants
    columns = ['Participant' + str(i + 1) for i in range(0, max_participants)]

    columns.append('Phone')

    return columns


def populate_df(columns, participants_list, max_participants):
    empty_participant_list = [['' for i in range(0, len(columns))] for i in range(0, len(participants_list))]
    df = pd.DataFrame(empty_participant_list, columns=columns)
    print(df)
    participant_loc = df.columns.get_loc('Participant1')
    plist_loc = 0
    for index, row in df.iterrows():
        # TODO - Add first column to be receipt no.
        i = 0
        if type(participants_list[plist_loc]['name']) == list:
            for participant in participants_list[plist_loc]['name']:
                row[participant_loc + i] = participant
                i += 1
        else:
            row[participant_loc] = participants_list[plist_loc]['name']

        row['Phone'] = participants_list[plist_loc]['phone']
        plist_loc += 1
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

    # List of participants
    participants_list = post['Event']['participants']

    columns = generate_columns(max_participants, max_rounds, participants_list)

    df = populate_df(columns, participants_list, max_participants)

    # print(df)
    df_list.append(df)

i = 0
for df in df_list:
    df.to_excel('df' + str(i) + '.xlsx')
    i += 1
