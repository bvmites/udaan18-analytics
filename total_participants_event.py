import pandas as pd
from pymongo import MongoClient


def generate_columns(max_participants, max_rounds, participants_list):
    columns = ['Total Participants']
    # insert EventName as first column
    columns.insert(0, 'EventName')
    return columns


def populate_df(columns, participants_list, max_participants, name):
    empty_participant_list = [['' for i in range(0, len(columns))]]
    df = pd.DataFrame(empty_participant_list, columns=columns)

    total_participants = len(participants_list)
    df.at[0, 'Total Participants'] = total_participants
    df.at[0, 'EventName'] = name
    print(df)
    return df


client = MongoClient('mongodb://localhost:27017/')

db = client.test_db

event_collection = db.Events

df_list = []

# columns = pd.DataFrame(event_collection.find()[1]['Event']['participants'])
# print(df)

data = db.Events.aggregate([{"$project": {"_id": 1, "name": 1, "participants": 1}},
                            {"$unwind": "$participants"}, {"$group": {"_id": {"_id": "$_id", "name": "$name"},
                                                                      "count": {"$sum": 1}}}])
print(list(data))

for post in event_collection.find():
    print(post)
    # Get the max number of participants and rounds
    max_participants = post['Event']['max_participants']
    max_rounds = len(post['Event']['rounds'])
    fee = post['Event']['fee']
    name = post['Event']['Name']

    # List of participants
    participants_list = post['Event']['participants']

    columns = generate_columns(max_participants, max_rounds, participants_list)

    df = populate_df(columns, participants_list, max_participants, name)

    # print(df)
    df_list.append(df)

i = 0
for df in df_list:
    df.to_excel('df' + str(i) + '.xlsx')
    i += 1
