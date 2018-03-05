import sys

import pandas as pd

sys.path.append('./src/')
import helper


def generate_columns(max_participants):
    """
    Generates columns for participants
    :param max_participants: Maximum Number of Participants 
    :return: a python list consisting of column names
    """
    columns = ['Participant' + str(i + 1) for i in range(0, max_participants)]
    columns.insert(0, 'Receipt No')
    columns.append('Phone')
    columns.append('Present/Absent')
    return columns


def populate_df(columns, participants_list, mapping):
    """
    Populates a pandas DataFrame based on the participants list
    :param columns: A python list containing the column names
    :param participants_list: A python list where each 
    :param mapping: 
    :return: 
    """
    # Initialize an empty DataFrame
    df = pd.DataFrame(index=range(len(participants_list)), columns=columns)

    # Get location of first Participant Column
    participant_loc = df.columns.get_loc('Participant1')
    plist_loc = 0  # Iterator for participants_list
    for index, row in df.iterrows():
        # Add Receipt No
        row['Receipt No'] = participants_list[plist_loc][mapping['pReceiptNo']]

        i = 0
        if type(participants_list[plist_loc][mapping['pName']]) == list:  # If pName is list
            for participant in participants_list[plist_loc][mapping['pName']]:
                row[participant_loc + i] = participant
                i += 1
        else:
            row[participant_loc] = participants_list[plist_loc][mapping['pName']]

        row['Phone'] = participants_list[plist_loc][mapping['pPhone']]
        plist_loc += 1

    return df


def main():
    db = helper.init_db()
    # Get collection
    event_collection = db.Events

    # Initialize Mapping Dict
    mapping = helper.get_mapping()

    df_list = []  # Dict to store dfs of all events

    for post in event_collection.find():
        # Get the max number of participants and rounds
        max_participants = post[mapping['maxParticipants']]

        # List of participants
        participants_list = post[mapping['participants']]

        # Generate Columns
        columns = generate_columns(max_participants)

        # Populate DataFrame
        df = populate_df(columns, participants_list, mapping)

        df_list.append(df)

    # Write to excel
    i = 0
    for df in df_list:
        df.to_excel('df' + str(i) + '.xlsx')
        i += 1


if __name__ == '__main__':
    main()
