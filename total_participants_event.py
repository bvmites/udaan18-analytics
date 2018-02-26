import json

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


def main():
    # Initialize MongoDB Client
    client = MongoClient('mongodb://localhost:27017/')

    # Choose the db
    db = client.test_db

    # Init Mapping
    mapping = get_mapping('fields_mapping.json')

    # Empty list for storing pandas DataFrames later
    df_list = []

    # List of Columns
    columns = ['Event Name', 'Total Participation']

    # Query
    data = db.Events.aggregate([{"$project": {"_id": 1, "name": 1, "participants": 1}},
                                {"$unwind": "$participants"}, {"$group": {"_id": {"_id": "$_id", "name": "$name"},
                                                                          "count": {"$sum": 1}}}])
    data = list(data)

    for event in data:
        df_list.append(
            pd.DataFrame(data=[[event['_id']['name'], event['count']]], columns=columns)
        )

    # Write to excel sheet
    i = 0
    for df in df_list:
        df.to_excel('df' + str(i) + '.xlsx')
        i += 1


if __name__ == '__main__':
    main()
