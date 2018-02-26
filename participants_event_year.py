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
    columns = ['Event Name', 'Year 1', 'Year 2', 'Year 3', 'Year 4']

    # Query
    query_output = db.Events.aggregate([{"$project": {"participants": 1, "name": 1}}, {"$unwind": "$participants"},
                                        {"$group": {
                                            "_id": {"eventID": "$_id", "year": "$participants.pYear", "name": "$name"},
                                            "count": {"$sum": 1}}},
                                        {"$group": {"_id": {"eventID": "$_id.eventID", "name": "$_id.name"},
                                                    "entries": {"$push": {"year": "$_id.year", "count": "$count"}}}}])

    query_output = list(query_output)
    # pp = pprint.PrettyPrinter(indent=4)
    # pp.pprint(query_output)

    # Loop through events
    for event in query_output:
        year_list = [0] * 4
        i = 0
        for entry in event['entries']:
            year_list[entry['year'] - 1] = entry['count']
        year_list.insert(0, event['_id']['name'])
        df_list.append(
            pd.DataFrame(data=[year_list], columns=columns)
        )

    i = 0
    for df in df_list:
        df.to_excel('df' + str(i) + '.xlsx')
        print(df)
        i += 1


if __name__ == '__main__':
    main()
