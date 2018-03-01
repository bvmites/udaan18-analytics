import datetime
import pprint
import sys

import pandas as pd
from pymongo import MongoClient

sys.path.append('./src/')
import helper


def main():
    # Initialize MongoDB Client
    client = MongoClient('mongodb://localhost:27017/')

    # Choose the db
    db = client.test_db

    # Init Mapping
    mapping = helper.get_mapping()

    # Empty list for storing pandas DataFrames later
    df_list = []

    # List of Columns
    columns = ['Event Name', 'Date', 'Collections']

    # Query
    # query_output = db.Events.aggregate([{"$unwind": "$participants"},
    #                              {"$group": {"_id": {"eventId": "$_id",
    #                                                  "name": "$name",
    #                                                  "date": {"$dayOfMonth": "$participants.pRegDate"},
    #                                                  "month": {"$month": "$participants.pRegDate"},
    #                                                  "fee": "$fee"},
    #                                          "count": {"$sum": 1}
    #                                          }},
    #                             {"$project": {"_id": 1, "count": 1,
    #                                           "totalCollection": {"$multiply": ["$_id.fee", "$count"]}}},
    #                             {"$group": {"_id":
    #                                             {"eventId": "$_id.eventId", "name": "$_id.name"},
    #                                         "collections": {"$push": {"date": "$_id.date", "month": "$_id.month",
    #                                                                   "totalCollection": "$totalCollection"}}}}
    #                              ])

    # Mapped Query
    query_output = db.Events.aggregate([{"$unwind": "$" + mapping["participants"]},
                                        {"$group": {"_id": {"eventId": "$_id",
                                                            "name": "$" + mapping["name"],
                                                            "date": {
                                                                "$dayOfMonth": "$" + mapping["participants"] + "." +
                                                                               mapping["pRegDate"]},
                                                            "month": {
                                                                "$month": "$" + mapping["participants"] + "." + mapping[
                                                                    "pRegDate"]},
                                                            "fee": "$" + mapping["fee"]},
                                                    "count": {"$sum": 1}
                                                    }},
                                        {"$project": {"_id": 1, "count": 1,
                                                      "totalCollection": {"$multiply": ["$_id.fee", "$count"]}}},
                                        {"$group": {"_id":
                                                        {"eventId": "$_id.eventId", "name": "$_id.name"},
                                                    "collections": {
                                                        "$push": {"date": "$_id.date", "month": "$_id.month",
                                                                  "totalCollection": "$totalCollection"}}}}
                                        ])
    query_output = list(query_output)
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(query_output)

    for event in query_output:
        event_collection_list = []
        for collection in event['collections']:
            event_collection_list.append([
                event['_id']['name'],
                datetime.datetime.strptime(str(collection['date'])
                                           + "-" + str(collection['month']) + "-2018", "%d-%m-%Y"),
                collection['totalCollection']
            ])
            event_collection_list.sort(key=lambda r: r[1])
        df = pd.DataFrame(data=event_collection_list, columns=columns)
        df['Date'] = df['Date'].map(lambda x: x.strftime("%d-%m-%Y"))
        df_list.append(df)

    # Write to excel
    i = 0
    for df in df_list:
        df.to_excel('df' + str(i) + '.xlsx')
        print(df)
        i += 1


if __name__ == '__main__':
    main()
