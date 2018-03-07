import datetime
import sys

import pandas as pd

sys.path.append('./src/')
import helper


def main():
    # Choose the db
    db = helper.init_db()

    # Init Mapping
    mapping = helper.get_mapping('fields_mapping.json')

    # Empty list for storing pandas DataFrames later
    df_list = []

    # List of Columns
    columns = ['Event Name', 'Date', 'Collections']

    events_output = sorted(
        (db.events.aggregate([{"$project": {"_id": 1, mapping["name"]: 1, mapping["fee"]: 1}}])),
        key=lambda k: k['_id']
    )

    query_output = db.participations.aggregate([{"$group": {"_id": {"eventId": "$eventId",
                                                            "date": {
                                                                "$dayOfMonth": "$" + mapping["pRegDate"]},
                                                            "month": {
                                                                "$month": "$" + mapping["pRegDate"]}
                                                                    },
                                                            "count": {"$sum": 1}
                                                    }},
                                                {"$group": {"_id": "$_id.eventId", "count_list": {
                                                    "$push": {"date": "$_id.date", "month": "$_id.month",
                                                              "count": "$count"}}}}
                                                ])

    query_output = sorted(list(query_output), key=lambda k: k['_id'])
    event_list = []

    i = 0
    for event in query_output:
        if events_output[i]['_id'] == event['_id']:
            # Append name to event_list
            event_list.append(events_output[i][mapping['name']])

            # generate a python list of collections
            event_collection_list = []
            for collection in event['count_list']:
                event_collection_list.append([
                    events_output[i][mapping['name']],
                    datetime.datetime.strptime(str(collection['date'])
                                               + "-" + str(collection['month']) + "-2018", "%d-%m-%Y"),
                    collection['count'] * events_output[i][mapping['fee']]
                ])
                event_collection_list.sort(key=lambda r: r[1])
            df = pd.DataFrame(data=event_collection_list, columns=columns)
            df['Date'] = df['Date'].map(lambda x: x.strftime("%d-%m-%Y"))
            df_list.append(df)

        i += 1

    helper.write_to_excel(df_list, event_list, 'collections')


if __name__ == '__main__':
    main()
