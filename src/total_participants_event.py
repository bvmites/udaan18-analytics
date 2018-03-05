import sys

import pandas as pd

sys.path.append('./src/')
import helper


def main():
    # Choose the db
    db = helper.init_db()

    # Init Mapping
    mapping = helper.get_mapping()

    # Empty list for storing pandas DataFrames later
    df_list = []

    # List of Columns
    columns = ['Event Name', 'Total Participation']

    # Query
    # data = db.Events.aggregate([{"$project": {"_id": 1, "name": 1, "participants": 1}},
    #                             {"$unwind": "$participants"}, {"$group": {"_id": {"_id": "$_id", "name": "$name"},
    #                                                                       "count": {"$sum": 1}}}])

    data = db.Events.aggregate([{"$project": {"_id": 1, mapping["name"]: 1, mapping["participants"]: 1}},
                                {"$unwind": "$" + mapping["participants"]},
                                {"$group": {"_id": {"_id": "$_id", "name": "$" + mapping["name"]},
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
