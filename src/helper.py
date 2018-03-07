import json
from os import path

import pandas as pd
from pymongo import MongoClient


def get_mapping(file_name):
    """
    Gets mapping file name and returns a JSON object
    :return: a python dict created from the JSON configuration
    """
    basepath = path.dirname(__file__)
    file_name = path.abspath(path.join(basepath, "..", file_name))
    try:
        with open(file_name) as fields_mapping:
            mapping = json.load(fields_mapping)
        return mapping
    except Exception as ex:
        print(ex)


def get_config():
    """
    Gets config file name and returns a JSON object 
    :return: a python dict created from the JSON configuration
    """
    basepath = path.dirname(__file__)
    file_name = path.abspath(path.join(basepath, "..", "config.json"))
    try:
        with open(file_name) as fields_mapping:
            mapping = json.load(fields_mapping)
        return mapping
    except Exception as ex:
        print(ex)


def init_db():
    """
    Initializes mongoDB client and database
    :return: instance of database
    """
    # Initialize mongodb client
    config = get_config()
    client = MongoClient(config['mongodb_connection_string'])
    # Get db
    db = client.udaan18
    return db


def write_to_excel(df_list, event_list, start_string):
    """
    Helper function to write a list of Pandas dataframe to excel file
    :param df_list: list of Pandas Dataframe
    :return:
    """
    i = 0
    print(event_list)
    for df in df_list:
        # Shift Index by 1
        df.index += 1
        j = 0
        max_list = []  # List that stores max length of strings in a column

        for col in df.columns.values:
            max_list.append(
                max(len(col), df[col].map(str).map(len).max())
            )

        writer = pd.ExcelWriter(start_string + '_' + str(event_list[i]) + '.xlsx', engine='xlsxwriter')
        df.to_excel(writer, sheet_name='Sheet1')

        workbook = writer.book
        worksheet = writer.sheets['Sheet1']

        j = 1
        for m in max_list:
            worksheet.set_column(j, j, m)
            j += 1

        i += 1
        workbook.close()
