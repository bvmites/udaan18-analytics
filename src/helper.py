import json
from os import path


def get_mapping():
    """
    Gets mapping file name and returns a JSON object
    :return: a python dict created from the JSON configuration
    """
    basepath = path.dirname(__file__)
    file_name = path.abspath(path.join(basepath, "..", "fields_mapping.json"))
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
