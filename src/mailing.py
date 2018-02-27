import sys
from os import path

import requests

sys.path.append('./src/')
import helper


def send_mail(subject, message, attachment_path=None):
    config = helper.get_config()
    print(config)
    attachment = None
    if attachment_path is not None:
        attachment = [{"attachment", open(attachment_path, "rb")}]
    return requests.post(
        config['mailgun_api_url'],
        auth=("api", config['mailgun_key']),
        data={"from": config['mailgun_sender'],
              "to": [config['mailgun_sample_receiver']],
              "subject": subject,
              "text": message},
        files=attachment)


def main():
    send_mail(subject='Message from Udaan 18!', message='Hello world from Udaan!')

    basepath = path.dirname(__file__)
    file_name = path.abspath(path.join(basepath, "..", "df1.pdf"))
    send_mail(subject='Message from Udaan 18!', message='Hello world from Udaan!', attachment_path=file_name)


if __name__ == '__main__':
    main()
