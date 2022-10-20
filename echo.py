import requests
import boto3
from boto import S3
import os
import json

from prefect import Task


@Task
def strip_email(metadata: str, replacement_email: str = ''):
    response = requests.post(
        url='https://emailsanitizer.labs.dans.knaw.nl/sanitize',
        json={
            'data': metadata,
            'replacement_email': replacement_email
        }
    )
    if response.status_code == 200:
        return response.json()
    else:
        return {"status": "Error", "details": response.json()}

@Task
def open_files(path: str):
    os.chdir(path)
    file_contents = []
    for filename in os.listdir():
        with open(filename, 'r') as contents:
            file_contents.append(contents.read())
    return file_contents

@Task
def parse_json(files: list = []):
    json_result = []
    for item in files:
        try:
            json_result.append(json.loads(item))
        except Exception as e:
            json_result.append(json.dumps({"result": "error", "original_contents": item, "exception": e}))
    return json_result
