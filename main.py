import json
import os
from dotenv import load_dotenv
import boto3
import pandas as pd
from botocore.exceptions import ClientError

load_dotenv()

service_name = os.getenv('service_name')
region_name = os.getenv('region_name')
aws_access_key_id = os.getenv('aws_access_key_id')
aws_secret_access_key = os.getenv('aws_secret_access_key')
s3 = boto3.resource(
    service_name=service_name,
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

bucket_name = os.getenv('bucket_name')

bucket = s3.Bucket(bucket_name)


def upload_file(file_name):
    new = './tmp/' + file_name
    dest_bucket = os.getenv('dest_bucket')
    try:
        s3.Bucket(dest_bucket).upload_file(new, Key=file_name)
    except ClientError as e:
        print(e)
        return False
    return True


for obj in bucket.objects.all():
    key = obj.key
    if key[-4:] == "json":
        filename = key[:-5:]
        body = obj.get()['Body'].read().decode('utf-8').splitlines()
        body1 = json.dumps(body)
        contents = json.loads(body1)
        newFilename = filename + '.csv'
        df = pd.DataFrame(contents)
        os.makedirs('tmp/demo1/', exist_ok=True)
        df.to_csv('./tmp/' + newFilename)
        file = pd.read_csv('./tmp/' + newFilename)
        print(file)
        print(upload_file(newFilename))
