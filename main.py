import json
import os
from dotenv import load_dotenv
import boto3
import pandas as pd
from path import Path
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

# for bucket in s3.buckets.all():
#     print(bucket.name)
bucket_name = os.getenv('bucket_name')

bucket = s3.Bucket(bucket_name)
for obj in bucket.objects.all():
    key = obj.key
    if key[-4:] == "json":
        filename = key[:-5:]
        print(filename)
        body = obj.get()['Body'].read().decode('utf-8').splitlines()
        # print(body)
        body1 = json.dumps(body)
        contents = json.loads(body1)
        print(contents)
        newFilename = filename + '.csv'
        df = pd.DataFrame(contents)
        print(df)
        # filepath = Path('folder/subfolder/out.csv')
        # filepath.parent.mkdir(parents=True, exist_ok=True)
        os.makedirs('tmp/demo1/', exist_ok=True)
        df.to_csv('./tmp/'+newFilename)
        file = pd.read_csv('./tmp/'+newFilename)
        print(file)

