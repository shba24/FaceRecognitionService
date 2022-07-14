#!/usr/bin/python3

import json
import logging
import boto3
from botocore.exceptions import ClientError

AWS_SERVER_PUBLIC_KEY=""
AWS_SERVER_SECRET_KEY=""
session = boto3.Session(
    aws_access_key_id=AWS_SERVER_PUBLIC_KEY,
    aws_secret_access_key=AWS_SERVER_SECRET_KEY
)
s3_client = boto3.client(
    's3',
    region_name='us-east-1',
    aws_access_key_id=AWS_SERVER_PUBLIC_KEY,
    aws_secret_access_key=AWS_SERVER_SECRET_KEY
)
dynamodb_client = boto3.client(
    'dynamodb',
    region_name='us-east-1',
    aws_access_key_id=AWS_SERVER_PUBLIC_KEY,
    aws_secret_access_key=AWS_SERVER_SECRET_KEY
)
dynamodb_resource = session.resource('dynamodb', region_name='us-east-1')
input_bucket = 'cse546-final-input-bucket'
output_bucket = 'cse546-final-output-bucket'
table_name = 'students'

def check_bucket(bucket_name):
    """Checks whether a particular S3 bucket exists

    :param bucket_name: Bucket to check
    :return: True if bucket exists, else False
    """
    try:
        response = s3_client.list_buckets()
        if bucket_name in response['Buckets']:
            return True
    except ClientError as e:
        logging.error(e)
    return False

def check_table(table_name):
    """Checks whether a particular dynamodb table exists

    :param table_name: Table to check
    :return: True if table exists, else False
    """
    try:
        if table_name in dynamodb_client.list_tables()['TableNames']:
            return True
    except ClientError as e:
        logging.error(e)
    return False

def create_bucket(bucket_name):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """
    if check_bucket(bucket_name):
        return True

    # Create bucket
    try:
        s3_client.create_bucket(Bucket=bucket_name, ACL='private')
        s3_client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                'BlockPublicAcls': True,
                'IgnorePublicAcls': True,
                'BlockPublicPolicy': True,
                'RestrictPublicBuckets': True
            },
        )
    except ClientError as e:
        logging.error(e)
        return False
    return True

def insert_into_table(table_name):
    """Reads student data from student_data.json and inserts into Dynamodb table
    
    :table_name: Existing table
    """
    try:
        table = dynamodb_resource.Table(table_name)
        with table.batch_writer() as batch:
            with open("./student_data.json") as fp:
                for student in json.load(fp):
                    batch.put_item(
                        Item={
                            'id': student['id'],
                            'name': student['name'],
                            'major': student['major'],
                            'year': student['year']
                        }
                    )
    except Exception as e:
        logging.error(e)
        return False
    return True

def create_table(table_name):
    """Creates an Dynamo DB table in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param table_name: Table to create
    :param region: String region to create table in, e.g., 'us-west-2'
    :return: True if table created, else False
    """
    if check_table(table_name):
        return True
    try:
        table = dynamodb_resource.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'name',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'name',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        table.wait_until_exists()
    except ClientError as e:
        logging.error(e)
        return False
    
    return True

if __name__=='__main__':
    create_bucket(input_bucket) ## input bucket
    create_bucket(output_bucket) ## output bucket

    create_table(table_name)                   ## creates dynamodb table
    insert_into_table(table_name)              ## insert into dynamodb table
