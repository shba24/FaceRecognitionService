#!/usr/bin/python3

import csv
import os
import pickle
import face_recognition
import urllib3
import boto3
from botocore.exceptions import ClientError

AWS_SERVER_PUBLIC_KEY="AKIAVPMB4KPNQN4VJRU5"
AWS_SERVER_SECRET_KEY="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"
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
table = dynamodb_client.Table('students')
input_bucket = 'cse546-final-input-bucket'
output_bucket = 'cse546-final-output-bucket'

# Function to read the 'encoding' file
def open_encoding(filename):
    file = open(filename, 'rb')
    data = pickle.load(file)
    file.close()
    return data

# Download object from input bucket
def download_object(bucket_name, item, dest):
    try:
        s3_client.download_file(bucket_name, item, dest)
        print('Ok')
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print('The video does not exist: s3://{}/{}'.format(bucket_name, item))
        else:
            raise
    return True

# Upload result to S3 output bucket
def upload_object(objects, bucket_name, item):
    try:
        s3_client.upload_file(objects, bucket_name, item)
        print('Upload Successful')
    except FileNotFoundError:
        print('The file was not found')
        return False
    except ClientError:
        print('ClientError...')
        return False
    return True

def face_recognition_handler(event, context):
    result = 'NA'
    # Listening bucket event
    bucket = event['Records'][0]['s3']['bucket']['name']

    # Getting the item: video name e.g. test_1.mp4
    key = urllib3.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'],
        encoding='utf-8')

    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        print('CONTENT TYPE: ' + response['ContentType'])
    except Exception as e:
        print(e)
        print(
            'Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as '
            'this function.'.format(
                key,
                bucket))
        raise e

    # Video path
    path = '/tmp/'
    video_file_path = str(path) + key

    # Downloading video
    download_object(bucket, key, video_file_path)

    # Extracting frames from video using ffmpeg
    os.system(
        'ffmpeg -i ' +
        str(video_file_path) +
        ' -r 1 ' +
        str(path) +
        'image-%3d.jpeg' +
        ' -loglevel 8'
    )

    # Using the first image generated, read face_encoding
    face_image = face_recognition.load_image_file(
        str(path) + 'image-001.jpeg')
    face_encoding = face_recognition.face_encodings(face_image)[0]

    # Read the 'encoding' file
    encoding_file = '/home/app/encoding'
    total_face_encoding = open_encoding(encoding_file)

    # For each known face, determine whether the current face matches it,
    # and the matching name is stored in the result
    for encoding in enumerate(total_face_encoding['encoding']):
        match = face_recognition.compare_faces(
            [encoding[1]], face_encoding)
        if match[0]:
            result = total_face_encoding['name'][encoding[0]]
            break

    # Query for matching records in dynamodb
    response = table.get_item(
        Key={
            'name': result
        }
    )
    item = response['Item']

    # Creating csv object, and write the content to a CSV file
    csv_name = key.split('.')[0]

    with open(path + csv_name, mode='w') as f:
        writer = csv.writer(f)
        writer.writerow([item['name'], item['major'], item['year']])

    # Uploading results to S3 output bucket
    upload_object(path + csv_name, output_bucket, csv_name)
