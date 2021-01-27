# thaw.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import boto3
import json
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
'''
Reference:
https://docs.python.org/3/library/configparser.html
'''
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('thaw_config.ini')

# Add utility code here
def thaw_archive():
    # Connect to SQS and get the Glacier Object
    sqs = boto3.resource('sqs', region_name = config['aws']['AwsRegionName'])
    queue = sqs.get_queue_by_name(QueueName = config['sqs']['ThawQueueName'])
    # Dynamodb Table 
    dynamodb = boto3.resource('dynamodb', region_name = config['aws']['AwsRegionName'])
    table = dynamodb.Table(config['dynamodb']['TableName'])
    # S3 Client
    s3_client = boto3.client('s3', region_name = config['aws']['AwsRegionName'])
    # Glacier
    glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
    # Poll the thaw queue in a loop 
    while True:
        # Attempt to read a message from the queue
        '''
        Reference:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.receive_messages
        '''
        messages = queue.receive_messages(
                AttributeNames = ['All'],
                MaxNumberOfMessages = int(config['sqs']['MaxNumberOfMessages']),
                VisibilityTimeout = int(config['sqs']['VisibilityTimeout']),
                WaitTimeSeconds = int(config['sqs']['WaitTimeSeconds'])
                )
        # If message read...
        if len(messages) > 0:
            print ("Receive {0} messages...".format(str(len(messages))))
            # Iterate through messages
            for msg in messages:
                # Download the job_output
                retrieval_status = json.loads(msg.body)['Message'] 
                retrieval_id = json.loads(retrieval_status)['JobId']
                job_id = json.loads(retrieval_status)['JobDescription']
                archive_id = json.loads(retrieval_status)['ArchiveId']
                '''
                Reference:
                https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.get_job_output
                '''
                try:
                    retrieval_output = glacier.get_job_output(
                            vaultName = config['glacier']['VaultName'],
                            jobId = retrieval_id
                            )
                except ClientError as err:
                    print (err)

                retrieval_body = retrieval_output['body']
    
                # Get relevant information from dynamodb 
                '''
                Reference:
                https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.get_item
                '''
                try:
                    relevant_info = table.get_item(
                            Key = {'job_id':job_id},
                            ProjectionExpression = 's3_key_result_file'
                            )
                except ClientError as err:
                    print(err)
                s3_key_result_file = relevant_info['Item']['s3_key_result_file']

                # Upload the job_output to s3 bucket 
                '''
                Reference:
                https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object
                '''
                try:
                    upload_output = s3_client.put_object(
                            ACL = config['s3']['ACL'],
                            Body = retrieval_body.read(),
                            Bucket = config['s3']['ResultBucketName'],
                            Key = s3_key_result_file
                            )
                except ClientError as err:
                    print (err)
             
                # Delete the object from Glacier
                '''
                Reference:
                https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.delete_archive
                '''
                try:
                    delete_archive = glacier.delete_archive(
                            vaultName = config['glacier']['VaultName'],
                            archiveId = archive_id
                            )
                except ClientError as err:
                    print (err)
                
                # Delete the message 
                try:
                    msg.delete()
                except Exception as err:
                    print (err) 


if __name__ == '__main__':
    thaw_archive()

### EOF
