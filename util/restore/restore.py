# restore.py
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
from boto3.dynamodb.conditions import Key, Attr
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
config.read('restore_config.ini')

# Add utility code here
def restore_archive():
    # Connect to SQS and get the subscriber
    sqs = boto3.resource('sqs', region_name = config['aws']['AwsRegionName'])
    queue = sqs.get_queue_by_name(QueueName = config['sqs']['RestoreQueueName'])
    # Dynamodb Table
    dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
    table = dynamodb.Table(config['dynamodb']['TableName'])
    # Glacier
    glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
    # Poll the result queue in a loop
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
        # If message read, extract the user_id from the msg body
        if len(messages) > 0:
            print ("Receive {0} messages...".format(str(len(messages))))
            #Iterate through each message and restore the data file for the user 
            for msg in messages:
                # Get user_id from msg
                user_id = json.loads(msg.body)['Message']
                print ("Restore data for user: {}".format(user_id))
                # Lookup archived data for the upgraded user 
                '''
                Reference:
                https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.query
                '''
                try:
                    archived_data = table.query(
                            IndexName = 'user_id_index',
                            KeyConditionExpression=Key('user_id').eq(user_id),
                            FilterExpression=Attr('archive_status').eq(True),
                            ProjectionExpression = 'job_id, results_file_archive_id'
                            )
                except ClientError as err:
                    print (err)

                # Initiate the retrieval job for each of the archived data
                '''
                Reference:
                https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
                '''
                for item in archived_data['Items']:
                    try:
                        expedited_retrieval = glacier.initiate_job(
                                vaultName = config['glacier']['VaultName'],
                                jobParameters = {
                                    'Type':'archive-retrieval',
                                    'ArchiveId':item['results_file_archive_id'],
                                    'Description':item['job_id'],
                                    'SNSTopic':config['sns']['SNSThaw'],
                                    'Tier': 'Expedited'
                                    }
                                )
                    except glacier.exceptions.InsufficientCapacityException as err:
                        print ('Expedited Retrieval fail, initiate Standard Retrieval.')
                        expedited_retrieval = glacier.initiate_job(
                                vaultName = config['glacier']['VaultName'],
                                jobParameters = {
                                    'Type':'archive-retrieval',
                                    'ArchiveId':item['results_file_archive_id'],
                                    'Description':item['job_id'],
                                    'SNSTopic':config['sns']['SNSThaw'],
                                    'Tier': 'Standard'
                                    }
                                )                       
                    except Exception as err:
                        print (err)
                
                # Delete the message from the message queue
                try:
                    msg.delete()
                except Exception as err:
                    print (err) 

if __name__ == '__main__':
    restore_archive()



### EOF
