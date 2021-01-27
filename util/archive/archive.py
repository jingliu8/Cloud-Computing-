# archive.py
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
import time, sched
from datetime import datetime
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
config.read('archive_config.ini')

# Add utility code here

def archive_results():
    # Connect to SQS and get the results 
    sqs = boto3.resource('sqs', region_name = config['aws']['AwsRegionName'])
    queue = sqs.get_queue_by_name(QueueName = config['sqs']['ArchiveQueueName'])
    # Dynamodb Table
    dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
    table = dynamodb.Table(config['dynamodb']['TableName'])
 
    # Poll the result queue in a loop
    while True:
        # Attempt to read a message from the queue
        '''
        Reference:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.receive_messages
        '''
        messages = queue.receive_messages(
                AttributeNames=['All'],
                MaxNumberOfMessages= int(config['sqs']['MaxNumberOfMessages']),
                VisibilityTimeout= int(config['sqs']['VisibilityTimeout']),
                WaitTimeSeconds= int(config['sqs']['WaitTimeSeconds'])
                )
        # If message read, extract job parameters from the msg body 
        if len(messages) > 0:
            print ("Received {0} messages...".format(str(len(messages))))
            # Iterate through each message and archive result for free user 
            for msg in messages:
                # Get job_id from msg
                job_id = json.loads(msg.body)['Message']
                # Get user_id from database 
                '''
                Reference:
                https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.get_item
                '''
                try:
                    response = table.get_item(
                            Key = {'job_id':job_id}
                            )
                except ClientError as err:
                    print (err) 
                
                # Get user_id from dynamodb and get_user_profile 
                user_id = response['Item']['user_id']
                s3_key_result_file = response['Item']['s3_key_result_file']
                user_profile = helpers.get_user_profile(user_id)
                
                # Archive free user data (result file) to Glacier 
                if user_profile['role'] == 'free_user':
                    # Archive result files 5 mins later
                    '''
                    Reference:
                    https://docs.python.org/3/library/sched.html
                    '''
                    ct = float(response['Item']['ct'])
                    at = ct + float(config['parameters']['FreeUserDataRetention'])
                    s = sched.scheduler(time.time, time.sleep)
                    s.enterabs(at,1,archive_job, (job_id, s3_key_result_file, table))
                    s.run()   

                # Delete the message from the queue
                try:
                    msg.delete()
                except Exception as err:
                    print (err)

def archive_job(job_id, s3_key_result_file, table):
    # Check if the user has updated to premium user 
    '''
    Reference:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.get_item
    '''
    try:
        response = table.get_item(
                Key = {'job_id':job_id},
                ProjectionExpression = 'upgrade_premium'
                )
    except ClientError as err:
        print (err)
    upgrade_premium = response['Item']['upgrade_premium']
    # If the user has not upgrade to the premium user, archive !
    if upgrade_premium == False:
        # Get result file from S3 result bucket 
        '''
        Reference:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object.get
        '''
        s3 = boto3.resource('s3', region_name = config['aws']['AwsRegionName'])
        s3_obj = s3.Object(config['s3']['ResultBucketName'], s3_key_result_file)
        result_file = s3_obj.get()['Body'].read()
        # Achive data
        '''
        Reference:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.upload_archive
        '''
        glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName']) 
        try:
            arch_response = glacier.upload_archive(
                    vaultName = config['glacier']['VaultName'],
                    body = result_file 
                    )
        except ClientError as err:
            print (err)
        archive_id = arch_response['archiveId']
        # Save the archiveId in dynamodb table
        '''
        Reference:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
        '''
        try:
            db_response = table.update_item(
                    Key={'job_id': job_id},
                    UpdateExpression='SET results_file_archive_id=:val1, archive_status=:val2 ',
                    ConditionExpression=Attr('job_status').eq('COMPLETED'),
                    ExpressionAttributeValues={':val1':archive_id,':val2':True},
                    ReturnValues='UPDATED_NEW'
                    )
        except ClientError as err:
            print (err)
        # Delete archived object from s3 
        del_response = s3_obj.delete()

if __name__ == '__main__':
    archive_results()


### EOF
