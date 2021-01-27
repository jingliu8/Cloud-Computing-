# notify.py
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
import logging
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
config.read('notify_config.ini')

# Add utility code here
def main(argv=None):
    # Connect to SQS and get the message queue
    sqs = boto3.resource('sqs', region_name = config['aws']['AwsRegionName'])
    queue = sqs.get_queue_by_name(QueueName = config['sqs']['QueueName'])
    # Dynamodb for accessing data
    dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
    table = dynamodb.Table(config['db']['Name'])

    # Poll the message queue in a loop
    while True:
        # Attempt to read a message from the queue 
        '''
        Reference:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.receive_messages
        '''
        messages = queue.receive_messages(
                AttributeNames=['All'],
                MaxNumberOfMessages=int(config['sqs']['MaxNumMsg']),
                VisibilityTimeout=int(config['sqs']['VisibilityTimeout']),
                WaitTimeSeconds=int(config['sqs']['WaitTime'])
                )
        # If message read, extract job parameters from the msg body
        if len(messages) > 0:
            print ("Received {0} messages...".format(str(len(messages))))
            # Iterate each message and send notification Emails
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
                            Key={'job_id': job_id},
                            )
                except ClientError as err1:
                    logging.error(err1)
                    print(err1)
                except KeyError as err2:
                    logging.error(err2)
                    print(err2) 
                # Get information from dynamodb
                s3_results_bucket = response['Item']['s3_results_bucket']
                s3_key_log_file = response['Item']['s3_key_log_file']
                s3_key_result_file = response['Item']['s3_key_result_file']
                user_id = response['Item']['user_id']
                # Get recipients Email
                user_profile = helpers.get_user_profile(user_id)
                email = user_profile['email']
                # Send notification Email
                msg_subject = config['msg']['Msg1'] + " " + job_id
                msg_body = config['msg']['Msg1'] + '\n' \
                            + 'job_id: ' + job_id + '\n' \
                            + 'user_id: ' + user_id + '\n' \
                            + 's3_results_bucket: ' + s3_results_bucket + '\n' \
                            + 's3_key_log_file: ' + s3_key_log_file + '\n' \
                            + 's3_key_result_file: '+ s3_key_result_file

                helpers.send_email_ses(
                        recipients=email,
                        subject=msg_subject,
                        body=msg_body
                        )
                # Delete the message from the queue
                try:
                    msg.delete()
                except Exception as e:
                    logging.error(e)
                    print(e)

if __name__ == '__main__':
    sys.exit(main())









### EOF
