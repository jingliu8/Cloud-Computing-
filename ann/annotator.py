from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from subprocess import Popen, PIPE, DEVNULL
from configparser import SafeConfigParser
import subprocess
import logging 
import boto3
import json
import sys
import os

def main(argv=None):
    '''
    Reference:
    https://docs.python.org/3/library/configparser.html
    '''
    config = SafeConfigParser(os.environ)
    config.read('ann_config.ini')
    
    AwsRegionName = config.get('AWS', 'AwsRegionName')
    DatabaseName = config.get('Dynamodb', 'DatabaseName')
    InputBucket = config.get('Bucket', 'InputBucket')

    RunFile = config.get('Path', 'RunFile')
    ResultFolder = config.get('Path', 'ResultFolder')
    
    QName = config.get('SQS', 'QueueName')
    max_num_msg = int(config.get('SQS', 'MaxNumberOfMessages'))
    vis_timeout = int(config.get('SQS', 'VisibilityTimeout'))
    wait_sec = int(config.get('SQS', 'WaitTimeSeconds'))

    # Connect to SQS and get the message queue
    '''
    Reference:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.receive_messages
    '''
    sqs = boto3.resource('sqs', region_name=AwsRegionName)
    queue = sqs.get_queue_by_name(QueueName=QName) 
    # Poll the message queue in a loop
    while True:
        # Attempt to read a message from the queue
        # Use long polling - DO NOT use sleep() to wait between polls
        messages = queue.receive_messages(
                AttributeNames=['All'],
                MaxNumberOfMessages=max_num_msg,
                VisibilityTimeout=vis_timeout,
                WaitTimeSeconds=wait_sec
                )
        # If message read, extract job parameters from the message body as before
        if len(messages) > 0:
            print ("Received {0} messages...".format(str(len(messages))))
            
            # Iterate each message
            for msg in messages:
                # msg_id = msg.message_id
                # msg_rh = msg.receipt_handle
                msg_body_m = json.loads(msg.body)['Message']
                data = json.loads(msg_body_m) 
                
                # Get the input file S3 object and copy it to a local file 
                # Create folder ('results') in anntools to save the objects from s3
                if not os.path.exists(ResultFolder):
                    os.makedirs(ResultFolder)

                # Download the input file from s3 to local
                '''
                Reference:
                https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.download_file
                ''' 
                s3 = boto3.client('s3', region_name=AwsRegionName)
                key = data['s3_key_input_file']
                filename = key.split('/')[-1]
                id_fn = filename.split('~')
                job_id = id_fn[0]
                fn = id_fn[1]
                directory = ResultFolder + '/' + filename
                s3.download_file(InputBucket, key, directory)
            
                # Launch annotation job as a background process
                '''
                Reference:
                https://pymotw.com/2/subprocess/
                '''
                try:
                    process = Popen([sys.executable, RunFile, directory, key])
                except subprocess.SubprocessError as e:
                    logging.error(e)
                    print (e)

                # Update the 'job_status' key in the database to running 
                '''
                Reference:
                https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
                '''
                dynamodb = boto3.resource('dynamodb', region_name=AwsRegionName)
                table = dynamodb.Table(DatabaseName)
                try:
                    response = table.update_item(
                            Key={'job_id': job_id},
                            UpdateExpression='SET job_status=:val',
                            ConditionExpression=Attr('job_status').eq('PENDING'),
                            ExpressionAttributeValues={':val':'RUNNING'},
                            ReturnValues='UPDATED_NEW'
                            )
                except ClientError as e:
                    logging.error(e)
                    print (e)

                # Delete the message from the queue, if job was successfully submitted
                try: 
                    msg.delete()
                except Exception as e:
                    logging.error(e)
                    print (e)

if __name__ == "__main__":
    sys.exit(main())
