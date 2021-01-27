# Wrapper script for running AnnTools

##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'
from botocore.exceptions import ClientError
from subprocess import Popen, PIPE, DEVNULL
from boto3.dynamodb.conditions import Key, Attr
from configparser import SafeConfigParser
from datetime import datetime
import os, subprocess, logging, sys, time, driver, boto3 

class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self
    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f'Approximate runtime: {self.secs:.2f} seconds')

if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            '''
            Reference:
            https://docs.python.org/3/library/configparser.html
            '''
            config = SafeConfigParser(os.environ)
            config.read('ann_config.ini')
            AwsRegionName = config.get('AWS', 'AwsRegionName')
            ResultBucket = config.get('Bucket', 'ResultBucket')
            DatabaseName = config.get('Dynamodb', 'DatabaseName')
            ResultFolder = config.get('Path', 'ResultFolder')
            annot_vcf = config.get('Extension', 'annot_vcf')
            count_log = config.get('Extension', 'count_log')
            ARN = config.get('SNS', 'ARN')

            driver.run(sys.argv[1], 'vcf')
            # Upload the result file and the log file 
            key = sys.argv[2]
            key_front = key.split('.')[0]
            filename = key.split('/')[-1]
            name = filename.split('.')[0]
            id_fn = filename.split('~')
            job_id = id_fn[0]

            # Parameters
            annot_file = ResultFolder + '/' + name + annot_vcf
            log_file = ResultFolder + '/' + name + count_log
            annot_key = key_front + annot_vcf
            log_key = key_front + count_log
            
            # Upload 
            '''
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_file
            '''
            s3_client = boto3.client('s3')
            try:
                s3_client.upload_file(annot_file, ResultBucket, annot_key)
                s3_client.upload_file(log_file, ResultBucket, log_key)
            except ClientError as e:
                logging.error(e)
                print(e)

            # Clean up local job files
            input_file = ResultFolder + '/' + filename 
            os.remove(input_file)
            os.remove(annot_file)
            os.remove(log_file)
            # Update Database item
            '''
            Reference:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
            '''
            ct = int(time.time())
            complete_time = str(datetime.fromtimestamp(ct))
            dynamodb = boto3.resource('dynamodb', region_name=AwsRegionName)
            table = dynamodb.Table(DatabaseName)
            try:
                response = table.update_item(
                        Key={'job_id':job_id},
                        UpdateExpression="SET s3_results_bucket=:val1, s3_key_result_file=:val2, \
                                s3_key_log_file=:val3, complete_time=:val4, ct=:val5, \
                                job_status=:val6, upgrade_premium=:val7, archive_status=:val8",
                        ExpressionAttributeValues={
                            ':val1':ResultBucket,
                            ':val2':annot_key,
                            ':val3':log_key,
                            ':val4':complete_time,
                            ':val5':ct,
                            ':val6':'COMPLETED',
                            ':val7':False,
                            ':val8':False
                            },
                        ReturnValues='UPDATED_NEW'
                        ) 
            except ClientError as e:
                logging.error(e)
                print(e)

            # Publish notification to jing3_job_results
            '''
            Reference:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
            '''
            client = boto3.client('sns', region_name=AwsRegionName)
            response = client.publish(
                    TopicArn = ARN,
                    Message = job_id
                    )

    else:
        print('A valid .vcf file must be provided as input to this program.')

### EOF





