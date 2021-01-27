# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template, 
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3', 
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))
   
  # Extract the job ID from the S3 key
  filename = s3_key.split('/')[-1]
  user_id = session['primary_identity']
  id_fn = filename.split('~')
  job_id = id_fn[0]
  fn = id_fn[1]
  st = int(time.time())
  submit_time = str(datetime.fromtimestamp(st))

  # Persist job to database
  # Move your code here...
  data = {"job_id": job_id,
          "user_id": user_id,
          "input_file_name": fn,
          "s3_inputs_bucket": bucket_name,
          "s3_key_input_file": s3_key,
          "st":st,
          "submit_time": submit_time,
          "job_status": "PENDING"}
  
  '''
  References:
  1. https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.put_item
  2. https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html
  '''
  dynamo = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  annotations = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  annotations.put_item(Item = data)
  
  # Send message to request queue
  # Move your code here...
  '''
  Reference:
  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
  '''
  client = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
  msg = json.dumps(data)
  response = client.publish(
          TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'], 
          Message=msg
          )

  return render_template('annotate_confirm.html', job_id=job_id)

  

"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  # Get list of annotations to display
  '''
  Reference:
  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.query
  '''
  user_id = session['primary_identity']
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  try:
    response = table.query(
            IndexName = 'user_id_index',
            ProjectionExpression='job_id,submit_time,input_file_name,job_status',
            KeyConditionExpression=Key('user_id').eq(user_id)
            )
  except ClientError as err:
    print (err)

  return render_template('annotations.html', annotations=response['Items'])


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  # Define variables 
  user_id = session['primary_identity']
  profile = get_profile(identity_id = user_id)
  free_access_expired = False 
  # Get information from dynamodb table
  '''
  Reference:
  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.get_item
  '''
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  try:
      annotation = table.get_item(
                    Key = {'job_id':id},
                    ProjectionExpression = 'job_id, submit_time, complete_time, ct, \
                                            input_file_name, job_status, user_id, \
                                            s3_key_result_file, s3_key_input_file'
                    )
  except ClientError as err:
      print(err)

  # Create input file url for user to download
  '''
  Reference:
  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_url
  '''
  s3_client = boto3.client('s3', config=Config(signature_version='s3v4'), \
          region_name=app.config['AWS_REGION_NAME'])
  try:
      input_url = s3_client.generate_presigned_url(
              ClientMethod = 'get_object',
              Params = {
                  'Bucket': app.config['AWS_S3_INPUTS_BUCKET'],
                  'Key': annotation['Item']['s3_key_input_file']
                  },
              ExpiresIn = app.config['AWS_SIGNED_REQUEST_EXPIRATION']
              )
  except ClientError as err:
      print (err)
  annotation['Item']['input_file_url'] = input_url

  # Create result file url for user to download
  if annotation['Item']['job_status'] == 'COMPLETED':
    try:
        result_url = s3_client.generate_presigned_url(
                ClientMethod = 'get_object',
                Params = {
                    'Bucket': app.config['AWS_S3_RESULTS_BUCKET'],
                    'Key': annotation['Item']['s3_key_result_file']
                    },
                ExpiresIn = app.config['AWS_SIGNED_REQUEST_EXPIRATION']
                )
    except ClientError as err:
        print (err)
    annotation['Item']['result_file_url'] = result_url  

    # Set restriction for free_user
    if profile.role == 'free_user':
        if time.time() >= annotation['Item']['ct'] + app.config['FREE_USER_DATA_RETENTION']:
            free_access_expired = True

  # Check that the requested job ID belongs to the user that is currently authenticated
  if annotation['Item']['user_id'] == user_id:
    return render_template('annotation_details.html', annotation=annotation['Item'], \
        free_access_expired=free_access_expired)
  else: 
    return 'Not authorized to view this job.'

"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  # Get information for dynamodb table 
  '''
  Reference:
  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.get_item
  '''
  user_id = session['primary_identity']
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  try:
      response = table.get_item(
                Key = {'job_id':id},
                ProjectionExpression = 'user_id, s3_key_log_file'
              )
  except ClientError as err:
      print(err)
  # Check that the requested job Id belongs to the user that is currently authenticated
  if response['Item']['user_id'] == user_id:
      key = response['Item']['s3_key_log_file']
      s3 = boto3.resource('s3')
      object = s3.Object(app.config['AWS_S3_RESULTS_BUCKET'], key)
      log_file_contents = object.get()['Body'].read().decode('utf-8')
      return render_template('view_log.html', job_id=id, log_file_contents=log_file_contents)
  else:
      return "Not authorized to view this job"

"""Subscription management handler
"""
import stripe

@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Process the subscription request
    token = str(request.form['stripe_token']).strip()

    # Create a customer on Stripe
    stripe.api_key = app.config['STRIPE_SECRET_KEY']
    try:
      customer = stripe.Customer.create(
        card = token,
        plan = "premium_plan",
        email = session.get('email'),
        description = session.get('name')
      )
    except Exception as e:
      app.logger.error(f"Failed to create customer billing record: {e}")
      return abort(500)

    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!

    # Find all the subsciber's jobs from annotation db  
    '''
    Reference:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.query
    '''
    dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    try:
         job_list = table.query(
                 IndexName = 'user_id_index',
                 KeyConditionExpression=Key('user_id').eq(session['primary_identity']),
                 ProjectionExpression = 'job_id'
                 )
    except ClientError as err:
        print (err)
    
    # Update the 'upgrade_premium' Attribute in the table 
    # And stop the archive process for the subscriber 
    '''
    Reference:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
    '''
    for item in job_list['Items']:
        job_id = item['job_id']
        try:
            update = table.update_item(
                    Key={'job_id':job_id},
                    UpdateExpression='SET upgrade_premium=:val',
                    ExpressionAttributeValues={':val':True},
                    ReturnValues='UPDATED_NEW'
                    )
        except ClientError as err:
            print (err)

    # Publish a notification message to the SNS topic to kick off the restoration
    '''
    Reference:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
    ''' 
    client = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
    response = client.publish(
            TopicArn = app.config['AWS_SNS_RESULTS_RESTORE_TOPIC'],
            Message = session['primary_identity']
            )

    # Display confirmation page
    return render_template('subscribe_confirm.html', 
      stripe_customer_id=str(customer['id']))


"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF
