#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 18 22:24:45 2019
@author: stenatu

Updated on Thu Nov 5 13:19:12 2020 IST
@author: sufiankaki

# Edit this lambda function which invokes your trained XgBoost Model deployed
# on the Greengrass Core to make predictions whenever new sensor data comes in
# The output of the lambda predictins are sent to IoT. If a Faulty part is found,
# the output is sent to SNS.

# To get this lambda function to work, fill out the TODOs.
"""

#
# Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#

import logging
import platform
import sys
from datetime import datetime
import greengrasssdk
import boto3
import random
import json
import xgboost as xgb
import pickle
import numpy as np


# Setup logging to stdout
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


# Creating a greengrass core sdk client
client = greengrasssdk.client("iot-data")

# Retrieving platform information to send from Greengrass Core
my_platform = platform.platform()
sns = boto3.client('sns')
model_path = '/greengrass-machine-learning/xgboost/xgboost-model'
TOPIC_ARN = 'arn:aws:sns:us-east-1:<ACCOUNT_ID>:<SNS_TopicName>' #TODO: enter your SNS Topic ARN here.
LAMBDA_TOPIC = 'xgboost/offline' #TODO: enter your subscription topic from Lambda to IoT

print("Imports invoked")

 # Load the model object.
model = pickle.load(open(model_path, 'rb'))



def predict_part(datapoint):
    
    data = [random.uniform(-1, 1)/10 for x in range(167)]
    data = [datapoint] + data
    
    dataarray = np.array([data])
    
    start = datetime.now()
    
    print(start)
    
   
    response = model.predict(xgb.DMatrix(dataarray))

    end = datetime.now()
    
    mytime = (end - start).total_seconds()*1000
    
    print("Offline Model RunTime = {} milliseconds".format((end - start).total_seconds()*1000))
    
    result = round(response[0])
    print(result)
    pred = round(result)
    
    # If Prediction == 1, then part is Faulty, else it is Not Faulty.
    if pred ==1:
        predicted_label = 'Faulty'
    else:
        predicted_label = 'Not Faulty'
    
    #publish results to local greengrass topic.
    if not my_platform:
        client.publish(topic=LAMBDA_TOPIC, payload='Predicted Label {} in {} milliseconds'.format(predicted_label, mytime))
    else:
        client.publish(topic=LAMBDA_TOPIC, payload=' Predicted Label {} in {} milliseconds. Sent from Greengrass Core running on platform: {}'.format(predicted_label, mytime, my_platform))
    
    #publish to SNS topic.
    if pred == 1:
        response = sns.publish(
        TopicArn=TOPIC_ARN,    
        Message='Faulty Part Found on Line 1. Immediate attention required.'    
        )
        print("Published to Topic")


# This is a dummy handler and will not be invoked
# Instead the code above will be executed in an infinite loop for our example
def function_handler(event, context):
    datapoint = json.loads(event["state"]["desired"]["property"])
    return predict_part(datapoint)
