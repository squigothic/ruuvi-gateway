#!/usr/bin/env python3
import time
import sys
from datetime import datetime
import boto3
import json
import simplejson as json
from ruuvitag_sensor.ruuvitag import RuuviTag, RuuviTagSensor
from config import taglist, bucket, user, timeout


s3 = boto3.resource('s3')
obj = s3.Object(bucket, taglist)
body = obj.get()['Body'].read()

taglist = json.loads(body)

tags = []

measurements = RuuviTagSensor.get_data_for_sensors(tags, timeout)

for tag in taglist['tags']:
   measurements[tag['mac']]['name'] = tag['name']
   measurements[tag['mac']]['friendlyname'] = tag['friendlyname']

dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('StagingRuuviMeasurements')

today = datetime.now()

try:
  with table.batch_writer() as batch:
    for measurement in measurements.values():
      timestamp_tag = str(int(time.time()))+"_"+taglist['user'][:3]+measurement['name'][-1]
      data = {
        'temperature': measurement['temperature'],
        'humidity': measurement['humidity'],
        'pressure' : int(measurement['pressure']),
        'timestamp': int(time.time()),
        'friendlyname': measurement['friendlyname'],
      }
      batch.put_item(
        Item={
          'Person': taglist['user'],
          'Timestamp_Tagname': timestamp_tag,
          'Data': json.dumps(data, ensure_ascii=False, encoding="utf8"),
          'MeasurementDate': today.strftime("%Y-%m-%d")
        }
      )

  print(f'Last successfull update at {str(time.ctime())}')
except:
  print(f'Failed to update at {str(time.ctime())}, reason {sys.exc_info()[0]}')
