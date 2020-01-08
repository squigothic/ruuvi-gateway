#!/usr/bin/env python3
import time
import boto3
import json
import pymysql
import simplejson as json
from ruuvitag_sensor.ruuvitag import RuuviTag, RuuviTagSensor
from config import taglist2, bucket, user, passwd, db, port

s3 = boto3.resource('s3')
obj = s3.Object(bucket, taglist2)
body = obj.get()['Body'].read()

taglist = json.loads(body)

tags = []

for tag in taglist['tags']:
  tags.append(tag['mac'])

timeout = 4

measurements = RuuviTagSensor.get_data_for_sensors(tags, timeout)

for tag in taglist['tags']:
   measurements[tag['mac']]['name'] = tag['name']
   measurements[tag['mac']]['friendlyname'] = tag['friendlyname']

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('RuuviMeasurements')

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
        'Data': json.dumps(data, ensure_ascii=False, encoding="utf8")
      }
    )

print(f'Last successfull update at {str(time.localtime())}')