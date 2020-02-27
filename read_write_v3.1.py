#!/usr/bin/env python3
import time
import sys
from datetime import datetime
import boto3
import requests
#import json
import simplejson as json
from ruuvitag_sensor.ruuvitag import RuuviTag, RuuviTagSensor
from config import taglist, bucket, user, timeout, username, password

URL = 'http://192.168.100.196:8080/login'
credentials = {
  'username': username,
  'password': password
}
r = requests.post(URL, json.dumps(credentials))
print(r)
response = json.loads(r.content)



s3 = boto3.resource('s3')
obj = s3.Object(bucket, 'taglist-testing.json')
body = obj.get()['Body'].read()

taglist = json.loads(body)
print(taglist)

measurements = RuuviTagSensor.get_data_for_sensors([], timeout)

unreachableTags = []

for tag in taglist['tags']:
  if tag['mac'] not in measurements.keys():
    unreachableTags.append(tag['mac'])

for result in measurements:
  for tag in taglist['tags']:
    if tag['mac'] == result:
      measurements[tag['mac']]['name'] = tag['name']
      measurements[tag['mac']]['friendlyname'] = tag['friendlyname']

print('measurements ', measurements)
dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('StagingRuuviMeasurements')

today = datetime.now()

try:
  # with table.batch_writer() as batch:
  #   for measurement in measurements.values():
  #     timestamp_tag = str(int(time.time()))+"_"+taglist['user'][:3]+measurement['name'][-1]
  #     data = {
  #       'temperature': measurement['temperature'],
  #       'humidity': measurement['humidity'],
  #       'pressure' : int(measurement['pressure']),
  #       'timestamp': int(time.time()),
  #       'friendlyname': measurement['friendlyname'],
  #     }
  #     batch.put_item(
  #       Item={
  #         'Person': taglist['user'],
  #         'Timestamp_Tagname': timestamp_tag,
  #         'Data': json.dumps(data, ensure_ascii=False, encoding="utf8"),
  #         'MeasurementDate': today.strftime("%Y-%m-%d")
  #       }
  #     )
  sendables = []

  for measurement in measurements.values():
    data = {
        'temperature': measurement['temperature'],
        'humidity': measurement['humidity'],
        'pressure' : int(measurement['pressure']),
        'timestamp': int(time.time()),
        'friendlyname': measurement['friendlyname']
    }
    sendable = {
      'user': taglist['user'],
      'timestamp_tag': str(int(time.time()))+"_"+taglist['user'][:3]+measurement['name'][-1],
      'data': json.dumps(data, ensure_ascii=False, encoding="utf8"),
      'dateOfMeasurement': today.strftime("%Y-%m-%d")
    }
    sendables.append(sendable)
  print(json.dumps(sendables))

  final = {
    'measurements': sendables
  }
  print('final'+json.dumps(final))
  
  URL = 'http://192.168.100.196:8080/measurements/'+username+'/add'
  r = requests.post(URL, headers = {'Authorization': 'Bearer '+response['token']}, json=json.dumps(final, ensure_ascii=False, encoding="utf8"))
  print(r)

  def checkForUnreachables():
    print('testing')
    if len(unreachableTags) > 0:
      missingTags = ''
      for i in unreachableTags:
        missingTags += i + ' '
      return 'Tags not found: ' + missingTags + '.'
    else:
      return ''

  print(f'{str(time.ctime())} Scan done, found {len(measurements)}. {checkForUnreachables()}')
except:
  print(f'Failed to update at {str(time.ctime())}, reason {sys.exc_info()[0]}')
