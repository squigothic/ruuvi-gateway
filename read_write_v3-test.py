#!/usr/bin/env python3
import time
import sys
from datetime import datetime
import boto3
import requests
import simplejson as json
from ruuvitag_sensor.ruuvitag import RuuviTag, RuuviTagSensor
from config import taglist, bucket, user, timeout


API_URL = 'http://192.168.1.60:8080'
username = 'test'
password = 'test'

URL = API_URL+'/login'
credentials = {
  'username': username,
  'password': password
}
r = requests.post(URL, json.dumps(credentials))

response = json.loads(r.content)



s3 = boto3.resource('s3')
obj = s3.Object(bucket, taglist)
body = obj.get()['Body'].read()

taglist = json.loads(body)

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


today = datetime.now()

try:
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
      'data': data,
      'dateOfMeasurement': today.strftime("%Y-%m-%d")
    }
    print(sendable)
    sendables.append(sendable)
  
  
  URL = API_URL+'/measurements/'+username+'/add'
  r = requests.post(URL, json.dumps(sendables, encoding="utf8"), headers = {
    'Authorization': 'Bearer '+response['token'], 'Content-Type': 'application/json;charset=UTF-8'}
    )
  print(r)

  def checkForUnreachables():
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
