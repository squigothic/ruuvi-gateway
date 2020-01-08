#!/usr/bin/env python3
import boto3
import json
import pymysql
from ruuvitag_sensor.ruuvitag import RuuviTag
from config import taglist, bucket, user, passwd, db, port, ACCESS_ID, ACCESS_KEY

s3 = boto3.resource('s3', aws_access_key_id=ACCESS_ID, aws_secret_access_key=ACCESS_KEY)

obj = s3.Object(bucket, taglist)

body = obj.get()['Body'].read()

tags = json.loads(body)

print(tags)

measurements = []

for tag in tags:
    sensor = RuuviTag(tags[tag])
    state = sensor.update()
    #print('state: ' + str(state))
    datas = (tag, state['temperature'], round(state['pressure']), round(state['humidity']))
    measurements.append(datas)

sql = '''INSERT INTO observations (tagname, date, time, temperature, pressure, relativehumidity, timestamp)
  VALUES(%s, CURRENT_DATE(), CURRENT_TIME(), %s, %s, %s, UNIX_TIMESTAMP())'''

db = pymysql.connect(
  host = "127.0.0.1",
	user =  user,
	passwd =  passwd,
	db =  db,
	port = port)

with db:
  curs=db.cursor()
  curs.executemany(sql, measurements)
  db.commit()
  curs.close()
  db.close()
