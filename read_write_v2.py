#!/usr/bin/env python3
import boto3
import json
import pymysql
from ruuvitag_sensor.ruuvitag import RuuviTag
from config import taglist, bucket, user, passwd, db, port

s3 = boto3.resource('s3')

obj = s3.Object(bucket, taglist)

body = obj.get()['Body'].read()

tags = json.loads(body)

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
