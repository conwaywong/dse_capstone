#!/bin/bash

import psycopg2
import json

'''
Generates json arrays used to populate the demo CHP heatmap.
'''


def write_file(rows, filename):
  stations_array = []
  f = open(filename, 'w')
  for row in rows:
      station_dict = {}
      station_dict["inc_num"] = row[0]
      station_dict["url"] = row[4]
      station_dict["lat"] = row[2]
      station_dict["lng"] = row[3]
      print station_dict
      stations_array.append(json.dumps(station_dict))
  f.write("markers = [")
  f.write(",".join(stations_array))
  f.write("];")
  f.close()

try:
    conn=psycopg2.connect(host='alvahouse322.mynetgear.com', port=55432, dbname='dse_traffic', user='cwong', password='csw129')
except:
    print "I am unable to connect to the database."

cur = conn.cursor()

try:
    cur.execute("""select inc_num, time, latitude, longitude, d.description, severity from chp_inc i, chp_desc d where d.id = i.desc_id""")
except:
      print "I can't SELECT from traffic_station"
rows = cur.fetchall()
write_file(rows, 'chp_all.json')

try:
    cur.execute("""select inc_num, time, latitude, longitude, d.description, severity from chp_inc_collision i, chp_desc d where d.id = i.desc_id""")
except:
      print "I can't SELECT from traffic_station"
rows = cur.fetchall()
write_file(rows, 'chp_collisions_all.json')

try:
    cur.execute("""select inc_num, time, latitude, longitude, d.description, severity from chp_inc_collision i, chp_desc d where time between '2010-01-04' AND '2010-01-09' and d.id = i.desc_id""")
except:
      print "I can't SELECT from traffic_station"
rows = cur.fetchall()
write_file(rows, 'chp_collisions_no_rain.json')

try:
    cur.execute("""select inc_num, time, latitude, longitude, d.description, severity from chp_inc_collision i, chp_desc d where time between '2010-01-18' AND '2010-01-23' and d.id = i.desc_id""")
except:
      print "I can't SELECT from traffic_station"
rows = cur.fetchall()
write_file(rows, 'chp_collisions_rain.json')

conn.close()
