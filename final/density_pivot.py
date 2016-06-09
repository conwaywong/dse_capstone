#!/usr/bin/env python

import json
import datetime
from pprint import pprint
import csv

def IntNone(i):
    try:
        return int(i)
    except:
        return None

def FloatNone(f):
    try:
        return float(f)
    except:
        return None

with open('Zip_Zhvi_AllHomes.csv') as cfile:
    crdr = csv.DictReader(cfile)
    for r in crdr:
        zipcode = IntNone(r['RegionName'])
        city = r['City'].replace("'", "")
        state = r['State']
        metro = r['Metro']
        county = r['CountyName']
        print "INSERT INTO County_Zip VALUES (%d, '%s', '%s', '%s', '%s');"%(zipcode,city,state,metro, county)
        # Insert into County_Zip
        for mk in [k for k in r if IntNone(k.split('-')[0]) >= 2008]:
            yr = IntNone(mk.split('-')[0])
            mon = IntNone(mk.split('-')[1])
            day = 1
            month = datetime.date(yr, mon, day)
            avg_val = IntNone(r[mk])
            if avg_val is None:
                avg_val = "null"
            print "INSERT INTO Zillo_Home_Value VALUES (%d, '%s', %s);"%(zipcode,month,avg_val)

with open('station_meta_density.json', 'r') as jfile:
    data = json.load(jfile)

pdata = {}
keys = [k for k in data]

for sk, sid in data['station'].iteritems():
    zipcode = IntNone(data['zip'][sk])
    if zipcode is None:
        zipcode = "NULL"
    urban = IntNone(data['urban'][sk])
    if urban is None:
        urban = 0
    density = FloatNone(data['Density Per Sq Mile'][sk])
    if density is None:
        density = "NULL"
    print "UPDATE Traffic_Station SET ZIPCODE=%s, Urban='%s', Density=%s WHERE PEMS_ID=%d;"%(zipcode, urban, density, sid)
