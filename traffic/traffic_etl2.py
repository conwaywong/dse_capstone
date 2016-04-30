#!/usr/bin/env python
import os
import sys
import glob
import pandas as pd
# Pandas display options
pd.options.display.max_rows = 999
pd.options.display.max_columns = 999
pd.set_option('expand_frame_repr', False)
import numpy as np
import csv
import datetime
from sqlalchemy import create_engine

debug = True

# Setup connection to the postgresql DB
engine = create_engine('postgresql://jgill:john13GR8@alvahouse322.mynetgear.com:55432/dse_traffic', echo=debug)

def truncTable(name):
    connection = engine.connect()
    trans = connection.begin()
    try:
        connection.execute('truncate table %s cascade;' % (name))
        trans.commit()
    except:
        trans.rollback()

def FloatNone(f):
    try:
        return float(f)
    except:
        return None

def IntNone(i):
    try:
        return int(i)
    except:
        return None

truncTable("observations")

files = glob.glob('output/*')
tf_name = 'all_obso'
obso_cols = ['station_id', 'district_id', 'year', 'doy', 'flow_coef']
with open(tf_name, "w") as outfile:
    outfile.write(','.join(obso_cols)+'\n')
    for i, f in enumerate(files):
        if i > 2:
            break
        print "Reading",i,f
        with open(f, "r") as infile:
            for line in infile:
                parts = line.translate(None, '"').split(",")
                key = map(lambda x: IntNone(FloatNone(x)), parts[:4])
                values = map(FloatNone, parts[6:])
                for v in key:
                    outfile.write('%s,'%(v))
                outfile.write('"'+str(values)+'"\n')
print "Transpose brackets"
os.system('tr "[]" "{}" < '+tf_name+' > '+tf_name+'_t')

print "COPY to postgresql"
# engine = create_engine('postgresql://jgill:john13GR8@alvahouse322.mynetgear.com:55432/dse_traffic', echo=True)
conn = engine.raw_connection()

with conn.cursor() as cur:
    with open(tf_name+'_t', 'r') as f:
        sql = "COPY {schema_name}.{table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)".format(
                                            schema_name="public", table_name='observations', columns=','.join(obso_cols))
        print sql
        cur.copy_expert(sql, f)
conn.commit()
# os.remove(tf_name)
# os.remove(tf_name+'_t')
