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

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

truncTable("observations")

files = glob.glob('output/*')
for i, f in enumerate(files):
    # if i > 0:
    #     break
    print "Reading",i,f
    obso_df = pd.read_csv(f, names=["station_id","district_id","year","doy","dow","f_dir","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9"],
                             dtype={"station_id":int,"district_id":int,"year":int,"doy":int,"dow":int,"f_dir":int})
    print obso_df.shape
    print obso_df.dtypes

    print "Drop dow, f_dir"
    obso_df.drop(["dow","f_dir"], inplace=True, axis=1)

    print "Convert to array"
    obso_df["flow_coef"] = obso_df.apply(lambda x: [x["e0"],x["e1"],x["e2"],x["e3"],x["e4"],x["e5"],x["e6"],x["e7"],x["e8"],x["e9"]], axis=1)

    print "Drop individual columns"
    obso_df.drop(["e0","e1","e2","e3","e4","e5","e6","e7","e8","e9"], inplace=True, axis=1)

    print "Write to csv"
    tf_name = 'all_obso'
    with open(tf_name, 'a') as obso_f:
        obso_df.to_csv(obso_f, header=None if i > 0 else True, index=False)

print "Transpose brackets"
os.system('tr "[]" "{}" < '+tf_name+' > '+tf_name+'_t')

print "COPY to postgresql"
obso_cols = ['station_id', 'district_id', 'year', 'doy', 'flow_coef']
# engine = create_engine('postgresql://jgill:john13GR8@alvahouse322.mynetgear.com:55432/dse_traffic', echo=True)
conn = engine.raw_connection()

with conn.cursor() as cur:
    with open(tf_name+'_t', 'r') as f:
        sql = "COPY {schema_name}.{table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)".format(
                                            schema_name="public", table_name='observations', columns=','.join(obso_cols))
        print sql
#         cur.copy_expert(sql, f)
# conn.commit()
# os.remove(tf_name)
