# pip install -U psycopg2
# pip install -U SQLAlchemy
# postgresql-9.4.1208.jre7.jar
# time spark-submit --driver-class-path postgresql-9.4.1208.jre7.jar  --jars postgresql-9.4.1208.jre7.jar --master local[4] traffic_etl_spark.py 2>spark.log
import os
import datetime
from glob import glob
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StructType, StructField, LongType, FloatType, IntegerType
from sqlalchemy import create_engine

sc = SparkContext("local", "Capstone ETL")
sqlContext = SQLContext(sc)
url = "jdbc:postgresql://localhost/dse_traffic"
#"jdbc:postgresql://localhost:3306/employees?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;
engine = create_engine('postgresql://johngill@localhost:5432/dse_traffic', echo=True)

def truncTable(name):
    print 'Truncating table',name
    connection = engine.connect()
    trans = connection.begin()
    try:
        connection.execute('truncate table %s cascade;' % (name))
        trans.commit()
    except:
        trans.rollback()

truncTable('freeways')
truncTable('county_city')
truncTable('traffic_station')
truncTable('chp_inc')

def ZeroFloat(f):
    try:
        return float(f)
    except:
        return None


def ZeroInt(i):
    try:
        return int(i)
    except:
        return None

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def date_row(f):
    f_date = map(int, os.path.basename(f).split('.')[0].split('_')[-3:])
    f_date = datetime.date(f_date[0], f_date[1], f_date[2])
    return sc.textFile(f).map(lambda x: (f_date, x))

def stripSPM(f):
    field = str(f)
    while(len(field) > 0 and not is_number(field)):
        field = field[1:]
    return float(field) if len(field) > 0 else 0.0
udf = UserDefinedFunction(lambda x: stripSPM(x), FloatType())

s_type = sqlContext.read.format('jdbc').options(url=url, dbtable='public.st_type').load()

# Read in the Metadata files
path = 'meta/*/*/*'
# path = 'meta/2008/*/*'
allFiles = glob(path + ".txt")
meta_files = sc.union([date_row(f) for f in allFiles])

md_rows = meta_files.map(lambda l: [l[0]]+l[1].split('\t')) \
                    .map(lambda p: Row(effective_start=p[0], pems_id=p[1], Fwy=ZeroInt(p[2]), Dir=p[3],
                                        district_id=ZeroInt(p[4]), County=ZeroInt(p[5]), City=ZeroInt(p[6]),
                                        state_pm=p[7], abs_pm=ZeroFloat(p[8]), latitude=ZeroFloat(p[9]),
                                        longitude=ZeroFloat(p[10]), length=ZeroFloat(p[11]), Type=p[12],
                                        num_lanes=ZeroFloat(p[13]), name=p[14]))

station_meta = sqlContext.createDataFrame(md_rows)
st_cols = station_meta.columns # Get the initial list of columns
station_meta = station_meta.fillna({'City': -1})
station_meta = station_meta.select(*[udf(column).alias('state_pm') if column == 'state_pm' else column for column in station_meta.columns])

# Drop duplicates w/o including file data
station_meta = station_meta.dropDuplicates([c for c in st_cols if c != 'effective_start'])
station_meta = station_meta.dropna(subset=['latitude','longitude'], how='all')

# Join against the ST_type to set the FK
st_cols = [c for c in st_cols if c != "Type"]+['id']
station_meta = station_meta.join(s_type, station_meta.Type == s_type.type, 'inner').select(st_cols).withColumnRenamed('id','type_id')
st_cols = station_meta.columns
station_meta.cache()

# Create the Freeway table
freeways = station_meta.select('Fwy', 'Dir')
freeways = freeways.dropDuplicates()
# Assign the ID
row_with_index = Row('num', 'direction', 'id')
freeways = sqlContext.createDataFrame(freeways.rdd.zipWithIndex().map(lambda r: row_with_index(*list(r[0]) + [r[1]])))
freeways.select('id', 'num', 'direction').write.jdbc(url=url, table='Freeways', mode='append')
freeways.cache()

# Join the Freeway information back to set the FK
cond = [station_meta.Fwy == freeways.num, station_meta.Dir == freeways.direction]
st_cols = [c for c in st_cols if not (c == 'Fwy' or c == 'Dir')]+['id']
station_meta = station_meta.join(freeways, cond, 'inner').select(st_cols).withColumnRenamed('id','fwy_id')
st_cols = station_meta.columns
station_meta.cache()

# Create the County_City table
county_city = station_meta.select('County', 'City')
county_city = county_city.dropDuplicates()
# Assign the ID
row_with_index = Row('County_FIPS_ID', 'City_FIPS_ID', 'id')
county_city = sqlContext.createDataFrame(county_city.rdd.zipWithIndex().map(lambda r: row_with_index(*list(r[0]) + [r[1]])))
county_city.select('id', 'County_FIPS_ID', 'City_FIPS_ID').write.jdbc(url=url, table='County_City', mode='append')
county_city.cache()

# Join the County_City information back to set the FK
cond = [station_meta.County == county_city.County_FIPS_ID, station_meta.City == county_city.City_FIPS_ID]
st_cols = [c for c in st_cols if not (c == 'County' or c == 'City')]+['id']
station_meta = station_meta.join(county_city, cond, 'inner').select(st_cols).withColumnRenamed('id','ccid_id')
st_cols = station_meta.columns
station_meta.cache()

# Setting the effective_end is just easier in pandas.
# Number of rows is relatively small at this point, just convert to Pandas and write to PG
station_meta = station_meta.toPandas()
station_meta['effective_end'] = None

for v,c in station_meta['pems_id'].value_counts().iteritems():
    if c == 1:
        continue
    cnt = 0
    prev_date = None
    for r in station_meta[station_meta['pems_id'] == v].sort_values('effective_start', ascending=False).iterrows():
        if cnt != 0:
            station_meta.loc[r[0],'effective_end']  = prev_date
        prev_date = r[1]['effective_start']
        cnt += 1

station_meta.reset_index(drop=True, inplace=True)
station_meta.to_sql('traffic_station', con=engine, if_exists='append', index_label='id')

# Read in the CHP files
path = 'chp_incidents_day/*/*/*'
# path = 'chp_incidents_day/2008/*/*'
allFiles = glob(path + ".txt.gz")
chp_files = sc.textFile(','.join(allFiles))

chp_rows = chp_files.map(lambda l: l.split(',')) \
                .map(lambda p: Row(cc_id=ZeroInt(p[0]), cc_code=p[1], inc_num=ZeroInt(p[2]), time=p[3], description=p[4],
                                   latitude=ZeroFloat(p[9]), longitude=ZeroFloat(p[10]), district_id=ZeroInt(p[11]),
                                   County_FIPS_ID=ZeroInt(p[12]), City_FIPS_ID=ZeroInt(p[13]), Fwy=ZeroInt(p[14]), Dir=p[15],
                                   state_pm=p[16], abs_pm=ZeroFloat(p[17]), severity=p[18], duration=ZeroInt(p[19])))

chp_incidents = sqlContext.createDataFrame(chp_rows)
chp_incidents = chp_incidents.fillna({'City_FIPS_ID': -1})
chp_incidents = chp_incidents.select(*[udf(column).alias('state_pm') if column == 'state_pm' else column for column in chp_incidents.columns])
chp_incidents = chp_incidents.dropna(subset=['latitude','longitude'], how='all')
chp_cols = chp_incidents.columns # Get the initial list of columns
chp_incidents.cache()

# Join the Freeway information back to set the FK
cond = [chp_incidents.Fwy == freeways.num, chp_incidents.Dir == freeways.direction]
chp_cols = [c for c in chp_cols if not (c == 'Fwy' or c == 'Dir')]+['id']
chp_incidents = chp_incidents.join(freeways, cond, 'inner').select(chp_cols).withColumnRenamed('id','fwy_id')
chp_cols = chp_incidents.columns
chp_incidents.cache()

# Join the County_City information back to set the FK
cond = [chp_incidents.County_FIPS_ID == county_city.County_FIPS_ID, chp_incidents.City_FIPS_ID == county_city.City_FIPS_ID]
chp_cols = [c for c in chp_cols if not (c == 'County_FIPS_ID' or c == 'City_FIPS_ID')]+['id']
chp_incidents = chp_incidents.join(county_city, cond, 'inner').select(chp_cols).withColumnRenamed('id','cc_id')
chp_incidents.withColumnRenamed('cc_id', 'id')
chp_incidents.cache()

chp_incidents = chp_incidents.toPandas()
chp_incidents.to_sql('chp_inc', con=engine, if_exists='append', index=False, chunksize=200000)
