# Databricks notebook source
df=spark.read.option("header","true").option("inferSchema","true").csv("/FileStore/tables/dataset.csv")

# COMMAND ----------

df.count()
df.show(5)
df.printSchema()

# COMMAND ----------

df_null=df.filter(df.pm2_5.isNull())
df_null.count()

df_top_ten=df.limit(10)#spark.sql("select top 10* from DEMO")
df_top_ten.show()

df.describe(['so2']).show()
df.filter(df.stn_code%2==0).show()

# COMMAND ----------

df.groupBy("date").agg({'no2':'sum'}).show(5)


# COMMAND ----------

df.filter(df.state.startswith('A')).count()


#df_with_A.show(5)
#df.state
def columnVal(str):
  sp=str.split("/")
  return sp[2]


# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import *

def get_last_letter(animal):
  return animal[-4:]

get_last_letter_udf = udf(get_last_letter, StringType())
df_temp=df.filter(df.date.contains('2008'))

df_date=df.withColumn('year',get_last_letter_udf(df.date))
df_sample=df_date.sampleBy('year',fractions={'2008':0.3})
df_sample.show(5)
#df_date.filter(df_date.year=='2008').show(5)

# COMMAND ----------

df_br=spark.read.option("header","true").option("inferSchema","true").csv("/FileStore/tables/breweries_us.csv")

# COMMAND ----------

df_br.printSchema()

# COMMAND ----------

df_br.show(5)


# COMMAND ----------

import re
def extractLast(s):
  return re.match('.*?([0-9]+)$', s).group(1)

get_zipcode = udf(extractLast, StringType())

df_zipcode=df_br.withColumn('zipcode',get_zipcode(df_br.address))
#df_zipcode.show(5)

def iforggis(s):
  if(s.contains("orggis.com")):
    return True
  else:
    return False
iforggis_udf=udf(iforggis,BooleanType())  
df_orggis=df_br.filter(df_br.website.contains("origgis.com"))
#df_orggis=df_br.filter(iforggis_udf(df_br.website))
df_orggis.show()

df_br.select("state").distinct().count() #distinct number
df_br.filter(df_br.website.isNull()).count()


# COMMAND ----------

df_data=spark.read.option('header','true').csv("/FileStore/tables/data_set.csv")
df_data.printSchema()
df_data.withColumnRenamed(' |origin|destination|internal_flight_ids|','value')
df_data.show()
import pyspark.sql.functions as F
split_col = F.split(df_data['|origin|destination|internal_flight_ids|'], '|')
df_split = df_data.select(F.split(df_data.value,"|")).rdd.map(
              lambda x: x).toDF(schema=["col1","col2","col3"])


# COMMAND ----------

from pyspark.sql.functions import col, split

df_temp=df_data.select(split('|origin|destination|internal_flight_ids|', '\\|')).rdd.flatMap(lambda x: x).toDF(schema=["x","origin","destination","internal_flight_ids","y"])
df_temp.select('origin','destination','internal_flight_ids').show()

# COMMAND ----------


