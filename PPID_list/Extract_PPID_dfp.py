
# coding: utf-8

from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import pyspark.sql.functions as func
from datetime import datetime, timedelta
import re
import subprocess
import sys
sc = SparkContext()
sql = SQLContext(sc)


#read a process click data from all the months
data_click_path = "gs://ds-taste-dfp/aggregated_data/click_aggregated_by_date/*/*.parquet"
data_clicks = sql.read.parquet(data_click_path)
data_aggre = data_clicks.groupby('orderid','line_itemid','rdid','IABTier1Categorization').agg(func.sum("impressions").alias('impressions'),func.sum("clicks").alias('Clicks'))


#Getting the users with atleast one click
data_final_aggre = data_aggre
sample = data_final_aggre.where(col('Clicks') > 0)

#pass the list of order names 
orders = sys.argv[1]

#process the list of orders
orders_list = map(str, orders.strip('[]').split(','))


#extract order ids  from the orders name
original_orders = sql.read.parquet("gs://ds-taste-dfp/order_details/")
included_orders = original_orders
included_orders = included_orders.withColumn('orders_ppid',col('Order_name'))

for i in orders_list:
    included_orders = included_orders.withColumn('orders_ppid',regexp_replace(col('orders_ppid'),i,'TRUE'))
raw_orders = included_orders.select('Order_id','Order_name','IOStartDate','IOEndDate').where(col('orders_ppid') == 'TRUE')

#print order names and order ids
print "\n Order details from DFP \n"
raw_orders.show()

#join to get the users with the specific orders
sample_one = sample.join(raw_orders,raw_orders.Order_id == sample.orderid).select([sample.orderid,sample.rdid]+[raw_orders.Order_name])
sample_two = sample_one.where(col('order_name') != 'null')

#Joining with demo data to get the adids
data_demo = sql.read.parquet("gs://ds-user-demographic/parquet/tastes_mau_1024.parquet/*.parquet")
rdids = sample_two.join(data_demo,data_demo.adid == sample_two.rdid).select([sample_two.orderid,sample_two.rdid]+[data_demo.adid])

#double check counts of the users
print "Check the orderids - Orders obtained from Click and Demo data\n"
rdids.select('orderid').distinct().show()

print "Check the number of campaigns\n"
rdids.select('orderid').distinct().count()

#get distinct adids
final_data =rdids.groupby('orderid','adid').count()

#convert to ppids
from hashlib import sha1
import binascii

SALT_A = "77EfzyR%iZtyTFAK7lyZ_X=B7e^b1atUx%W+Rx0A3ja~b5yjGL-q==po%nkL"

def adid_to_ppid(adid):
    text = adid + SALT_A 
    sha1_dig = sha1(text).digest()
    sha1hash = [ord(c) for c in sha1_dig]
    return binascii.hexlify(bytearray(sha1hash))

adid_ppid_udf = udf(adid_to_ppid, StringType())

final_data = final_data.withColumn('ppid',adid_ppid_udf('adid'))

#write to a folder in 3 partitions 
path = sys.argv[2]
final_data.coalesce(3).select('adid').write.mode('overwrite').format("com.databricks.spark.csv").save(path)
print "Written to Bucket in three partitions" + path


# In[96]:




