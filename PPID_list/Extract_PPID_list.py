
# coding: utf-8

# In[16]:
import dfp_iab_data

#Crawl the data from dfp
print "Crawling data from dfp"
dfp_iab_data.main()


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
raw_orders = included_orders.select('Order_id','Order_name','Start_date','End_date').where(col('orders_ppid') == 'TRUE')

#print order names and order ids
print "\n Order details from DFP \n"
raw_orders.show()

#get the month details for orders
raw_orders = raw_orders.withColumn("month_start",raw_orders.Start_date.substr(6, 2))
raw_orders = raw_orders.withColumn("month_end",raw_orders.End_date.substr(6, 2))
raw_orders = raw_orders.withColumn("final_month",when(raw_orders.month_start == raw_orders.month_end,raw_orders.month_start).otherwise(raw_orders.month_end))
month_list = raw_orders.select('final_month').collect()
dict_month = {"8":"August","9":"September","10":"October","11":"November","12":"Decemeber"}

Dist_month =set(month_list)
Dist_month_list = list(Dist_month)


import pyspark.sql.functions as func
import re 

n = len(Dist_month_list)
print "Number of months of campaigns:",n
for i in range(0,n):
    if Dist_month_list[i].final_month in dict_month.keys():
        month = dict_month[Dist_month_list[i].final_month]
        print "Processing for the month of",month
        
        data_click_path = 'gs://ds-taste-dfp/aggregated_data/click_aggregated_by_date/' + month + '/*.parquet'
        data_clicks = sql.read.parquet(data_click_path)
        data_aggre = data_clicks.groupby('orderid','line_itemid','rdid','IABTier1Categorization').agg(func.sum("impressions").alias('impressions'),func.sum("clicks").alias('Clicks'))


        #Getting the users with atleast one click
        data_final_aggre = data_aggre
        sample = data_final_aggre.where(col('Clicks') > 0)


        #join to get the users with the specific orders
        sample_one = sample.join(raw_orders,raw_orders.Order_id == sample.orderid).select([sample.orderid,sample.rdid]+[raw_orders.Order_name])
        sample_two = sample_one.where(col('Order_name') != 'null')

        #Joining with demo data to get the adids
        data_demo = sql.read.parquet("gs://ds-user-demographic/parquet/tastes_mau_1024.parquet/*.parquet")
        rdids = sample_two.join(data_demo,data_demo.adid == sample_two.rdid).select([sample_two.orderid,sample_two.rdid]+[data_demo.adid])

        #double check counts of the users
        print "Check the orderids - Orders obtained from Click and Demo data\n"
        rdids.select('orderid').distinct().show()

        print "Check the number of campaigns:"
        print rdids.select('orderid').distinct().count()
        

        #get distinct adids
        final_data =rdids.groupby('orderid','adid').count()
        
        #number of adids
        print "Number of Adids:"
        print rdids.select('adid').distinct().count()

        #convert to ppids
        from hashlib import sha1
        import binascii

        SALT_A = "77EfzyR%iZtyTFAK7lyZ_X=B7e^b1atUx%W+Rx0A3ja~b5yjGL-q==po%nkL"

        def adid_to_ppid(adid):
            sha1hash = []
            pattern = re.compile("^[a-zA-Z0-9\-]{8}-[a-zA-z0-9]{4}-[a-zA-z0-9]{4}-[a-zA-z0-9]{4}-[a-zA-z0-9]{12}")
            if re.match(pattern,adid) is not None:
                text = adid + SALT_A
                sha1_dig = sha1(text).digest()
                sha1hash = [ord(c) for c in sha1_dig]
            return binascii.hexlify(bytearray(sha1hash))

        adid_ppid_udf = udf(adid_to_ppid, StringType())


        final_data = final_data.withColumn('ppid',adid_ppid_udf('adid'))

        
        #number of ppids
        print "Number of ppids:"
        print final_data.select('ppid').where(col('ppid') != 'null').distinct().count()
        
        #write to a folder in 3 partitions
	get_path =  sys.argv[2]
        path = get_path + month
        final_data.select('ppid').distinct().coalesce(3).write.mode('overwrite').format("com.databricks.spark.csv").save(path)
        print "Written to Bucket in three partitions " + path

        

