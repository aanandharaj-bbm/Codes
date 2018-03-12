
# coding: utf-8

from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
sc =SparkContext()
sqlContext = SQLContext(sc)
import pyspark.sql.functions as func
import sys
from pyspark.sql.functions import countDistinct

#reading in the data
path=sys.argv[1]
df = sc.textFile(path).map(lambda line: line.split(",")).map(lambda line: (line[0],line[1])).toDF(['id','type'])
#Cleaning the data    
from pyspark.sql.types import StringType
import re
remove_quotes = udf(lambda x: re.sub('"','',x), StringType())
remove_slashes = udf(lambda x: re.sub('\\\\','',x), StringType())
#removing quotes from the data 
new_df = df.select(*[remove_quotes(column).alias(column) for column in df.columns])
new_df =  new_df.select(*[remove_slashes(column).alias(column) for column in df.columns])

# print the basic stats of stickers

print "\n Total number of stickers and their types:",new_df.dropDuplicates().count()
print "Distribution of stickers types"
new_df.groupby('type').agg(countDistinct('id').alias('cnt')).show()
path_write=sys.argv[2]
print "write to " + path_write
new_df.write.mode('overwrite').parquet(path_write)
