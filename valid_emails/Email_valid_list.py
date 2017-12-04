
# coding: utf-8

# In[815]:


from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
#sc.stop()
from pyspark.sql import SQLContext
sc =SparkContext()
sqlContext = SQLContext(sc)
import pyspark.sql.functions as func
import sys
from pyspark.sql.functions import countDistinct
import re
import sys

# In[816]:

path =sys.argv[1]
rdd = sc.textFile(path)
temp_var = rdd.map(lambda k: k.split(" "))
from pyspark.sql.types import *
fields = [StructField('emails', StringType(), True)]
schema = StructType(fields) 
final_data = sqlContext.createDataFrame(temp_var, schema) 


# In[817]:


1#Dealing with most of rules in IETF standards
def filter_one(email):
    indicate = ''
    if "\"" in email:
        filtered_email = email
    else:
        pattern = re.compile(r"((^[^.][(comment)|a-zA-Z0-9_.!#$%&'*+-/=?^_`{|}~+-.*!.]{0,64})@(([\[0-9:A-Z\]]+)|([a-zA-Z0-9-]){0,255})([\.]{1}[a-zA-Z0-9-.]+)?$)")
        if re.match(pattern,email) is not None:
            filtered_email = email
        else:
            filtered_email = None
    return(filtered_email)

filter_1 = udf(filter_one, StringType())


#dealing with full stops
def filter_two(email):
    local = ''
    domain = ''
    indicate = ''
    new_email = ''
    if email is not None :
        splitting = email.split("@")
        local = splitting[0]
        domain = splitting[1]
        pattern_one = re.compile(r"(^[^.][(comment)|a-zA-Z0-9_.!#$%&'*+-/=?^_`{|}~+-.*!.]*[^.])$")
        pattern_two = re.compile(r"(?<!\.)\.\.(?!\.)")
        if re.match(pattern_one,local) is not None:
            if re.search(pattern_two,local) is None:
                if re.search(pattern_two,domain) is None:
                    new_email = email
                else:
                    new_email = None
            else:
                    new_email = None
        else:
            new_email = None
    else:
        new_email = None
    return(new_email)

filter_2 = udf(filter_two, StringType())



formatted_data = final_data.withColumn('Filter_1',filter_1('emails'))
formatted_data = formatted_data.withColumn('Filter_2',filter_2('Filter_1'))
formatted_data = formatted_data.withColumn('valid_format',regexp_replace(col('Filter_2'),r"\(comment\)",""))





valid_emails = formatted_data.select('emails').where(col('valid_format') != 'null')
invalid_emails = formatted_data.select('emails').where(col('valid_format').isNull())

valid_emails.coalesce(1).write.mode('overwrite').format("csv").save("gs://ds-url-catag/emails_formats/valid_emails/")
invalid_emails.coalesce(1).write.mode('overwrite').format("csv").save("gs://ds-url-catag/emails_formats/invalid_emails/")

