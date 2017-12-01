
# coding: utf-8

# In[6]:



# Import appropriate modules from the client library.
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from googleads import dfp
from collections import defaultdict
from pyspark.sql.types import *
from pyspark.sql import functions as sf
from pyspark.sql.functions import expr
from pyspark.sql.functions import *
import pandas as pd
import time
sc = SparkContext()
sql = SQLContext(sc)


#connect to the dfp network
from googleads import dfp
dfp_client = dfp.DfpClient.LoadFromStorage()
network_service = dfp_client.GetService('NetworkService', version='v201708')
current_network = network_service.getCurrentNetwork()



def orders():
    print "Obtaining Campaign detals"
    client = dfp.DfpClient.LoadFromStorage()
    # Initialize appropriate service.
    order_service = client.GetService('OrderService', version='v201708')

    # Create a statement to select orders.
    statement = dfp.StatementBuilder()
    count = 0
    data_orders = pd.DataFrame(columns=['order_id','order_name','start_year','start_month','start_day','end_year','end_month','end_day','order_custom_fields','order_custom_fields_value'])
    while True:
        response = order_service.getOrdersByStatement(statement.ToStatement())      
        if 'results' in response:
                for order in response['results']:
                    list_items = []

                    if "customFieldValues" in order:
                        for custom_field in order["customFieldValues"]:
                            if dfp.DfpClassType(custom_field) == 'CustomFieldValue':                                           
                                data_orders.loc[count] = [order.id,str(order.name),order.startDateTime.date.year,order.startDateTime.date.month,order.startDateTime.date.day,order.endDateTime.date.year,order.endDateTime.date.month,order.endDateTime.date.day,custom_field.customFieldId,str(custom_field.value.value)]
                                count += 1
                            elif dfp.DfpClassType(custom_field) == 'DropDownCustomFieldValue':
                                data_orders.loc[count] = [order.id,str(order.name),order.startDateTime.date.year,order.startDateTime.date.month,order.startDateTime.date.day,order.endDateTime.date.year,order.endDateTime.date.month,order.endDateTime.date.day,custom_field.customFieldId,str(custom_field.customFieldOptionId)]   
                                count += 1
                statement.offset += statement.limit
        else:
            break

    data_orders_df = sql.createDataFrame(data_orders)
    data_orders_df = data_orders_df.withColumn("start_date",sf.concat(sf.col('start_year'),sf.lit('-'), sf.col('start_month'),sf.lit('-'), sf.col('start_day')))
    data_orders_df = data_orders_df.withColumn("end_date",sf.concat(sf.col('end_year'),sf.lit('-'), sf.col('end_month'),sf.lit('-'), sf.col('end_day')))                                               
    data_orders_df.write.mode('overwrite').parquet("gs://ds-url-catag/dfp_new/orders")
    print "Details obtained"
    return()



def get_customfields_highlevel():
    print "Obtaining custom fields high level"
    start_time = time.time()
    # Initialize appropriate service.
    client = dfp.DfpClient.LoadFromStorage()
    custom_field_service = client.GetService('CustomFieldService', version='v201708')

    # Create a statement to select custom fields.
    statement = dfp.StatementBuilder()
    # Retrieve a small amount of custom fields at a time, paging
    # through until all custom fields have been retrieved.
    count = 0
    data_cus_high = pd.DataFrame(columns=['cus_id','cus_name'])
    while True:
        response = custom_field_service.getCustomFieldsByStatement(statement.ToStatement())
        if 'results' in response:
            for custom_field in response['results']:
                # Print out some information for each custom field.
                data_cus_high.loc[count] = [str(custom_field['id']),str(custom_field['name'])]
                count += 1
            statement.offset += statement.limit
        else:
            break
    data_cus_high_df = sql.createDataFrame(data_cus_high)
    data_cus_high_df.write.mode('overwrite').parquet("gs://ds-url-catag/dfp_new/custom_fields_high_level")
    print "Custom fields high level obtained"
    return(start_time)

def get_customfields_detailed():
    print "Obtaining custom fields detailed"
    # Initialize appropriate service.
    client = dfp.DfpClient.LoadFromStorage()
    custom_field_service = client.GetService('CustomFieldService', version='v201708')

    # Create a statement to select custom fields.
    statement = dfp.StatementBuilder()
    count = 0
    data_cus_detailed = pd.DataFrame(columns=['customFieldId','id','displayName'])
    while True:
        response = custom_field_service.getCustomFieldsByStatement(statement.ToStatement())
        if 'results' in response:
            for custom_field in response['results']:
                if  custom_field['id'] == 8796 or custom_field['id'] == 8916:
                    for sub_field in custom_field['options']:
                        data_cus_detailed.loc[count] = [str(sub_field['id']),str(sub_field['customFieldId']),str(sub_field['displayName'])]
                        count += 1
            statement.offset += statement.limit
        else:
            break
    print "Custom fields detailed level obtained"
    data_cus_detailed_df = sql.createDataFrame(data_cus_detailed)
    data_cus_detailed_df.write.mode('overwrite').parquet("gs://ds-url-catag/dfp_new/custom_fields_detailed")
    return()
    
    
#processing orderdata 
def process_order():
    print "processing Campaigns"
    data_orders_new = sql.read.parquet("gs://ds-url-catag/dfp_new/orders/")

    #preprocess orders
    #reading in data
    custom_fields = sql.read.parquet("gs://ds-url-catag/dfp_new/custom_fields_detailed/")
    custom_fields_high = sql.read.parquet("gs://ds-url-catag/dfp_new/custom_fields_high_level/")
    #joining to get the values of ids
    version_1 = data_orders_new.join(custom_fields,custom_fields.customFieldId == data_orders_new.order_custom_fields_value,'left_outer').select([(xx) for xx in data_orders_new.columns+[custom_fields.displayName.alias('Values')]])
    version_2 = version_1.join(custom_fields_high,custom_fields_high.cus_id == version_1.order_custom_fields,'left_outer').select([(xx) for xx in version_1.columns+[custom_fields_high.cus_name]])

    #transformations
    df = version_2.withColumn('Values',when(col('Values').isNull() ,col('order_custom_fields_value')).otherwise(col('Values')))

    #transposing rows to columns

    data_orders_piv = df.groupBy("order_id","order_name","start_date","end_date").pivot("cus_name").agg(expr("coalesce(first(Values))"))

    data_orders_fin = data_orders_piv.select(col("order_id").alias("Order_id"),col("order_name").alias("Order_name"),col("start_date").alias("Start_date"),col("end_date").alias("End_date"),col("BBM Performance Category").alias("BBMPerformanceCategory"), col("IAB Tier 1 Categorization").alias("IABTier1Categorization"), col("IO End Date").alias("IOEndDate"), col("IO Start Date").alias("IOStartDate"), col("MAI Campaign Flag").alias("MAICampaignFlag"), col("Net IO Value (After Discount)").alias("NetIOValue"), col("Offer Type").alias("OfferType"))
    data_orders_fin.write.mode('overwrite').parquet("gs://ds-url-catag/dfp_new/orders_final")
    data_orders_fin.write.mode('overwrite').parquet("gs://ds-taste-dfp/order_details")
    print "Done"
    
    return()

    
def main():

     orders()
     get_customfields_highlevel()
     get_customfields_detailed()
     process_order()
     sc.stop()

