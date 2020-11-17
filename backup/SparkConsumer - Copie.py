import findspark
findspark.find()
#findspark.init("C:/Spark/spark-2.4.7-bin-hadoop2.7")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StringType,IntegerType

import time

import os
## Import Parameters from config. file
from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini')
#Kafka
TOPIC = config.get('kafka','topic')
BOOTSTRAP_SERVERS = config.get('kafka','bootstrap_servers')
# Postgres
HOST = config.get('postgresql','postgresql_host')
PORT = config.get('postgresql','postgresql_port')
TABLE = config.get('postgresql','postgresql_table')
URL = config.get('postgresql','postgresql_url')
DRIVER = config.get('postgresql','postgresql_driver')
USER = config.get('postgresql','postgresql_user')
PWD = config.get('postgresql','postgresql_pwd')


#Set our configuration
kafka_topic_name = TOPIC
kafka_bootstrap_servers = BOOTSTRAP_SERVERS

postgresql_host=HOST
postgresql_port=PORT



def write_to_postgresql(df,epoch_id):
    df.write \
    .format('jdbc') \
    .options(url=URL,
            driver=DRIVER,
            dbtable=TABLE,
            user=USER,
            password=PWD,
            ) \
    .mode('append') \
    .save()

if __name__ == "__main__":
    print("Welcome to DataMaking !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of orders_df: ")
    orders_df.printSchema()

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define a schema for the orders data
    # order_id,order_product_name,order_card_type,order_amount,order_datetime,order_country_name,order_city_name,order_ecommerce_website_name
    orders_schema=StructType() \
	.add("order_id",StringType()) \
	.add("created_at",StringType()) \
	.add("customer_id",StringType()) \
	.add("discount",StringType()) \
	.add("product_id",StringType()) \
	.add("quantity",StringType()) \
	.add("subtotal",StringType()) \
	.add("tax",StringType()) \
	.add("total",StringType())

    # 8,Wrist Band,MasterCard,137.13,2020-10-21 18:37:02,United Kingdom,London,www.datamaking.com
    orders_df2 = orders_df1.select(from_json(col("value"), orders_schema).alias("order"), "timestamp")

    orders_df3 = orders_df2.select("order.*", "timestamp")
    orders_df3.printSchema()

    #-----Getting customer Data from HDFS
    customer_df=spark.read.csv("./data/customers.csv",header=True,inferSchema=True)
    for i in customer_df.columns:
        customer_df=customer_df.withColumnRenamed(i,i.lower())

    customer_df=customer_df.withColumnRenamed('id','customer_id')
    customer_df.show(5,False)

    #----Joining Orders and Customers table by customer_id----- 
    orders_df4=orders_df3.join(customer_df,orders_df3.customer_id==customer_df.customer_id,how='inner')
    orders_df4.printSchema()

    #-----Simple Aggregation--------
    orders_df5=orders_df4.groupBy('source','state') \
    .agg({'total':'sum'}).select('source','state',col('sum(total)').alias('total_sum_amount'))

    orders_df5.printSchema()


    #------Writing aggrgated data in PostgreSQL for Analysis-------
    postgresql_stream=orders_df5.writeStream \
        .trigger(processingTime='10 seconds') \
        .outputMode('update') \
        .foreachBatch(write_to_postgresql) \
        .start()\
        .awaitTermination()


    orders_agg_write_stream = orders_df5 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .option("checkpointLocation", "C:/Users/yomke/Desktop/DE/Real_time_Data_Analytics/spark-checkpoint") \
        .format("console") \
        .start() \
        .awaitTermination()

    print("Stream Data Processing Application Completed.")