import findspark
findspark.find()
#findspark.init("C:/Spark/spark-2.4.7-bin-hadoop2.7")

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType, DoubleType

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


# Define a function to write data to postgres
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
    print("Stream Data Processing Starting ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from kafka topic
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of orders_df: ")
    df_stream.printSchema()

    df_time = df_stream.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define a schema for the data
    orders_schema=StructType() \
	.add("Invoice",StringType()) \
	.add("StockCode",StringType()) \
	.add("Description",StringType()) \
	.add("Quantity",StringType()) \
	.add("InvoiceDate",StringType()) \
	.add("Price",StringType()) \
	.add("CustomerID",StringType()) \
	.add("Country",StringType())

    df = df_time.select(F.from_json(F.col("value"), orders_schema).alias("order"), "timestamp")

    df1 = df.select("order.*", "timestamp")
    df1.printSchema()

    # Execute some data-manipulation on our stream data
    df2 = df1.withColumn('Description', F.when(F.isnan(F.col('Description')), 'Description Not Provided').otherwise(F.col('Description')))
    df3 = df2.withColumn("Cancelled",F.col("Invoice").rlike("^C"))\
    .withColumn('Cancelled', F.when(F.col('Cancelled') == 'true', '1').otherwise('0'))
    df4 = df3.where(F.col("CustomerID").isNotNull())
    df5 = df4.withColumn('Price', F.when(F.col('Price') < '0', '0').otherwise(F.col('Price'))) \
    .withColumn('Quantity', F.when(F.col('Quantity') < '0', '0').otherwise(F.col('Quantity')))
    df6 = df5.withColumn("Total", F.round(df5.Price*df5.Quantity, 2)) \
    .withColumn("Price", df5["Price"].cast(DoubleType())) \
    .withColumn("Quantity", df5["Quantity"].cast(DoubleType())) \
    .withColumn("InvoiceDate", df5["InvoiceDate"].cast(TimestampType()))


    # Calculate Total Amount per Invoice(Country, Date and Consumer)
    df7=df6.groupBy('Invoice','InvoiceDate','CustomerID','Country','Cancelled') \
    .agg({'Total':'sum'}).select('Invoice','InvoiceDate','CustomerID','Country','Cancelled',F.col('sum(total)').alias('total_amount'))


    df7.printSchema()


    # # ------Writing aggrgated data in PostgreSQL for Analysis-------
    postgresql_stream=df7.writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode('update') \
        .foreachBatch(write_to_postgresql) \
        .start()
        # .awaitTermination()


    orders_agg_write_stream = df7 \
        .writeStream \
        .trigger(processingTime='2 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .option("checkpointLocation", "temp/spark-checkpoint") \
        .format("console") \
        .start()
        # .awaitTermination()

    spark.streams.awaitAnyTermination()


    print("Stream Processing Successfully Completed ! ! !")