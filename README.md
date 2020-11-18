# All in one: Kafka, Spark Streaming, PostgreSQL and Superset

This repository contains a docker-compose stack with Kafka, PostgreSQL and Superset.

Spark and Python should be installed localy.

We will start by simulating a streaming process then using Spark Stuctured streaming to execute some data-manipulation and write to PostgreSQL

Data used in this project [Online Retail II Data Set](https://archive.ics.uci.edu/ml/datasets/Online+Retail+II).

## Pipeline Architecture

![](demo/pipeline.png)


## Docker Compose Details

| Container | Image | port | 
|-|-|-|
| zookeeper | wurstmeister/zookeeper | 2181 |
| kafka | wurstmeister/kafka | 9092 |
| kafka_manager | hlebalbau/kafka_manager | 9000 |
| postgres | postgres | 5416 |
| pgadmin | dpage/pgadmin4 | 5050 |
| superset | amancevice/superset | 8088 |

# Quickstart

## Clone the Repository and install the requirements

```
git clone https://github.com/amine-akrout/Spark_Stuctured_Streaming.git
```

```
cd Spark_Stuctured_Streaming
pip install -r requirement.txt
```

Make sure you have Installed [Spark 2.4.7](https://spark.apache.org/news/spark-2-4-7-released.html)

## Running Docker Compose

To run docker compose simply run the following command:

```
docker-compose up -d
```
![](demo/docker-stack.PNG)



## PostgreSQL configuration

You can access the pgadmin GUI through http://localhost:8080

### 1) Create a Server
- hostname : postgres
- port : 5432
- username : superset
- password : superset 

You can change them in the config file

![](demo/pg-server.PNG)

### 2) Create a Database and a table
```
cd postgresql
python create_database.py
python create_table
```

This will create a database orders and a table retail using psycopg2 library (you can change the names using the data.ini file)

![](demo/pg-data-table.PNG)



## Kafka Spark Stuctured Streaming configuration

### 1) Kafka Producer
Start the stream of data 
```
spark-submit kafka_producer.py
```
this will simulate a streaming process from our data (20 rows/sec)

#### Setup Kafka Manager

Access http://localhos:9000/addCluster :

* Cluster name: Kafka
* Zookeeper hosts: localhost:2181

### 2) Spark Consumer 
```
spark-submit --conf "spark.driver.extraClassPath=$SPARK_HOME/jars/kafka-clients-2.2.2.jar" \
             --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7,org.postgresql:postgresql:42.2.18 \
             SparkConsumer.py
```

This will allow Spark to :
* Construct a streaming DataFrame that reads from kafka topic
* Execute some data-manipulation on our stream data
* Write aggrgated data in PostgreSQL
* Optionally : write batches in the console for debugging

You can check if the table retail is updated in pgAdmin
![](demo/pg-update-table.PNG)

For Debugging you can print the batches in the console 
![](demo/batches.PNG)


#### Spark UI
![](demo/spark.PNG)


## Superset configuration

### 1) Initialise Database

```
docker exec -it superset superset-init
```
Initialize the database with an admin user and Superset tables


You can always change them in the superset_config.py file


### 2) Connect our table "retail" to superset

To set up superset we need to configure it. In order to do this, access http://localhost:9000 and create a new source following the fields:
* Database: orders
* SQLalchemy URI =postgresql+psycopg2://superset:superset@postgres:5432/orders

This will create a database orders and a table retail using psycopg2 library (you can change the names using the data.ini file)

![](demo/pg-data-table.PNG)




### 4) Create Dashboard

Finally !

We can now create some charts using superset and use them to make a real time Dashboard
