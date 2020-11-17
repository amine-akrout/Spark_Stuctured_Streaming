from kafka import KafkaProducer
from json import dumps
import time
import csv
from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini')

TOPIC = config.get('kafka','topic')
BOOTSTRAP_SERVERS = config.get('kafka','bootstrap_servers')
FILEPATH = config.get('kafka','csv_filepath')





#creating producer object for ingest data to kafka topic
producer=KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,api_version=(0,10,1))

if __name__ == "__main__":
    #ingesting data as json into kafka topic
    with open(FILEPATH,'r') as csv_file:
        csv_reader=csv.reader(csv_file,delimiter=',')
        next(csv_reader)
        msg={}
        for row in csv_reader:
            msg['Invoice']=row[0]
            msg['StockCode']=row[1]
            msg['Description']=row[2]
            msg['Quantity']=row[3]
            msg['InvoiceDate']=row[4]
            msg['Price']=row[5]
            msg['CustomerID']=row[6]
            msg['Country']=row[7]
            print(msg)
            producer.send(TOPIC,dumps(msg).encode('utf-8'))
            time.sleep(0.05)