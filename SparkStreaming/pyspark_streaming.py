# @Author: Hang Wu <Dukecat>
# @Date:   2017-02-05T22:33:52-05:00
# @Email:  wuhang0613@gmail.com
# @Last modified by:   Dukecat
# @Last modified time: 2017-02-08T18:47:08-05:00

# - read data from kafka
# - do computation
# - wirte back to kafka

import argparse
import sys
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging
import atexit



class sparkStreaming():
    def __init__(self,topic,target_topic,kafka_broker):
        self.topic=topic
        self.kafka_broker=kafka_broker
        self.kafka_producer=KafkaProducer(bootstrap_servers=kafka_broker)
        self.target_topic=target_topic
        sc=SparkContext("local[2]","AveragePrice")
        sc.setLogLevel("INFO")
        self.ssc=StreamingContext(sc,5)

        logging.basicConfig()
        self.logger=logging.getLogger()
        self.logger.setLevel(logging.INFO)



    def process(self,timeobj,rdd):
        # - groupby stock symbols
        def group(record):
            data=json.loads(record[1].decode("utf-8"))[0]
            return data.get("StockSymbol"),(float(data.get("LastTradePrice")),1)
        newRDD=rdd.map(group).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).map(lambda (symbol,price):(symbol,price[0]/price[1]))
        results=newRDD.collect()
        for ele in results:
            msg={
                "StockSymbol":ele[0],
                "AveragePrice":ele[1]
            }
            try :
                self.kafka_producer.send(self.target_topic,value=json.dumps(msg))
                self.logger.info("successfully send processed data to '%s', %s " %(self.target_topic,msg))
            except KafkaError as kerrors:
                self.logger.warn("Falied to send data, the error is '%s' " %(kerrors.message))


    def creatStream(self):

        directKafkaStream=KafkaUtils.createDirectStream(self.ssc,[self.topic],{"metadata.broker.list":self.kafka_broker})
        return directKafkaStream

    def run(self):
        directKafkaStream=self.creatStream()

        directKafkaStream.foreachRDD(self.process)
        self.ssc.start()
        self.ssc.awaitTermination()


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument("topic",help="this is the topic to receive data from kafka producer")
    parser.add_argument("target_topic",help="this is the topic to send processed data to kafka broker")
    parser.add_argument("kafka_broker",help="this is the kafka broker")
    args=parser.parse_args()
    topic=args.topic
    target_topic=args.target_topic
    kafka_broker=args.kafka_broker

    KafkaSpark=sparkStreaming(topic,target_topic,kafka_broker)

    KafkaSpark.run()
