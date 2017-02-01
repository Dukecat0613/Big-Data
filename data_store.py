# @Author: Hang Wu <Dukecat>
# @Date:   2017-01-28T22:29:41-05:00
# @Email:  wuhang0613@gmail.com
# @Last modified by:   Dukecat
# @Last modified time: 2017-01-31T23:51:40-05:00

#-kafka read data
#- write data to cassandra
#-get message from kafka write to cassandra

import argparse
from kafka import KafkaConsumer
import logging
import json
import atexit
from kafka.errors import KafkaError
from cassandra.cluster import Cluster

class data_store():

    def __init__(self,topic,kbroker,cbroker,keyspace,data_table):
        self.__topic=topic
        self.__kbroker=kbroker
        self.__cbroker=cbroker
        self.__keyspace=keyspace
        self.__data_table=data_table
        self.consumer=KafkaConsumer(topic,bootstrap_servers=kbroker)
        self.cassandra_cluster=Cluster(contact_points=cassandra_broker.split(','))
        self.cassandra_session=self.cassandra_cluster.connect()

    # - logging file configuration

    def logger(self):
        Format="%(asctime)s - %(levelname)s - %(message)s"
        logging.basicConfig(format=Format)
        logger=logging.getLogger()
        logger.setLevel(logging.INFO)
        return logger

    def create(self):
        self.cassandra_session.execute("create keyspace if not exists %s with replication={'class':'SimpleStrategy','replication_factor':'3'} and durable_writes= 'true'" % self.__keyspace)
        self.cassandra_session.set_keyspace(self.__keyspace)
        self.cassandra_session.execute("create table if not exists %s(stock_symbol text,trade_time timestamp,trade_price float, primary key ((stock_symbol),trade_time))" %self.__data_table)

    def persist_data(self,stock_data):
        logger=self.logger()
        try:
            logger.debug('start to save data to cassandra %s', stock_data)
            parsed=json.loads(stock_data)[0]
            stock_symbol=parsed.get("StockSymbol")
            price=float(parsed.get("LastTradePrice"))
            tradetime=parsed.get("LastTradeDateTime")
            # - insert into cassandra
            statement="insert into %s (stock_symbol,trade_time,trade_price) values ('%s','%s',%f)" %(self.__data_table,stock_symbol,tradetime,price)
            self.cassandra_session.execute(statement)

            logger.info("Finished to save data for symbol: %s, price:%f,tradetime:%s" %(stock_symbol,price,tradetime))

        except Exception as ke:
            logger.error('Failed to save data to cassandra %s: ',ke)

    def store(self):
        for msg in self.consumer:
            self.persist_data(msg.value)


    def shut_down():
        try:
            self.consumer.close()
            self.session.shutdown()
        except KafkaError:
            self.logger().warn("falied to close data consumer")




if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument("topic_name",help='the kafka topic')
    parser.add_argument('kafka_broker',help='kafka broker')
    parser.add_argument("cassandra_broker",help='the ip of cassandra')
    parser.add_argument("keyspace",help='the keyspace to use in cassandra')
    parser.add_argument("data_table",help="the table to use")

    # -parse arguments
    args=parser.parse_args()
    topic_name=args.topic_name
    kafka_broker=args.kafka_broker
    cassandra_broker=args.cassandra_broker
    keyspace=args.keyspace
    data_table=args.data_table

    store=data_store(topic_name,kafka_broker,cassandra_broker,keyspace,data_table)

    # - create keyspace
    store.create()
    # - shut_down hook 
    atexit.register(store.shut_down)

    store.store()
