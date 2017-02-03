# @Author: Hang Wu <Dukecat>
# @Date:   2017-02-02T11:24:29-05:00
# @Email:  wuhang0613@gmail.com
# @Last modified by:   Dukecat
# @Last modified time: 2017-02-03T11:49:45-05:00

from kafka import KafkaConsumer
import json
import logging
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import numpy as np

class ElasticSearch():

    def __init__(self,topic,kbroker):
        self.__topic=topic
        self.__kbroker=kbroker
        self.consumer=KafkaConsumer(topic,bootstrap_servers=self.__kbroker)
        self.es=Elasticsearch(['localhost'],http_auth=('elastic', 'changeme'),port=9200)

        self.actions=[]

    def create_index(self):
        request_body = {
	    "settings" : {
	        "number_of_shards": 5,
	        "number_of_replicas": 1
	    },

	    'mappings': {
	        'examplecase': {
	            'properties': {
	                'price': {'index': 'analyzed', 'type': 'float'},
	                'tradetime': {'index': 'analyzed', 'format': 'dateOptionalTime', 'type': 'date'},
	            }}}
	}
        self.es.indices.create(index="test_index",body=request_body)

    def deliver(self):
        bulk=[]
        for msg in self.consumer:
            parsed=json.loads(msg.value)[0]
            price=float(parsed.get("LastTradePrice"))
            tradetime=parsed.gete=("LastTradeDateTime")
            dct={"price":price,"tradetime":tradetime}
            op_dict = {
	        "index": {
	            "_index": 'test_index',
	            "_type": 'examplecase'
	               }
	           }
            bulk.append(op_dict)
            bulk.append(dct)
            self.es.bulk(index="test_index",body=bulk)
            bulk=[]
if __name__ == '__main__':
    ES=ElasticSearch("test","192.168.99.100:9092")
    ES.create_index()
    ES.deliver()
