#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 21 23:32:54 2017
@author: Wu Hang
"""

import argparse
import logging
import googlefinance
import json
from urllib2 import HTTPError
from kafka import KafkaProducer
import time
import atexit
import schedule

class get_stock():
      #set the three arguments, stock code, kafka broker location, kafka topic
      def __init__(self,code,server,topic):
            self.__code=code
            self.__server=server
            self.__topic=topic
            self.__producer=KafkaProducer(bootstrap_servers=self.__server)
      #reassign new values to variables
      def set_code(self,code):
            self.__code=code
      def set_server(self,server):
            self.__server=server
      def set_topic(self,topic):
            self.__topic=topic
      
      #initialied the logging 
      def logger(self):
            logging.basicConfig()
            logger=logging.getLogger()
            logger.setLevel(logging.DEBUG)
            return logger
      
      #Both print the log to console and store the log in log files
      def print_log(self,msg):
            logger=self.logger()
            f=logging.FileHandler("/Users/Dukecat/python_practice/stock.log")
            f.setLevel(logging.DEBUG)
            formatter = logging.Formatter("%(asctime)-15s %(message)s")
            f.setFormatter(formatter)
            logger.addHandler(f)
            logger.debug(msg)
      
      #check whether the stock is valid or not
      def getQuotes(self,code):
            try:
                  msg=json.dumps(googlefinance.getQuotes(code))
                  return msg
            except HTTPError:
                  self.logger().error("Please enter correct stock code!")
      
      #send the message from kafka producer
      def send(self,topic,code):
            msg=self.getQuotes(code)
            self.__producer.send(topic=topic,value=msg,timestamp_ms=time.time())
            self.print_log(msg)
      #execute the send message job              
      def deliver(self):
            schedule.every(1).second.do(self.send,self.__topic,self.__code)
            while True:
                  schedule.run_pending()
                  time.sleep(1)
      # close kafka producer
      def shut_down(self):
            self.logger().debug('exiting the program')
            self.__producer.flush(10)
            self.__producer.close()
            self.logger().debug('kafka producer has been closed')

            
if __name__=='__main__':
      
      # setup command line arguments
      parser = argparse.ArgumentParser()
      parser.add_argument('code', help='the code of the stock to collect')
      parser.add_argument('topic', help='the kafka topic push to')
      parser.add_argument('server', help='the location of the kafka broker server')
      args=parser.parse_args()
      code=args.code
      topic=args.topic
      server=args.server
      #instantiate the get_stock
      stock=get_stock(code,server,topic)
      #release the kafka producer resources
      atexit.register(stock.shut_down)
      #run 
      stock.deliver()

