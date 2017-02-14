# Big-Data
This repo is my experience and code with big data technologies, including Kafka, Cassandra, Spark, ElasticSearch. All the code is written in Python. Kafka as the high volume data transmitter, Cassandra as the NoSQL database, Spark can do streaming process, ElasticSearch as the fast search engine, node.js as the front end server.

# What did I do?
Firstly, I get stock data from google finance and transmitted the data by Kafka; Then utilized spark streaming processed the raw data from KafkaBroker and computed the average price of stock of every timestamp; Pushed the data to redis hub for server to read; Finally, displaying the real-time dynamic data using Bootstrap，jQuery and D3.js. 
