# @Author: Hang Wu <Dukecat>
# @Date:   2017-02-11T22:04:34-05:00
# @Email:  wuhang0613@gmail.com
# @Last modified by:   Dukecat
# @Last modified time: 2017-02-11T22:12:41-05:00

# -read from kafka topic
# - publish to redis pub


from kafka import KafkaConsumer

import redis
import logging
import argparse
import  atexit

logging.basicConfig()
logger=logging.getLogger()
logger.setLevel(logging.INFO)

if __name__ == '__main__':
    # -set up command line

    parser=argparse.ArgumentParser()
    parser.add_argument("topic_name",help="the topic kafka consume from")
    parser.add_argument("kafka_broker")
    parser.add_argument("redis_channel",help='channel to publish to')
    parser.add_argument("redis_host")
    parser.add_argument("redis_port")

    args=parser.parse_args()
    topic_name=args.topic_name
    kafka_broker=args.kafka_broker
    redis_channel=args.redis_channel
    redis_host=args.redis_host
    redis_port=args.redis_port

    # - kafka consumer

    kafka_consumer=KafkaConsumer(topic_name,
        bootstrap_servers=kafka_broker)

    # - redis client

    redis_client=redis.StrictRedis(host=redis_host,port=redis_port)

    for msg in kafka_consumer:
        logger.info("receive new data from kafka %s" %msg.value)
        redis_client.publish(redis_channel,msg.value)
