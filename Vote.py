import random
import time
from datetime import datetime

import psycopg2
import simplejson as json
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer
from Main import delivery_report

kafka_host = "164.92.85.68"
conf = {
    'bootstrap.servers':  f'{kafka_host}:9092',
}

consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(conf)
