import random
import time
from datetime import datetime
import psycopg2
import simplejson as json
# from confluent_kafka import Consumer,KafkaException,KafkaError,serializing_producer,ConsumerGroupState,SerializingProducer
from kafka import KafkaConsumer
# from Stream import delivery_report
# def Consume_votes():
#     kafka_host = "164.92.85.68"
#     conf = {
#         'bootstrap.servers':  f'{kafka_host}:9092',
#     }

    # consumer = Consumer(conf | {
    #     'group.id': 'voting-group',
    #     'auto.offset.reset': 'earliest',
    #     'enable.auto.commit': False
    # })
def consume_kafka_messages():
    try:
        print("Connecting To Database............")
        connect = psycopg2.connect(
                    host= "db-postgresql-lon1-10501-do-user-15128192-0.c.db.ondigitalocean.com",
                    database= "defaultdb",
                    user= "doadmin",
                    password= "AVNS_18bVhfxQtTTBCXwY6Lw",
                    port=25060
                )
        cur = connect.cursor() 
        KAFKA_BOOTSTRAP_SERVER = "164.92.85.68" + ":9092"
        KAFKA_TOPIC_NAME = "voters_topic"
        print(KAFKA_TOPIC_NAME)

        print("starting Kafka....")
        consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        value_deserializer = lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest'
        )

        print("setup Kafka....")
        consumer.subscribe(topics='voters_topic')
        print("subscribed")
        for message in consumer:
            print ("%d:%d: v=%s" % (message.partition,message.offset,message.value))
            

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

        print('Database connection closed.')
consume_kafka_messages()
