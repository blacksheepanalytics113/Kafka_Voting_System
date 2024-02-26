import random
import time
from datetime import datetime
import psycopg2
import simplejson as json
from confluent_kafka import Consumer,KafkaException,KafkaError,serializing_producer,ConsumerGroupState
from kafka import KafkaConsumer
# from Kafka_Create_Topic import kafka_create_topic_main
from Stream import delivery_report
import time
import logging
from Kafka_Create_Topic import kafka_create_topic_main

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def fetch_and_insert_messages():
    print('Connecting To Database............')
    connect = psycopg2.connect(
                    host= "db-postgresql-lon1-10501-do-user-15128192-0.c.db.ondigitalocean.com",
                    database= "defaultdb",
                    user= "doadmin",
                    password= "AVNS_18bVhfxQtTTBCXwY6Lw",
                    port=25060
                )
    cur = connect.cursor()
    # topic = kafka_create_topic_main()
    topic = 'voters_topic'
    print("starting Kafka.........")
    KAFKA_BOOTSTRAP_SERVER = "164.92.85.68" + ":9092"
    consumer = KafkaConsumer(bootstrap_servers = KAFKA_BOOTSTRAP_SERVER,
        group_id = "cassandra_consumer_group",
        auto_offset_reset='earliest',
        value_deserializer = lambda v: json.loads(v.decode('utf-8'))
        )
    print("Set up Kafka..............")
    topic = topic
    run_duration_secs = 30

    # consumer = Consumer(consumer)
    consumer = consumer
    consumer.subscribe(topics='voters_topic')
    print("subscribed.......")
    

    start_time = time.time()
    try:
        while True:
            elapsed_time = time.time() - start_time
            if elapsed_time >= run_duration_secs:
                break
            
            msg = consumer.poll(timeout_ms=1)
            # print(msg)
            if msg is None:
                continue
            else:
                voter =lambda msg:json.loads(msg.decode('utf-8'))
                # print(voter)

                # Insert And Verify Data within the database 
                cur.execute("""
                    SELECT * FROM voters;
                """)
                voters = cur.fetchall()
                # print(voters)
                for voter_id in voters:
                    if voter:
                        logger.warning(f'Skipped existing voters: voter_id={voter_id}')
                    else:
                        cur.execute(voter)
                    logger.info(f'Received and inserted: Voter_Identity={voter_id}')
                            
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt. Closing consumer.")
    finally:
        consumer.close()
        connect.commit()
        cur.close()
fetch_and_insert_messages()
        


# def consume_kafka_messages():
#     try:
#         print("Connecting To Database............")
#         connect = psycopg2.connect(
#                     host= "db-postgresql-lon1-10501-do-user-15128192-0.c.db.ondigitalocean.com",
#                     database= "defaultdb",
#                     user= "doadmin",
#                     password= "AVNS_18bVhfxQtTTBCXwY6Lw",
#                     port=25060
#                 )
#         cur = connect.cursor() 
#         KAFKA_BOOTSTRAP_SERVER = "164.92.85.68" + ":9092"
#         KAFKA_TOPIC_NAME = "voters_topic"
#         print(KAFKA_TOPIC_NAME)

#         print("starting Kafka....")
#         consumer = KafkaConsumer(
#         bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
#         value_deserializer = lambda v: json.loads(v.decode('utf-8')),
#         auto_offset_reset='earliest'
#         )

#         print("setup Kafka....")
#         consumer.subscribe(topics='voters_topic')
#         print("subscribed")
#         for message in consumer:
#             print ("%d:%d: v=%s" % (message.partition,message.offset,message.value))
            

#     except (Exception, psycopg2.DatabaseError) as error:
#         print(error)

#         print('Database connection closed.')
# consume_kafka_messages()
