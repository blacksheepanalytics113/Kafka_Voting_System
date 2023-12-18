import random
import time
from datetime import datetime
import psycopg2
import simplejson as json
from confluent_kafka import Consumer,KafkaException,KafkaError,serializing_producer,ConsumerGroupState,SerializingProducer
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
        producer = SerializingProducer(conf)
        result = []
        consumer.subscribe(['candidates_topic'])
        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break
                else:
                    result.append(json.loads(msg.value().decode('utf-8')))
                    if len(result) == 3:
                        return result

                # time.sleep(5)
        except KafkaException as e:
            print(e)
            connect.commit()     
            # close the communication with the PostgreSQL
            cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
   
        print('Database connection closed.')
