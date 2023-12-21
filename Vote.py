import random
from socket import timeout
import time
from datetime import datetime
import logging
from jwt import decode
import psycopg2
import simplejson as json
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer
from Stream import delivery_report
from kafka import KafkaConsumer
import time
conf = {
    'bootstrap.servers': '164.92.85.68:9092',
}

consumer = KafkaConsumer(
        'voters_topic',
        bootstrap_servers=[conf],
        auto_offset_reset='latest', 
        enable_auto_commit=False
        # value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
conf = {
'bootstrap.servers': '164.92.85.68:9092',
}
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("read_kafka_write_postgres")

logger.info("Kafka connection successful")

# producer = SerializingProducer(conf)


def consume_messages():
    result = []
    consumer.subscribe(['candidates_topic'])
    try:
        while True:
            msg = consumer.poll(timeout_ms=10)
            if msg is None:
                continue
            end_time = time.time() + 120 # the script will run for 2 minutes
            for ms in msg: 
                print(ms)
                if time.time() > end_time:
                    break
            # elif msg.
            # elif msg.error():
            #     if msg.error().code() == KafkaError._PARTITION_EOF:
            #         continue
            #     else:
            #         print(msg.error())
            #         break
            # else:
            #     result.append(json.loads(msg.value().decode('utf-8')))
            #     if len(result) == 3:
            #         print(result)
                    # return result

            time.sleep(5)
    except KafkaException as e:
        print(e)

def consume_voters_topics():
    if __name__ == "__main__":
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

            # candidates
            candidates_query = cur.execute("""
                
                    SELECT * FROM voters;
            """)
            voters = cur.fetchall()
            # print(voters)
            voters = [voters[0] for voter in voters]
            if len(voters) == 0:
                raise Exception("No voters found in database")
            else:
                # print(voters)

                consumer.subscribe(['voters_topic'])
                try:
                    while True:
                        msg = consumer.poll(timeout_ms=1)
                        if msg is None:
                            continue                                          
                        else:
                            # voer = json.loads(msg.values())
                            voter =lambda msg:json.loads(msg.decode('utf-8'))
                            
                            print(voter)
                except KeyboardInterrupt:
                    pass
                finally:
                    consumer.close()
                            # chosen_candidate = random.choice(voters)
                            # vote = voter | chosen_candidate | {
                            #     "voting_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                            #     "vote": 1
                            # }
                            # print(vote)                       
#                             try:
#                                 print("User {} is voting for candidate: {}".format(vote['voter_id'], vote['candidate_id']))
#                                 cur.execute("""
#                                         INSERT INTO votes (voter_id, candidate_id, voting_time)
#                                         VALUES (%s, %s, %s)
#                                     """, (vote['voter_id'], vote['candidate_id'], vote['voting_time']))

                                # connect.commit()
                

                            #     producer.produce(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
                            #         'votes_topic',
                            #         key=vote["voter_id"],
                            #         value=json.dumps(vote),
                            #         on_delivery=delivery_report
                            #     )
                
                            # except Exception as e:
                            # print("Error: {}".format(e))
                    connect.rollback()
                    # continue
                    time.sleep(0.2)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

            print('Database connection closed.')
consume_voters_topics()