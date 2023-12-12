import random
import psycopg2
import requests
import simplejson as json
from confluent_kafka import SerializingProducer
import random()



BASE_URL = 'https://randomuser.me/api/?nat=gb'
PARTIES = ["Management Party", "Savior Party", "Tech Republic Party"]
random.seed(42)


def generate_voter_data():
    response = requests.get(BASE_URL)
generate_voter_data()