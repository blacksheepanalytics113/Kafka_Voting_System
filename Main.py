import random
import psycopg2
import requests
import simplejson as json
from confluent_kafka import SerializingProducer



BASE_URL = 'https://randomuser.me/api/?nat=gb'
PARTIES = ["Management Party", "Savior Party", "Tech Republic Party"]
random.seed(42)


def generate_voter_data():
    response = requests.get(BASE_URL)
    # print(response.json())
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        # Get Column Values 
        return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "cell_number": user_data['cell'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }
    else:
        return "Error fetching data"
# generate_voter_data()


def generate_candidate_data():
    candidate_number = 2 == 1
    total_parties = PARTIES[candidate_number]
    response = requests.get(BASE_URL + '&gender=' + ('female' if candidate_number  else 'male'))
    
    if response.status_code == 200:
        candidate_data = response.json()['results'][0]
        # print(candidate_data)
        return{
            "candidate_id": candidate_data['login']['uuid'],
            "candidate_name": f"{candidate_data['name']['first']} {candidate_data['name']['last']}",
            "party_affiliation": total_parties,
            "biography": "A brief bio of the candidate.",
            "campaign_platform": "Key campaign promises or platform.",
            "photo_url": candidate_data['picture']['large']
        }
    else:
        return "Error fetching data"
# generate_candidate_data()


def create_candidate_tables():
    try:
        print("connecting To PostgreSQL")
        connect = psycopg2.connect(
                    host= "db-postgresql-lon1-10501-do-user-15128192-0.c.db.ondigitalocean.com",
                    database= "defaultdb",
                    user= "doadmin",
                    password= "AVNS_18bVhfxQtTTBCXwY6Lw",
                    port=25060
                )
        cur = connect.cursor() 
        cur.execute("""
            CREATE TABLE IF NOT EXISTS candidates (
                candidate_id VARCHAR(255) PRIMARY KEY,
                candidate_name VARCHAR(255),
                party_affiliation VARCHAR(255),
                biography TEXT,
                campaign_platform TEXT,
                photo_url TEXT
            )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS votes (
                voter_id VARCHAR(255) UNIQUE,
                candidate_id VARCHAR(255),
                voting_time TIMESTAMP,
                vote int DEFAULT 1,
                PRIMARY KEY (voter_id, candidate_id)
            )
        """)   
        connect.commit()     
                # close the communication with the PostgreSQL
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
   
        print('Database connection closed.')
# # create_candidate_tables()


def Insert_voter_data():
    print('Connecting To Database............')
    connect = psycopg2.connect(
                    host= "db-postgresql-lon1-10501-do-user-15128192-0.c.db.ondigitalocean.com",
                    database= "defaultdb",
                    user= "doadmin",
                    password= "AVNS_18bVhfxQtTTBCXwY6Lw",
                    port=25060
                )
    cur = connect.cursor()
    response = requests.get(BASE_URL)
    # print(response.json())
    if response.status_code == 200:
        user_data = response.json()['results'][0]

        kafka_data = {}
        # Get Column Values 
        user_data['login']['uuid']
        print({
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "cell_number": user_data['cell'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        })
        # cur.execute("""
        #             INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number, address_street, address_city, address_state, address_country, address_postcode, email, phone_number, cell_number, picture, registered_age)
        #             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s)
        #             """,(user_data['login']['uuid'],f"{user_data['name']['first']} {user_data['name']['last']}",user_data['dob']['date'],user_data['gender'],user_data['nat'],
        #                 user_data['login']['username'],f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
        #                 user_data['location']['city'],user_data['location']['state'],user_data['location']['country'],user_data['location']['postcode'],user_data['email'],
        #                 user_data['phone'],user_data['cell'],user_data['picture']['large'],user_data['registered']['age']))
        connect.commit()
        cur.close()
# Insert_voter_data()

def insert_candidate_data():
         # get candidates from db 
    print('Connecting To Database............')
    connect = psycopg2.connect(
                    host= "db-postgresql-lon1-10501-do-user-15128192-0.c.db.ondigitalocean.com",
                    database= "defaultdb",
                    user= "doadmin",
                    password= "AVNS_18bVhfxQtTTBCXwY6Lw",
                    port=25060
                )
    cur = connect.cursor()
    response = requests.get(BASE_URL)
    # print(response.json())
    if response.status_code == 200:
        user_data = response.json()['results'][0] 
        cur.execute("""
            SELECT * FROM candidates
        """)
        candidates = cur.fetchall()
        # print(candidates)
        
        if len(candidates) == 0:
            for candidate in range(3):
                candidate_number = 2 == 1
                total_parties = PARTIES[candidate_number]
                candidate = generate_candidate_data()
                
                
                # print(candidate)
                return candidate['candidate_id']
                return candidate['candidate_name']
                return candidate['party_affiliation']
                return candidate['biography']
                return candidate['campaign_platform']
                return candidate['photo_url']
                # cur.execute("""INSERT INTO candidates (candidate_id,candidate_name) VALUES (%s,%s)""",str(candidate['candidate_id']),candidate['candidate_name'])
                connect.commit()
                cur.close()
# insert_candidate_data()

"""
Send Data from coming API TO Kafa Topic & postreSQL Simultaneously
voters_topic = 'voters_topic'
candidates_topic = 'candidates_topic'

"""

# producer = SerializingProducer({'bootstrap.servers': 'localhost:9092',})
for i in range(1000):
        voter_data = generate_voter_data()
        candidate_data = insert_candidate_data()
        print(voter_data)
        print(candidate_data)
