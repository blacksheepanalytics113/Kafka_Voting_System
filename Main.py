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

