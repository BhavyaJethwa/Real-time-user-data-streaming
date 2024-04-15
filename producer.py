from datetime import datetime

def get_data():
    import json
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    return res 

def format_data(res):
    data = {}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = str(res['location']['street']['number']) + " " + str(res['location']['street']['name']) + res['location']['city'] + " " + res['location']['state'] + " " + res['location']['country']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['birth_date'] = res['dob']['date']
    data['age'] = res['dob']['age']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time

    res = get_data()
    res = format_data(res)

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer = lambda v: json.dumps(v).encode("utf-8"))

    producer.send('user' , res)

x=0
while x < 50:
    stream_data()
    x+=1