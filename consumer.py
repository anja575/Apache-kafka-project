from kafka import KafkaConsumer
import requests, json

consumer = KafkaConsumer('topic2', bootstrap_servers='localhost:9092', group_id='my-group')

nodejs_server_url = 'http://localhost:8000/api/receive-data'

for message in consumer:
    
    weather_data = message.value.decode('utf-8')
    weather_data_json = json.loads(weather_data)
    
    response = requests.post(nodejs_server_url, json=weather_data_json)
    
    print(f"Response from Node.js server: {response.text}")

consumer.close()