from kafka import KafkaConsumer, KafkaProducer
import json, time, requests

consumer = KafkaConsumer('topic1', bootstrap_servers='localhost:9092', group_id='consumer-producer-group')

producer = KafkaProducer(bootstrap_servers='localhost:9092', acks='all')  
 
for message in consumer:
    try:
        
        city = message.value.decode('utf-8')
        print(f"City: {city}")

        
        if city == 'Beograd':
            lat, lon = 44.787197, 20.457273
        elif city == 'Novi Sad':
            lat, lon = 45.267136, 19.833549
        elif city == 'Nis':
            lat, lon = 43.320902, 21.895758
        elif city == 'Kragujevac':
            lat, lon = 44.012792, 20.911419
        elif city == 'Subotica':
            lat, lon = 46.100844, 19.667652
        elif city == 'Uzice':
            lat, lon = 43.856258, 19.837570
        elif city == 'Zlatibor':
            lat, lon = 43.741189, 19.711782
        elif city == 'Cacak':
            lat, lon = 43.891674, 20.353506
        elif city == 'Priboj':
            lat, lon = 43.587326, 19.525106
        else:
            print(f"Unknown city: {city}")
            continue

       
        api_key = 'adbdd3a5a4ce47b9eaf32860554ca8ae'
        weather_api_url = f'https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}'
    
        response = requests.get(weather_api_url)
        
        
        weather_data = json.dumps(response.json())
        producer.send('topic2', value=weather_data.encode('utf-8'))
    
    except Exception as e:
        print(f"Error processing message: {e}")



producer.close() 
consumer.close()
