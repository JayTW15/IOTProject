import json
import random
from confluent_kafka import Producer
import time

locationName = {"TEMP_COLD_AISLE_A01":{'min': 64.4, 'max': 68 }, 
                "TEMP_HOT_AISLE_A01":{'min': 77.0, 'max': 86.0}, 
                "TEMP_CRAC_UNIT_01":{'min': 61.0, 'max': 64.0},
                "TEMP_UPS_ROOM_01":{'min':72.0, 'max': 79.0}, 
                "TEMP_NETWORK_RACK_R05":{'min': 68.0, 'max': 75.0}, 
                "TEMP_STORAGE_ZONE_S01":{'min':70.0, 'max': 77.0}}

def generateData(): #generates sensor data
    result = []
    result.append({"timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                   "sensor_type": "temperature"})
    for key, value in locationName.items():
        temp = random.uniform(value['min'], value['max'])
        result.append({"device_id": key,
                    "value": round(temp, 2),
                    "unit": "fahrenheit"})
    return json.dumps(result, indent=2)

def produce_data(): #generates data, creates producer and produces it to kafka
    res = generateData()
    producer = Producer({'bootstrap.servers':'localhost:9092'})
    producer.produce('temp_recorded', value=res.encode('utf-8'))
    producer.flush()
    
if __name__ == '__main__': #every 2 seconds generate mock data for kafka

    #temp_consumer = start_consumer(group_id = 'temp_consumer')
    try:
        while True:
            produce_data()
            time.sleep(2)
            #consumeData(temp_consumer)
    except KeyboardInterrupt:
        print("Stopping Data generator...")