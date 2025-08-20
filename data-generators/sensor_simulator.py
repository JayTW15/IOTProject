import json
import random
from confluent_kafka import Producer
locationName = {"TEMP_COLD_AISLE_A01":{'min': 64.4, 'max': 68 }, 
                "TEMP_HOT_AISLE_A01":{'min': 77.0, 'max': 86.0}, 
                "TEMP_CRAC_UNIT_01":{'min': 61.0, 'max': 64.0},
                "TEMP_UPS_ROOM_01":{'min':72.0, 'max': 79.0}, 
                "TEMP_NETWORK_RACK_R05":{'min': 68.0, 'max': 75.0}, 
                "TEMP_STORAGE_ZONE_S01":{'min':70.0, 'max': 77.0}}

def getData():
    result = []
    for key, value in locationName.items():
        temp = random.uniform(value['min'], value['max'])
        result.append({"location": key,
                    "temperature": round(temp, 2),
                    "unit": "fahrenheit"})
    return json.dumps(result, indent=2)

def streamData():
    res = getData()
    producer = Producer({'bootstrap.servers':'localhost:9092'})
    producer.produce('temp_recorded', value=res.encode('utf-8'))
    producer.flush()

streamData()