import json
import random
from confluent_kafka import Producer, Consumer, KafkaError
import time

locationName = {"TEMP_COLD_AISLE_A01":{'min': 64.4, 'max': 68 }, 
                "TEMP_HOT_AISLE_A01":{'min': 77.0, 'max': 86.0}, 
                "TEMP_CRAC_UNIT_01":{'min': 61.0, 'max': 64.0},
                "TEMP_UPS_ROOM_01":{'min':72.0, 'max': 79.0}, 
                "TEMP_NETWORK_RACK_R05":{'min': 68.0, 'max': 75.0}, 
                "TEMP_STORAGE_ZONE_S01":{'min':70.0, 'max': 77.0}}

def getData():
    result = []
    result.append({"timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                   "sensor_type": "temperature"})
    for key, value in locationName.items():
        temp = random.uniform(value['min'], value['max'])
        result.append({"device_id": key,
                    "value": round(temp, 2),
                    "unit": "fahrenheit"})
    return json.dumps(result, indent=2)

def streamData():
    res = getData()
    producer = Producer({'bootstrap.servers':'localhost:9092'})
    producer.produce('temp_recorded', value=res.encode('utf-8'))
    producer.flush()

def start_consumer(bootstrap_server = 'localhost:9092', group_id = None):
    config = {
        'bootstrap.servers': bootstrap_server, #for now this is local connection, later once we containerize the code we switch to 9093
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    consumer = Consumer(config)
    consumer.subscribe(["temp_recorded"])
    return consumer

def consumeData(consumer):
    message = consumer.poll(timeout=0.1)
    if message is None:
        return  # This prevents the crash
        
    if message.error():
        if message.error().code() == KafkaError._PARTITION_EOF:
            return
        else:
            print(f"Error: {message.error()}")
            return
    raw_value = message.value().decode('utf-8')
    print(raw_value)
    


temp_consumer = start_consumer(group_id = 'temp_consumer')

while True:
    streamData()
    time.sleep(2)
    consumeData(temp_consumer)
