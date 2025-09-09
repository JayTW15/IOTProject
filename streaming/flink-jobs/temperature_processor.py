from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Types
import json

def print_raw_data(json_str):
    """Just print exactly what we receive from Kafka"""
    print(f"=== RECEIVED FROM KAFKA ===")
    print(json_str)
    print(f"=== END MESSAGE ===")
    
    # Return the same data unchanged
    return json_str

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file:///opt/flink/lib/flink-connector-kafka-3.1.0-1.18.jar")

    config = {
        'bootstrap.servers': "kafka:9093",
        'group.id': "temp_recorded",
        'auto.offset.reset': 'latest',
        'enable.auto.commit': 'true'
    }
    
    #create the consumer
    consumer = FlinkKafkaConsumer(
        topics='temp_recorded',
        deserialization_schema=SimpleStringSchema(),
        properties=config
    )
    
    temp_stream = env.add_source(consumer)
    
    processed_stream = temp_stream.map(
        print_raw_data,
        output_type=Types.STRING()
    )

    processed_stream.print()

    env.execute("Temperature Processing Stream")
if __name__ == '__main__':
    main()

#to run in flink, use the commands:
#                                   docker exec -it iotproject-1-jobmanager-1 bash
#                                   flink run -py /opt/flink/jobs/temperature_processor.py
#otherwise run like normal for local use. can use docker compose logs taskmanager to see logs and view outp in terminal