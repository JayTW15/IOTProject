from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types
from confluent_kafka import Consumer

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    config = {
        'bootstrap.servers': "kafka:9093", #for now this is local connection, later once we containerize the code we switch to 9093
        'group.id': "temp_recorded",
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    #create the consumer
    consumer = Consumer(config)
    temp_consumer = consumer.subscribe(["temp_recorded"])
    
    data_stream = env.from_collection([1, 2, 3, 4, 5], type_info=Types.INT())
    data_stream.print()
    
    env.execute("Temperature Test Job")

if __name__ == '__main__':
    main()

#to run in flink, use the commands: docker exec -it iotproject-jobmanager-1 bash
#                                   flink run -py /opt/flink/jobs/temp_processor.py
#otherwise run like normal for local use. can use docker compose logs taskmanager to see logs and view outp in terminal