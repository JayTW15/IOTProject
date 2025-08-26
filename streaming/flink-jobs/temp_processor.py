from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    data_stream = env.from_collection([1, 2, 3, 4, 5], type_info=Types.INT())
    data_stream.print()
    
    env.execute("Temperature Test Job")

if __name__ == '__main__':
    main()

#to run in flink, use the commands: docker exec -it iotproject-jobmanager-1 bash
#                                   flink run -py /opt/flink/jobs/temp_processor.py
#otherwise run like normal for local use. can use docker compose logs taskmanager to see logs and view outp in terminal