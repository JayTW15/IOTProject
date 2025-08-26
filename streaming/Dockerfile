FROM flink:1.18.1-scala_2.12-java11

USER root

# Install Python
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Install PyFlink
RUN pip3 install apache-flink==1.18.1 apache-flink-libraries kafka-python

USER flink

# If you need to copy files from streaming folder:
# COPY streaming/some-config-file /opt/flink/conf/