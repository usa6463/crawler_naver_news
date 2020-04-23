FROM store/oracle/mysql-enterprise-server:5.7

RUN apt-get install -y python3 python3-pip
RUN mkdir crawler
COPY scheduler.py /crawler/scheduler.py
COPY worker.py /crawler/worker.py

RUN cd crawler

# ENTRYPOINT /kafka/bin/kafka-server-start.sh /kafka/config/server.properties