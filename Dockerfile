FROM store/oracle/mysql-enterprise-server:5.7

RUN mkdir crawler
COPY scheduler.py /crawler/scheduler.py
COPY worker.py /crawler/worker.py

RUN cd crawler

# ENTRYPOINT /kafka/bin/kafka-server-start.sh /kafka/config/server.properties