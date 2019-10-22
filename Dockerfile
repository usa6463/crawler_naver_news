FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y wget git

# 
RUN git clone https://github.com/usa6463/crawler_naver_news.git


RUN mkdir data
ADD server.properties /kafka/config/server.properties

ENTRYPOINT /kafka/bin/kafka-server-start.sh /kafka/config/server.properties