# coding:utf-8 
import re, requests, json, datetime
from bs4 import BeautifulSoup as bs
from kafka.admin import KafkaAdminClient, NewTopic
import kafka
from kafka import KafkaProducer
import pymysql

url = "https://news.naver.com/main/read.nhn?mode=LSD&mid=shm&sid1=100&oid=022&aid=0003468990"

req = requests.get(url)
html = req.text
soup = bs(html, 'html.parser')

title = soup.select('#articleTitle')[0].text
content = soup.select('#articleBodyContents')[0].text
reg_dt = soup.select('span.t11')[0].text
writer = type(soup.div)

p = re.compile('&oid=(\\d{1,100})&aid=(\\d{1,100})')
search_result = p.search(url)
oid = search_result.group(1)
aid = search_result.group(2)

result = {}
result['oid'] = oid
result['aid'] = aid
result['title'] = title
result['reg_dt'] = reg_dt
result['content'] = content
result['writer'] = writer 

for key in result:
    print(key + ' : ' + str(result[key]))

