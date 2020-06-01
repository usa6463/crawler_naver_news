# coding:utf-8 
import re, requests, json, datetime
from bs4 import BeautifulSoup as bs
from kafka.admin import KafkaAdminClient, NewTopic
import kafka
from kafka import KafkaProducer
import pymysql

url = "https://news.naver.com/main/read.nhn?mode=LSD&mid=shm&sid1=102&oid=055&aid=0000818571"

req = requests.get(url)
html = req.text
soup = bs(html, 'html.parser')

title = soup.select('#articleTitle')[0].text
content = soup.select('#articleBodyContents')[0].text
content = re.compile('// flash 오류를 우회하기 위한 함수 추가').sub('', content)
content = re.compile('function _flash_removeCallback.*').sub('', content)
content = re.compile('\n').sub('', content)
print(content[0])

reg_dt = soup.select('span.t11')[0].text
writer = soup.find('div', class_='press_logo').img['title']

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

