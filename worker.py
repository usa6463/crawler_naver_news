# coding:utf-8 
import re, requests
from bs4 import BeautifulSoup as bs

def worker_main(url, queue, logger):
    logger.info('worker process : ' + url)
    queue.put('https://news.naver.com/main/ranking/read.nhn?mid=etc&sid1=111&rankingType=popular_day&oid=015&aid=0004222911&date=20191013&type=1&rankingSeq=1&rankingSectionId=105')

    print('check_naver_news_page : ' + str(check_naver_news_page(url)))
    print('check_article : ' + str(check_article(url)))

    # if check_naver_news_page(url):
    #     log_read(url)

    #     # 뉴스기사면
    #     if check_article(url):
    #         parse_news(url)

    #     # 그외 페이지라면
    #     else:
    #         links = get_links(url)
    #         for link in links:
    #             if check_already_read(url):
    #                 queue.put(url)
            

# url이 네이버 뉴스 도메인인지 체크. true, false 반환
def check_naver_news_page(url):
    return str.startswith(url, 'https://news.naver.com/')

# url이 뉴스기사인지 확인하여 true, false 반환
def check_article(url):
    p_1 = re.compile('read.nhn')
    p_2 = re.compile('&oid=\\d{1,100}&aid=\\d{1,100}')
    if p_1.search(url) and p_2.search(url):
        return True
    else :
        return False

# url 페이지 안에 있는 모든 링크 파싱하여 반환
def get_links(url):
    
    return []

# 뉴스기사 파싱
def parse_news(url):
    return 

# 이미 파싱했던 url인지 확인하여 true, false 반환
def check_already_read(url):
    return False

# 파싱한 url임을 기록
def log_read(url):
    return 

# kafka에 message 전송
def send_kafak(message):
    return 