# coding:utf-8 
import re, requests, json, datetime
from bs4 import BeautifulSoup as bs
from kafka.admin import KafkaAdminClient, NewTopic
import kafka
from kafka import KafkaProducer
import pymysql

class worker:
    logger = None
    config = None

    def worker_main(self, url, queue, logger, config):
        self.logger = logger
        self.logger.info('worker process : ' + url)
        self.config = config

        url = self.check_naver_news_page(url)
        if url:
            self.log_read(url)

            # 뉴스기사면
            if self.check_article(url):
                self.parse_news(url)

            # 그외 페이지라면
            else:
                links = self.get_links(url)
                for link in links:
                    if not self.check_already_read(link):
                        queue.put(link)
                        # self.logger.info(link)
                

    # url이 네이버 뉴스 도메인인지 체크. 틀리면 None, 맞으면 https 까지 붙은 full url 반환.
    def check_naver_news_page(self, url):
        if str.startswith(url, self.config['url_start']):
            return url 
        elif str.startswith(url, '/'):
            return self.config['url_start'] + url[1:]
        else :
            return None

    # url이 뉴스기사인지 확인하여 true, false 반환
    def check_article(self, url):
        p_1 = re.compile('read.nhn')
        p_2 = re.compile('&oid=\\d{1,100}&aid=\\d{1,100}')
        if p_1.search(url) and p_2.search(url):
            return True
        else :
            return False

    # url 페이지 안에 있는 모든 링크 파싱하여 반환
    def get_links(self, url):
        req = requests.get(url)
        html = req.text
        soup = bs(html, 'html.parser')
        links = soup.select('a[href]')
        links = list(set([self.check_naver_news_page(link.get('href')) for link in links]))
        return links

    # 뉴스기사 파싱
    def parse_news(self, url):
        req = requests.get(url)
        html = req.text
        soup = bs(html, 'html.parser')

        title = soup.select('#articleTitle')[0].text
        content = soup.select('#articleBodyContents')[0].text
        reg_dt = soup.select('span.t11')[0].text

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

        # self.send_kafka(result)
        return

    # 이미 파싱했던 url인지 확인하여 true, false 반환
    def check_already_read(self, url):
        conn = pymysql.connect(host = self.config['db_addr'], user = self.config['db_user'], password = self.config['db_pw'])
        cursor = conn.cursor()
        cursor.execute('use {}'.format(self.config['db_database_name']))
        sql = '''
            select *
            from {table_name}
            where url = '{url_name}'
        '''
        result = cursor.execute(sql.format(table_name=self.config['db_table_name']+'_'+datetime.date.today().strftime('%Y%m%d'), url_name=url))
        conn.close()

        if result>=1:
            return True
        return False

    # 파싱한 url임을 기록
    def log_read(self, url):
        conn = pymysql.connect(host = self.config['db_addr'], user = self.config['db_user'], password = self.config['db_pw'])
        cursor = conn.cursor()
        cursor.execute('use {}'.format(self.config['db_database_name']))
        sql = '''
            insert into {table_name}
            (url) values
            ('{url_name}')
        '''
        cursor.execute(sql.format(table_name=self.config['db_table_name']+'_'+datetime.date.today().strftime('%Y%m%d'), url_name=url))
        conn.commit()
        conn.close()

        return 

    # kafka에 message 전송
    def send_kafka(self, message):
        message = json.dumps(message)

        kafka_client = kafka.KafkaClient(self.config['kafka_addr'])
        server_topics = kafka_client.topic_partitions

        try:
            if not self.config['topic_name'] in server_topics:
                self.logger.info('no topic')
                admin_client = KafkaAdminClient(bootstrap_servers=self.config['kafka_addr'])
                admin_client.create_topics(self.config['topic_name'])
                self.logger.info('topic create')
            else:
                self.logger.info('already topic exist')
        except Exception as e:
            self.logger.info('topic create error : '+str(e))

        producer = KafkaProducer(bootstrap_servers=self.config['kafka_addr'])
        producer.send(self.config['topic_name'], message)
        self.logger.info('message send')

        return 