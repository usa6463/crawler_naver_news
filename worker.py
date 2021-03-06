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
    jobday = None

    def worker_main(self, url, queue, logger, config, jobday):
        self.logger = logger
        self.config = config
        self.jobday = jobday

        conn = pymysql.connect(host = self.config['db_addr'], user = self.config['db_user'], password = self.config['db_pw'])

        redirect_url = self.check_naver_news_page(requests.get(url).url)
        url = self.check_naver_news_page(url)
    
        if url and redirect_url:
            # 뉴스기사면
            if self.check_article(url):
                self.parse_news(url, conn)
                self.logger.info('news : '+url)

            # 그외 페이지라면
            else:
                self.parse_path(url, conn, queue)
                self.logger.info('path : '+url)
                
        conn.close()

    def parse_path(self, url, conn, queue):
        if not self.check_path_already_read(url, conn):
            self.log_path_read(url, conn)
            
            links = self.get_links(url)
            for link in links:
                if link is None:
                    pass
                elif self.check_article(link):
                    if not self.check_news_already_read(link, conn):
                        queue.put(link)    
                else :
                    if not self.check_path_already_read(link, conn) and re.compile('date={jobday}[a-zA-Z;=&0-9]{{0,100}}page='.format(jobday=self.jobday)).search(link):  # 안 읽은거 & jobday 일치하는 네비게이션페이지
                        queue.put(link)    

    # url이 네이버 뉴스 도메인인지 체크. 틀리면 None, 맞으면 https 까지 붙은 full url 반환.
    # 추가로 파싱하고 싶지 않은 URL 필터링
    def check_naver_news_page(self, url, cur_url=None):
        flag = 0

        # 절대경로가 아닌 경우
        if not str.startswith(url, 'http'):

            # /로 시작하는 경우
            if str.startswith(url, '/'):
                url = self.config['url_start'] + url[1:]

            elif str.startswith(url, '?'):
                url = re.sub('amp;', '', url)
                key_idx = cur_url.find('?')
                if key_idx > -1:
                    url = cur_url[:key_idx] + url
                else :
                    flag=1

            elif str.startswith(url, '#'):
                url = re.sub('amp;', '', url)
                key_idx = cur_url.find('#')
                if key_idx > -1:
                    url = cur_url[:key_idx] + url
                else :
                    flag=1            

        # 뉴스 도메인으로 시작하는 url이 아닌 경우 제외
        if not str.startswith(url, self.config['url_start']):
            flag = 1

        # jsessionid 들어가는 경우 제외
        p = re.compile('jsessionid')
        if p.search(url):
            flag = 1

        # 시간이 들어가는 페이지인 경우
        p = re.compile('&time=')
        if p.search(url):
            flag = 1

        # 연예, 스포츠 등 페이지인 경우

        if flag==1 :
            return None
        else :
            return url
        

    # url이 뉴스기사인지 확인하여 true, false 반환
    def check_article(self, url):
        p_1 = re.compile('read.nhn')
        p_2 = re.compile('&oid=\\d{1,100}&aid=\\d{1,100}')
        if p_1.search(url) and p_2.search(url):
            return True
        else :
            return False

    # url 페이지 안 content내에 있는 모든 링크 파싱하여 반환
    def get_links(self, url):
        req = requests.get(url)
        html = req.text
        soup = bs(html, 'html.parser')
        main_content = soup.find('td', class_='content')
        links = main_content.select('a[href]')
        links = list(set([self.check_naver_news_page(link.get('href'), url) for link in links]))
        return links

    # 뉴스기사 파싱
    def parse_news(self, url, conn):

        if not self.check_news_already_read(url, conn):

            try:
                req = requests.get(url)
                html = req.text
                soup = bs(html, 'html.parser')

                title = soup.select('#articleTitle')[0].text
                title = re.compile("'").sub("\\'", title)

                content = soup.select('#articleBodyContents')[0].text
                content = re.compile('// flash 오류를 우회하기 위한 함수 추가').sub('', content)
                content = re.compile('function _flash_removeCallback.*').sub('', content)
                content = re.compile('\n').sub('', content)
                content = re.compile("'").sub("\\'", content)

                reg_dt = soup.select('span.t11')[0].text
                modify_reg_dt = re.compile('오[전-후]').sub('', reg_dt)
                dt = datetime.datetime.strptime(modify_reg_dt, '%Y.%m.%d.  %H:%M')
                if re.compile('오후').search(reg_dt):
                    dt = dt + datetime.timedelta(hours=12)
                reg_dt = dt.strftime('%Y-%m-%d %H:%M')
                
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
                print(result)

                self.log_news_read(url, conn, result)
                # self.send_kafka(result)

            except Exception as e:
                self.logger.info(str(e) + ' : ' + url)
        return

    # 이미 파싱했던 뉴스 url인지 확인하여 true, false 반환
    def check_news_already_read(self, url, conn):
        cursor = conn.cursor()
        cursor.execute('use {}'.format(self.config['db_database_name']))
        sql = '''
            select *
            from {table_name}
            where url = '{url_name}'
        '''
        result = cursor.execute(sql.format(table_name=self.config['db_news_table_name'].format(jobday=self.jobday), url_name=url))

        if result>=1:
            return True
        return False

    # 이미 파싱했던 path url인지 확인하여 true, false 반환
    def check_path_already_read(self, url, conn):
        cursor = conn.cursor()
        cursor.execute('use {}'.format(self.config['db_database_name']))
        sql = '''
            select *
            from {table_name}
            where url = '{url_name}'
        '''
        result = cursor.execute(sql.format(table_name=self.config['db_path_table_name'].format(jobday=self.jobday), url_name=url))

        if result>=1:
            return True
        return False


    # 파싱한 뉴스 url임을 기록
    def log_news_read(self, url, conn, result):
        cursor = conn.cursor()
        cursor.execute('use {}'.format(self.config['db_database_name']))
        sql = '''
            insert into {table_name}
            (url, news_regdatetime, oid, aid, title, content, writer) values
            ('{url_name}', '{regdatetime}', {oid}, {aid}, '{title}', '{content}', '{writer}')
        '''
        cursor.execute(sql.format(table_name=self.config['db_news_table_name'].format(jobday=self.jobday), url_name=url, regdatetime=result['reg_dt'], \
            oid=result['oid'], aid=result['aid'], title=result['title'], content=result['content'], writer=result['writer']))
        conn.commit()

        return 

    # 파싱한 path url임을 기록
    def log_path_read(self, url, conn):
        cursor = conn.cursor()
        cursor.execute('use {}'.format(self.config['db_database_name']))
        sql = '''
            insert into {table_name}
            (url) values
            ('{url_name}')
        '''
        cursor.execute(sql.format(table_name=self.config['db_path_table_name'].format(jobday=self.jobday), url_name=url))
        conn.commit()

        return 

    # kafka에 message 전송
    def send_kafka(self, message):
        kafka_client = kafka.KafkaClient(self.config['kafka_addr'])
        server_topics = kafka_client.topic_partitions

        try:
            if not self.config['topic_name'] in server_topics:
                self.logger.info('no topic')
                admin_client = KafkaAdminClient(bootstrap_servers=self.config['kafka_addr'])
                admin_client.create_topics(self.config['topic_name'])
                self.logger.info('topic create')
            else:
                pass
        except Exception as e:
            self.logger.info('topic create error : '+str(e))

        producer = KafkaProducer(bootstrap_servers=self.config['kafka_addr'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        producer.send(self.config['topic_name'], message)
        producer.flush()
        # self.logger.info('message send')

        return 

    def make_table(self, conn, config, date):
        cursor = conn.cursor()
        # table set
        sql = '''
        show tables like '{table_name}'
        '''
        if cursor.execute(sql.format(table_name=config['db_table_name']+'_'+date)) == 0:
            sql = '''
                create table {table_name} (
                    regdatetime datetime NOT NULL default CURRENT_TIMESTAMP,
                    url varchar(255) NOT NULL,
                    PRIMARY KEY (url)
                );
            '''
            cursor.execute(sql.format(table_name=config['db_table_name']+'_'+date))