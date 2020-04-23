# coding:utf-8 
import re, requests, json, datetime
from bs4 import BeautifulSoup as bs
from kafka.admin import KafkaAdminClient, NewTopic
import kafka
from kafka import KafkaProducer
import pymysql

# config
config = {}
config['seed'] = 'https://news.naver.com/'
config['process_num'] = 10
config['iteration_interval'] = 60*10
config['url_start'] = 'https://news.naver.com/'
config['kafka_addr'] = "192.168.0.23:9092"
config['topic_name'] = "naver_news_python"
config['db_addr'] = "localhost"
config['db_user'] = "root"
config['db_pw'] = "1234"
config['db_database_name'] = "crawler_meta"
config['db_news_table_name'] = "news"
config['db_path_table_name'] = "path"
config['db_global_path_table_name'] = "global_path"

def worker_main( url, queue, logger, config):
    logger = logger
    logger.info('worker process : ' + url)
    config = config

    conn = pymysql.connect(host = config['db_addr'], user = config['db_user'], password = config['db_pw'])

    url = check_naver_news_page(url)
    if url:
        # 뉴스기사면
        if check_article(url):
            parse_news(url, conn)

        # 그외 페이지라면
        else:
            parse_path(url, conn, queue)
            
    conn.close()

def parse_path( url, conn, queue):
    cursor = conn.cursor()

    if not check_path_already_read(url, conn):
        log_path_read(url, conn)

        # global path table에 저장
        sql = '''
        select * 
        from {table_name}
        where url='{url_name}'
        '''
        if cursor.execute(sql.format(table_name=config['db_global_path_table_name'], url_name=url)) == 0:
            sql = '''
                insert into {table_name}
                (url) values
                ('{url_name}')
            '''
            cursor.execute(sql.format(table_name=config['db_global_path_table_name'], url_name=url))

        links = get_links(url)
        for link in links:
            if link is None:
                pass
            elif check_article(url):
                if not check_news_already_read(link, conn):
                    queue.put(link)    
            else :
                if not check_path_already_read(link, conn):
                    queue.put(link)    

# url이 네이버 뉴스 도메인인지 체크. 틀리면 None, 맞으면 https 까지 붙은 full url 반환.
# 추가로 파싱하고 싶지 않은 URL 필터링
def check_naver_news_page( url):
    flag = 0

    # 상대경로만 있는 경우 뉴스 도메인 추가
    if str.startswith(url, '/'):
        url = config['url_start'] + url[1:]
    # 뉴스 도메인으로 시작하는 url이 아닌 경우 제외
    if not str.startswith(url, config['url_start']):
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
def check_article( url):
    p_1 = re.compile('read.nhn')
    p_2 = re.compile('&oid=\\d{1,100}&aid=\\d{1,100}')
    if p_1.search(url) and p_2.search(url):
        return True
    else :
        return False

# url 페이지 안에 있는 모든 링크 파싱하여 반환
def get_links( url):
    req = requests.get(url)
    html = req.text
    soup = bs(html, 'html.parser')
    links = soup.select('a[href]')
    links = list(set([check_naver_news_page(link.get('href')) for link in links]))
    return links

# 뉴스기사 파싱
def parse_news( url, conn):

    if not check_news_already_read(url, conn):
        log_news_read(url,conn)

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

        # send_kafka(result)
    return

# 이미 파싱했던 뉴스 url인지 확인하여 true, false 반환
def check_news_already_read( url, conn):
    cursor = conn.cursor()
    cursor.execute('use {}'.format(config['db_database_name']))
    sql = '''
        select *
        from {table_name}
        where url = '{url_name}'
    '''
    result = cursor.execute(sql.format(table_name=config['db_news_table_name'], url_name=url))

    if result>=1:
        return True
    return False

# 이미 파싱했던 path url인지 확인하여 true, false 반환
def check_path_already_read( url, conn):
    cursor = conn.cursor()
    cursor.execute('use {}'.format(config['db_database_name']))
    sql = '''
        select *
        from {table_name}
        where url = '{url_name}'
    '''
    result = cursor.execute(sql.format(table_name=config['db_path_table_name'], url_name=url))

    if result>=1:
        return True
    return False


# 파싱한 뉴스 url임을 기록
def log_news_read( url, conn):
    cursor = conn.cursor()
    cursor.execute('use {}'.format(config['db_database_name']))
    sql = '''
        insert into {table_name}
        (url) values
        ('{url_name}')
    '''
    cursor.execute(sql.format(table_name=config['db_news_table_name'], url_name=url))
    conn.commit()

    return 

# 파싱한 path url임을 기록
def log_path_read( url, conn):
    cursor = conn.cursor()
    cursor.execute('use {}'.format(config['db_database_name']))
    sql = '''
        insert into {table_name}
        (url) values
        ('{url_name}')
    '''
    cursor.execute(sql.format(table_name=config['db_path_table_name'], url_name=url))
    conn.commit()

    return 

# kafka에 message 전송
def send_kafka( message):
    kafka_client = kafka.KafkaClient(config['kafka_addr'])
    server_topics = kafka_client.topic_partitions

    try:
        if not config['topic_name'] in server_topics:
            logger.info('no topic')
            admin_client = KafkaAdminClient(bootstrap_servers=config['kafka_addr'])
            admin_client.create_topics(config['topic_name'])
            logger.info('topic create')
        else:
            pass
    except Exception as e:
        logger.info('topic create error : '+str(e))

    producer = KafkaProducer(bootstrap_servers=config['kafka_addr'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send(config['topic_name'], message)
    producer.flush()
    # logger.info('message send')

    return 

def make_table( conn, config, date):
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




result = get_links("https://news.naver.com/main/main.nhn?mode=LSD&mid=shm&sid1=100")

for url in result[:-2]:
    p = re.compile('https')
    if url is not None:
        if p.search(url):
            print(url)
