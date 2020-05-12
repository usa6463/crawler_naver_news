# coding:utf-8 
import time, logging, pymysql
from datetime import datetime, timedelta
from multiprocessing import Pool, Process, Queue
from worker import worker

# config
config = {}
config['seed'] = ['https://news.naver.com/']
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

# logger setting
logger = logging.getLogger('naver-news-crawler')
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
file_handler = logging.FileHandler('file.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def set_db():
    # db connect
    conn = pymysql.connect(host = config['db_addr'], user = config['db_user'], password = config['db_pw'])
    cursor = conn.cursor()

    # database set
    sql = '''
    show databases like '{database_name}'
    '''
    if cursor.execute(sql.format(database_name=config['db_database_name'])) == 0:
        sql = '''
            create database {database_name}
        '''
        cursor.execute(sql.format(database_name=config['db_database_name']))
    
    cursor.execute("use {database_name}".format(database_name=config['db_database_name']))

    # news table set
    sql = '''
    show tables like '{table_name}'
    '''
    if cursor.execute(sql.format(table_name=config['db_news_table_name'])) == 0:
        sql = '''
            create table {table_name} (
                regdatetime datetime NOT NULL default CURRENT_TIMESTAMP,
                url varchar(255) NOT NULL,
                PRIMARY KEY (url),
                INDEX(url)
            );
        '''
        cursor.execute(sql.format(table_name=config['db_news_table_name']))

    conn.close()
        
if __name__ == '__main__':
    jobday = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')
    logger.info('crawler scheduler start')
    set_db()
    main_queue = config['seed']

    while len(main_queue)>0:
        queue = Queue()   
        logger.info('queue 에 남은 링크 수 : ' + str(len(main_queue)))   
        procs = []

        for val in range(config['process_num']):
            url = None
            if len(main_queue)>0:
                url = main_queue.pop(0)
                worker_obj = worker()
                proc = Process(target=worker_obj.worker_main, args=(url, queue, logger, config, jobday))
                procs.append(proc)
                proc.start()

        while 1:
            running = any(p.is_alive() for p in procs)
            while not queue.empty():
                main_queue.append(queue.get())
            if not running:
                break

        main_queue = list(set(main_queue))
        queue.close()
        # logger.info('프로세스들 join 끝')
        time.sleep(1)
        