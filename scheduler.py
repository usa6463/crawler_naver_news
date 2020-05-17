# coding:utf-8 
import time, logging, pymysql
from datetime import datetime, timedelta
from multiprocessing import Pool, Process, Queue
from worker import worker

# config
config = {}
config['seed'] = ['https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=264&sid1=100&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=265&sid1=100&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=268&sid1=100&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=266&sid1=100&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=267&sid1=100&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=269&sid1=100&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=259&sid1=101&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=258&sid1=101&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=261&sid1=101&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=771&sid1=101&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=260&sid1=101&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=262&sid1=101&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=310&sid1=101&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=263&sid1=101&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=249&sid1=102&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=250&sid1=102&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=251&sid1=102&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=254&sid1=102&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=252&sid1=102&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=59b&sid1=102&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=255&sid1=102&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=256&sid1=102&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=276&sid1=102&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=257&sid1=102&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=241&sid1=103&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=239&sid1=103&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=240&sid1=103&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=237&sid1=103&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=238&sid1=103&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=376&sid1=103&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=242&sid1=103&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=243&sid1=103&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=244&sid1=103&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=248&sid1=103&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=245&sid1=103&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=231&sid1=104&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=232&sid1=104&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=233&sid1=104&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=234&sid1=104&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=322&sid1=104&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=731&sid1=105&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=226&sid1=105&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=227&sid1=105&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=230&sid1=105&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=732&sid1=105&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=283&sid1=105&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=229&sid1=105&date={jobday}','https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid2=228&sid1=105&date={jobday}','https://news.naver.com/main/list.nhn?mode=LSD&mid=sec&sid1=100&date={jobday}','https://news.naver.com/main/list.nhn?mode=LSD&mid=sec&sid1=101&date={jobday}','https://news.naver.com/main/list.nhn?mode=LSD&mid=sec&sid1=102&date={jobday}','https://news.naver.com/main/list.nhn?mode=LSD&mid=sec&sid1=103&date={jobday}','https://news.naver.com/main/list.nhn?mode=LSD&mid=sec&sid1=104&date={jobday}','https://news.naver.com/main/list.nhn?mode=LSD&mid=sec&sid1=105&date={jobday}','https://news.naver.com/main/list.nhn?mode=LSD&mid=sec&sid1=110&date={jobday}','https://news.naver.com/main/list.nhn?mode=LPOD&sid2=140&sid1=001&mid=sec&oid=001&isYeonhapFlash=Y&date={jobday}']
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
    main_queue = [x.format(jobday=jobday) for x in config['seed']]

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
        