# coding:utf-8 
import time, logging
from multiprocessing import Pool, Process, Queue
from worker import worker

# config
config = {}
config['seed'] = 'https://news.naver.com/'
config['process_num'] = 2
config['iteration_interval'] = 60*10
config['url_start'] = 'https://news.naver.com/'
config['kafka_addr'] = "192.168.0.23:9092"
config['topic_name'] = "naver_news_python"
config['db_addr'] = "local"
config['db_user'] = "root"
config['db_pw'] = "1234"

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

if __name__ == '__main__':
    logger.info('crawler scheduler start')
    main_queue = [config['seed']]
    
    while True: 

        while len(main_queue)>0:
            queue = Queue()   
            logger.info('queue 에 남은 링크 수 : ' + str(len(main_queue)))   
            procs = []

            for val in range(config['process_num']):
                url = None
                if len(main_queue)>0:
                    url = main_queue.pop(0)
                    worker_obj = worker()
                    proc = Process(target=worker_obj.worker_main, args=(url, queue, logger, config))
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

        logger.info('ITERATION_INTERVAL 대기 중')    
        logger.info('ITERATION_INTERVAL : ' + str(config['iteration_interval']))    
        time.sleep(config['iteration_interval'])
        logger.info('ITERATION_INTERVAL 종료')    