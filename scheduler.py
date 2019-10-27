# coding:utf-8 
import time, logging
from multiprocessing import Pool, Process, Queue
from worker import worker

SEED = 'https://news.naver.com/'
PROCESS_NUM = 4
ITERATION_INTERVAL = 60*10

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
    queue = Queue()
    queue.put(SEED)

    while True:
        
        while not queue.empty():
            logger.info('queue 에 남은 링크 수 : ' + str(queue.qsize()))   
            procs = []

            for val in range(PROCESS_NUM):
                url = None
                if not queue.empty():
                    url = queue.get()
                    worker_obj = worker()
                    proc = Process(target=worker_obj.worker_main, args=(url, queue, logger))
                    procs.append(proc)
                    proc.start()

            logger.info('프로세스들 join 시작')
            for proc in procs:
                proc.join()
            
            logger.info('프로세스들 join 끝')

            time.sleep(1)

        logger.info('ITERATION_INTERVAL 대기 중')    
        logger.info('ITERATION_INTERVAL : ' + str(ITERATION_INTERVAL))    
        time.sleep(ITERATION_INTERVAL)
        logger.info('ITERATION_INTERVAL 종료')    