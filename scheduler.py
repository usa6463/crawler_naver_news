# coding:utf-8 
import time, logging
from multiprocessing import Pool, Process, Queue
from worker import worker_main

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
            procs = []

            for val in range(PROCESS_NUM):
                url = None
                if not queue.empty():
                    url = queue.get()
                    proc = Process(target=worker_main, args=(url,queue, logger))
                    procs.append(proc)
                    proc.start()

            for proc in procs:
                proc.join()
            
        time.sleep(ITERATION_INTERVAL)