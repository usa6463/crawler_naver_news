# coding:utf-8 
import time, logging
from multiprocessing import Pool, Process, Queue
from worker import worker

SEED = 'https://news.naver.com/'
PROCESS_NUM = 5
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
    main_queue = [SEED]
    
    while True: 

        while len(main_queue)>0:
            queue = Queue()   
            logger.info('queue 에 남은 링크 수 : ' + str(len(main_queue)))   
            procs = []

            for val in range(PROCESS_NUM):
                url = None
                if len(main_queue)>0:
                    url = main_queue.pop(0)
                    worker_obj = worker()
                    proc = Process(target=worker_obj.worker_main, args=(url, queue, logger))
                    procs.append(proc)
                    proc.start()

            # logger.info('프로세스들 join 시작')

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
        logger.info('ITERATION_INTERVAL : ' + str(ITERATION_INTERVAL))    
        time.sleep(ITERATION_INTERVAL)
        logger.info('ITERATION_INTERVAL 종료')    