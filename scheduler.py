# coding:utf-8 
import time
import requests
from bs4 import BeautifulSoup as bs
from multiprocessing import Pool, Process, Queue
from worker import worker_func

SEED = 'news.naver.com'
PROCESS_NUM = 4
ITERATION_INTERVAL = 60 * 10

if __name__ == '__main__':
    queue = Queue()
    queue.put(SEED)

    while True:
        
        while not queue.empty():
            procs = []

            for val in range(PROCESS_NUM):
                url = None
                if not queue.empty():
                    url = queue.get()
                    proc = Process(target=worker_func, args=(url,queue))
                    procs.append(proc)
                    proc.start()

            for proc in procs:
                proc.join()
            
        time.sleep(ITERATION_INTERVAL)