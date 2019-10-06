# coding:utf-8 
import time
import requests
from bs4 import BeautifulSoup as bs
from multiprocessing import Pool

from worker import worker_func

SEED = 'news.naver.com'

if __name__ == '__main__':
    queue = []

    queue.append(SEED)
    pool = Pool(processes=4)

    while True:
        
        while len(queue)>0:
            urls = queue[0:4]
            del queue[0:4]

            pool.map(worker_func, [(x, queue) for x in urls])
            time.sleep(1)
            print(queue)
            
        time.sleep(60*10)

'''
이슈
- 큐가 레퍼런스로 전달되지 않는듯.
- map이 프로세스들 작업 끝날때까지 기다려주지 않는듯. 
'''
