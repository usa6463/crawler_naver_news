# coding:utf-8 

def worker_func(url, queue, logger):
    logger.info('worker process : ' + url)
    queue.put('hello-'+url)