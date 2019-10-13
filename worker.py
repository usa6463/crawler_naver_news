# coding:utf-8 

def worker_func(url, queue):
    print(url)
    queue.put('hello-'+url)