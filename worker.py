# coding:utf-8 

def worker_func(args):
    url = args[0]
    queue = args[1]
    print(url)
    queue.append('hello')