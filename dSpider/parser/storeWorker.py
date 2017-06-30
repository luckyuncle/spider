# -*- coding: utf-8 -*-
import time
import threading
from multiprocessing.managers import BaseManager
from dSpider_V2.parser import store

server_address = '127.0.0.1'
server_port = 5000
key = 'abc123'


class QueueManager(BaseManager):
    pass


class StoreWorker(object):

    def __init__(self):
        self.command = ''

    def set_command(self, command):
        self.command = command

    # 开始工作 从json队列中取json， 将json请求放入特殊url队列
    # noinspection PyUnresolvedReferences,SpellCheckingInspection
    def work(self, spider_type):
        print spider_type
        QueueManager.register('network_json_queue')
        QueueManager.register('network_special_url_queue')
        worker = QueueManager(address=(server_address, server_port), authkey=key)
        worker.connect()
        network_json_queue = worker.network_json_queue()
        network_special_url_queue = worker.network_special_url_queue()
        # 开启json请求线程
        t1 = threading.Thread(target=store.json_request, args=(spider_type, network_special_url_queue))
        t1.start()
        # t2 = threading.Thread(target=store.store, args=(spider_type, ))
        # t2.start()
        while True:
            if self.command == 'end':
                store.command = 'end'
                break
            if network_json_queue.empty() is False:
                json_json = network_json_queue.get()
                store.handle(spider_type, json_json)
            else:
                time.sleep(1)
