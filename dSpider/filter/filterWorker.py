# -*- coding: utf-8 -*-
from multiprocessing.managers import BaseManager
import time
import json
from urlparse import urljoin
import re
import socket
import threading
from pybloom import ScalableBloomFilter
from pymongo import MongoClient

connection = MongoClient('127.0.0.1', 27017)
j_db = connection.jingdong
t_db = connection.taobao
s_db = connection.suning


server_address = '127.0.0.1'
server_port = 5000
key = 'abc123'
connection_port = 8000
work_thread = []


class QueueManager(BaseManager):
    pass


class FilterWorker(object):

    def __init__(self):
        self.init_url = None
        self.end_flag = False
        self.sbf = ScalableBloomFilter(mode=ScalableBloomFilter.SMALL_SET_GROWTH)
        self.command = ''

    # 开始工作 从url_filter队列中取url，去重后放入一级url或二级url队列中
    # noinspection PyUnresolvedReferences
    def work(self, spider_type):
        print spider_type
        QueueManager.register('network_first_url_queue')
        QueueManager.register('network_second_url_queue')
        QueueManager.register('network_url_filter_queue')
        worker = QueueManager(address=(server_address, server_port), authkey=key)
        worker.connect()
        network_first_url_queue = worker.network_first_url_queue()
        network_second_url_queue = worker.network_second_url_queue()
        network_url_filter_queue = worker.network_url_filter_queue()
        init_url_json = network_url_filter_queue.get()  # 从url_filter中取url
        init_url_dist = json.loads(init_url_json)
        self.init_url = init_url_dist['url']
        # 加入href or url
        href_filter = re.compile(r'/.*').search(self.init_url).group()
        self.sbf.add(href_filter)  # 放入到布隆过滤器中
        print href_filter
        network_first_url_queue.put(init_url_json)  # 放入一级url队列中
        while True:
            if self.command == 'end':
                break
            if network_url_filter_queue.empty() is False:
                href_json = network_url_filter_queue.get()  # 从url_filter中取url
                href_dist = json.loads(href_json)
                href_filter = href_dist['href']
                print href_filter
                if href_filter not in self.sbf:  # 进行去重判断
                    self.sbf.add(href_filter)  # 放入到布隆过滤器中
                    if re.compile(r'/item').search(href_filter) is not None:
                        # if href_dist['key_word_item'] is False:
                        del href_dist['href']
                        url_dist = href_dist
                        url_dist['url'] = urljoin(href_dist['refer_url'], href_filter)
                        network_first_url_queue.put(json.dumps(url_dist))  # 放入一级url队列中
                    elif re.compile(r'/list').search(href_filter) is not None:
                        del href_dist['href']
                        url_dist = href_dist
                        url_dist['url'] = urljoin(href_dist['refer_url'], href_filter)
                        network_first_url_queue.put(json.dumps(url_dist))  # 放入一级url队列中
                    elif re.compile(r'/\.jpg').search(href_filter) is not None:
                        pass
                    else:
                        del href_dist['href']
                        url_dist = href_dist
                        url_dist['url'] = urljoin(href_dist['refer_url'], href_filter)
                        network_second_url_queue.put(json.dumps(url_dist))  # 放入二级url队列中
            else:
                time.sleep(1)

    # 与‘主人’进行连接
    # noinspection SpellCheckingInspection
    def conn_master(self):
        print 'connection OK'
        command_dist = {}
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((server_address, connection_port))
            command_dist['name'] = 'filter'
            command_dist['process_number'] = len(work_thread)
            s.send(json.dumps(command_dist))  # 向‘主人’声明自己的身份和初始数量
            command_dist.clear()
        except IOError as e:
            print e.message
        while True:
            try:
                recv_data = s.recv(1024)
                data_dist = json.loads(recv_data)
                if 'name' in data_dist.keys():
                    if data_dist['name'] == 'start':  # 接收到开始命令
                        spider_type = data_dist['spider_type']
                        t1 = threading.Thread(target=self.work, args=(spider_type, ))  # 开始工作
                        t2 = threading.Thread(target=self.store_url, args=(spider_type, ))  # 开始储存布隆过滤器中的url信息
                        work_thread.append(t1)
                        t1.start()
                        t2.start()
                        command_dist['start'] = 'ok'
                        s.send(json.dumps(command_dist))
                    elif data_dist['name'] == 'end':  # 接收到结束命令
                        self.command = 'end'  # 设置结束标志
                        command_dist['end'] = 'ok'
                        s.send(json.dumps(command_dist))
                        break
                    elif data_dist['name'] == 'number':  # 接收到询问数量信息
                        print len(work_thread)
                        command_dist['worker_type'] = 'filter'
                        command_dist['process_number'] = len(work_thread)  # 计算数量并返回
                        s.send(json.dumps(command_dist))
                    command_dist.clear()
                # if 'number' in data_dist.keys():
                #     print len(work_thread)
                #     command_dist['worker_type'] = 'filter'
                #     command_dist['process_number'] = len(work_thread)
                #     s.send(json.dumps(command_dist))
                #     command_dist.clear()
            except IOError as e:
                print e.message
                break

    # 保留布隆过滤器中的信息
    # noinspection SpellCheckingInspection
    def store_url(self, spider_type):
        number = 0
        while True:
            if spider_type == 'jingdong':
                url_filter_bit = j_db.url_filter_bit
            elif spider_type == 'taobao':
                url_filter_bit = t_db.url_filter_bit
            elif spider_type == 'suning':
                url_filter_bit = s_db.url_filter_bit
            else:
                url_filter_bit = j_db.url_filter_bit
            if self.command == 'end':  # 结束时将内存中的保存
                i = 0
                for url_filter in self.sbf.filters:
                    i += 1
                    url_filter_dist = url_filter_bit.find_one({'name': 'filter' + str(i)})
                    if url_filter_dist is not None:
                        url_filter_dist['bitarray'] = str(url_filter.bitarray)
                        url_filter_bit.save(url_filter_dist)
                    else:
                        url_filter_dist = {'name': 'filter' + str(i), 'bitarray': str(url_filter.bitarray)}
                        url_filter_bit.save(url_filter_dist)
                connection.close()
                break
            else:  # 两秒进行一次持久化操作
                number += 1
                time.sleep(2)
                if number == 15:
                    number = 0
                    i = 0
                    for url_filter in self.sbf.filters:
                        i += 1
                        url_filter_dist = url_filter_bit.find_one({'name': 'filter' + str(i)})
                        if url_filter_dist is not None:
                            url_filter_dist['bitarray'] = str(url_filter.bitarray)
                            url_filter_bit.save(url_filter_dist)
                        else:
                            url_filter_dist = {'name': 'filter' + str(i), 'bitarray': str(url_filter.bitarray)}
                            url_filter_bit.save(url_filter_dist)


if __name__ == '__main__':
    filterWorker = FilterWorker()
    filterWorker.conn_master()
