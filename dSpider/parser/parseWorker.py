# -*- coding: utf-8 -*-
from multiprocessing.managers import BaseManager
import jparse
import tparse
import sparse
import json
import re
import time
import socket
import threading
from storeWorker import StoreWorker

server_address = '127.0.0.1'
server_port = 5000
key = 'abc123'
connection_port = 8000
work_thread = []
storeWorker = StoreWorker()


class QueueManager(BaseManager):
    pass


class ParseWorker(object):

    def __init__(self):
        self.command = ''
        # self.spider_type = ''

    # 开始工作 从html队列中取html，处理后放入url_filter队列中
    # noinspection PyUnresolvedReferences,SpellCheckingInspection
    def work(self, spider_type):
        print spider_type
        QueueManager.register('network_url_filter_queue')
        QueueManager.register('network_html_queue')
        worker = QueueManager(address=(server_address, server_port), authkey=key)
        worker.connect()
        network_url_filter_queue = worker.network_url_filter_queue()
        network_html_queue = worker.network_html_queue()
        # if network_html_queue.empty() is False:
        # init_html_json = network_html_queue.get()
        # self.key_word, href_list = parse.init_parse(init_html_json)
        # for href in href_list:
        #     network_url_filter_queue.put(href)
        while True:
            if self.command == 'end':
                break
            if network_html_queue.empty() is False:
                html_json = network_html_queue.get()
                if spider_type == 'taobao':
                    # key_word_item,
                    url, key_word, href_list = tparse.t_parse(html_json)
                elif spider_type == 'jingdong':
                    # key_word_item,
                    url, key_word, href_list = jparse.j_parse(html_json)
                elif spider_type == 'suning':
                    # key_word_item,
                    url, key_word, href_list = sparse.s_parse(html_json)
                else:
                    # key_word_item,
                    url, key_word, href_list = jparse.j_parse(html_json)
                for href in href_list:
                    if re.compile(r'/').match(href) is not None:
                        print href
                        href_dist = {
                            'refer_url': url,
                            'href': href,
                            'key_word': key_word
                            # 'key_word_item': key_word_item
                        }
                        network_url_filter_queue.put(json.dumps(href_dist))
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
            command_dist['name'] = 'parser'
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
                        t2 = threading.Thread(target=storeWorker.work, args=(spider_type, ))
                        work_thread.append(t1)
                        t1.start()
                        t2.start()
                        command_dist['start'] = 'ok'
                        s.send(json.dumps(command_dist))
                    elif data_dist['name'] == 'end':  # 接收到结束命令
                        self.command = 'end'
                        storeWorker.set_command('end')
                        command_dist['end'] = 'ok'
                        s.send(json.dumps(command_dist))
                        break
                    elif data_dist['name'] == 'number':  # 接收到询问数量信息
                        print len(work_thread)
                        command_dist['worker_type'] = 'parser'
                        command_dist['process_number'] = len(work_thread)
                        s.send(json.dumps(command_dist))
                    command_dist.clear()
                # if 'number' in data_dist.keys():
                #     print len(work_thread)
                #     command_dist['worker_type'] = 'paser'
                #     command_dist['process_number'] = len(work_thread)
                #     s.send(json.dumps(command_dist))
                #     command_dist.clear()
            except IOError as e:
                print e.message
                break


if __name__ == '__main__':
    parseWorker = ParseWorker()
    parseWorker.conn_master()
