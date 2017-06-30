# -*- coding: utf-8 -*-
from multiprocessing.managers import BaseManager
import grab
import time
import socket
import threading
import json
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


class GrabWorker(object):

    def __init__(self):
        self.command = ''

    # 开始工作 从一级url队列、二级url队列、特殊url队列中取url，处理后放入html或json队列中
    # noinspection PyUnresolvedReferences
    def work(self, spider_type):
        print spider_type
        QueueManager.register('network_first_url_queue')
        QueueManager.register('network_second_url_queue')
        QueueManager.register('network_html_queue')
        QueueManager.register('network_special_url_queue')
        QueueManager.register('network_json_queue')
        worker = QueueManager(address=(server_address, server_port), authkey=key)
        worker.connect()
        network_first_url_queue = worker.network_first_url_queue()
        network_second_url_queue = worker.network_second_url_queue()
        network_html_queue = worker.network_html_queue()
        network_special_url_queue = worker.network_special_url_queue()
        network_json_queue = worker.network_json_queue()
        while True:
            if self.command == 'end':  # 结束后将所有url队列中的url存入数据库
                special_url_list = []
                first_url_list = []
                second_url_list = []
                while network_special_url_queue.empty() is False:
                    special_url_json = network_special_url_queue.get()
                    special_url_dist = json.loads(special_url_json)
                    special_url_list.append(special_url_dist['special_url'])
                if len(special_url_list) > 0:
                    if spider_type == 'jingdong':
                        url_not_grab = j_db.url_not_grab
                    elif spider_type == 'taobao':
                        url_not_grab = t_db.url_not_grab
                    elif spider_type == 'suning':
                        url_not_grab = s_db.url_not_grab
                    else:
                        url_not_grab = j_db.url_not_grab
                    url_not_grab.save({'special_url': special_url_list})
                while network_first_url_queue.empty() is False:
                    first_url_json = network_first_url_queue.get()
                    first_url_dist = json.loads(first_url_json)
                    first_url_list.append(first_url_dist['url'])
                if len(first_url_list) > 0:
                    if spider_type == 'jingdong':
                        url_not_grab = j_db.url_not_grab
                    elif spider_type == 'taobao':
                        url_not_grab = t_db.url_not_grab
                    elif spider_type == 'suning':
                        url_not_grab = s_db.url_not_grab
                    else:
                        url_not_grab = j_db.url_not_grab
                    url_not_grab.save({'first_url': first_url_list})
                while network_second_url_queue.empty() is False:
                    second_url_json = network_second_url_queue.get()
                    second_url_dist = json.loads(second_url_json)
                    second_url_list.append(second_url_dist['url'])
                if len(second_url_list) > 0:
                    if spider_type == 'jingdong':
                        url_not_grab = j_db.url_not_grab
                    elif spider_type == 'taobao':
                        url_not_grab = t_db.url_not_grab
                    elif spider_type == 'suning':
                        url_not_grab = s_db.url_not_grab
                    else:
                        url_not_grab = j_db.url_not_grab
                    url_not_grab.save({'second_url': second_url_list})
                break
            if network_special_url_queue.empty() is False:
                special_url_json = network_special_url_queue.get()  # 从特殊url队列中取json请求
                json_json = grab.special_url_grab(special_url_json)
                if json_json is not None:
                    network_json_queue.put(json_json)  # 放入json队列
            elif network_first_url_queue.empty() is False:
                url_json = network_first_url_queue.get()  # 从一级url队列中取url
                html_json = grab.url_grab(url_json)
                if html_json is not None:
                    network_html_queue.put(html_json)  # 放入html队列
            elif network_second_url_queue.empty() is False:
                url_json = network_second_url_queue.get()  # 从二级url队列中取url
                html_json = grab.url_grab(url_json)
                if html_json is not None:
                    network_html_queue.put(html_json)  # 放入html队列
            else:
                time.sleep(1)
            connection.close()

    # 与‘主人’进行连接
    # noinspection SpellCheckingInspection
    def conn_master(self):
        print 'connection OK'
        spider_type = ''
        command_dist = {}
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((server_address, connection_port))
            command_dist['name'] = 'graber'
            command_dist['process_number_init'] = len(work_thread)
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
                        t = threading.Thread(target=self.work, args=(spider_type, ))  # 开始工作
                        work_thread.append(t)
                        t.start()
                        command_dist['start'] = 'ok'
                        s.send(json.dumps(command_dist))
                    elif data_dist['name'] == 'end':  # 接收到结束命令
                        self.command = 'end'  # 设置结束标志
                        command_dist['end'] = 'ok'
                        s.send(json.dumps(command_dist))
                        break
                    elif data_dist['name'] == 'number':  # 接收到询问数量信息
                        print len(work_thread)
                        command_dist['worker_type'] = 'graber'
                        command_dist['process_number'] = len(work_thread)  # 计算数量并返回
                        s.send(json.dumps(command_dist))
                    command_dist.clear()
                if 'create_process' in data_dist.keys():  # 创建抓取工人
                    number = data_dist['create_process']
                    for i in range(0, number):
                        t1 = threading.Thread(target=self.work, args=(spider_type, ))
                        work_thread.append(t1)
                        t1.start()
                    command_dist['process_number'] = len(work_thread)
                    s.send(json.dumps(command_dist))
                    command_dist.clear()
            except IOError as e:
                print e.message
                break


if __name__ == '__main__':
    grabWorker = GrabWorker()
    grabWorker.conn_master()
