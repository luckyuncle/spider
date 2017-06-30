# -*- coding: utf-8 -*-
from multiprocessing.managers import BaseManager
from multiprocessing import Queue, Process
import socket
import threading
import json
import time
from Tkinter import *
import ttk
import tkMessageBox


address = '127.0.0.1'
server_port = 5000
key = 'abc123'
connection_port = 8000


class QueueManager(BaseManager):
    pass


class Master(object):

    def __init__(self):
        self.first_url_queue = Queue()
        self.second_url_queue = Queue()
        self.html_queue = Queue()
        self.json_queue = Queue()
        self.special_url_queue = Queue()
        self.url_filter_queue = Queue()
        self.worker_dist = {}
        self.command = ''
        self.exit_command = ''
        self.s = None
        self.root = None
        self.queue = Queue()
        self.text = None
        self.url_text = None
        self.key_text = None
        self.spider_type = ''
        self.spider = None

    def get_first_url_queue(self):
        return self.first_url_queue

    def get_second_url_queue(self):
        return self.second_url_queue

    def get_html_queue(self):
        return self.html_queue

    def get_json_queue(self):
        return self.json_queue

    def get_special_url_queue(self):
        return self.special_url_queue

    def get_url_filter_queue(self):
        return self.url_filter_queue

    def set_exit_command(self):
        self.command = 'end'

    # 开始抓取前的准备
    # noinspection SpellCheckingInspection,PyUnresolvedReferences
    def set_start_command(self):
        init_url = self.url_text.get()
        key_word = self.key_text.get()
        if self.spider.get() == u'京东':
            self.spider_type = 'jingdong'
        elif self.spider.get() == u'淘宝':
            self.spider_type = 'taobao'
        elif self.spider.get() == u'苏宁':
            self.spider_type = 'suning'
        else:
            self.spider_type = 'jingdong'
        if init_url.strip() == '' or init_url is None:
            tkMessageBox.showinfo('提示', 'url不能为空！')
        elif re.compile(u'https://').match(init_url.strip()) is None and \
                re.compile(u'http://').match(init_url.strip()) is None:
            tkMessageBox.showinfo('提示', 'url不合法！')
        else:
            init_url_json = json.dumps({
                'url': init_url.strip(),
                'key_word': key_word.strip(),
                # 'key_word_item': False
            })
            # network_url_filter_queue = server.network_url_filter_queue()
            self.url_filter_queue.put(init_url_json)
            self.command = 'start'

    # 主界面创建
    # noinspection SpellCheckingInspection
    def create_widgets(self):
        self.root = Tk()
        self.root.geometry('400x300+450+150')
        self.root.resizable(width=False, height=False)
        label = Label(self.root, text='分布式爬虫', font='Helvetica -30 bold')
        label.place(x=130, y=10)
        t1 = threading.Thread(target=master.provide_connection, args=())
        open_server = Button(self.root, text='开启服务器', command=t1.start)
        open_server.place(x=15, y=60)
        close_server = Button(self.root, text='关闭服务器退出', command=self.exit_all)
        close_server.place(x=95, y=60)
        # t2 = threading.Thread(target=master.provide_queue, args=())
        type_label = Label(self.root, text='选择网站')
        type_label.place(x=10, y=100)
        self.spider = StringVar()
        type_chosen = ttk.Combobox(self.root, width=12, textvariable=self.spider)
        type_chosen['values'] = ('京东', '淘宝', '苏宁')
        type_chosen.current(0)
        type_chosen.place(x=70, y=100)
        url_label = Label(self.root, text='输入url')
        url_label.place(x=10, y=140)
        self.url_text = Entry(self.root, width=18)
        self.url_text.place(x=60, y=140)
        key_label = Label(self.root, text='输入关键字')
        key_label.place(x=10, y=180)
        self.key_text = Entry(self.root, width=15)
        self.key_text.place(x=80, y=180)
        open_grab = Button(self.root, text='开始抓取', command=self.set_start_command)
        open_grab.place(x=20, y=220)
        close_grab = Button(self.root, text='结束抓取', command=self.set_exit_command)
        close_grab.place(x=100, y=220)
        frame = Frame(self.root)
        frame.place(x=200, y=65)
        self.text = Text(frame, height=15, width=23)
        self.text.bind("<KeyPress>", lambda e: "break")
        scrl = Scrollbar(frame)
        scrl.pack(side=RIGHT, fill=Y)
        self.text.configure(yscrollcommand=scrl.set)
        self.text.pack(side=LEFT, fill=BOTH)
        scrl['command'] = self.text.yview
        self.root.mainloop()

    # 系统退出
    def exit_all(self):
        self.exit_command = 'end'
        self.queue.put(self.exit_command)
        if self.s is not None:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((address, connection_port))
            s.close()
            self.s.close()
        if self.root is not None:
            self.root.quit()

    # 提供队列
    # noinspection PyUnresolvedReferences
    def provide_queue(self):
        QueueManager.register('network_first_url_queue', callable=self.get_first_url_queue)  # 一级url队列
        QueueManager.register('network_second_url_queue', callable=self.get_second_url_queue)  # 二级url队列
        QueueManager.register('network_html_queue', callable=self.get_html_queue)  # html队列
        QueueManager.register('network_json_queue', callable=self.get_json_queue)  # json队列
        QueueManager.register('network_special_url_queue', callable=self.get_special_url_queue)  # 存放json请求的队列
        QueueManager.register('network_url_filter_queue', callable=self.get_url_filter_queue)  # url去重队列
        server = QueueManager(address=(address, server_port), authkey=key)
        server.start()
        while True:
            if self.queue.get() == 'end':
                break
            else:
                time.sleep(1)
        server.shutdown()

    # 提供与‘工人’的连接
    # noinspection SpellCheckingInspection
    def provide_connection(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind((address, connection_port))
        self.s.listen(5)
        threading.Thread(target=self.send_message, args=()).start()
        while True:
            sock, addr = self.s.accept()
            if self.exit_command == 'end':
                break
            t = threading.Thread(target=self.tcplink, args=(sock, addr))
            t.start()

    # 接收‘工人’反馈的消息
    # noinspection SpellCheckingInspection
    def tcplink(self, sock, addr):
        self.worker_dist[sock] = {}
        print addr
        while True:
            try:
                recv_data = sock.recv(1024)
                data_dist = json.loads(recv_data)
                if 'name' in data_dist.keys():  # ‘工人’连接准备的消息
                    self.worker_dist[sock]['name'] = data_dist['name']
                    print data_dist['name'] + ' is ready'
                    self.text.insert(END, data_dist['name'] + ' is ready\n')
                if 'process_number_init' in data_dist.keys():  # 各类‘工人’初始数量信息
                    self.worker_dist[sock]['process_number'] = data_dist['process_number_init']  # 工人数量的记录
                if 'worker_type' in data_dist.keys():  # 各类‘工人’数量信息
                    self.worker_dist[sock]['process_number'] = data_dist['process_number']  # 工人数量的记录
                    print data_dist['process_number']
                    worker_type = data_dist['worker_type']
                    self.text.insert(END, str(data_dist['process_number']) + ' ' + worker_type + ' work\n')
                if 'end' in data_dist.keys():  # ‘工人’结束消息
                    print self.worker_dist[sock]['name'] + ' end'
                    self.text.insert(END, self.worker_dist[sock]['name'] + ' end\n')
                    sock.close()
                    break
                if 'start' in data_dist.keys():  # ‘工人’开始消息
                    print self.worker_dist[sock]['name'] + ' start'
                    self.text.insert(END, self.worker_dist[sock]['name'] + ' start\n')
            except Exception as e:
                print e.message
                break

    # 向工人发送信息
    # noinspection SpellCheckingInspection
    def send_message(self):
        i = 0
        while True:
            command_dist = {}
            if self.command == 'start':  # 向‘工人’发送开始消息
                for worker_key in self.worker_dist.keys():
                    self.command = 'grab'
                    command_dist['name'] = 'start'
                    command_dist['spider_type'] = self.spider_type
                    try:
                        worker_key.send(json.dumps(command_dist))
                    except IOError as e:
                        print e.message
            elif self.command == 'end':  # 向‘工人’发送结束消息
                for worker_key in self.worker_dist.keys():
                    self.command = ''
                    command_dist['name'] = 'end'
                    try:
                        worker_key.send(json.dumps(command_dist))
                    except IOError as e:
                        print e.message
                break
            elif self.first_url_queue.qsize() + self.second_url_queue.qsize() + self.special_url_queue.qsize() > 100:
                for worker_key in self.worker_dist.keys():
                    if self.worker_dist[worker_key]['name'] == 'graber':
                        print 'send: ' + str(self.worker_dist[worker_key]['process_number'])
                        if self.worker_dist[worker_key]['process_number'] < 0:
                            command_dist['create_process'] = 1
                            try:
                                worker_key.send(json.dumps(command_dist))
                            except IOError as e:
                                print e.message
            command_dist = {}
            if self.command == 'grab':  # 每过10秒钟向各个‘工人’询问数量信息
                i += 1
                if i == 10:
                    i = 0
                    for worker_key in self.worker_dist.keys():
                        command_dist['name'] = 'number'
                        try:
                            worker_key.send(json.dumps(command_dist))
                        except IOError as e:
                            print e.message
            time.sleep(1)


if __name__ == '__main__':
    master = Master()
    Process(target=master.provide_queue, args=()).start()
    master.create_widgets()
