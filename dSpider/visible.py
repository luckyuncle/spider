# -*- coding: utf-8 -*-
from Tkinter import *
import numpy as np
import matplotlib.pyplot as plt
from pymongo import MongoClient
from multiprocessing import Process
import ttk


class Visible(object):

    def __init__(self):
        pass

    # 店铺商品数量显示
    # noinspection PyMethodMayBeStatic,SpellCheckingInspection
    def shop_goods_show(self, spider_type):
        shop_map = {}
        connection = MongoClient('127.0.0.1', 27017)
        if spider_type == u'京东':
            print '京东'
            db = connection.jingdong
        elif spider_type == u'淘宝':
            print '淘宝'
            db = connection.taobao
        elif spider_type == u'苏宁':
            print '苏宁'
            db = connection.suning
        else:
            print 'jingdong'
            db = connection.jingdong
        goods_shop = db.goods_shop
        posts = goods_shop.find()
        for post in posts:
            for key in post.keys():
                if isinstance(post[key], dict):
                    if post[key]['id'] in shop_map:
                        shop_map[post[key]['id']] += 1
                    else:
                        shop_map[post[key]['id']] = 1
        connection.close()
        one_goods_list = []
        two_goods_list = []
        three_goods_list = []
        four_goods_list = []
        five_goods_list = []
        six_goods_list = []
        seven_goods_list = []
        eight_goods_list = []
        nine_goods_list = []
        ten_over_goods_list = []
        for item in shop_map.iteritems():
            if item[1] == 1:
                one_goods_list.append(item[0])
            elif item[1] == 2:
                two_goods_list.append(item[0])
            elif item[1] == 3:
                three_goods_list.append(item[0])
            elif item[1] == 4:
                four_goods_list.append(item[0])
            elif item[1] == 5:
                five_goods_list.append(item[0])
            elif item[1] == 6:
                six_goods_list.append(item[0])
            elif item[1] == 7:
                seven_goods_list.append(item[0])
            elif item[1] == 8:
                eight_goods_list.append(item[0])
            elif item[1] == 9:
                nine_goods_list.append(item[0])
            else:
                ten_over_goods_list.append(item[0])
        n_groups = 10
        shop_number = (len(one_goods_list), len(two_goods_list), len(three_goods_list), len(four_goods_list),
                       len(five_goods_list), len(six_goods_list), len(seven_goods_list), len(eight_goods_list),
                       len(nine_goods_list), len(ten_over_goods_list))
        index = np.arange(n_groups)
        bar_width = 0.4
        opacity = 0.4
        rects = plt.bar(index, shop_number, bar_width, alpha=opacity, color='b')
        for rect in rects:
            height = rect.get_height()
            plt.text(rect.get_x() + rect.get_width() / 2.0, 1.01 * height, '%d' % int(height), ha='center', va='bottom')
        plt.title('The number of shop\'goods')
        plt.xlabel('goods\'number')
        plt.ylabel('number')
        plt.xticks(index, ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10+'))
        plt.legend((rects,), ('shop',))
        plt.show()

    # 商品价格显示
    # noinspection PyMethodMayBeStatic,SpellCheckingInspection
    def goods_price_show(self, spider_type):
        price_list = []
        connection = MongoClient('127.0.0.1', 27017)
        if spider_type == u'京东':
            print '京东'
            db = connection.jingdong
        elif spider_type == u'淘宝':
            print '淘宝'
            db = connection.taobao
        elif spider_type == u'苏宁':
            print '苏宁'
            db = connection.suning
        else:
            print 'jingdong'
            db = connection.jingdong
        goods_price = db.goods_price
        posts = goods_price.find()
        for post in posts:
            for key in post.keys():
                if isinstance(post[key], dict):
                    goods = (key, post[key]['price'])
                    price_list.append(goods)
        connection.close()
        one_list = []
        two_list = []
        three_list = []
        four_list = []
        five_list = []
        six_list = []
        seven_list = []
        eight_list = []
        for price in price_list:
            if float(price[1]) <= 60:
                one_list.append(price[0])
            elif 60.0 < float(price[1]) <= 90:
                two_list.append(price[0])
            elif 90.0 < float(price[1]) <= 120:
                three_list.append(price[0])
            elif 120.0 < float(price[1]) <= 150:
                four_list.append(price[0])
            elif 150.0 < float(price[1]) <= 180:
                five_list.append(price[0])
            elif 180.0 < float(price[1]) <= 210:
                six_list.append(price[0])
            elif 210.0 < float(price[1]) <= 240:
                seven_list.append(price[0])
            else:
                eight_list.append(price[0])
        n_groups = 8
        shop_number = (len(one_list), len(two_list), len(three_list), len(four_list), len(five_list),
                       len(six_list), len(seven_list), len(eight_list))
        index = np.arange(n_groups)
        bar_width = 0.4
        opacity = 0.4
        rects = plt.bar(index, shop_number, bar_width, alpha=opacity, color='b')
        for rect in rects:
            height = rect.get_height()
            plt.text(rect.get_x() + rect.get_width() / 2.0, 1.01 * height, '%d' % int(height), ha='center', va='bottom')
        plt.title('The price of goods')
        plt.xlabel('price')
        plt.ylabel('number')
        plt.xticks(index, ('<60', '60~90', '90~120', '120~150', '150~180', '180~210', '210~240', '>240'), fontsize=9)
        plt.legend((rects,), ('goods',))
        plt.show()

    # 商品评论数量显示
    # noinspection PyMethodMayBeStatic,SpellCheckingInspection
    def goods_comment_show(self, spider_type):
        comment_map = {}
        connection = MongoClient('127.0.0.1', 27017)
        if spider_type == u'京东':
            print '京东'
            db = connection.jingdong
        elif spider_type == u'淘宝':
            print '淘宝'
            db = connection.taobao
        elif spider_type == u'苏宁':
            print '苏宁'
            db = connection.suning
        else:
            print 'jingdong'
            db = connection.jingdong
        goods_comment = db.goods_comment
        posts = goods_comment.find()
        for post in posts:
            for key in post.keys():
                if isinstance(post[key], dict):
                    comment_map[key] = {}
                    comment_map[key]['all'] = post[key]['CommentCount']
        connection.close()
        one_list = []
        two_list = []
        three_list = []
        four_list = []
        five_list = []
        six_list = []
        for item in comment_map.iteritems():
            if item[1]['all'] <= 20:
                one_list.append(item[0])
            elif 20 < item[1]['all'] <= 50:
                two_list.append(item[0])
            elif 50 < item[1]['all'] <= 100:
                three_list.append(item[0])
            elif 100 < item[1]['all'] <= 300:
                four_list.append(item[0])
            elif 300 < item[1]['all'] <= 500:
                five_list.append(item[0])
            else:
                six_list.append(item[0])
        n_groups = 6
        shop_number = (len(one_list), len(two_list), len(three_list), len(four_list), len(five_list), len(six_list))
        index = np.arange(n_groups)
        bar_width = 0.4
        opacity = 0.4
        rects = plt.bar(index, shop_number, bar_width, alpha=opacity, color='b')
        for rect in rects:
            height = rect.get_height()
            plt.text(rect.get_x() + rect.get_width() / 2.0, 1.01 * height, '%d' % int(height), ha='center', va='bottom')
        plt.title('The comment of goods')
        plt.xlabel('comment')
        plt.ylabel('number')
        plt.xticks(index, ('<20', '20~50', '50~100', '100~300', '300~500', '500 +'))
        plt.legend((rects,), ('goods',))
        plt.show()

    # 商品数量排名前5的店铺
    # noinspection PyMethodMayBeStatic,SpellCheckingInspection
    def top_five_shop(self, spider_type):
        connection = MongoClient('127.0.0.1', 27017)
        if spider_type == u'京东':
            print '京东'
            db = connection.jingdong
        elif spider_type == u'淘宝':
            print '淘宝'
            db = connection.taobao
        elif spider_type == u'苏宁':
            print '苏宁'
            db = connection.suning
        else:
            print 'jingdong'
            db = connection.jingdong
        goods_shop = db.goods_shop
        posts = goods_shop.find()
        shop_map = {}
        for post in posts:
            for key in post.keys():
                if isinstance(post[key], dict):
                    if post[key]['id'] in shop_map:
                        shop_map[post[key]['id']]['goods_number'] += 1
                    else:
                        shop_map[post[key]['id']] = {}
                        shop_map[post[key]['id']]['name'] = post[key]['name']
                        shop_map[post[key]['id']]['goods_number'] = 1
        good_list = []
        goods_shop_list = []
        goods_number_list = []
        for item in shop_map.iteritems():
            good_list.append((item[0], item[1]))
        good_list.sort(lambda a, b: -cmp(a[1]['goods_number'], b[1]['goods_number']))
        for goods in good_list[1:6]:
            goods_shop_list.append(goods[1]['name'])
            goods_number_list.append(goods[1]['goods_number'])
        plt.rcParams['font.sans-serif'] = ['SimHei']
        n_groups = 5
        index = np.arange(n_groups)
        bar_width = 0.35
        opacity = 0.4
        print goods_number_list
        rects = plt.bar(index, goods_number_list, bar_width, alpha=opacity, color='b')
        for rect in rects:
            height = rect.get_height()
            plt.text(rect.get_x() + rect.get_width() / 2.0, 1.01 * height, '%d' % int(height), ha='center', va='bottom')
        plt.title('Top five shop of goods\'number')
        plt.xlabel('shop')
        plt.ylabel('number')
        plt.xticks(index, goods_shop_list, fontsize=7)
        plt.legend((rects,), ('goods',))
        plt.show()

    # 评论排名前5的商品评价
    # noinspection PyMethodMayBeStatic,SpellCheckingInspection
    def top_five_comment(self, spider_type):
        connection = MongoClient('127.0.0.1', 27017)
        if spider_type == u'京东':
            print '京东'
            db = connection.jingdong
        elif spider_type == u'淘宝':
            print '淘宝'
            db = connection.taobao
        elif spider_type == u'苏宁':
            print '苏宁'
            db = connection.suning
        else:
            print 'jingdong'
            db = connection.jingdong
        goods_comment = db.goods_comment
        posts = goods_comment.find()
        comment_map = {}
        for post in posts:
            for key in post.keys():
                if isinstance(post[key], dict):
                    comment_map[key] = {}
                    comment_map[key]['good'] = post[key]['GoodCount']
                    comment_map[key]['poor'] = post[key]['PoorCount']
                    comment_map[key]['general'] = post[key]['GeneralCount']
                    comment_map[key]['all'] = post[key]['CommentCount']
        connection.close()
        comment_list = []
        good_comment_list = []
        general_comment_list = []
        poor_comment_list = []
        for item in comment_map.iteritems():
            comment_list.append((item[0], item[1]['all']))
        comment_list.sort(lambda a, b: -cmp(a[1], b[1]))
        for comment in comment_list[0:5]:
            good_comment_list.append(comment_map[comment[0]]['good'])
            general_comment_list.append(comment_map[comment[0]]['general'])
            poor_comment_list.append(comment_map[comment[0]]['poor'])
        n_groups = 5
        index = np.arange(n_groups)
        bar_width = 0.4
        opacity = 0.4
        poor_bottom_list = []
        for i in range(0, 5):
            poor_bottom_list.append(good_comment_list[i] + general_comment_list[i])
        general_bottom_list = good_comment_list
        rects1 = plt.bar(index, good_comment_list, bar_width, alpha=opacity, color='b')
        rects2 = plt.bar(index, general_comment_list, bar_width, alpha=opacity, color='g', bottom=general_bottom_list)
        rects3 = plt.bar(index, poor_comment_list, bar_width, alpha=opacity, color='r', bottom=poor_bottom_list)
        plt.title('Top five number of comment')
        plt.xlabel('comment')
        plt.ylabel('number')
        plt.xticks(index, ('1', '2', '3', '4', '5'))
        plt.legend((rects1, rects2, rects3), ('good', 'general', 'poor'))
        plt.show()

    # noinspection PyMethodMayBeStatic,SpellCheckingInspection
    def top_five(self, spider_type):
        price_list = []
        comment_list = []
        connection = MongoClient('127.0.0.1', 27017)
        if spider_type == u'京东':
            print '京东'
            db = connection.jingdong
        elif spider_type == u'淘宝':
            print '淘宝'
            db = connection.taobao
        elif spider_type == u'苏宁':
            print '苏宁'
            db = connection.suning
        else:
            print 'jingdong'
            db = connection.jingdong
        goods_price = db.goods_price
        goods_name = db.goods_name
        goods_comment = db.goods_comment
        price_posts = goods_price.find()
        name_posts = goods_name.find()
        comment_posts = goods_comment.find()
        for post in price_posts:
            for key in post.keys():
                if isinstance(post[key], dict):
                    goods = (key, post[key]['price'])
                    price_list.append(goods)
        for post in comment_posts:
            for key in post.keys():
                if isinstance(post[key], dict):
                    goods = (key, post[key]['CommentCount'])
                    comment_list.append(goods)
        price_list.sort(lambda a, b: -cmp(float(a[1]), float(b[1])))
        comment_list.sort(lambda a, b: -cmp(int(a[1]), int(b[1])))
        high_price_list = []
        high_comment_list = []
        # low_price_list = []
        # l_price_list = []
        for high_price in price_list[0:5]:
            high_price_list.append(high_price[0])
        for high_comment in comment_list[0:5]:
            high_comment_list.append(high_comment[0])
        # for i in range(-5, 0):
        #     l_price_list.append(price_list[i][0])
        # l_price_list.reverse()
        # i = -1
        # while True:
        #     if float(price_list[i][1]) > 0:
        #         low_price_list.append(price_list[i][0])
        #     if len(low_price_list) == 5:
        #         break
        #     i -= 1
        # print low_price_list
        for post in name_posts:
            for key in post.keys():
                if key in high_price_list:
                    print 'high:' + post[key]
                if key in high_comment_list:
                    print 'comment:' + post[key]
                # elif key in low_price_list:
                #     print 'low:' + post[key]
                # elif key in l_price_list:
                #     print 'l:' + post[key]
                else:
                    continue
        connection.close()

    # def top_five_low_price(self):
    #     pass
    #
    # def top_five_goods_comment(self):
    #     pass

    def create_goods_price(self, spider_type):
        p = Process(target=self.goods_price_show, args=(spider_type, ))
        p.start()

    def create_shop_goods(self, spider_type):
        p = Process(target=self.shop_goods_show, args=(spider_type, ))
        p.start()

    def create_goods_comment(self, spider_type):
        p = Process(target=self.goods_comment_show, args=(spider_type, ))
        p.start()

    def create_top_five_shop(self, spider_type):
        p = Process(target=self.top_five_shop, args=(spider_type, ))
        p.start()

    def create_top_five_comment(self, spider_type):
        p = Process(target=self.top_five_comment, args=(spider_type, ))
        p.start()

    def create_root(self):
        root = Tk()
        root.geometry('400x300+450+150')
        label = Label(root, text='商品统计', font='Helvetica -30 bold')
        label.place(x=120, y=10)
        type_label = Label(root, text='选择网站')
        type_label.place(x=70, y=70)
        spider = StringVar()
        type_chosen = ttk.Combobox(root, width=12, textvariable=spider)
        type_chosen['values'] = ('京东', '淘宝', '苏宁')
        type_chosen.current(0)
        type_chosen.place(x=130, y=70)
        create_goods_price = Button(root, text='价格区间分布', command=lambda: self.create_goods_price(spider.get()))
        create_goods_price.place(x=60, y=110)
        create_shop_goods = Button(root, text='店铺商品数量分布', command=lambda: self.create_shop_goods(spider.get()))
        create_shop_goods.place(x=210, y=110)
        create_goods_comment = Button(root, text='评价数量分布', command=lambda: self.create_goods_comment(spider.get()))
        create_goods_comment.place(x=60, y=150)
        create_top_five_shop = Button(root, text='商品数量最多的前5店铺',
                                      command=lambda: self.create_top_five_shop(spider.get()))
        create_top_five_shop.place(x=210, y=150)
        create_top_five_comment = Button(root, text='评价数量最多的前5商品评价情况',
                                         command=lambda: self.create_top_five_comment(spider.get()))
        create_top_five_comment.place(x=60, y=190)
        root.mainloop()

if __name__ == '__main__':
    v = Visible()
    # v.top_five()
    v.create_root()
