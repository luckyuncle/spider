# -*- coding: utf-8 -*-
# 苏宁
import sys
from lxml import etree
import json
import re
from redis import Redis
from pymongo import MongoClient
import random

reload(sys)
# noinspection PyUnresolvedReferences
sys.setdefaultencoding('utf-8')
r0 = Redis(host='127.0.0.1', port=6379, db=0)
r1 = Redis(host='127.0.0.1', port=6379, db=2)
connection = MongoClient('127.0.0.1', 27017)
db = connection.suning
goods_name = db.goods_name
goods_shop = db.goods_shop


# 对html进行分类
# noinspection SpellCheckingInspection,PyTypeChecker
def s_parse(html_json):
    html_dist = json.loads(html_json)
    url = html_dist['url']
    print url
    html = html_dist['html']
    key_word = html_dist['key_word']
    # key_word_item = False
    if url == 'http://list.suning.com/':  # 全部分类页
        href_list = all_subject_parse(html, key_word)
    elif re.compile(r'http://list.suning.com/0-').match(url) is not None:  # 商品列表页
        # key_word_item, key_word
        href_list = list_parse(html)
    else:  # 其他页
        href_list = common_parse(html)
    # key_word_item,
    return url, key_word, href_list


# 普通网页的解析
# noinspection SpellCheckingInspection
def common_parse(html):
    print 'common'
    page = etree.HTML(html)
    href_list = []
    all_href_list = page.xpath(u"//a/@href")  # 所有的连接
    for href in all_href_list:
        if re.compile(r'//list.suning.com').search(href) is not None:
            href_list.append(href)
    return href_list


# 全部商品分类页的解析
# noinspection SpellCheckingInspection
def all_subject_parse(html, key_word):
    print 'all_subject'
    page = etree.HTML(html)
    if key_word is not None and key_word != '':  # 对关键字的判断
        content = page.xpath(u"//div[@class='search-wrap clearfix']/div[@class='search-main introduce clearfix']"
                             u"/div/h2")  # 一级分类
        for c in content:
            if key_word in c.text.replace('/', ''):
                href_list = c.xpath(u"../div[@class='title-box clearfix']//a/@href")
                if len(href_list) > 0:
                    return href_list
                return []
        print '关键字不准确,抓取可能不准确'
    href_list = page.xpath(u"//div[@class='search-wrap clearfix']/div[@class='search-main introduce clearfix']"
                           u"/div/div[@class='title-box clearfix']//a/@href")
    if len(href_list) > 0:
        return href_list
    return []


# 商品列表解析
# noinspection SpellCheckingInspection,PyTypeChecker
# , key_word
def list_parse(html):
    print 'list'
    page = etree.HTML(html)
    # key_word_item = True
    href_list = []
    # if key_word is not None and key_word != '':
    #     key_words = page.xpath(u"/html/head/meta")
    #     for kws in key_words:
    #         kw = kws.xpath(u"./@name")
    #         if len(kw) > 0 and kw[0].lower() == 'keywords'.lower():
    #             content = kws.xpath(u"./@content")
    #             print content[0].replace('、', '')
    #             if len(content) > 0 and key_word in content[0].replace('、', ''):
    #                 list_extract(page)
    #                 parent_node = page.xpath(u"//script")
    #                 for x in parent_node:
    #                     if x.text is not None:
    #                         if re.compile(r'varparam=').search(x.text.strip()) is not None:
    #                             n = x.text
    #                             dist = json.loads(n[n.find('{'): n.find('}') + 1])
    #                             cur_page = dist['currentPage']
    #                             total_page = dist['pageNumbers']
    #                             if int(cur_page) < int(total_page):
    #                                 print cur_page
    #                                 next_page = int(cur_page) + 2
    #                                 href_list.append(page.xpath(u"//div[@id='bottom_pager']/"
    #                                                            u"a[@pagenum='" + unicode(next_page) + u"']/@href")[0])
    # else:
    list_extract(page)
    # 下一页的处理
    parent_node = page.xpath(u"//script")
    for x in parent_node:
        if x.text is not None:
            if re.compile(r'var param').search(x.text) is not None:
                n = x.text
                dist = json.loads(n[n.find('{'): n.find('}') + 1])
                cur_page = dist['currentPage']
                total_page = dist['pageNumbers']
                if int(cur_page) < int(total_page):
                    print cur_page
                    next_page = int(cur_page) + 2
                    href_list.append(page.xpath(u"//div[@id='bottom_pager']/"
                                                u"a[@pagenum='" + unicode(next_page) + u"']/@href")[0])
    # key_word_item,
    return href_list


# 商品列表解析
# noinspection SpellCheckingInspection
def list_extract(page):
    goods_name_map = {}
    goods_shop_map = {}
    content = page.xpath(u"//div[@class='search-results clearfix mt10']//"
                         u"div[@id='filter-results']/ul[@class='clearfix']/li//div[@class='res-info']")
    for c in content:
        data_list = []
        goods_id = ''
        shop_id = ''
        data_sku = c.xpath(u"./p[@class='prive-tag']/em/@datasku")[0]  # 商品id和店铺id
        for data in data_sku.split('|'):
            if data.strip() != '':
                data_list.append(data)
        if len(data_list) == 2:
            goods_id = data_list[0]
            shop_id = data_list[1]
            joint_url(goods_id, shop_id)
        if len(data_list) == 3:
            goods_id = data_list[0]
            mid_id = data_list[1]
            shop_id = data_list[2]
            joint_url(goods_id, shop_id, mid_id)
        if goods_id != '':
            goods_name_map[goods_id] = {}
            goods_name_map[goods_id]['goods_name'] = c.xpath(u"./p[@class='sell-point']/a/text()")[0]  # 商品名
            if len(c.xpath(u"./p[@class='sell-point']/a/em/text()")) > 0:
                goods_name_map[goods_id]['goods_style'] = c.xpath(u"./p[@class='sell-point']/a/em/text()")[0]
            goods_shop_map[goods_id] = {}
            goods_shop_map[goods_id]['id'] = shop_id
            if len(c.xpath(u"./p/@salesname")) > 0:
                goods_shop_map[goods_id]['name'] = c.xpath(u"./p/@salesname")[0]  # 店铺名
            elif len(c.xpath(u"./p/@salesName")) > 0:
                goods_shop_map[goods_id]['name'] = c.xpath(u"./p/@salesName")[0]
    goods_name.save(goods_name_map)
    goods_shop.save(goods_shop_map)


# 价格、评论json请求的拼接
# noinspection SpellCheckingInspection
def joint_url(goods_id, shop_id, mid_id=None):
    call = str(random.randint(1, 90))
    if mid_id is None:
        if shop_id == '0000000000':
            price_url = 'http://ds.suning.cn/ds/generalForTile/000000000' + goods_id + \
                        '_-010-2-0000000000-1--ds000000000' + call + '.jsonp?callback=ds000000000' + call
        else:
            price_url = 'http://ds.suning.cn/ds/generalForTile/000000000' + goods_id + '__2_' + shop_id + \
                        '_-010-2-0000000000-1--ds000000000' + call + '.jsonp?callback=ds000000000' + call
    else:
        if shop_id == '0000000000':
            price_url = 'http://ds.suning.cn/ds/generalForTile/000000000' + goods_id + \
                        '_-010-' + mid_id + '-0000000000-1--ds000000000' + call + '.jsonp?callback=ds000000000' + call
        else:
            price_url = 'http://ds.suning.cn/ds/generalForTile/000000000' + goods_id + '__2_' + shop_id + \
                        '_-010-' + mid_id + '-0000000000-1--ds000000000' + call + '.jsonp?callback=ds000000000' + call
    r0.lpush('price_url', price_url)
    comment_url = 'http://review.suning.com/ajax/review_satisfy/general-000000000' + goods_id + '-' + shop_id + \
                  '-----satisfy.htm?callback=satisfy'
    r1.lpush('comment_url', comment_url)
