# -*- coding: utf-8 -*-
# 淘宝
import sys
from lxml import etree
import json
import re
from redis import Redis
from pymongo import MongoClient

reload(sys)
# noinspection PyUnresolvedReferences
sys.setdefaultencoding('utf-8')

r1 = Redis(host='127.0.0.1', port=6379, db=2)
connection = MongoClient('127.0.0.1', 27017)
db = connection.taobao
goods_name = db.goods_name
goods_shop = db.goods_shop
goods_price = db.goods_price
# goods_id_list = []


# 对html进行分类
# noinspection SpellCheckingInspection,PyTypeChecker
def t_parse(html_json):
    html_dist = json.loads(html_json)
    url = html_dist['url']
    html = html_dist['html']
    key_word = html_dist['key_word']
    # key_word_item = False
    # print url
    if re.compile(r'https://www.taobao.com/tbhome/page/market-list\?spm=').match(url) is not None:  # 全部分类页
        href_list = all_subject_parse(html, key_word)
    elif re.compile(r'https://s.taobao.com/list\?').match(url) is not None:  # 商品列表页
        # key_word_item,
        href_list = list_parse(url, html)
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
    all_href_list = page.xpath(u"//a/@href") # 所有的连接
    for href in all_href_list:
        if re.compile(r'//s.taobao.com/list\?').search(href) is not None:
            print href
            href_list.append(href)
    return href_list


# 全部商品分类页的解析
def all_subject_parse(html, key_word):
    print 'all_subject'
    page = etree.HTML(html)
    if key_word is not None and key_word != '':  # 对关键字的判断
        content = page.xpath(u"//div[@class='home-category-list J_Module']/div[@class='module-wrap']"
                             u"/a[@class='category-name category-name-level1 J_category_hash']")
        for c in content:
            if key_word in c.text.replace('、', ''):
                print key_word
                parent_node = c.xpath(u"../ul[@class='category-list']")
                if len(parent_node) > 0:
                    href_list = parent_node[0].xpath(u".//a[@class='category-name']/@href")
                    if len(href_list) > 0:
                        return href_list
                    return []
        print '关键字不准确,抓取可能不准确'
    href_list = page.xpath(u"//div[@class='home-category-list J_Module']//a/@href")
    if len(href_list) > 0:
        return href_list
    return []


# 商品列表解析
# noinspection SpellCheckingInspection
def list_parse(url, html):
    print 'list'
    page = etree.HTML(html)
    # key_word_item = True
    parent_node = page.xpath(u"//script")
    href_list = []
    print url
    for x in parent_node:
        if x.text is not None:
            if re.compile(r'g_page_config').search(x.text) is not None:
                n = re.compile(r'{.*}').search(x.text).group()
                # print n
                dist = json.loads(n)
                if 'grid' in dist['mods'].keys():
                    goods_list = dist['mods']['grid']['data']['spus']
                    for goods in goods_list:
                        href_list.append(goods['url'])
                    if 'pager' in dist['mods'].keys():
                        if 'data' in dist['mods']['pager'].keys():
                            page_size = dist['mods']['pager']['data']['pageSize']
                            total_page = dist['mods']['pager']['data']['totalPage']
                            current_page = dist['mods']['pager']['data']['currentPage']
                        else:
                            # print dist['mods']['pager']['status']
                            if dist['mods']['pager']['status'] == 'hide':
                                # key_word_item,
                                return href_list
                            page_size = dist['mods']['pager']['pageSize']
                            total_page = dist['mods']['pager']['totalPage']
                            current_page = dist['mods']['pager']['currentPage']
                        if current_page < total_page:
                            if re.compile(r'//.*&s=').search(url) is None:
                                href_list.append(re.compile(r'//.*').search(url + '&s=' +
                                                                            str(current_page * page_size)).group())
                            else:
                                href_list.append(re.compile(r'//.*').search(re.compile(r'//.*&s=').search(url).group() +
                                                 str(current_page * page_size)).group())
                if 'itemlist' in dist['mods'].keys():
                    goods_name_map = {}
                    goods_shop_map = {}
                    goods_price_map = {}
                    try:
                        goods_list = dist['mods']['itemlist']['data']['auctions']
                    except Exception as e:
                        print e.message
                        goods_list = []
                    for goods in goods_list:
                        nid = goods['nid']
                        user_id = goods['user_id']
                        goods_name_map[nid] = {}
                        goods_shop_map[nid] = {}
                        goods_price_map[nid] = {}
                        if 'title' in goods.keys():
                            goods_name_map[nid]['name'] = goods['title']
                        if 'raw_title' in goods.keys():
                            goods_name_map[nid]['raw_name'] = goods['raw_title']
                        if 'view_sales' in goods.keys():
                            goods_name_map[nid]['view_sales'] = goods['view_sales']
                        if 'view_price' in goods.keys():
                            goods_price_map[nid]['price'] = goods['view_price']
                        if 'reserve_price' in goods.keys():
                            goods_price_map[nid]['reserve_price'] = goods['reserve_price']
                        goods_shop_map[nid]['id'] = user_id
                        if 'nick' in goods.keys():
                            goods_shop_map[nid]['name'] = goods['nick']
                        if 'item_loc' in goods.keys():
                            goods_shop_map[nid]['address'] = goods['item_loc']
                        json_url = 'https://rate.taobao.com/detailCommon.htm?auctionNumId=' + nid + '&userNumId=' + \
                                   user_id + '880489729&ua=086UW5TcyMNYQwiAiwTR3tCf0J%2FQnhEcUpk' \
                                             'MmQ%3D%7CUm5Ockt%2FRXlDfEB8QX9FcCY%3D%7CU2xMHDJ%2BH2QJZwBxX39RaVZ4WHY' \
                                             'qSy1BJlgiDFoM%7CVGhXd1llXGhSblRrV2peZltgV2pIfUVwSXRAekVwTnBLc05wXgg%3D' \
                                             '%7CVWldfS0TMwsyDy8TJgYoWDVnJls%2FVjkSMgsrFzJkTmQPIXch%7CVmJCbEIU%7CV2' \
                                             'lJGS0TMw4uEiwVIQE0CjMOLhIsFywMNg04GCQaIRo6AD8KXAo%3D%7CWGFcYUF8XGND' \
                                             'f0Z6WmRcZkZ8R2dZDw%3D%3D&callback=json_tbc_rate_summary'
                        # print json_url
                        r1.lpush('json_url', json_url)
                    goods_name.save(goods_name_map)
                    goods_price.save(goods_price_map)
                    goods_shop.save(goods_shop_map)
                    if 'pager' in dist['mods'].keys():
                        if 'data' in dist['mods']['pager'].keys():
                            page_size = dist['mods']['pager']['data']['pageSize']
                            total_page = dist['mods']['pager']['data']['totalPage']
                            current_page = dist['mods']['pager']['data']['currentPage']
                        else:
                            # print dist['mods']['pager']['status']
                            if dist['mods']['pager']['status'] == 'hide':
                                # key_word_item,
                                return href_list
                            page_size = dist['mods']['pager']['pageSize']
                            total_page = dist['mods']['pager']['totalPage']
                            current_page = dist['mods']['pager']['currentPage']
                        if current_page < total_page:
                            if re.compile(r'//.*&s=').search(url) is None:
                                href_list.append(re.compile(r'//.*').search(url + '&s=' +
                                                                            str(current_page * page_size)).group())
                            else:
                                href_list.append(re.compile(r'//.*').search(re.compile(r'//.*&s=').search(url).group() +
                                                 str(current_page * page_size)).group())
                        # print href_list
    # key_word_item,
    return href_list
