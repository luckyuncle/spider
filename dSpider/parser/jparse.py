# -*- coding: utf-8 -*-
# 京东
import sys
from lxml import etree
import json
import re
from redis import Redis
from pymongo import MongoClient

reload(sys)
# noinspection PyUnresolvedReferences
sys.setdefaultencoding('utf-8')
r0 = Redis(host='127.0.0.1', port=6379, db=0)
r1 = Redis(host='127.0.0.1', port=6379, db=2)
r2 = Redis(host='127.0.0.1', port=6379, db=3)
connection = MongoClient('127.0.0.1', 27017)
db = connection.jingdong
goods_name = db.goods_name


# 对html进行分类
# noinspection SpellCheckingInspection,PyTypeChecker
def j_parse(html_json):
    html_dist = json.loads(html_json)
    url = html_dist['url']
    html = html_dist['html']
    key_word = html_dist['key_word']
    # key_word_item = False
    if re.compile(r'https://www.jd.com/allSort.aspx').match(url) is not None:  # 全部分类页
        href_list = all_subject_parse(html, key_word)
    elif re.compile(r'https://list.jd.com/list.html\?cat=').match(url) is not None:  # 商品列表页
        # key_word_item,
        href_list = list_parse(html, key_word)
    # elif re.compile(r'https://item.jd.com/').match(url) is not None:
    #     href_list = item_parse(html, key_word)
        # , html_dist['key_word_item']
    else:
        href_list = common_parse(html, key_word)  # 其他页
    # key_word_item,
    return url, key_word, href_list


# 普通网页的解析
def common_parse(html, key_word):
    print 'common'
    page = etree.HTML(html)
    href_list = []
    if key_word is not None and key_word != '':  # 对关键字的判断
        key_words = page.xpath(u"/html/head/meta")
        for kws in key_words:
            kw = kws.xpath(u"./@name")
            if len(kw) > 0 and kw[0] == 'keywords':
                content = kws.xpath(u"./@content")
                print content[0].replace('、', '')
                if len(content) > 0 and key_word in content[0].replace('、', ''):
                    all_href_list = page.xpath(u"//a/@href")  # 所有的连接
                    for href in all_href_list:
                        if re.compile(r'/list.jd.com/list.html\?cat=').search(href) is not None:
                            href_list.append(href)
                    if len(href_list) > 0:
                        return href_list
        return []
    else:
        all_href_list = page.xpath(u"//a/@href")  # 所有的连接
        for href in all_href_list:
            if re.compile(r'/list.jd.com/list.html\?cat=').search(href) is not None:
                href_list.append(href)
        if len(href_list) > 0:
            return href_list
        return []


# 全部商品分类页的解析
def all_subject_parse(html, key_word):
    print 'all_subject'
    page = etree.HTML(html)
    if key_word is not None and key_word != '':  # 对关键字的判断
        # 一级分类
        content = page.xpath(u"//div[@class='category-item m']/div[@class='mt']/h2[@class='item-title']/span")
        for c in content:
            if key_word in c.text.replace('、', ''):
                parent_node = c.xpath(u"../../../div[@class='mc']/div[@class='items']")
                if len(parent_node) > 0:
                    href_list = parent_node[0].xpath(u".//a/@href")
                    if len(href_list) > 0:
                        return href_list
                    return []
        print '关键字不准确,抓取可能不准确'
    href_list = page.xpath(u"//div[@class='category-item m']//a/@href")
    if len(href_list) > 0:
        return href_list
    return []


# 商品列表解析
def list_parse(html, key_word):
    print 'list'
    href_list = []
    page = etree.HTML(html)
    # key_word_item = True
    if key_word is not None and key_word != '':
        key_words = page.xpath(u"/html/head/meta")
        for kws in key_words:
            kw = kws.xpath(u"./@name")
            if len(kw) > 0 and kw[0].lower() == 'keywords'.lower():
                # if kw[0] == 'keywords':
                content = kws.xpath(u"./@content")
                print content[0].replace('、', '')
                if len(content) > 0 and key_word in content[0].replace('、', ''):
                    list_extract(page)
                    # href_list = page.xpath(u"//li[@class='gl-item']/div[@class='gl-i-wrap j-sku-item']"
                    #                        u"/div[@class='p-name']/a/@href")
                    # 下一页
                    next_page = page.xpath(u"//div[@class='p-wrap']/span[@class='p-num']/a[@class='pn-next']/@href")
                    if len(next_page) > 0:
                        href_list.append(next_page[0])
                    # if len(href_list) > 0:
                    #     key_word_item,
                        return href_list
        # key_word_item,
        return []
    else:
        list_extract(page)
        # href_list = page.xpath(u"//li[@class='gl-item']/div[@class='gl-i-wrap j-sku-item']"
        #                        u"/div[@class='p-name']/a/@href")
        # 下一页
        next_page = page.xpath(u"//div[@class='p-wrap']/span[@class='p-num']/a[@class='pn-next']/@href")
        if len(next_page) > 0:
            href_list.append(next_page[0])
        # if len(href_list) > 0:
        #     key_word_item,
            return href_list
        # key_word_item,
        return []


# def item_parse(html, key_word):
#     print 'item'
#     page = etree.HTML(html)
#     if key_word is not None and key_word != '':
#         key_words = page.xpath(u"/html/head/meta")
#         for kws in key_words:
#             kw = kws.xpath(u"./@name")
#             if len(kw) > 0 and kw[0] == 'keywords':
#                 content = kws.xpath(u"./@content")
#                 print content[0].replace('、', '')
#                 if len(content) > 0 and key_word in content[0].replace('、', ''):
#                     item_extract(page)
#     else:
#         item_extract(page)
#     return ''


# 商品列表解析
# noinspection SpellCheckingInspection
def list_extract(page):
    goods_map = {}
    content = page.xpath(u"//li[@class='gl-item']/div[@class='gl-i-wrap j-sku-item']")
    for c in content:
        goods_id = c.attrib['data-sku']  # 商品id
        goods_map[goods_id] = {}
        # r1.lpush('id', goods_id)
        goods_name_list = c.xpath(u"div[@class='p-name']/a/em/text()")  # 商品名
        # goods_shop_list = c.xpath(u"div[@class='p-shop hide']/span/a/text()")
        if len(goods_name_list) > 0:
            goods_map[goods_id]['goods_name'] = goods_name_list[0]
            # r0.hset('goods_name', goods_id, goods_name)
        # if len(goods_shop_list) > 0:
        #     goods_shop = goods_shop_list[0]
        #     print goods_shop
        #     r0.hset(goods_id, 'goods_shop', goods_shop)
    joint_url(goods_map.keys())
    goods_name.save(goods_map)


# 店铺、价格、评论json请求的拼接
# noinspection SpellCheckingInspection
def joint_url(id_list):
    if len(id_list) > 0:
        base_shop_url = '&pidList=' + id_list[0]
        base_price_url = '&skuIds=J_' + id_list[0]
        base_comment_url = '&referenceIds=' + id_list[0]
        # base_news_url = '&skuids=AD_' + url_list[0]
        for i in range(1, len(id_list)):
            base_shop_url += ',' + id_list[i]
            base_price_url += '%2CJ_' + id_list[i]
            base_comment_url += ',' + id_list[i]
            # base_news_url += ',AD_' + url_list[i]
        shop_url = 'https://chat1.jd.com/api/checkChat?my=list' + base_shop_url
        price_url = 'https://p.3.cn/prices/mgets?type=1&area=1_72_2799_0' + base_price_url + \
                    '&pdbp=0&pdtk=&pdpin=&pduid=1375019140&source=list_pc_front'
        comment_url = 'https://club.jd.com/clubservice.aspx?method=GetCommentsCount' + base_comment_url
        # news_url = 'https://ad.3.cn/ads/mgets?&my=list_adWords&source=JDList' + base_news_url
        # 用redis缓存
        r0.lpush('shop_url', shop_url)
        r1.lpush('price_url', price_url)
        r2.lpush('comment_url', comment_url)
    else:
        print 'id_list为空'


# def item_extract(page):
#     # goods_map = {}
#     goods_ids = page.xpath(u"//div[@id='preview']/div[@class='preview-info']//a[@class='follow J-follow']/@data-id")
#     if len(goods_ids) > 0:
#         goods_id = goods_ids[0]
#         r1.lpush('id', goods_id)
#         # goods_id_list.append(goods_id)
#         # goods_shops = page.xpath(u"//div[@id='crumb-wrap']//div[@class='J-hove-wrap EDropdown fr']"
#         #                          u"//div[@class='name']/a/text()")
#         goods_names = page.xpath(u"//div[@class='product-intro clearfix']/div[@class='itemInfo-wrap']/"
#                                  u"div[@class='sku-name']/text()")
#         # goods_map[goods_id] = {}
#         # if len(goods_shops) > 0:
#         #     goods_shop = goods_shops[0]
#         # goods_map[goods_id]['goods_shop'] = goods_shop
#         # r0.hset(goods_id, 'goods_shop', goods_shop)
#         if len(goods_names) > 0:
#             goods_name = goods_names[0]
#             # goods_map[goods_id]['goods_name'] = goods_name
#             r0.hset('goods_name', goods_id, goods_name)
#         # print goods_map
