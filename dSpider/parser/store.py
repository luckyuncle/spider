# -*- coding: utf-8 -*-
import json
from redis import Redis
import time
import re
from pymongo import MongoClient

r0 = Redis(host='127.0.0.1', port=6379, db=0)
r1 = Redis(host='127.0.0.1', port=6379, db=2)
r2 = Redis(host='127.0.0.1', port=6379, db=3)
connection = MongoClient('127.0.0.1', 27017)
j_db = connection.jingdong
t_db = connection.taobao
s_db = connection.suning
command = ''


# 处理不同的json请求
# noinspection SpellCheckingInspection
def handle(spider_type, json_json):
    json_dist = json.loads(json_json)
    special_url = json_dist['special_url']
    if spider_type == 'taobao':
        json_dist['json'] = json.loads(re.compile(r'{.*}').search(json_dist['json']).group())
        if re.compile(r'https://rate.taobao.com/detailCommon.htm\?auctionNumId=').match(special_url) is not None:
            t_comment_handle(json_dist['json'], special_url)
    elif spider_type == 'jingdong':
        json_dist['json'] = json.loads(re.compile(r'(\[).*(\])').search(json_dist['json']).group())
        if re.compile(r'https://p.3.cn/prices/').match(special_url) is not None:
            j_price_handle(json_dist['json'])
        elif re.compile(r'https://club.jd.com/clubservice.aspx\?method=GetCommentsCount')\
                .match(special_url) is not None:
            j_comment_handle(json_dist['json'])
        # elif re.compile(r'https://ad.3.cn/ads/').match(special_url) is not None:
        #     news_handle(json_dist['json'])
        elif re.compile(r'https://chat1.jd.com/api/checkChat\?my=list').match(special_url) is not None:
            j_shop_handle(json_dist['json'])
        else:
            pass
    elif spider_type == 'suning':
        json_dist['json'] = json.loads(re.compile(r'{.*}').search(json_dist['json']).group())
        if re.compile(r'http://ds.suning.cn/ds/generalForTile/').match(special_url) is not None:
            s_price_handle(json_dist['json'])
        elif re.compile(r'http://review.suning.com/ajax/review_satisfy/').match(special_url) is not None:
            s_comment_handle(json_dist['json'], special_url)
        else:
            pass


# 苏宁价格json请求的处理
# noinspection SpellCheckingInspection
def s_price_handle(price_json):
    goods_map = {}
    price_json = price_json['rs'][0]
    goods_id = price_json['cmmdtyCode'][9:]
    goods_map[goods_id] = {}
    if 'price' in price_json.keys():
        goods_map[goods_id]['price'] = price_json['price']
    if 'snPrice' in price_json.keys():
        goods_map[goods_id]['snPrice'] = price_json['snPrice']
    if 'refPrice' in price_json.keys():
        goods_map[goods_id]['refPrice'] = price_json['refPrice']
    if 'originalPrice' in price_json.keys():
        goods_map[goods_id]['originalPrice'] = price_json['originalPrice']
    s_goods_price = s_db.goods_price
    s_goods_price.save(goods_map)


# 苏宁评论json请求的处理
def s_comment_handle(comment_json, special_url):
    goods_map = {}
    goods_id = special_url.split('-')[1][9:]
    goods_map[goods_id] = {}
    comment_json = comment_json['reviewCounts'][0]
    goods_map[goods_id]['oneStarCount'] = comment_json['oneStarCount']
    goods_map[goods_id]['twoStarCount'] = comment_json['twoStarCount']
    goods_map[goods_id]['threeStarCount'] = comment_json['threeStarCount']
    goods_map[goods_id]['fourStarCount'] = comment_json['fourStarCount']
    goods_map[goods_id]['fiveStarCount'] = comment_json['fiveStarCount']
    goods_map[goods_id]['againCount'] = comment_json['againCount']
    goods_map[goods_id]['CommentCount'] = comment_json['totalCount']
    goods_map[goods_id]['picFlagCount'] = comment_json['picFlagCount']
    goods_map[goods_id]['qualityStar'] = comment_json['qualityStar']
    goods_map[goods_id]['installCount'] = comment_json['installCount']
    goods_map[goods_id]['smallVideoCount'] = comment_json['smallVideoCount']
    goods_map[goods_id]['GoodCount'] = comment_json['fourStarCount'] + comment_json['fiveStarCount']
    goods_map[goods_id]['GeneralCount'] = comment_json['twoStarCount'] + comment_json['threeStarCount']
    goods_map[goods_id]['PoorCount'] = comment_json['oneStarCount']
    s_goods_comment = s_db.goods_comment
    s_goods_comment.save(goods_map)


# 淘宝评论json请求的处理
# noinspection SpellCheckingInspection
def t_comment_handle(comment_json, special_url):
    goods_map = {}
    goods_id = special_url[special_url.find('?') + 1:special_url.find('&')].split('=')[1]
    goods_map[goods_id] = {}
    if 'correspond' in comment_json['data']:
        goods_map[goods_id]['correspond'] = comment_json['data']['correspond']
    comment_json = comment_json['data']['count']
    if 'total' in comment_json.keys():
        goods_map[goods_id]['CommentCount'] = comment_json['total']
    if 'tryReport' in comment_json.keys():
        goods_map[goods_id]['tryReport'] = comment_json['tryReport']
    if 'additional' in comment_json.keys():
        goods_map[goods_id]['additional'] = comment_json['additional']
    if 'normal' in comment_json.keys():
        goods_map[goods_id]['normal'] = comment_json['normal']
    if 'hascontent' in comment_json.keys():
        goods_map[goods_id]['hascontent'] = comment_json['hascontent']
    if 'good' in comment_json.keys():
        goods_map[goods_id]['GoodCount'] = comment_json['good']
    if 'pic' in comment_json.keys():
        goods_map[goods_id]['GeneralCount'] = comment_json['pic']
    if 'bad' in comment_json.keys():
        goods_map[goods_id]['PoorCount'] = comment_json['bad']
    t_goods_comment = t_db.goods_comment
    t_goods_comment.save(goods_map)


# 京东店铺json请求的处理
def j_shop_handle(shop_json):
    goods_map = {}
    for shop in shop_json:
        goods_id = str(shop['pid'])
        goods_map[goods_id] = {}
        if 'shopId' in shop.keys():
            goods_map[goods_id]['id'] = shop['shopId']
        if 'seller' in shop.keys():
            goods_map[goods_id]['name'] = shop['seller']
        else:
            goods_map[goods_id]['name'] = '京东自营'
    j_goods_shop = j_db.goods_shop
    j_goods_shop.save(goods_map)


# 京东价格json请求的处理
def j_price_handle(price_json):
    goods_map = {}
    for price in price_json:
        goods_id = str(price['id'])[2:]
        goods_map[goods_id] = {}
        if 'p' in price.keys():
            goods_map[goods_id]['price'] = price['p']
        if 'm' in price.keys():
            goods_map[goods_id]['m_price'] = price['m']
        if 'op' in price.keys():
            goods_map[goods_id]['op_price'] = price['op']
    j_goods_price = j_db.goods_price
    j_goods_price.save(goods_map)


# 京东评论json请求的处理
def j_comment_handle(comment_json):
    # ['CommentsCount']
    goods_map = {}
    for comment in comment_json:
        goods_id = str(comment['ProductId'])
        goods_map[goods_id] = {}
        if 'CommentCount' in comment.keys():
            goods_map[goods_id]['CommentCount'] = comment['CommentCount']
        if 'Score1Count' in comment.keys():
            goods_map[goods_id]['Score1Count'] = comment['Score1Count']
        if 'Score2Count' in comment.keys():
            goods_map[goods_id]['Score2Count'] = comment['Score2Count']
        if 'Score3Count' in comment.keys():
            goods_map[goods_id]['Score3Count'] = comment['Score3Count']
        if 'Score4Count' in comment.keys():
            goods_map[goods_id]['Score4Count'] = comment['Score4Count']
        if 'Score5Count' in comment.keys():
            goods_map[goods_id]['Score5Count'] = comment['Score5Count']
        if 'AverageScore' in comment.keys():
            goods_map[goods_id]['AverageScore'] = comment['AverageScore']
        if 'ShowCount' in comment.keys():
            goods_map[goods_id]['ShowCount'] = comment['ShowCount']
        if 'GoodCount' in comment.keys():
            goods_map[goods_id]['GoodCount'] = comment['GoodCount']
        if 'GoodRate' in comment.keys():
            goods_map[goods_id]['GoodRate'] = comment['GoodRate']
        if 'GeneralCount' in comment.keys():
            goods_map[goods_id]['GeneralCount'] = comment['GeneralCount']
        if 'GeneralRate' in comment.keys():
            goods_map[goods_id]['GeneralRate'] = comment['GeneralRate']
        if 'PoorCount' in comment.keys():
            goods_map[goods_id]['PoorCount'] = comment['PoorCount']
        if 'PoorRate' in comment.keys():
            goods_map[goods_id]['PoorRate'] = comment['PoorRate']
        if 'AfterCount' in comment.keys():
            goods_map[goods_id]['AfterCount'] = comment['AfterCount']
    j_goods_comment = j_db.goods_comment
    j_goods_comment.save(goods_map)


# def news_handle(news_json):
#     for news in news_json:
#         r0.hset(news['id'][3:], 'goods_news', news['ad'])


# 向json队列中添加json请求
# noinspection SpellCheckingInspection
def json_request(spider_type, network_special_url_queue):
    while True:
        if command == 'end':
            if spider_type == 'jingdong':
                shop_url_list = []
                price_url_list = []
                comment_url_list = []
                j_url_not_grab = j_db.url_not_grab
                while r0.llen('shop_url') > 0:
                    shop_url = r0.lpop('shop_url')
                    shop_url_list.append(shop_url)
                j_url_not_grab.save({'shop_url': shop_url_list})
                while r1.llen('price_url') > 0:
                    price_url = r1.lpop('price_url')
                    price_url_list.append(price_url)
                j_url_not_grab.save({'price_url': price_url_list})
                while r2.llen('comment_url') > 0:
                    comment_url = r2.lpop('comment_url')
                    comment_url_list.append(comment_url)
                j_url_not_grab.save({'comment_url': comment_url_list})
                # goods_id_list = []
                # goods_map = {}
                # while r1.llen('id') > 0:
                #     goods_id = r1.rpop('id')
                #     goods_id_list.append(goods_id)
                # j_goods_id_not_grab = j_db.goods_id_not_grab
                # j_goods_id_not_grab.save({'id': goods_id_list})
                # for key in r0.hkeys('goods_name'):
                #     goods_map[key] = r0.hget('goods_name', key)
                #     r0.hdel('goods_name', key)
                # if len(goods_map.keys()) > 0:
                #     j_goods_name = j_db.goods_name
                #     j_goods_name.save(goods_map)
            elif spider_type == 'taobao':
                comment_url_list = []
                while r1.llen('json_url') > 0:
                    comment_url = r1.lpop('json_url')
                    comment_url_list.append(comment_url)
                t_url_not_grab = t_db.url_not_grab
                t_url_not_grab.save({'comment_url': comment_url_list})
            elif spider_type == 'suning':
                price_url_list = []
                comment_url_list = []
                s_url_not_grab = s_db.url_not_grab
                while r0.llen('price_url') > 0:
                    price_url = r0.lpop('price_url')
                    price_url_list.append(price_url)
                s_url_not_grab.save({'price_url': price_url_list})
                while r1.llen('comment_url') > 0:
                    comment_url = r0.lpop('comment_url')
                    comment_url_list.append(comment_url)
                s_url_not_grab.save({'comment_url': comment_url_list})
            else:
                pass
            connection.close()
            break
        if spider_type == 'taobao':
            if r1.llen('json_url') > 0:
                comment_url = r1.rpop('json_url')
                special_url_dist = {
                    'special_url': comment_url
                }
                network_special_url_queue.put(json.dumps(special_url_dist))
            else:
                time.sleep(1)
        elif spider_type == 'jingdong':
            if r0.llen('shop_url') > 0:
                shop_url = r0.rpop('shop_url')
                special_url_dist = {
                    'special_url': shop_url
                }
                network_special_url_queue.put(json.dumps(special_url_dist))
            if r1.llen('price_url') > 0:
                price_url = r1.rpop('price_url')
                special_url_dist = {
                    'special_url': price_url
                }
                network_special_url_queue.put(json.dumps(special_url_dist))
            if r2.llen('comment_url') > 0:
                comment_url = r2.rpop('comment_url')
                special_url_dist = {
                    'special_url': comment_url
                }
                network_special_url_queue.put(json.dumps(special_url_dist))
            if r0.llen('shop_url') == 0 and r1.llen('price_url') == 0 and r2.llen('comment_url') == 0:
                time.sleep(1)
            # if r1.llen('id') > 50:
            #     goods_id = r1.rpop('id')
            #     base_shop_url = '&pidList=' + goods_id
            #     base_price_url = '&skuIds=J_' + goods_id
            #     base_comment_url = '&referenceIds=' + goods_id
            #     # base_news_url = '&skuids=AD_' + goods_id
            #     for i in range(2, 50):
            #         goods_id = r1.rpop('id')
            #         base_shop_url += ',' + goods_id
            #         base_price_url += '%2CJ_' + goods_id
            #         base_comment_url += ',' + goods_id
            #         # base_news_url += ',AD_' + goods_id
            #     shop_url = 'https://chat1.jd.com/api/checkChat?my=list' + base_shop_url
            #     price_url = 'https://p.3.cn/prices/mgets?type=1&area=1_72_2799_0' + base_price_url +\
            #                 '&pdbp=0&pdtk=&pdpin=&pduid=1375019140&source=list_pc_front'
            #     comment_url = 'https://club.jd.com/clubservice.aspx?method=GetCommentsCount' + base_comment_url
            #     # news_url = 'https://ad.3.cn/ads/mgets?&my=list_adWords&source=JDList' + base_news_url
            #     shop_url_dist = {
            #         'special_url': shop_url
            #     }
            #     price_url_dist = {
            #         'special_url': price_url
            #     }
            #     comment_url_dist = {
            #         'special_url': comment_url
            #     }
            #     # news_url_dist = {
            #     #     'special_url': news_url
            #     # }
            #     network_special_url_queue.put(json.dumps(shop_url_dist))
            #     network_special_url_queue.put(json.dumps(price_url_dist))
            #     network_special_url_queue.put(json.dumps(comment_url_dist))
            #     # network_special_url_queue.put(json.dumps(news_url_dist))
            # else:
            #     time.sleep(1)
        elif spider_type == 'suning':
            if r0.llen('price_url') > 0:
                price_url = r0.rpop('price_url')
                special_url_dist = {
                    'special_url': price_url
                }
                network_special_url_queue.put(json.dumps(special_url_dist))
            if r1.llen('comment_url') > 0:
                comment_url = r1.rpop('comment_url')
                special_url_dist = {
                    'special_url': comment_url
                }
                network_special_url_queue.put(json.dumps(special_url_dist))
            if r0.llen('price_url') == 0 and r1.llen('comment_url') == 0:
                time.sleep(1)
        else:
            pass


# # noinspection SpellCheckingInspection
# def store(spider_type):
#     while True:
#         if spider_type == 'jingdong':
#             if command == 'end':
#                 # goods_id_list = []
#                 # goods_map = {}
#                 # while r1.llen('id') > 0:
#                 #     goods_id = r1.rpop('id')
#                 #     goods_id_list.append(goods_id)
#                 # j_goods_id_not_grab = j_db.goods_id_not_grab
#                 # j_goods_id_not_grab.save({'id': goods_id_list})
#                 # for key in r0.hkeys('goods_name'):
#                 #     goods_map[key] = r0.hget('goods_name', key)
#                 #     r0.hdel('goods_name', key)
#                 # if len(goods_map.keys()) > 0:
#                 #     j_goods_name = j_db.goods_name
#                 #     j_goods_name.save(goods_map)
#                 # connection.close()
#                 break
#             if r0.hlen('goods_name') > 50:
#                 goods_map = {}
#                 for key in r0.hkeys('goods_name'):
#                     goods_map[key] = r0.hget('goods_name', key)
#                     r0.hdel('goods_name', key)
#                 j_goods_name = j_db.goods_name
#                 j_goods_name.save(goods_map)
#             else:
#                 time.sleep(2)
#         else:
#             break
