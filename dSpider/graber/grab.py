# -*- coding: utf-8 -*-
import requests
import sys
import json
import time

reload(sys)
# noinspection PyUnresolvedReferences
sys.setdefaultencoding('utf-8')
# 设置请求头
user_agent = 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:52.0) Gecko/20100101 Firefox/52.0'
accept = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
accept_language = 'zh-CN, zh;q=0.8, en-US;q=0.5, en;q=0.3'
cookies = 't=17ba61beaecdf40bc32639c9f3c46579; thw=cn; cna=nuZTEV0BewkCAXWIXFi7J1K3; ' \
          'l=Anx8iPPEdkBU4HVRva7h9vG-TBAvmCCC; isg=Avb2Hb0zsxS9ykZ-U2teNGV0Riw4vxHnndnV9mDf4l' \
          'l0o5Y9yKeKYVxRzUQx; _med=dw:1366&dh:768&pw:1366&ph:768&ist:0; um=55F3A8BFC9C50DDA' \
          '43689C285106586D8CE93761304FB310A881A5081A6FAC78563CA893297BDA23CD43AD3E795C914CE20E49' \
          '05B080F976571C5FD35B541744; cookie2=1cfe4043850594f5969b51d33eeeca0b; v=0'
headers = {'User-Agent': user_agent, 'Accept': accept, 'Accept-Language': accept_language, 'Cookie': cookies}
session = requests.Session()


# 普通url的抓取
# noinspection PyStatementEffect
def url_grab(url_json, num_retries=2):  # 设置两次重新抓取机会
    url_dist = json.loads(url_json)
    url = url_dist['url']
    print url
    html_json = None
    time.sleep(1)
    try:
        request = session.get(url, headers=headers)
        html = requests.utils.get_unicode_from_response(request)
        html_dist = url_dist
        html_dist['html'] = html
        html_json = json.dumps(html_dist)
    except Exception as e:
        print e.message
        if num_retries > 0:
            time.sleep(2)
            url_grab(url_json, num_retries - 1)
        else:
            time.sleep(30)
    return html_json


# 对json请求进行抓取
def special_url_grab(special_url_json, num_retries=2):  # 设置两次重新抓取机会
    special_url_dist = json.loads(special_url_json)
    special_url = special_url_dist['special_url']
    print special_url
    json_json = None
    time.sleep(1)
    try:
        request = session.get(special_url, headers=headers)
        json_dist = special_url_dist
        # json_dist['json'] = json.loads(re.compile(r'(\[).*(\])').search(request.content).group())
        json_dist['json'] = requests.utils.get_unicode_from_response(request)
        # json_dist['json'] = json.loads(re.compile(r'{.*}').search(request.content).group())
        json_json = json.dumps(json_dist)
    except Exception as e:
        print e.message
        if num_retries > 0:
            time.sleep(2)
            special_url_grab(special_url_json, num_retries - 1)
        else:
            time.sleep(30)
    return json_json
