# import libraries
import os
from bs4 import BeautifulSoup
import requests
import httpx
import re
import pandas as pd
import datetime
import numpy as np
import akshare as ak
from utils import *
import json
import math as m
from io import StringIO, BytesIO
import dateutil.relativedelta
from akshare.futures import cons, receipt
import warnings
from akshare.option.cons import (
    get_calendar,
    convert_date,
    DCE_DAILY_OPTION_URL,
    SHFE_OPTION_URL,
    CZCE_DAILY_OPTION_URL_3,
    SHFE_HEADERS,
)
import execjs
from position import *
from cn_fut_opt import *
from selenium import webdriver
import brotli

# url = 'http://www.cffex.com.cn/sj/hqsj/rtj/202307/07/20230707_1.csv'
# r = requests.get(url)
# table_df = pd.read_csv(BytesIO(r.content), encoding='gb2312')
# table_df["合约代码"] = table_df["合约代码"].str.strip()
# table_df.rename(columns={'今开盘':'开盘价', '今收盘':'收盘价'}, inplace=True)
# print(table_df)


# df=pd.DataFrame(columns=['a','b',], data=[['1-',2],['1-11',2],['11111',2],['111---1111',2]])
# df['a'] = df['a'].replace('1-',None)
# df.drop(index=2, inplace=True)
# print(df)
# df.reset_index(drop=True, inplace=True)
# # print(df)

# def div2(directory):
#     cs = ['index','c1','c2','c3','c4','c5','c6','c7','c8','c9']
#     for _, _, files in os.walk(directory):
#         for file in files:
#             if (not('_spot' in file)) and (not('_stock' in file)):
#                 print(file)
#                 path = os.path.join(directory, file)
#                 fut_df = pd.read_csv(path, header=[0,1])
#                 t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
#                 w = np.where(t < pd.to_datetime('2020-01-01', format='%Y-%m-%d'))[0]
#                 if (len(w) > 0):
#                     for c in cs:
#                         fut_df.loc[w, pd.IndexSlice[c, 'vol']] = fut_df.loc[w, pd.IndexSlice[c, 'vol']] / 2
#                         fut_df.loc[w, pd.IndexSlice[c, 'oi']] = fut_df.loc[w, pd.IndexSlice[c, 'oi']] / 2

#                     fut_df.to_csv(path, encoding='utf-8', index=False)

# path = os.path.join(future_price_dir, 'czce')
# div2(path)

# d = ak.get_shfe_rank_table(date='20230710')
# print(list(d.keys()))

# headers = {
#     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
#     "Accept-Encoding": "gzip, deflate",
#     "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
#     "Cache-Control": "no-cache",
#     "Host": "www.pbc.gov.cn",
#     'Upgrade-Insecure-Requests': '1',
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
# }

# url = 'http://www.pbc.gov.cn/diaochatongjisi/resource/cms/2023/06/2023061518095642554.htm'
# se = requests.session()
# r = se.get(url, headers=headers)
# s=r.content.decode('utf-8')

# soup = bs4.BeautifulSoup(s, 'html.parser')
# js_code = soup.select('script')[0].string
# js_code = re.sub(r'atob\(', 'window["atob"](', js_code)
# js_fn = 'function getURL(){ var window = {};' + js_code + 'return window["location"];}'
# ctx = execjs.compile(js_fn)
# tail = ctx.call('getURL')
# print(tail)
# url = 'http://www.pbc.gov.cn' + tail
# r = se.get(url, headers=headers)

# s = (r.content.decode('gbk'))
# soup = bs4.BeautifulSoup(s, 'html.parser')
# trs = soup.find_all('tr', attrs={'height':26})
# for tr in trs:
#     tds = tr.find_all('td')
#     for td in tds:
#         print(td.get_text().strip())

# futures_hog_info_df = ak.futures_hog_info(symbol="二元母猪价格")
# print(futures_hog_info_df)

# futures_hog_info_df = ak.futures_hog_info(symbol="猪企销售简报-销售量")
# print(futures_hog_info_df)

# futures_hog_info_df = ak.futures_hog_info(symbol="白条肉")
# print(futures_hog_info_df)


# [{"110000":"北京"},{"120000":"天津"},{"130100":"石家庄"},{"130200":"唐山"},{"130300":"秦皇岛"},{"140100":"太原"},{"150100":"呼和浩特"},{"150200":"包头"},{"210100":"沈阳"},{"210200":"大连"},{"210600":"丹东"},{"210700":"锦州"},{"220100":"长春"},{"220200":"吉林"},{"230100":"哈尔滨"},{"231000":"牡丹江"},{"310000":"上海"},{"320100":"南京"},{"320200":"无锡"},{"320300":"徐州"},{"321000":"扬州"},{"330100":"杭州"},{"330200":"宁波"},{"330300":"温州"},{"330700":"金华"},{"340100":"合肥"},{"340300":"蚌埠"},{"340800":"安庆"},{"350100":"福州"},{"350200":"厦门"},{"350500":"泉州"},{"360100":"南昌"},{"360400":"九江"},{"360700":"赣州"},{"370100":"济南"},{"370200":"青岛"},{"370600":"烟台"},{"370800":"济宁"},{"410100":"郑州"},{"410300":"洛阳"},{"410400":"平顶山"},{"420100":"武汉"},{"420500":"宜昌"},{"420600":"襄阳"},{"430100":"长沙"},{"430600":"岳阳"},{"430700":"常德"},{"440100":"广州"},{"440200":"韶关"},{"440300":"深圳"},{"440800":"湛江"},{"441300":"惠州"},{"450100":"南宁"},{"450300":"桂林"},{"450500":"北海"},{"460100":"海口"},{"460200":"三亚"},{"500000":"重庆"},{"510100":"成都"},{"510500":"泸州"},{"511300":"南充"},{"520100":"贵阳"},{"520300":"遵义"},{"530100":"昆明"},{"532900":"大理"},{"540100":"拉萨"},{"610100":"西安"},{"620100":"兰州"},{"630100":"西宁"},{"640100":"银川"},{"650100":"乌鲁木齐"}]

# path = os.path.join(data_dir, '36个城市居民消费价格分类指数-2016'+'.csv') 
# df1 = pd.read_csv(path, header=[0,1])

# path = os.path.join(data_dir, '36个城市居民消费价格分类指数2016-'+'.csv') 
# df2 = pd.read_csv(path, header=[0,1])

# df = pd.concat([df1, df2], axis=0)
# path = os.path.join(data_dir, '36个城市居民消费价格分类指数'+'.csv') 
# df.to_csv(path, encoding='utf-8', index=False)



# compare_two_future_data('a', 'b', start_time='2015-01-01')



# url1 = 'https://www.shfe.com.cn/statements/delaymarket_cuOption.html'



# plot_term_structure('czce', 'PK')


def add_dom_data():
    for exchange in ['shfe', 'dce', 'czce', 'gfex', 'cffex']:
        directory = os.path.join(future_price_dir, exchange)
        cs = ['c1','c2','c3','c4','c5','c6','c7','c8','c9']
        for _, _, files in os.walk(directory):
            for file in files:
                if (('spot' in file)) or (('stock' in file)) or (('intraday' in file)) or (('tas' in file)):
                    continue

                path = os.path.join(directory, file)
                print(path)
                df = pd.read_csv(path, header=[0,1])
                for i in range(len(df)):
                    idx = df.loc[i, pd.IndexSlice[cs, 'vol']].idxmax()
                    df.loc[i, pd.IndexSlice['dom', 'inst_id']] = df.loc[i, pd.IndexSlice[idx[0], 'inst_id']]
                    df.loc[i, pd.IndexSlice['dom', 'open']] = df.loc[i, pd.IndexSlice[idx[0], 'open']]
                    df.loc[i, pd.IndexSlice['dom', 'high']] = df.loc[i, pd.IndexSlice[idx[0], 'high']]
                    df.loc[i, pd.IndexSlice['dom', 'low']] = df.loc[i, pd.IndexSlice[idx[0], 'low']]
                    df.loc[i, pd.IndexSlice['dom', 'close']] = df.loc[i, pd.IndexSlice[idx[0], 'close']]
                    df.loc[i, pd.IndexSlice['dom', 'vol']] = df.loc[i, pd.IndexSlice[idx[0], 'vol']]
                    df.loc[i, pd.IndexSlice['dom', 'oi']] = df.loc[i, pd.IndexSlice[idx[0], 'oi']]
                    df.loc[i, pd.IndexSlice['dom', 'settle']] = df.loc[i, pd.IndexSlice[idx[0], 'settle']]

                df.to_csv(path, encoding='utf-8', index=False)


# add_dom_data()

# path = os.path.join(data_dir, 'SP500 EARNING YIELD.csv')
# df = pd.read_csv(path)
# df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
# df.sort_values(by = 'time', inplace=True)
# df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
# t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
# ey = np.array(df['data'], dtype=float)


# path = os.path.join(data_dir, '国债收益率.csv')
# df = pd.read_csv(path)
# t2 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
# us10y = np.array(df['美国国债收益率10年'], dtype=float)

# t3, diff = data_sub(t, ey, t2, us10y)
# datas = [
#         [[[t3, diff,'SP500 EARNING YIELD - US10Y','color=red'], ],
#         [],''],
# ]
# plot_many_figure(datas, max_height=600)

# option_sse_codes_sina_df = ak.option_sse_codes_sina(trade_date="202310", underlying="510300")
# print(option_sse_codes_sina_df)

# option_sse_spot_price_sina_df = ak.option_sse_spot_price_sina(symbol="10005933")
# print(option_sse_spot_price_sina_df)


# def get_libor_data():
#     df = ak.rate_interbank(market="伦敦银行同业拆借市场", symbol="Libor美元", indicator="3月")
#     df.rename(columns={"报告日":"time", '利率':'3M'}, inplace=True)
#     path = os.path.join(data_dir, 'libor.csv')
#     df.to_csv(path, encoding='utf-8', index=False)


# sss = [
    
#     # '',
# ]

# df = pd.DataFrame()
# for s in sss:
#     d = json.loads(s)
#     _df = pd.DataFrame(d)
#     _df['t'] = _df['t'].apply(lambda x:datetime.datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S"))
#     _df = _df[['t','o','h','l','c']]
#     _df.rename(columns={"t":"time", 'o':'open', 'h':'high', 'l':'low', 'c':'close'}, inplace=True)
#     df = pd.concat([df, _df], axis=0)

# path = os.path.join(cfd_dir, '???_intraday'+'.csv')
# df.drop_duplicates(subset=['time'], keep='last', inplace=True)
# df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
# df.sort_values(by = 'time', inplace=True)
# df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
# df['open'] = df['open'].apply(lambda x:round(x,4))
# df['high'] = df['high'].apply(lambda x:round(x,4))
# df['low'] = df['low'].apply(lambda x:round(x,4))
# df['close'] = df['close'].apply(lambda x:round(x,4))

# df.to_csv(path, encoding='utf-8', index=False)
                


# # # driver_fut.close()
# se = requests.session()
# headers = {
#     "Accept": "application/json, text/plain, */*",
#     "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
#     "Accept-Encoding": "gzip, deflate, br",
#     "Cache-Control": "no-cache",
#     "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
#     "Host": "www.dce.com.cn",
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
# }

# # # url = 'http://www.dce.com.cn/ptusuw4InS1g/Rgvf1lnixM9d.ceef71d.js'
# # # r = se.get(url)
# # # js1 = r.text
# # # # print(r.text)
# # print(c)
# url = 'http://www.dce.com.cn/webquote/futures_quote_ajax?varietyid='+'a'
# headers['Cookie'] = c
# r = se.get(url, headers=headers)
# print(r.text)
# s=r.content.decode('utf-8')
# print(s)
# soup = bs4.BeautifulSoup(s, 'html.parser')
# js_code1 = soup.select('script')[0].string
# js_code1 = re.sub(r'atob\(', 'window["atob"](', js_code1)
# # js_code2 = soup.find_all(name='script')[2].get_text().split('(')[0]
# # print(js_code2)
# js_fn = 'function getURL(){ var window = {};var document = {};' + js_code1 + js1 +'}'
# # js_fn = 'var window = {}; var document = {};' + js_code1 + js1

# ctx = execjs.compile(js_fn)
# tail = ctx.call('getURL')

# print(tail)

# path = os.path.join(future_price_dir, 'shfe', 'au'+'.csv')
# fut_df1 = pd.read_csv(path, header=[0,1])
# t1 = pd.DatetimeIndex(pd.to_datetime(fut_df1['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
# data1 = np.array(fut_df1['index']['close'], dtype=float)

S0 =475
K=476
sig=0.2
T = 20/365
sqrt_T=T**(0.5)
r=0.02

z1 = m.log(S0/K) / sqrt_T
z2 = sqrt_T/2

print(z1, z2)
d1 = ((m.log(S0/K)) + (r+ (sig*sig)/2)*T)/(sig*sqrt_T)
print(d1, ss.norm.cdf(d1))
