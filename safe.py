import os
import numpy as np
import pandas as pd
import datetime
from utils import *
import requests
from bs4 import BeautifulSoup
import re
from io import StringIO, BytesIO

##### 外汇管理局 #####

# 结售汇
def update_fx_settlement_and_sale():
    se = requests.session()
    SAFE_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                    'Host': 'www.safe.gov.cn'}
    url = 'http://www.safe.gov.cn/safe/yhjsh/index.html'
    r = se.get(url, headers=SAFE_HEADERS)
    s = r.content.decode('utf-8')
    soup = BeautifulSoup(s, 'html.parser')
    a = soup.find_all(name='a', string=re.compile('银行结售汇数据'))[0]

    url = 'http://www.safe.gov.cn' + a['href']
    r = se.get(url, headers=SAFE_HEADERS)
    s = r.content.decode('utf-8')
    soup = BeautifulSoup(s, 'html.parser')
    a = soup.find_all(name='a', string=re.compile('银行结售汇数据时间序列'))[0]

    url = 'http://www.safe.gov.cn' + a['href']
    r = se.get(url, headers=SAFE_HEADERS)

    names = ['以人民币计价（月度）', '以美元计价（月度）']
    for name in names:
        print('银行结售汇 ' + name)
        df = pd.read_excel(r.content, sheet_name=name)
        df = df.loc[2:,]
        df.dropna(how='any', axis=1, inplace=True)
        df = df.T
        df.reset_index(inplace=True, drop=True)
        df.replace('-', 0, inplace=True)

        col = ['time', 
            '结汇', 
            '结汇:银行自身', 
            '结汇:银行代客', 
            '结汇:银行代客:经常项目', 
            '结汇:银行代客:经常项目:货物贸易', '结汇:银行代客:经常项目:服务贸易', '结汇:银行代客:经常项目:收益和经常转移',
            '结汇:银行代客:资本与金融项目', 
            '结汇:银行代客:资本与金融项目:直接投资', '结汇:银行代客:资本与金融项目:证券投资', 
            '售汇', 
            '售汇:银行自身', 
            '售汇:银行代客', 
            '售汇:银行代客:经常项目', 
            '售汇:银行代客:经常项目:货物贸易', '售汇:银行代客:经常项目:服务贸易', '售汇:银行代客:经常项目:收益和经常转移',
            '售汇:银行代客:资本与金融项目', 
            '售汇:银行代客:资本与金融项目:直接投资', '售汇:银行代客:资本与金融项目:证券投资', 
            '差额', 
            '差额:银行自身',
            '差额:银行代客', 
            '差额:银行代客:经常项目', 
            '差额:银行代客:经常项目:货物贸易', '差额:银行代客:经常项目:服务贸易', '差额:银行代客:经常项目:收益和经常转移',
            '差额:银行代客:资本与金融项目', 
            '差额:银行代客:资本与金融项目:直接投资', '差额:银行代客:资本与金融项目:证券投资', 
            '远期结售汇签约额:结汇', '远期结售汇签约额:售汇', '远期结售汇签约额:差额',
            '远期结售汇平仓额:差额', '远期结售汇展期额:差额',
            '本期末远期结售汇累计未到期额:结汇', '本期末远期结售汇累计未到期额:售汇', '本期末远期结售汇累计未到期额:差额',
            '未到期期权Delta净敞口',
            ]

        df.columns = col
        dt0 = pd.to_datetime('1900-01-01', format='%Y-%m-%d')
        for i in range(len(df)):
            n = df.loc[i, 'time']
            dt = dt0 + pd.Timedelta(days=n-2)
            last_day = calendar.monthrange(dt.year, dt.month)[-1]
            dt = datetime.datetime(dt.year, dt.month, last_day)
            df.loc[i, 'time'] = dt.strftime('%Y-%m-%d')

        path = os.path.join(safe_dir, '银行结售汇 ' + name + '.csv')
        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            old_df.sort_values(by = 'time', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df.to_csv(path, encoding='utf-8', index=False)  



if __name__=="__main__":
    update_fx_settlement_and_sale()



    pass