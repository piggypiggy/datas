import os
import pandas as pd
import datetime
import numpy as np
import requests
import bs4
from utils import *
import pdfplumber
from io import StringIO, BytesIO

##### 上海黄金交易所 #####

def update_sge_trade_data_url():
    se = requests.session()
    SGE_HEADERS = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Host": "www.sge.com.cn",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    }
    SGE_TRADE_DATA_URL = 'https://www.sge.com.cn/sjzx/mrhqsj?p={}'

    end_page = 222
    earlist_time = '2014-09-18'
    
    path = os.path.join(sge_dir, 'trade_data_url'+'.csv')
    if (os.path.exists(path)):
        old_df = pd.read_csv(path)
        t = pd.DatetimeIndex(pd.to_datetime(old_df['time'], format='%Y-%m-%d'))
        start_time_dt = t[-1] + pd.Timedelta(days=1)
    else:
        old_df = pd.DataFrame()
        start_time_dt = pd.to_datetime(earlist_time, format='%Y-%m-%d')

    page = 1
    url_time_dt = datetime.datetime.now()
    while (1):
        if (page > end_page) or (url_time_dt <= start_time_dt):
            break

        url = SGE_TRADE_DATA_URL.format(str(page))
        print('trade_data_url' + ' page', page)
        page += 1

        while (1):
            try:
                r = se.get(url, headers=SGE_HEADERS)
                soup = bs4.BeautifulSoup(r.text, 'html.parser')
                break
            except Exception as e:
                print(e)
                time.sleep(5)

        div = soup.find_all(name='div', class_="articleList border_ea mt30 mb30")[0]
        lis = div.find_all('li')
        L = len(lis)
        href_list = []
        t_list = []
        for i in range(L):
            href = lis[i].find(name='a')['href']
            t = lis[i].find(name='span', class_="fr").get_text()
            href_list.append(href)
            t_list.append(t)
        
        url_time_dt = pd.to_datetime(t, format='%Y-%m-%d')

        df = pd.DataFrame(columns=['time', 'url'])
        df['time'] = t_list
        df['url'] = href_list

        old_df = pd.concat([old_df, df], axis=0)
        old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
        old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
        old_df.sort_values(by = 'time', inplace=True)
        old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
        old_df.to_csv(path, encoding='utf-8', index=False)


def update_sge_trade_data():
    se = requests.session()
    SGE_HEADERS = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Host": "www.sge.com.cn",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    }
    SGE_TRADE_DATA_URL = 'https://www.sge.com.cn/'

    url_path = os.path.join(sge_dir, 'trade_data_url'+'.csv')
    url_df = pd.read_csv(url_path)
    url_t = pd.DatetimeIndex(pd.to_datetime(url_df['time'], format='%Y-%m-%d'))
    urls = np.array(url_df['url'], dtype=str)

    earlist_time = '2014-09-16'

    path = os.path.join(sge_dir, 'trade_data'+'.csv')
    if (os.path.exists(path)):
        df = pd.read_csv(path, header=[0,1])
        t = pd.DatetimeIndex(pd.to_datetime(df['time']['time'], format='%Y-%m-%d'))
        start_time_dt = t[-1] + pd.Timedelta(days=1)
    else:
        start_time_dt = pd.to_datetime(earlist_time, format='%Y-%m-%d')

    w = np.where(url_t >= start_time_dt)[0]
    if len(w) > 0:
        w = w[0]
    else:
        return

    df = pd.DataFrame()
    n = 0
    for i in range(w, len(url_t)):
        time.sleep(0.15)
        url = SGE_TRADE_DATA_URL + urls[i]
        print('sge trade data:', url_t[i].strftime('%Y-%m-%d'))
        if (url_t[i].strftime('%Y-%m-%d') == '2016-04-12' or
            url_t[i].strftime('%Y-%m-%d') == '2016-12-20'):
            continue

        while (1):
            try:
                r = se.get(url, headers=SGE_HEADERS)
                temp_df = pd.read_html(r.text)[0]
                break
            except Exception as e:
                print(e)
                time.sleep(5)


        temp_df = temp_df.loc[1:,]
        temp_df[1].replace('-', np.nan, inplace=True)
        temp_df[2].replace('-', np.nan, inplace=True)
        temp_df[3].replace('-', np.nan, inplace=True)
        temp_df[4].replace('-', np.nan, inplace=True)
        temp_df[7].replace('-', np.nan, inplace=True)
        temp_df[10].replace('-', 0, inplace=True)
        temp_df[11].replace('多支付给空', 'long to short', inplace=True)
        temp_df[11].replace('空支付给多', 'short to long', inplace=True)

        varieties = np.array(temp_df[0])
        temp_df = temp_df[[1,2,3,4,7,8,9,10,11,12]]

        col1 = ['time']
        col2 = ['time']
        data = [url_t[i].strftime('%Y-%m-%d')]

        for k in range(0, len(varieties)):
            variety = varieties[k]
            if variety == 'Au9995':
                variety = 'Au99.95'
            if variety == 'Au9999':
                variety = 'Au99.99'                
            if variety == 'Pt9995':
                variety = 'Pt99.95'
            if variety == 'iAu9999':
                variety = 'iAu99.99'
            if variety == 'Ag9999':
                variety = 'Ag99.99'
            col1 += [variety,variety,variety,variety,variety,variety,variety,variety,variety,variety]
            col2 += ['open','high','low','close','avg','volume','amount','oi','交收方向','交收量']

        data += temp_df.values.flatten().tolist()
        _df = pd.DataFrame(columns=[col1,col2], data=[data])

        df = pd.concat([df, _df], axis=0)
        n += 1
        if (n >= 20):
            if os.path.exists(path):
                old_df = pd.read_csv(path, header=[0,1])
                old_df = pd.concat([old_df, df], axis=0)
                old_df.drop_duplicates(subset=[('time','time')], keep='last', inplace=True) # last
                old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
                old_df.sort_values(by = ('time','time'), inplace=True)
                old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
                old_df.to_csv(path, encoding='utf-8', index=False)
            else:
                df.loc[:, pd.IndexSlice['time','time']] = df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
                df.sort_values(by = ('time','time'), inplace=True)
                df.loc[:, pd.IndexSlice['time','time']] = df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
                df.to_csv(path, encoding='utf-8', index=False)

            df = pd.DataFrame()
            n = 0

    if not(df.empty):
        if os.path.exists(path):
            old_df = pd.read_csv(path, header=[0,1])
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=[('time','time')], keep='last', inplace=True) # last
            old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            old_df.sort_values(by = ('time','time'), inplace=True)
            old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df.loc[:, pd.IndexSlice['time','time']] = df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            df.sort_values(by = ('time','time'), inplace=True)
            df.loc[:, pd.IndexSlice['time','time']] = df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            df.to_csv(path, encoding='utf-8', index=False)


def update_sge_forwaord_curve():
    se = requests.session()
    SGE_HEADERS = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Host": "www.sge.com.cn",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    }

    URL = 'https://www.sge.com.cn/graph/downloadExcelyqjgqx?forwardType=all&start={}&end={}'

    earlist_time = '2013-09-16'
    now = datetime.datetime.now()
    path = os.path.join(sge_dir, 'forward_curve'+'.csv')
    if (os.path.exists(path)):
        df = pd.read_csv(path, header=[0,1])
        t = pd.DatetimeIndex(pd.to_datetime(df['time']['time'], format='%Y-%m-%d'))
        start_time_dt = t[-1] + pd.Timedelta(days=1)
    else:
        start_time_dt = pd.to_datetime(earlist_time, format='%Y-%m-%d')

    while (start_time_dt <= now):
        end_time_dt = start_time_dt + pd.Timedelta(days=90)
        url = URL.format(start_time_dt.strftime('%Y-%m-%d'), end_time_dt.strftime('%Y-%m-%d'))
        print('forward curve ' + start_time_dt.strftime('%Y-%m-%d') + ' - ' + end_time_dt.strftime('%Y-%m-%d'))
        start_time_dt = end_time_dt + pd.Timedelta(days=1)
        r = se.get(url, headers=SGE_HEADERS)
        temp_df = pd.read_excel(r.content)
        t_list = temp_df['日期'].values.tolist()
        ts = list(set(t_list))
        t_list = np.array(t_list, dtype=str)
        col = temp_df.columns.tolist()
        
        df = pd.DataFrame()
        for t_str in ts:
            w = np.where(t_list == t_str)[0]

            col1 = ['time']
            col2 = ['time']
            data = [t_str]

            for i in w:
                variety = temp_df.loc[i, '合约']
                if variety == 'AUX.CNY':
                    variety = 'Au99.95'
                if variety == 'AUY.CNY':
                    variety = 'Au99.99'
                if variety == 'PAg99.99':
                    variety = 'Ag99.99'
                col1 += [variety for _ in range(len(col)-2)]
                col2 += col[2:]
            data += temp_df.loc[w, col[2:]].values.flatten().tolist()

            _df = pd.DataFrame(columns=[col1,col2], data=[data])
            df = pd.concat([df, _df], axis=0)

        path = os.path.join(sge_dir, 'forward_curve'+'.csv')
        if os.path.exists(path):
            old_df = pd.read_csv(path, header=[0,1])
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=[('time','time')], keep='last', inplace=True) # last
            old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            old_df.sort_values(by = ('time','time'), inplace=True)
            old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df.loc[:, pd.IndexSlice['time','time']] = df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            df.sort_values(by = ('time','time'), inplace=True)
            df.loc[:, pd.IndexSlice['time','time']] = df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            df.to_csv(path, encoding='utf-8', index=False)


def update_sge_option_implied_volatility():
    se = requests.session()
    SGE_HEADERS = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Host": "www.sge.com.cn",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    }

    URL = 'https://www.sge.com.cn/graph/downloadExcelGoldForwards?start={}&end={}'

    earlist_time = '2017-11-20'
    now = datetime.datetime.now()
    path = os.path.join(sge_dir, 'implied_volatility'+'.csv')
    if (os.path.exists(path)):
        df = pd.read_csv(path, header=[0,1])
        t = pd.DatetimeIndex(pd.to_datetime(df['time']['time'], format='%Y-%m-%d'))
        start_time_dt = t[-1] + pd.Timedelta(days=1)
    else:
        start_time_dt = pd.to_datetime(earlist_time, format='%Y-%m-%d')

    while (start_time_dt <= now):
        end_time_dt = start_time_dt + pd.Timedelta(days=90)
        url = URL.format(start_time_dt.strftime('%Y-%m-%d'), end_time_dt.strftime('%Y-%m-%d'))
        print('implied volatility ' + start_time_dt.strftime('%Y-%m-%d') + ' - ' + end_time_dt.strftime('%Y-%m-%d'))
        start_time_dt = end_time_dt + pd.Timedelta(days=1)
        r = se.get(url, headers=SGE_HEADERS)
        temp_df = pd.read_excel(r.content)
        t_list = temp_df['日期'].values.tolist()
        ts = list(set(t_list))
        t_list = np.array(t_list, dtype=str)
        col = temp_df.columns.tolist()

        df = pd.DataFrame()
        for t_str in ts:
            w = np.where(t_list == t_str)[0]

            col1 = ['time']
            col2 = ['time']
            data = [t_str]

            for i in w:
                term = temp_df.loc[i, '期限']
                col1 += [term for _ in range(len(col)-3)]
                col2 += col[3:]
            data += temp_df.loc[w, col[3:]].values.flatten().tolist()

            _df = pd.DataFrame(columns=[col1,col2], data=[data])
            df = pd.concat([df, _df], axis=0)

        path = os.path.join(sge_dir, 'implied_volatility'+'.csv')
        if os.path.exists(path):
            old_df = pd.read_csv(path, header=[0,1])
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=[('time','time')], keep='last', inplace=True) # last
            old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            old_df.sort_values(by = ('time','time'), inplace=True)
            old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df.loc[:, pd.IndexSlice['time','time']] = df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            df.sort_values(by = ('time','time'), inplace=True)
            df.loc[:, pd.IndexSlice['time','time']] = df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            df.to_csv(path, encoding='utf-8', index=False)


def update_sge_lease_rate_url():
    se = requests.session()
    SGE_HEADERS = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Host": "www.sge.com.cn",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    }
    SGE_LEASR_RATE_URL = 'https://www.sge.com.cn/sjzx/hjzjckll?p={}'

    end_page = 25
    earlist_time = '2019-12-30'
  
    path = os.path.join(sge_dir, 'lease_rate_url'+'.csv')
    if (os.path.exists(path)):
        old_df = pd.read_csv(path)
        t = pd.DatetimeIndex(pd.to_datetime(old_df['start_time'], format='%Y-%m-%d'))
        start_time_dt = t[-1] + pd.Timedelta(days=1)
    else:
        old_df = pd.DataFrame()
        start_time_dt = pd.to_datetime(earlist_time, format='%Y-%m-%d')

    page = 1
    url_time_dt = datetime.datetime.now()
    while (1):
        if (page > end_page) or (url_time_dt <= start_time_dt):
            break

        url = SGE_LEASR_RATE_URL.format(str(page))
        print('lease_rate_url' + ' page', page)
        page += 1

        while (1):
            try:
                r = se.get(url, headers=SGE_HEADERS)
                soup = bs4.BeautifulSoup(r.text, 'html.parser')
                break
            except Exception as e:
                print(e)
                time.sleep(5)

        aa = soup.find_all(name='a', class_="txt fl")
        href_list = []
        for a in aa:
            href_list.append(a['href'])

        spans = soup.find_all(name='span', class_="fl")
        start_time_list = []
        last_text = ''
        for span in spans:
            text = span.get_text()
            # print(text)
            if '租借' in text:
                text = text.split('（')[1].split('-')[0]
                if len(text) == 9:
                    text = text[0:6] + text[8:]
                text = pd.to_datetime(text, format='%Y%m%d').strftime('%Y-%m-%d')
                
                if not(text in start_time_list):
                    start_time_list.append(text)
                    last_text = text

        url_time_dt = pd.to_datetime(last_text, format='%Y-%m-%d')
        df = pd.DataFrame(columns=['start_time', 'url'])
        df['start_time'] = start_time_list
        df['url'] = href_list

        old_df = pd.concat([old_df, df], axis=0)
        old_df.drop_duplicates(subset=['start_time'], keep='last', inplace=True)
        old_df['start_time'] = old_df['start_time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
        old_df.sort_values(by = 'start_time', inplace=True)
        old_df['start_time'] = old_df['start_time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
        old_df.to_csv(path, encoding='utf-8', index=False)


def update_sge_lease_rate():
    se = requests.session()
    SGE_HEADERS = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Host": "www.sge.com.cn",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    }

    url_path = os.path.join(sge_dir, 'lease_rate_url'+'.csv')
    url_df = pd.read_csv(url_path)
    url_t = pd.DatetimeIndex(pd.to_datetime(url_df['start_time'], format='%Y-%m-%d'))
    urls = np.array(url_df['url'], dtype=str)

    earlist_time = '2019-12-30'
    path = os.path.join(sge_dir, 'lease_rate'+'.csv')
    if (os.path.exists(path)):
        df = pd.read_csv(path)
        t = pd.DatetimeIndex(pd.to_datetime(df['start_time'], format='%Y-%m-%d'))
        start_time_dt = t[-1] + pd.Timedelta(days=1)
    else:
        start_time_dt = pd.to_datetime(earlist_time, format='%Y-%m-%d')

    w = np.where(url_t >= start_time_dt)[0]
    if len(w) > 0:
        w = w[0]
    else:
        return

    df = pd.DataFrame()
    for i in range(w, len(url_t)):
        time.sleep(0.15)
        url = urls[i]
        if not('https' in url):
            url = 'https://www.sge.com.cn' + url
        print('lease rate:', url_t[i].strftime('%Y-%m-%d'))

        while (1):
            try:
                r = se.get(url, headers=SGE_HEADERS)
                pdf = pdfplumber.open(BytesIO(r.content))
                break
            except Exception as e:
                print(e)
                time.sleep(5)

        page = pdf.pages[0]
        table = page.extract_text_lines()

        col = ['start_time', 'end_time', '算术平均利率6M', '加权平均利率6M', '最高利率6M', '最低利率6M', '算术平均利率1Y', '加权平均利率1Y', '最高利率1Y', '最低利率1Y']

        s = table[1]['text']
        if ('-' in s):
            z = s.split('-')
            try:
                z[0] = pd.to_datetime(z[0][1:], format='%Y%m%d').strftime('%Y-%m-%d')
            except:
                z[0] = url_t[i].strftime('%Y-%m-%d')
            z[1] = pd.to_datetime(z[1][:8], format='%Y%m%d').strftime('%Y-%m-%d')
            print(z)
        else:
            z = [pd.to_datetime(s[1:9], format='%Y%m%d').strftime('%Y-%m-%d'),
                 pd.to_datetime(s[1:9], format='%Y%m%d').strftime('%Y-%m-%d')]

        data = z

        s = table[3]['text']
        data += s.split(' ')[1:]

        s = table[4]['text']
        data += s.split(' ')[1:]

        df = pd.DataFrame(columns=col, data=[data])

        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=['start_time'], keep='last', inplace=True)
            old_df['start_time'] = old_df['start_time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            old_df.sort_values(by = 'start_time', inplace=True)
            old_df['start_time'] = old_df['start_time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df['start_time'] = df['start_time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            df.sort_values(by = 'start_time', inplace=True)
            df['start_time'] = df['start_time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            df.to_csv(path, encoding='utf-8', index=False)


def update_all_sge_data():
    # 远期曲线
    update_sge_forwaord_curve()

    # 询价期权隐含波动率
    update_sge_option_implied_volatility()

    # 每日行情的 url
    update_sge_trade_data_url()

    # 每日行情
    update_sge_trade_data()

    # 场内黄金同业租借参考利率 url
    update_sge_lease_rate_url()

    # 场内黄金同业租借参考利率
    update_sge_lease_rate()


def plot_sge_au9999_imp_vol():
    path = os.path.join(sge_dir, 'implied_volatility'+'.csv')
    df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(df['time']['time'], format='%Y-%m-%d'))
    _1w_atm = np.array(df['1W']['ATMF'], dtype=float)
    _1w_25d_call = np.array(df['1W']['25D Call'], dtype=float)
    _1w_25d_put = np.array(df['1W']['25D Put'], dtype=float)
    _1w_25d_skew = _1w_25d_put - _1w_25d_call

    _1m_atm = np.array(df['1M']['ATMF'], dtype=float)
    _1m_25d_call = np.array(df['1M']['25D Call'], dtype=float)
    _1m_25d_put = np.array(df['1M']['25D Put'], dtype=float)
    _1m_25d_skew = _1m_25d_put - _1m_25d_call

    _3m_atm = np.array(df['3M']['ATMF'], dtype=float)
    _3m_25d_call = np.array(df['3M']['25D Call'], dtype=float)
    _3m_25d_put = np.array(df['3M']['25D Put'], dtype=float)
    _3m_25d_skew = _3m_25d_put - _3m_25d_call

    _6m_atm = np.array(df['6M']['ATMF'], dtype=float)
    _6m_25d_call = np.array(df['6M']['25D Call'], dtype=float)
    _6m_25d_put = np.array(df['6M']['25D Put'], dtype=float)
    _6m_25d_skew = _6m_25d_put - _6m_25d_call

    _1y_atm = np.array(df['1Y']['ATMF'], dtype=float)
    _1y_25d_call = np.array(df['1Y']['25D Call'], dtype=float)
    _1y_25d_put = np.array(df['1Y']['25D Put'], dtype=float)
    _1y_25d_skew = _1y_25d_put - _1y_25d_call


    path = os.path.join(future_price_dir, 'shfe', 'au'+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    index_close = np.array(fut_df['index']['close'], dtype=float)


    datas = [[[[t,_1w_25d_call,'1w_25d_call','color=red'],
               [t,_1w_25d_put,'1w_25d_put','color=darkgreen'],
              ],
              [[t,_1w_25d_skew,'1w_25d_put-call','color=gray,style=vbar'],],'SGE AU99.99 IMPLIED VOLATILITY'],

             [[[t,_1m_25d_call,'1m_25d_call','color=red'],
               [t,_1m_25d_put,'1m_25d_put','color=darkgreen'],
              ],
              [[t,_1m_25d_skew,'1m_25d_put-call','color=gray,style=vbar'],],''],

             [[[fut_t,index_close,'au指数','color=black'],
              ],
              [],''],

             [[[t,_3m_25d_call,'3m_25d_call','color=red'],
               [t,_3m_25d_put,'3m_25d_put','color=darkgreen'],
              ],
              [[t,_3m_25d_skew,'3m_25d_put-call','color=gray,style=vbar'],],''],

             [[[t,_6m_25d_call,'6m_25d_call','color=red'],
               [t,_6m_25d_put,'6m_25d_put','color=darkgreen'],
              ],
              [[t,_6m_25d_skew,'6m_25d_put-call','color=gray,style=vbar'],],''],

             [[[t,_1y_25d_call,'1y_25d_call','color=red'],
               [t,_1y_25d_put,'1y_25d_put','color=darkgreen'],
              ],
              [[t,_1y_25d_skew,'1y_25d_put-call','color=gray,style=vbar'],],''],
    ]

    plot_many_figure(datas, max_height=1000)


def plot_sge_au9999_lease_rate():
    path = os.path.join(sge_dir, 'lease_rate'+'.csv')
    df = pd.read_csv(path)
    start_t = pd.DatetimeIndex(pd.to_datetime(df['start_time'], format='%Y-%m-%d'))
    end_t = pd.DatetimeIndex(pd.to_datetime(df['end_time'], format='%Y-%m-%d'))
    t = np.append(start_t, end_t)
    sort = np.argsort(t)
    t = t[sort]

    arithmetic_avg_6m = np.array(df['算术平均利率6M'], dtype=float)
    arithmetic_avg_6m = np.append(arithmetic_avg_6m, arithmetic_avg_6m)[sort]
    weighted_avg_6m = np.array(df['加权平均利率6M'], dtype=float)
    weighted_avg_6m = np.append(weighted_avg_6m, weighted_avg_6m)[sort]
    high_6m = np.array(df['最高利率6M'], dtype=float)
    high_6m = np.append(high_6m, high_6m)[sort]
    low_6m = np.array(df['最低利率6M'], dtype=float)
    low_6m = np.append(low_6m, low_6m)[sort]

    arithmetic_avg_1y = np.array(df['算术平均利率1Y'], dtype=float)
    arithmetic_avg_1y = np.append(arithmetic_avg_1y, arithmetic_avg_1y)[sort]
    weighted_avg_1y = np.array(df['加权平均利率1Y'], dtype=float)
    weighted_avg_1y = np.append(weighted_avg_1y, weighted_avg_1y)[sort]
    high_1y = np.array(df['最高利率1Y'], dtype=float)
    high_1y = np.append(high_1y, high_1y)[sort]
    low_1y = np.array(df['最低利率1Y'], dtype=float)
    low_1y = np.append(low_1y, low_1y)[sort]


    path = os.path.join(future_price_dir, 'shfe', 'au'+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    index_close = np.array(fut_df['index']['close'], dtype=float)

    datas = [[[[t,weighted_avg_6m,'加权平均租赁利率6M',''],
               [t,arithmetic_avg_6m,'算术平均租赁利率6M',''],
              ],
              [],'SGE AU99.99 场内黄金同业租借参考利率'],

             [[[fut_t,index_close,'au指数','color=black'],
              ],
              [],''],

             [[[t,weighted_avg_1y,'加权平均租赁利率1Y','color=red'],
               [t,arithmetic_avg_1y,'算术平均租赁利率1Y',''],
              ],
              [],''],
    ]
    plot_many_figure(datas)


# T+D
def plot_sge_td_data(variety):
    path = os.path.join(sge_dir, 'trade_data'+'.csv')
    df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(df['time']['time'], format='%Y-%m-%d'))
    price = np.array(df[variety]['close'], dtype=float)
    volume = np.array(df[variety]['volume'], dtype=float)
    oi = np.array(df[variety]['oi'], dtype=float)
    direction = np.array(df[variety]['交收方向'], dtype=str)
    w = np.where(direction == 'long to short')[0]
    direction_long = np.ones(len(w))
    t_long = t[w].copy()
    w = np.where(direction == 'short to long')[0]
    direction_short = np.ones(len(w))*-1
    t_short = t[w].copy()
    direction = np.append(direction_long, direction_short)
    t_direction = np.append(t_long, t_short)
    sort = np.argsort(t_direction)
    t_direction = t_direction[sort]
    direction = direction[sort]

    delivery = np.array(df[variety]['交收量'], dtype=float)

    datas = [[[[t_direction,direction,variety+'交收方向','color=gray'],
              ],
              [[t,delivery,variety+'交收量','color=purple'],],variety],

             [[[t,price,variety+' CLOSE','color=black'],
              ],
              [[t,oi,variety+' OI','']],''],

             [[[t,volume,variety+' volume',''],
              ],
              [],''],
    ]
    plot_many_figure(datas)


def plot_sge_au9999_data():
    plot_sge_au9999_imp_vol()
    plot_sge_td_data('Au(T+D)')
    plot_sge_au9999_lease_rate()


def plot_sge_forward(variety):
    path = os.path.join(sge_dir, 'forward_curve'+'.csv')
    df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(df['time']['time'], format='%Y-%m-%d'))

    if variety[0:2] == 'Au':
        path = os.path.join(sge_dir, 'lease_rate'+'.csv')
        lr_df = pd.read_csv(path)
        lr_start_t = pd.DatetimeIndex(pd.to_datetime(lr_df['start_time'], format='%Y-%m-%d'))
        lr_end_t = pd.DatetimeIndex(pd.to_datetime(lr_df['end_time'], format='%Y-%m-%d'))
        lr_6m_weekly = np.array(lr_df['加权平均利率6M'], dtype=float)
        lr_1y_weekly = np.array(lr_df['加权平均利率1Y'], dtype=float)
        lr_t = None
        lr_6m = None
        lr_1y = None
        for i in range(len(lr_start_t)):
            if lr_t is None:
                lr_t = pd.date_range(lr_start_t[i], lr_end_t[i])
                lr_6m = [lr_6m_weekly[i]] * ((lr_end_t[i] - lr_start_t[i]).days+1)
                lr_1y = [lr_1y_weekly[i]] * ((lr_end_t[i] - lr_start_t[i]).days+1)
            else:
                lr_t = lr_t.append(pd.date_range(lr_start_t[i], lr_end_t[i]))
                lr_6m += [lr_6m_weekly[i]] * ((lr_end_t[i] - lr_start_t[i]).days+1)
                lr_1y += [lr_1y_weekly[i]] * ((lr_end_t[i] - lr_start_t[i]).days+1)
        lr_6m = np.array(lr_6m, dtype=float)
        lr_1y = np.array(lr_1y, dtype=float)

    # path = os.path.join(interest_rate_dir, '国债收益率'+'.csv') 
    # gov_df = pd.read_csv(path)
    # gov_t = pd.DatetimeIndex(pd.to_datetime(gov_df['time'], format='%Y-%m-%d'))
    # cn03m = np.array(gov_df['3M'], dtype=float)
    # cn06m = np.array(gov_df['6M'], dtype=float)
    # cn01y = np.array(gov_df['1Y'], dtype=float)

    path = os.path.join(interest_rate_dir, 'shibor'+'.csv') 
    sh_df = pd.read_csv(path)
    sh_t = pd.DatetimeIndex(pd.to_datetime(sh_df['time'], format='%Y-%m-%d'))
    sh03m = np.array(sh_df['3M'], dtype=float)
    sh06m = np.array(sh_df['6M'], dtype=float)
    sh01y = np.array(sh_df['1Y'], dtype=float)

    path = os.path.join(sge_dir, 'trade_data'+'.csv') 
    tr_df = pd.read_csv(path, header=[0,1])
    tr_t = pd.DatetimeIndex(pd.to_datetime(tr_df['time']['time'], format='%Y-%m-%d'))


    if variety[0:2] == 'Au':
        adjust = 1/100
    elif variety[0:2] == 'Ag':
        adjust = 1
    else:
        adjust = 1

    _3M = np.array(df[variety]['3M'], dtype=float) * adjust
    _6M = np.array(df[variety]['6M'], dtype=float) * adjust
    _1Y = np.array(df[variety]['1Y'], dtype=float) * adjust

    if (variety == 'Ag99.99'):
        price = np.array(tr_df['Ag(T+D)']['close'], dtype=float)
    else:
        price = np.array(tr_df[variety]['close'], dtype=float)

    t2, imp_rate2 = data_div(t, _3M, tr_t, price)
    imp_rate2 *= 4

    t3, imp_rate3 = data_div(t, _6M, tr_t, price)
    imp_rate3 *= 2

    t4, imp_rate4 = data_div(t, _1Y, tr_t, price)

    # 百分比
    imp_rate2 *= 100
    imp_rate3 *= 100
    imp_rate4 *= 100

    # 
    t33, diff3 = data_sub(sh_t, sh06m, t3, imp_rate3)
    t44, diff4 = data_sub(sh_t, sh01y, t4, imp_rate4)

    if variety[0:2] == 'Au':
        datas = [
                    [[[sh_t, sh06m, 'SHIOBR 6M', ''],
                    [t3, imp_rate3, 'FORWARD RATE 6M', ''],
                    ],
                    [[t33, diff3, 'SPREAD', 'style=vbar'],],variety],

                    [[[tr_t, price, variety, 'color=black'],
                    ],
                    [],''],

                    [[[sh_t, sh01y, 'SHIOBR 1Y', ''],
                    [t4, imp_rate4, 'FORWARD RATE 1Y', ''],
                    ],
                    [[t44, diff4, 'SPREAD', 'style=vbar'],],''],

                    [[[lr_t, lr_6m, 'LEASE RATE 6M', ''],
                    [t33, diff3, 'SPREAD', 'style=vbar'],
                    ],
                    [],''],

                    [[[lr_t, lr_1y, 'LEASE RATE 1Y', ''],
                    [t44, diff4, 'SPREAD', 'style=vbar'],
                    ],
                    [],''],
        ]
    
    if variety[0:2] == 'Ag':
        datas = [
                    [[[sh_t, sh06m, 'SHIOBR 6M', ''],
                    [t3, imp_rate3, 'FORWARD RATE 6M', ''],
                    ],
                    [[t33, diff3, 'SPREAD', 'style=vbar'],],variety],

                    [[[tr_t, price, variety, 'color=black'],
                    ],
                    [],''],

                    [[[sh_t, sh01y, 'SHIOBR 1Y', ''],
                    [t4, imp_rate4, 'FORWARD RATE 1Y', ''],
                    ],
                    [[t44, diff4, 'SPREAD', 'style=vbar'],],''],

        ]
    plot_many_figure(datas, start_time='2015-01-01')


# https://www.zerohedge.com/commodities/chinas-pboc-manipulates-shanghai-exchange-gold-price
def plot_sge_vs_sgei():
    path = os.path.join(sge_dir, 'trade_data'+'.csv')
    df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(df['time']['time'], format='%Y-%m-%d'))
    mb_close = np.array(df['Au99.99']['close'], dtype=float)
    ib_close = np.array(df['iAu99.99']['close'], dtype=float)
    mb_avg = np.array(df['Au99.99']['avg'], dtype=float)
    ib_avg = np.array(df['iAu99.99']['avg'], dtype=float)
    # sge vs sgei premium
    close_premium = mb_close/ib_close - 1
    avg_premium = mb_avg/ib_avg - 1

    path = os.path.join(fx_dir, 'USDCNY'+'.csv')
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    usdcny = np.array(df['close'], dtype=float)

    datas = [
             [[[t, close_premium, 'SGE vs SGEI 收盘价 PREMIUM', ''],
               [t, avg_premium, 'SGE vs SGEI 加权平均价 PREMIUM', ''],
              ],
              [[t1, usdcny, 'USDCNY', 'color=red'],],''],

             [[[t, mb_close, 'Au99.99', ''],
              ],
              [[t1, usdcny, 'USDCNY', 'color=red'],],''],
        ]
    plot_many_figure(datas, start_time='2014-01-01')


if __name__=="__main__":
    # plot_sge_forward('Au99.99')
    # plot_sge_forward('Ag99.99')

    plot_sge_vs_sgei()

    # plot_sge_au9999_data()

    # update_all_sge_data()


    pass

