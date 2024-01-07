import os
import requests
import pandas as pd
import datetime
import numpy as np
from utils import *
from io import StringIO, BytesIO
from selenium import webdriver
import execjs
import re
import bs4

dce_driver = None
driver_fut = None
driver_fut_open = False
driver_opt = None
driver_opt_open = False

################## CFFEX #################

def update_future_intraday_price_cffex():
    se = requests.session()
    CFFEX_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                    'Host': 'www.cffex.com.cn'}
    URL1 = 'http://www.cffex.com.cn/quote_{}.txt'
    URL2 = 'http://www.cffex.com.cn/yshqtz/hqtym/quoteDatas/{}_price.txt'

    variety = 'IF'

    url = URL1.format('IF')
    r = se.get(url, headers=CFFEX_HEADERS)
    f = StringIO(r.text)
    table_df = pd.read_table(f, encoding="utf-8", sep=",")

    inst_id = table_df.loc[0, 'instrument']
    # get latest time
    url = URL2.format(inst_id)
    r = se.get(url, headers=CFFEX_HEADERS)
    f = StringIO(r.text)
    table_df = pd.read_table(f, encoding="utf-8", sep=",")
    if len(table_df) > 0:
        t =  table_df.loc[len(table_df)-1, 'time']
    else:
        return None
    
    dt = pd.to_datetime(t, format='%Y/%m/%d %H:%M')
    t = dt.strftime('%Y-%m-%d %H:%M:%S')
    
    for variety in exchange_dict['cffex']:
        print(variety)
        url = URL1.format(variety)
        r = se.get(url, headers=CFFEX_HEADERS)
        f = StringIO(r.text)
        table_df = pd.read_table(f, encoding="utf-8", sep=",")
        n = len(table_df)
        
        c1 = ['time','index','index','index']
        c1_add = ['c1','c2','c3','c4','c5','c6','c7','c8','c9']
        c2 = ['time','close','vol','oi']
        c2_add = ['inst_id','close','vol','oi']
        null_data = [None,None,None,None]
        for i in range(len(c1_add)):
            for j in range(len(c2_add)): # 
                c1.append(c1_add[i])
                c2.append(c2_add[j])

        df = pd.DataFrame(columns=[c1, c2])

        close = np.array(table_df['lastprice'], dtype=float)
        volumn = np.array(table_df['volume'], dtype=float)
        oi = np.array(table_df['position'], dtype=float)
        nominator = 0
        denominator = 0
        for i in range(len(close)):
            if not(np.isnan(close[i])):
                nominator = nominator + close[i]*oi[i]
                denominator = denominator + oi[i]
        index_close = nominator/denominator
        index_volumn = np.nansum(volumn)
        index_oi = np.nansum(oi)

        row = [t]
        row += [index_close, index_volumn, index_oi]

        if (n >= 9):
            row += table_df[['instrument','lastprice','volume','position']].values.flatten().tolist()
        else:
            row += table_df[['instrument','lastprice','volume','position']].values.flatten().tolist()
            for i in range(9-n):
                row += null_data

        df.loc[0] = row
        path = os.path.join(future_price_dir, 'cffex', variety+'_intraday'+'.csv') 
        if os.path.exists(path):
            old_df = pd.read_csv(path, header=[0,1])
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=[('time','time')], keep='first', inplace=True) # last
            old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
            old_df.sort_values(by = ('time','time'), inplace=True)
            old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df.to_csv(path, encoding='utf-8', index=False)
    
    return t


def update_option_intraday_price_cffex(t=None):
    if t is None:
        return

    se = requests.session()
    CFFEX_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                    'Host': 'www.cffex.com.cn'}
    opt_variety_dict = {
        'IF': 'IO',
        'IH': 'HO',
        'IM': 'MO',
    }

    opt_variety_dict_reverse = {
        'IO': 'IF',
        'HO': 'IH',
        'MO': 'IM',
    }

    URL = 'http://www.cffex.com.cn/quote_{}.txt'

    for variety in exchange_option_dict['cffex']:
        opt_variety = opt_variety_dict[variety]
        url = URL.format(opt_variety)
        r = se.get(url, headers=CFFEX_HEADERS)
        f = StringIO(r.text)
        table_df = pd.read_table(f, encoding="utf-8", sep=",")
        n = len(table_df)

        # table_df.replace(opt_variety,variety,inplace=True)

        info_df = pd.DataFrame(columns=['time', 'inst_ids'])
        info_df.loc[0, 'time'] = t

        inst_ids = np.array(table_df['instrument'], dtype=str)
        table_df = table_df[['lastprice', 'volume', 'position']]
        name_dict = {}
        names = ''
        for i in range((len(inst_ids))):
            name = inst_ids[i][:6]
            opt_variety = name[0:2]
            opt_variety = opt_variety_dict_reverse[opt_variety]
            name = opt_variety + name[2:]
            if not(name in name_dict):
                # 从 idx_offset 开始
                name_dict[name] = [i, i]
                names = names + name
                names = names + ','
            else:
                name_dict[name][1] = name_dict[name][1] + 1
        info_df.loc[0, 'inst_ids'] = names

        print(name_dict)
        for name in name_dict:
            print(name)
            col1 = ['time']
            col2 = ['time']
            col3 = ['time']
            row = [t]

            start_idx = name_dict[name][0]
            end_idx = name_dict[name][1]
            tmp = table_df.loc[name_dict[name][0]:name_dict[name][1], :]
            row += tmp.values.flatten().tolist()
            
            for k in range(start_idx, end_idx+1):
                otype = inst_ids[k][7]
                strike = inst_ids[k][9:]
                col1 += [otype, otype, otype]
                col2 += [strike, strike, strike]
                col3 += ['close', 'volume', 'oi']

            df = pd.DataFrame(columns=[col1, col2, col3], data=[row])

            path = os.path.join(option_price_dir, 'cffex', name+'_intraday'+'.csv') 
            if os.path.exists(path):
                old_df = pd.read_csv(path, header=[0,1,2])
                old_df = pd.concat([old_df, df], axis=0)
                old_df.drop_duplicates(subset=[('time','time','time')], keep='last', inplace=True) # last
                old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
                old_df.sort_values(by = ('time','time','time'), inplace=True)
                old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
                old_df.to_csv(path, encoding='utf-8', index=False)
            else:
                df.to_csv(path, encoding='utf-8', index=False)

        path = os.path.join(option_price_dir, 'cffex', variety+'_intraday_info'+'.csv')
        # print(df)
        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, info_df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
            old_df.sort_values(by = 'time', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            info_df.to_csv(path, encoding='utf-8', index=False)  


############### DCE #################
def dce_delay_data_time():
    now = datetime.datetime.now()
    hour = now.hour
    minute = now.minute

    if (hour < 9):
        dt = None
    elif (hour == 9) and (minute <= 1):
        dt = None
    elif (hour == 10) and ((minute >= 15) and (minute <= 31)):
        dt = None
    elif (hour == 11) and (minute >= 30)and (minute < 35):
        dt = datetime.datetime(year=now.year, month=now.month, day=now.day, hour=11, minute=30, second=0)
    elif (hour == 11) and (minute >= 35):
        dt = None
    elif (hour == 12) or ((hour == 13) and (minute <= 31)):
        dt = None
    elif (hour == 15) and (minute < 5):
        dt = datetime.datetime(year=now.year, month=now.month, day=now.day, hour=15, minute=0, second=0)
    elif (hour >= 15) and (hour < 21):
        dt = None
    elif (hour == 21) and (minute <= 1):
        dt = None
    elif (hour >= 23) and (minute <= 10):
        dt = datetime.datetime(year=now.year, month=now.month, day=now.day, hour=23, minute=0, second=0)
    elif (hour >= 23) and (minute > 10):
        dt = None
    else:
        dt = datetime.datetime(year=now.year, month=now.month, day=now.day, hour=now.hour, minute=now.minute, second=0) - pd.Timedelta(minutes=1)
    
    if dt is not None:
        t = dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        t = None

    return t


# DCE_COOKIES = {
#     'AUjhoZgfrTo9S': '60g97GuRAliDmpSlcqitlWQ6u6bH9WLIYJabCYlf1MU89guLVOLM6Ss90NLdrdW3yvly1YJfUYbLpIq7QhbdlOAa',
#     'AUjhoZgfrTo9T': '0cHMtCMnEmWIpqsKDZAC47C3p.kN7HeTsvXqfN3YmBs5FdhtRKLgOpR02M3iXKxXPx5EKXxjpOxnRNpEQWNeFK.N3rYtqc0Ri6QA2qBEZJt7vttw0g1Nfb7TuVQti1MOVZH7TMFuSmlSDG2LdcPNx9.wEwCtQMOydq3Ui0q4rwKzSKGtEkQw7gg2VZp1qA_b8jHntG9F4afniD74B2xN4_ReFxT5Hd2MietiPD_hw9AAdATl97w4C5HKVuS1Ckckmlb08bACqnWlhENefaBuPOyE9N4FiJEZ7rOJNDjt5CrTVgvilMb8bze6r0gbiOXgUXMNpuceOzEJv_V0ATpMOXu8hFyw5uXekYM.75m1VL0Qws8SeXeAdGMIzMeFfQTq4dD9gMhbM82KAIQeUEzpkx_NPWpR0hzz2QXCQzA9VrxpOm7KJH.MNA.SLWR3ics7X',
#     'JSESSIONID': 'EB7BCF287377176EEEAD0F43495853D9',
#     'UqZBpD3n3iTGAwBS': 'v1pZY5Jc6c4cC',
#     'UqZBpD3n3iTPAABS':	"v1pZY5JSyg0o0",
#     'UqZBpD3n3iXPAw1X9DmY+0mqerc8xLAdPYGU69E_':"v13OMuiUWB0xP-NzVn2Wre5l7i7yA",
#     'VlRBYr90kaK9S':"5rv6fUftXJ_s3KZ6IKmRhN4PqEk1KWo6CM6qt73vB1H0EIckiPYjtzaX0jNSBCIjpFrwh1TL2MR6zlVPsYanmJG",
#     'WMONID':"Vt4L0yAYPmh",
# }


def update_future_intraday_price_dce():
    global dce_driver
    se = requests.session()
    t = dce_delay_data_time()

    DCE_HEADERS = {"User-Agent": "User-Agent Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
                    'Host': 'www.dce.com.cn',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    }

    if t is not None:
        for variety in exchange_dict['dce']:
            url = 'http://www.dce.com.cn/webquote/futures_quote_ajax?varietyid='+variety
            # 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'

            while (1):
                try:
                    r = se.get(url, headers=DCE_HEADERS)

                    data_json = r.json()
                    # print(data_json)
                    temp_df = pd.DataFrame(data_json['contractQuote'])
                    temp_df.loc['lastPrice',:].replace('--', None, inplace=True)
                    temp_df.loc['matchTotQty',:].replace('--', '0', inplace=True)
                    temp_df.loc['openInterest',:].replace('--', '0', inplace=True)
                    print(variety)
                    break
                except Exception as e:
                    # fuck dce
                    dce_driver.get('http://www.dce.com.cn/webquote/futures_quote_ajax?varietyid='+'a')
                    cookie = dce_driver.get_cookies()
                    print('FUCK DCE')

                    c = ''
                    for i in range(len(cookie)):
                        c += cookie[i]['name']
                        c += '='
                        c += cookie[i]['value']
                        c += ';'
                    DCE_HEADERS['Cookie'] = c

                    print(c)
                    time.sleep(3)

            cols = temp_df.columns.tolist()
            n = len(cols)

            c1 = ['time','index','index','index']
            c1_add = ['c1','c2','c3','c4','c5','c6','c7','c8','c9']
            c2 = ['time','close','vol','oi']
            c2_add = ['inst_id','close','vol','oi']
            null_data = [None,None,None,None]
            for i in range(len(c1_add)):
                for j in range(len(c2_add)): # 
                    c1.append(c1_add[i])
                    c2.append(c2_add[j])

            df = pd.DataFrame(columns=[c1, c2])

            close = np.array(temp_df.loc['lastPrice', :], dtype=float)
            volumn = np.array(temp_df.loc['matchTotQty', :], dtype=float)
            oi = np.array(temp_df.loc['openInterest', :], dtype=float)
            nominator = 0
            denominator = 0
            for i in range(len(close)):
                if not(np.isnan(close[i])):
                    nominator = nominator + close[i]*oi[i]
                    denominator = denominator + oi[i]
            index_close = nominator/denominator
            index_volumn = np.nansum(volumn)
            index_oi = np.nansum(oi)

            row = [t]
            row += [index_close, index_volumn, index_oi]

            if (n >= 9):
                for i in range(9):
                    row += temp_df.loc[['contractID','lastPrice','matchTotQty','openInterest'], cols[i]].values.tolist()
            else:
                for i in range(n):
                    row += temp_df.loc[['contractID','lastPrice','matchTotQty','openInterest'], cols[i]].values.tolist()
                for i in range(9-n):
                    row += null_data

            df.loc[0] = row
            path = os.path.join(future_price_dir, 'dce', variety+'_intraday'+'.csv') 
            if os.path.exists(path):
                old_df = pd.read_csv(path, header=[0,1])
                old_df = pd.concat([old_df, df], axis=0)
                old_df.drop_duplicates(subset=[('time','time')], keep='first', inplace=True) # last
                old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
                old_df.sort_values(by = ('time','time'), inplace=True)
                old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
                old_df.to_csv(path, encoding='utf-8', index=False)
            else:
                df.to_csv(path, encoding='utf-8', index=False)

    return t


def update_option_intraday_price_dce(se=None, t=None):
    global dce_driver
    if se is None:
        se = requests.session()

    if t is None:
        print('DCE t is None')
        return

    DCE_HEADERS = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    "User-Agent": "User-Agent Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
                    'Host': 'www.dce.com.cn',
    }

    for variety in exchange_option_dict['dce']:
        url = 'http://www.dce.com.cn/webquote/option_quote.jsp?varietyid='+variety
        # 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
        # 'User-Agent Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0'

        while(1):
            try:
                r = se.get(url, headers=DCE_HEADERS, timeout=10)
                soup = bs4.BeautifulSoup(r.text, 'html.parser')
                trs = soup.find_all('select', attrs={'id':"contractselect"})
                namelist = []
                for tr in trs:
                    tds = tr.find_all('option')
                    for td in tds:
                        namelist.append(td.get_text())
                if len(namelist) == 0:
                    raise('DCE')
                break
            except Exception as e:
                print('DCE OPTION INTRADAT ERROR 1', e)

                # fuck dce
                dce_driver.get('http://www.dce.com.cn/webquote/futures_quote_ajax?varietyid='+'a')
                cookie = dce_driver.get_cookies()
                print('FUCK DCE')

                c = ''
                for i in range(len(cookie)):
                    c += cookie[i]['name']
                    c += '='
                    c += cookie[i]['value']
                    c += ';'
                DCE_HEADERS['Cookie'] = c

                time.sleep(5)

        print(namelist)
        inst_ids = ''
        for name in namelist:
            inst_ids += name
            inst_ids += ','

        if len(inst_ids) == 0:
            continue
        info_df = pd.DataFrame(columns=['time', 'inst_ids'], data=[[t, inst_ids]])

        for name in namelist:
            url = 'http://www.dce.com.cn/webquote/option_quote_ajax?varietyid='+'variety'+'&contractid='+name
            while(1):
                try:
                    print(name)
                    r = se.get(url, headers=DCE_HEADERS, timeout=10)
                    data_json = r.json()
                    temp_df = pd.DataFrame(data_json['optionQuote'])
                    break
                except Exception as e:
                    print('DCE OPTION INTRADAT ERROR 2', e)

                    # fuck dce
                    dce_driver.get('http://www.dce.com.cn/webquote/futures_quote_ajax?varietyid='+'a')
                    cookie = dce_driver.get_cookies()
                    print('FUCK DCE')

                    c = ''
                    for i in range(len(cookie)):
                        c += cookie[i]['name']
                        c += '='
                        c += cookie[i]['value']
                        c += ';'
                    DCE_HEADERS['Cookie'] = c

                    time.sleep(5)

            col1 = ['time']
            col2 = ['time']
            col3 = ['time']
            row = [t]

            if len(temp_df) > 0:
                temp_df.loc['matchCTotQty',:].replace('--', '0', inplace=True)
                temp_df.loc['matchPTotQty',:].replace('--', '0', inplace=True)
                temp_df.loc['openCInterest',:].replace('--', '0', inplace=True)
                temp_df.loc['openPInterest',:].replace('--', '0', inplace=True)
                temp_df.loc['lastCPrice',:].replace('--', None, inplace=True)
                temp_df.loc['lastPPrice',:].replace('--', None, inplace=True)

                strikes = temp_df.columns.tolist()

                for strike in strikes:
                    col1 += ['C', 'C', 'C']
                    col2 += [strike, strike, strike]
                    col3 += ['close', 'volume', 'oi']
                    row += [temp_df.loc['lastCPrice', strike], temp_df.loc['matchCTotQty', strike], temp_df.loc['openCInterest', strike]]

                    col1 += ['P', 'P', 'P']
                    col2 += [strike, strike, strike]
                    col3 += ['close', 'volume', 'oi']
                    row += [temp_df.loc['lastPPrice', strike], temp_df.loc['matchPTotQty', strike], temp_df.loc['openPInterest', strike]]

            df = pd.DataFrame(columns=[col1, col2, col3], data=[row])
            path = os.path.join(option_price_dir,'dce', name+'_intraday'+'.csv') 
            if os.path.exists(path):
                old_df = pd.read_csv(path, header=[0,1,2])
                old_df = pd.concat([old_df, df], axis=0)
                old_df.drop_duplicates(subset=[('time','time','time')], keep='last', inplace=True) # last
                old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
                old_df.sort_values(by = ('time','time','time'), inplace=True)
                old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
                old_df.to_csv(path, encoding='utf-8', index=False)
            else:
                df.to_csv(path, encoding='utf-8', index=False)

        path = os.path.join(option_price_dir, 'dce', variety+'_intraday_info'+'.csv')
        # print(df)
        if os.path.exists(path):
            info_df.to_csv(path, mode='a', encoding='utf-8', index=False, header=None)
        else:
            info_df.to_csv(path, encoding='utf-8', index=False)  


################## gfex ##################
def gfex_delay_data_time():
    now = datetime.datetime.now()
    hour = now.hour
    minute = now.minute

    if (hour < 9):
        dt = None
    elif (hour == 9) and (minute <= 1):
        dt = None
    elif (hour == 10) and ((minute >= 15) and (minute <= 31)):
        dt = None
    elif (hour == 11) and (minute >= 35):
        dt = None
    elif (hour == 12) or ((hour == 13) and (minute <= 31)):
        dt = None
    elif (hour == 15) and (minute <= 10):
        dt = datetime.datetime(year=now.year, month=now.month, day=now.day, hour=15, minute=0, second=0)
    elif (hour >= 15):
        dt = None
    else:
        dt = datetime.datetime(year=now.year, month=now.month, day=now.day, hour=now.hour, minute=now.minute, second=0) - pd.Timedelta(minutes=1)
    
    if dt is not None:
        t = dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        t = None

    return t


def update_future_intraday_price_gfex():
    se = requests.session()
    t = gfex_delay_data_time()
    # t = '2022-12-12 09:09:09'
    if t is not None:
        for variety in exchange_dict['gfex']:
            url = 'http://www.gfex.com.cn/gfexweb/Quote/getQuote_ftr'
            GFEX_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                            'Host': 'www.gfex.com.cn'}
            payload = {
                "varietyid": variety,
            }
            while(1):
                try:
                    r = se.post(url, headers=GFEX_HEADERS, data=payload)
                    data_json = r.json()
                    break
                except Exception as e:
                    print('GFEX FUTURE INTRADAT ERROR 2', e)
                    time.sleep(5)
            # print(data_json)
            temp_df = pd.DataFrame(data_json['contractQuote'])
            temp_df.loc['lastPrice',:].replace('--', None, inplace=True)
            temp_df.loc['matchTotQty',:].replace('--', '0', inplace=True)
            temp_df.loc['openInterest',:].replace('--', '0', inplace=True)
            print(variety)

            cols = temp_df.columns.tolist()
            n = len(cols)

            c1 = ['time','index','index','index']
            c1_add = ['c1','c2','c3','c4','c5','c6','c7','c8','c9']
            c2 = ['time','close','vol','oi']
            c2_add = ['inst_id','close','vol','oi']
            null_data = [None,None,None,None]
            for i in range(len(c1_add)):
                for j in range(len(c2_add)): # 
                    c1.append(c1_add[i])
                    c2.append(c2_add[j])

            df = pd.DataFrame(columns=[c1, c2])

            close = np.array(temp_df.loc['lastPrice', :], dtype=float)
            volumn = np.array(temp_df.loc['matchTotQty', :], dtype=float)
            oi = np.array(temp_df.loc['openInterest', :], dtype=float)
            nominator = 0
            denominator = 0
            for i in range(len(close)):
                if not(np.isnan(close[i])):
                    nominator = nominator + close[i]*oi[i]
                    denominator = denominator + oi[i]
            index_close = nominator/denominator
            index_volumn = np.nansum(volumn)
            index_oi = np.nansum(oi)

            row = [t]
            row += [index_close, index_volumn, index_oi]

            if (n >= 9):
                for i in range(9):
                    row += temp_df.loc[['contractID','lastPrice','matchTotQty','openInterest'], cols[i]].values.tolist()
            else:
                for i in range(n):
                    row += temp_df.loc[['contractID','lastPrice','matchTotQty','openInterest'], cols[i]].values.tolist()
                for i in range(9-n):
                    row += null_data

            df.loc[0] = row
            path = os.path.join(future_price_dir, 'gfex', variety+'_intraday'+'.csv') 
            if os.path.exists(path):
                old_df = pd.read_csv(path, header=[0,1])
                old_df = pd.concat([old_df, df], axis=0)
                old_df.drop_duplicates(subset=[('time','time')], keep='first', inplace=True) # last
                old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
                old_df.sort_values(by = ('time','time'), inplace=True)
                old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
                old_df.to_csv(path, encoding='utf-8', index=False)
            else:
                df.to_csv(path, encoding='utf-8', index=False)

    return t




def update_option_intraday_price_gfex(se=None, t=None):
    if se is None:
        se = requests.session()

    if t is None:
        print('GFEX t is None')
        return
    
    for variety in exchange_option_dict['gfex']:
        url = 'http://www.gfex.com.cn/gfexweb/Variety/getVariety_all'
        GFEX_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                        'Host': 'www.gfex.com.cn'}
        payload = {
            "varietyid": variety,
        }
        while(1):
            try:
                r = se.post(url, headers=GFEX_HEADERS, data=payload)
                data_json = r.json()
                namelist = data_json["contractIDList"]
                break
            except Exception as e:
                print('GFEX OPTION INTRADAT ERROR 1', e)
                time.sleep(5)
        
        inst_ids = ''
        for name in namelist:
            inst_ids += name
            inst_ids += ','
        info_df = pd.DataFrame(columns=['time', 'inst_ids'], data=[[t, inst_ids]])

        for name in namelist:
            print(name)
            url = 'http://www.gfex.com.cn/gfexweb/Quote/getQuote_opt'
            payload = {
                "contractid": name,
            }
            while(1):
                try:
                    r = se.post(url, headers=GFEX_HEADERS, data=payload)
                    data_json = r.json()
                    temp_df = pd.DataFrame(data_json['optionQuote'])
                    break
                except Exception as e:
                    print('GFEX OPTION INTRADAT ERROR 2', e)
                    time.sleep(5)

            if (len(temp_df) == 0):
                continue
            temp_df.loc['matchCTotQty',:].replace('--', '0', inplace=True)
            temp_df.loc['matchPTotQty',:].replace('--', '0', inplace=True)
            temp_df.loc['openCInterest',:].replace('--', '0', inplace=True)
            temp_df.loc['openPInterest',:].replace('--', '0', inplace=True)
            temp_df.loc['lastCPrice',:].replace('--', None, inplace=True)
            temp_df.loc['lastPPrice',:].replace('--', None, inplace=True)

            strikes = temp_df.columns.tolist()

            col1 = ['time']
            col2 = ['time']
            col3 = ['time']
            row = [t]

            for strike in strikes:
                col1 += ['C', 'C', 'C']
                col2 += [strike, strike, strike]
                col3 += ['close', 'volume', 'oi']
                row += [temp_df.loc['lastCPrice', strike], temp_df.loc['matchCTotQty', strike], temp_df.loc['openCInterest', strike]]

                col1 += ['P', 'P', 'P']
                col2 += [strike, strike, strike]
                col3 += ['close', 'volume', 'oi']
                row += [temp_df.loc['lastPPrice', strike], temp_df.loc['matchPTotQty', strike], temp_df.loc['openPInterest', strike]]
            
            df = pd.DataFrame(columns=[col1, col2, col3], data=[row])

            path = os.path.join(option_price_dir,'gfex', name+'_intraday'+'.csv') 
            if os.path.exists(path):
                old_df = pd.read_csv(path, header=[0,1,2])
                old_df = pd.concat([old_df, df], axis=0)
                old_df.drop_duplicates(subset=[('time','time','time')], keep='last', inplace=True) # last
                old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
                old_df.sort_values(by = ('time','time','time'), inplace=True)
                old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
                old_df.to_csv(path, encoding='utf-8', index=False)
            else:
                df.to_csv(path, encoding='utf-8', index=False)
            
        path = os.path.join(option_price_dir, 'gfex', variety+'_intraday_info'+'.csv')
        # print(df)
        if os.path.exists(path):
            info_df.to_csv(path, mode='a', encoding='utf-8', index=False, header=None)
        else:
            info_df.to_csv(path, encoding='utf-8', index=False)  




def update_future_intraday_price(exchange, se=None):
    global driver_fut_open
    if se is None:
        se = requests.session()

    if exchange == 'shfe':
        now = datetime.datetime.now()
        if (now.hour == 21) and (now.minute <= 1):
            return None
        url = 'https://www.shfe.com.cn/statements/delaymarket_all.html'
        SHFE_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)"}
        r = se.get(url, headers=SHFE_HEADERS)
        temp_df = pd.read_html(r.text)[0]

        cols = temp_df.columns.tolist()
        L = len(cols)
        t = temp_df.loc[0, cols[L-1]][:19]
        dt = pd.to_datetime(t, format='%Y-%m-%d %H:%M:%S')
        hour = dt.hour
        minute = dt.minute
        if (hour > 2 and hour < 9):
            dt = datetime.datetime(year=dt.year, month=dt.month, day=dt.day, hour=2, minute=30, second=0)
        if (hour >= 15 and hour < 20):
            dt = datetime.datetime(year=dt.year, month=dt.month, day=dt.day, hour=15, minute=0, second=0)
        
        t = dt.strftime('%Y-%m-%d %H:%M:%S')
        print(t)
        cols = temp_df.loc[1, :].tolist()

        temp_df = temp_df.loc[2:, :]
        temp_df.columns = cols
        temp_df = temp_df[['合约名称', '最新价', '成交量', '持仓量']]
        temp_df.rename(columns={'合约名称':'inst_id', '最新价':'close', '成交量':'vol', '持仓量':'oi'}, inplace=True)
        temp_df.reset_index(inplace=True, drop=True)


        inst_ids = np.array(temp_df['inst_id'], dtype=str)
        name_dict = {}
        for i in range((len(inst_ids))):
            if (inst_ids[i][2].isdigit()):
                name = inst_ids[i][:2]
            else:
                name = inst_ids[i][0]

            if not(name in name_dict):
                # start_idx, end_idx
                name_dict[name] = [i, i]
            else:
                name_dict[name][1] = i

        print(name_dict)
        c1 = ['time','index','index','index']
        c1_add = ['c1','c2','c3','c4','c5','c6','c7','c8','c9']
        c2 = ['time','close','vol','oi']
        c2_add = ['inst_id','close','vol','oi']
        null_data = [None,None,None,None]
        for i in range(len(c1_add)):
            for j in range(len(c2_add)): # 
                c1.append(c1_add[i])
                c2.append(c2_add[j])

        for name in name_dict:
            df = pd.DataFrame(columns=[c1, c2])
            row = [t]
            n = name_dict[name][1] + 1 - name_dict[name][0]

            # 指数合约数据
            tmp = temp_df.loc[name_dict[name][0]:name_dict[name][1], :]
            close = np.array(tmp['close'], dtype=float)
            volumn = np.array(tmp['vol'], dtype=float)
            oi = np.array(tmp['oi'], dtype=float)
            # 指数合约代码
            index_oi = np.sum(oi)
            if (index_oi > 0):
                nominator = 0
                denominator = 0
                for i in range(len(close)):
                    if not(np.isnan(close[i])):
                        nominator = nominator + close[i]*oi[i]
                        denominator = denominator + oi[i]
                index_close = nominator/denominator
                index_volumn = np.sum(volumn)
                row += [index_close, index_volumn, index_oi]
            else:
                row += [0, 0, 0]

            if (n >= 9):
                # 有至少9个合约
                row += (tmp.loc[name_dict[name][0]:name_dict[name][0]+8]).values.flatten().tolist()
            elif (n > 0):
                # 不足9个合约
                row += (tmp.loc[name_dict[name][0]:name_dict[name][0]+n-1]).values.flatten().tolist()
                for _ in range(9-n):
                    row += null_data

            df.loc[0] = row
            path = os.path.join(future_price_dir, exchange, name+'_intraday'+'.csv') 
            if os.path.exists(path):
                old_df = pd.read_csv(path, header=[0,1])
                old_df = pd.concat([old_df, df], axis=0)
                old_df.drop_duplicates(subset=[('time','time')], keep='first', inplace=True) # last
                old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
                old_df.sort_values(by = ('time','time'), inplace=True)
                old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
                old_df.to_csv(path, encoding='utf-8', index=False)
            else:
                df.to_csv(path, encoding='utf-8', index=False)

    elif exchange == 'czce':
        global option
        global driver
        now = datetime.datetime.now()
        if (now.hour < 9) or (now.hour == 23 and now.minute >= 10):
            return None
        url = 'http://www.czce.com.cn/cn/DFSStaticFiles/Future/Quotation/ChineseFutureQuotation.htm'
        CZCE_HEADERS = {"User-Agent": "User-Agent Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
                        'Host': 'www.czce.com.cn',
                        'Connection': 'keep-alive',
                        'Referer':	'http://www.czce.com.cn/cn/wzxx/yshq/qhyshq/H77070401index_1.htm',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                        }
        while (1):
            try:
                # r = se.get(url, headers=CZCE_HEADERS)
                # print(r.status_code)
                # temp_df = pd.read_html(r.content)[0]
                if driver_fut_open == False:
                    driver_fut.get('http://www.czce.com.cn/cn/DFSStaticFiles/Future/Quotation/ChineseFutureQuotation.htm')
                driver_fut.refresh()
                html = driver_fut.page_source
                driver_fut_open = True
                temp_df = pd.read_html(StringIO(html))[0]
                break
            except Exception as e:
                print(e)
                time.sleep(5)


        L = len(temp_df)
        t = temp_df.loc[L-1, '卖盘量']
        dt = pd.to_datetime(t, format='%Y-%m-%d %H:%M:%S')
        hour = dt.hour
        minute = dt.minute
        if (hour < 9):
            dt = datetime.datetime(year=dt.year, month=dt.month, day=dt.day, hour=2, minute=30, second=0)
            return None
        if (hour >= 15 and hour < 20):
            dt = datetime.datetime(year=dt.year, month=dt.month, day=dt.day, hour=15, minute=0, second=0)
            return None
        if (hour >= 23 and minute < 10):
            return None
        t = dt.strftime('%Y-%m-%d %H:%M:%S')
        print(t)

        temp_df = temp_df[['合约', '最新价', '成交量(手)', '持仓量']]
        temp_df.rename(columns={'合约':'inst_id', '最新价':'close', '成交量(手)':'vol', '持仓量':'oi'}, inplace=True)
        temp_df['close'] = temp_df['close'].replace('0',None)
        inst_ids = np.array(temp_df['inst_id'], dtype=str)
        name_dict = {}
        for i in range((len(inst_ids))):
            if len(inst_ids[i]) > 2:
                if (inst_ids[i][2].isdigit()):
                    name = inst_ids[i][:2]
                else:
                    name = inst_ids[i][0]

                if not(name in name_dict):
                    # start_idx, end_idx
                    name_dict[name] = [i, i]
                else:
                    name_dict[name][1] = i

        print(name_dict)
        c1 = ['time','index','index','index']
        c1_add = ['c1','c2','c3','c4','c5','c6','c7','c8','c9']
        c2 = ['time','close','vol','oi']
        c2_add = ['inst_id','close','vol','oi']
        null_data = [None,None,None,None]
        for i in range(len(c1_add)):
            for j in range(len(c2_add)): # 
                c1.append(c1_add[i])
                c2.append(c2_add[j])

        for name in name_dict:
            df = pd.DataFrame(columns=[c1, c2])
            row = [t]
            n = name_dict[name][1] + 1 - name_dict[name][0]

            # 指数合约数据
            tmp = temp_df.loc[name_dict[name][0]:name_dict[name][1], :]
            close = np.array(tmp['close'], dtype=float)
            volumn = np.array(tmp['vol'], dtype=float)
            oi = np.array(tmp['oi'], dtype=float)
            index_oi = np.sum(oi)
            if (index_oi > 0):
                nominator = 0
                denominator = 0
                for i in range(len(close)):
                    if not(np.isnan(close[i])):
                        nominator = nominator + close[i]*oi[i]
                        denominator = denominator + oi[i]
                index_close = nominator/denominator
                index_volumn = np.sum(volumn)
                row += [index_close, index_volumn, index_oi]
            else:
                row += [0, 0, 0]

            if (n >= 9):
                # 有至少9个合约
                row += (tmp.loc[name_dict[name][0]:name_dict[name][0]+8]).values.flatten().tolist()
            elif (n > 0):
                # 不足9个合约
                row += (tmp.loc[name_dict[name][0]:name_dict[name][0]+n-1]).values.flatten().tolist()
                for _ in range(9-n):
                    row += null_data

            # 有些品种没有夜盘
            if index_volumn > 1:
                df.loc[0] = row

                path = os.path.join(future_price_dir, 'czce', name+'_intraday'+'.csv') 
                if os.path.exists(path):
                    old_df = pd.read_csv(path, header=[0,1])
                    old_df = pd.concat([old_df, df], axis=0)
                    old_df.drop_duplicates(subset=[('time','time')], keep='first', inplace=True) # last
                    old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
                    old_df.sort_values(by = ('time','time'), inplace=True)
                    old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
                    old_df.to_csv(path, encoding='utf-8', index=False)
                else:
                    df.to_csv(path, encoding='utf-8', index=False)

    elif exchange == 'dce':
        t = update_future_intraday_price_dce()

    return t



def update_option_intraday_price(exchange, t=None, se=None):
    global driver_opt_open
    if se is None:
        se = requests.session()

    if exchange == 'cffex':
        n1 = 0
        n2 = 6
        n3 = 7
        n4 = 9
    elif exchange == 'shfe':
        n1 = 0
        n2 = 6
        n3 = 6
        n4 = 7
    elif exchange == 'czce':
        n1 = 0
        n2 = 5
        n3 = 5
        n4 = 6
    elif exchange == 'gfex':
        n1 = 0
        n2 = 6
        n3 = 7
        n4 = 9

    if exchange == 'shfe':
        if t is None:
            return
        vars = ['cu','au','ag','al','zn','ru','rb','sc','br']
        URL = 'https://www.shfe.com.cn/statements/delaymarket_{}Option.html'
        SHFE_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)"}
        for var in vars:
            print('OPTION INTRADAY: ', var)
            url = URL.format(var)
            while (1):
                try:
                    r = se.get(url, headers=SHFE_HEADERS)
                    temp_df = pd.read_html(r.text)[0] 
                    if len(temp_df) > 3:
                        break
                except Exception as e:
                    print(e, 'OPTION INTRADAY ERROR: ', var)
                    time.sleep(5)

            cols = temp_df.columns.tolist()
            L = len(cols)
            if t is None:
                t = temp_df.loc[0, cols[L-1]][:19]
                print(t)
                dt = pd.to_datetime(t, format='%Y-%m-%d %H:%M:%S')
                hour = dt.hour
                minute = dt.minute
                if ((hour >= 3 and hour < 9)):
                    dt = datetime.datetime(year=dt.year, month=dt.month, day=dt.day, hour=2, minute=30, second=0)
                if ((hour >= 15 and hour < 21)):
                    dt = datetime.datetime(year=dt.year, month=dt.month, day=dt.day, hour=15, minute=0, second=0)
                t = dt.strftime('%Y-%m-%d %H:%M:%S')
            # if ((hour > 2 and hour < 9) or (hour >= 15 and hour < 21)):
            #     return
            cols = temp_df.loc[1, :].tolist()

            temp_df = temp_df.loc[2:, :]
            temp_df.columns = cols
            inst_ids = np.array(temp_df['合约代码'], dtype=str)
            temp_df = temp_df[['最新价', '成交量', '持仓量']]
            temp_df.rename(columns={'最新价':'close', '成交量':'vol', '持仓量':'oi'}, inplace=True)
            temp_df.reset_index(inplace=True, drop=True)
            temp_df['close'] = temp_df['close'].replace('0',None)

            name_dict = {}
            info_df = pd.DataFrame(columns=['time', 'inst_ids'])
            info_df.loc[0, 'time'] = t
            names = ''
            for i in range((len(inst_ids))):
                name = inst_ids[i][n1:n2]
                if not(name in name_dict):
                    # 从 idx_offset 开始
                    name_dict[name] = [i, i]
                    names = names + name
                    names = names + ','
                else:
                    name_dict[name][1] = name_dict[name][1] + 1
            info_df.loc[0, 'inst_ids'] = names
            
            print(name_dict)
            for name in name_dict:
                col1 = ['time']
                col2 = ['time']
                col3 = ['time']
                row = [t]

                start_idx = name_dict[name][0]
                end_idx = name_dict[name][1]
                tmp = temp_df.loc[name_dict[name][0]:name_dict[name][1], :]
                row += tmp.values.flatten().tolist()
                
                for k in range(start_idx, end_idx+1):
                    otype = inst_ids[k][n3]
                    strike = inst_ids[k][n4:]
                    col1 += [otype, otype, otype]
                    col2 += [strike, strike, strike]
                    col3 += ['close', 'volume', 'oi']

                df = pd.DataFrame(columns=[col1, col2, col3], data=[row])

                path = os.path.join(option_price_dir, exchange, name+'_intraday'+'.csv') 
                if os.path.exists(path):
                    old_df = pd.read_csv(path, header=[0,1,2])
                    old_df = pd.concat([old_df, df], axis=0)
                    old_df.drop_duplicates(subset=[('time','time','time')], keep='last', inplace=True) # last
                    old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
                    old_df.sort_values(by = ('time','time','time'), inplace=True)
                    old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
                    old_df.to_csv(path, encoding='utf-8', index=False)
                else:
                    df.to_csv(path, encoding='utf-8', index=False)

            path = os.path.join(option_price_dir, exchange, var+'_intraday_info'+'.csv')
            # print(df)
            if os.path.exists(path):
                info_df.to_csv(path, mode='a', encoding='utf-8', index=False, header=None)
            else:
                info_df.to_csv(path, encoding='utf-8', index=False)  

    elif exchange == 'czce':
        if t is None:
            return
        
        while(1):
            try:
                # url = 'http://www.czce.com.cn/cn/DFSStaticFiles/Option/Quotation/ChineseOptionQuotation.htm'
                # CZCE_HEADERS = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                #                 "User-Agent": "User-Agent Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
                #                 'Host': 'www.czce.com.cn'}
                # r = se.get(url, headers=CZCE_HEADERS)
                # temp_df = pd.read_html(r.content)[0]
                if driver_opt_open == False:
                    driver_opt.get('http://www.czce.com.cn/cn/DFSStaticFiles/Option/Quotation/ChineseOptionQuotation.htm')
                driver_opt.refresh()
                html = driver_opt.page_source
                temp_df = pd.read_html(StringIO(html))[0]
                driver_opt_open = True
                break
            except Exception as e:
                print(e)
                time.sleep(5)


        L = len(temp_df)
        if t is None:
            t = temp_df.loc[L-1, '卖盘量']
            dt = pd.to_datetime(t, format='%Y-%m-%d %H:%M:%S')
            hour = dt.hour
            minute = dt.minute
            if (hour > 2 and hour < 9):
                dt = datetime.datetime(year=dt.year, month=dt.month, day=dt.day, hour=2, minute=30, second=0)
            if (hour >= 15 and hour < 20):
                dt = datetime.datetime(year=dt.year, month=dt.month, day=dt.day, hour=15, minute=0, second=0)
            if (hour >= 23 and hour < 20):
                dt = datetime.datetime(year=dt.year, month=dt.month, day=dt.day, hour=23, minute=0, second=0)
            t = dt.strftime('%Y-%m-%d %H:%M:%S')

        temp_df = temp_df[['合约', '最新价', '成交量(手)', '持仓量']]
        temp_df.rename(columns={'合约':'inst_id', '最新价':'close', '成交量(手)':'vol', '持仓量':'oi'}, inplace=True)


        for variety in exchange_option_dict['czce']:
            contract_df = temp_df[temp_df["inst_id"].str.contains(variety)]
            contract_df = contract_df.reset_index(drop=True)

            inst_ids = np.array(contract_df['inst_id'], dtype=str)
            vol = np.array(contract_df['vol'], dtype=float)
            contract_df = contract_df[['close', 'vol', 'oi']]
            contract_df['close'].replace()
            info_df = pd.DataFrame(columns=['time', 'inst_ids'])
            info_df.loc[0, 'time'] = t
            name_dict = {}
            names = ''
            for i in range((len(inst_ids))):
                name = inst_ids[i][:5]
                if not(name in name_dict):
                    # 从 idx_offset 开始
                    name_dict[name] = [i, i]
                    # names = names + name
                    # names = names + ','
                else:
                    name_dict[name][1] = name_dict[name][1] + 1
            # info_df.loc[0, 'inst_ids'] = names


            print(name_dict)
            for name in name_dict:
                col1 = ['time']
                col2 = ['time']
                col3 = ['time']
                row = [t]

                start_idx = name_dict[name][0]
                end_idx = name_dict[name][1]
                tmp = contract_df.loc[name_dict[name][0]:name_dict[name][1], :]
                row += tmp.values.flatten().tolist()
                volume_sum = 0

                for k in range(start_idx, end_idx+1):
                    otype = inst_ids[k][5]
                    strike = inst_ids[k][6:]
                    col1 += [otype, otype, otype]
                    col2 += [strike, strike, strike]
                    col3 += ['close', 'volume', 'oi']
                    volume_sum += vol[k]

                if volume_sum > 1:
                    names = names + name
                    names = names + ','
                    df = pd.DataFrame(columns=[col1, col2, col3], data=[row])

                    path = os.path.join(option_price_dir, 'czce', name+'_intraday'+'.csv') 
                    if os.path.exists(path):
                        old_df = pd.read_csv(path, header=[0,1,2])
                        old_df = pd.concat([old_df, df], axis=0)
                        old_df.drop_duplicates(subset=[('time','time','time')], keep='last', inplace=True) # last
                        old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
                        old_df.sort_values(by = ('time','time','time'), inplace=True)
                        old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
                        old_df.to_csv(path, encoding='utf-8', index=False)
                    else:
                        df.to_csv(path, encoding='utf-8', index=False)

            if len(names) > 2:
                info_df.loc[0, 'inst_ids'] = names
                path = os.path.join(option_price_dir, 'czce', variety+'_intraday_info'+'.csv')
                # print(df)
                if os.path.exists(path):
                    old_df = pd.read_csv(path)
                    old_df = pd.concat([old_df, info_df], axis=0)
                    old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
                    old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
                    old_df.sort_values(by = 'time', inplace=True)
                    old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
                    old_df.to_csv(path, encoding='utf-8', index=False)
                else:
                    info_df.to_csv(path, encoding='utf-8', index=False)  

    elif exchange == 'dce':
        update_option_intraday_price_dce(se, t)


def update_option_intraday_info_detail(exchange):
    for variety in exchange_option_dict[exchange]:
    # for variety in ['cu']:
        path = os.path.join(option_price_dir, exchange, variety+'_intraday_info'+'.csv')
        if not(os.path.exists(path)):
            return

        intraday_info_df = pd.read_csv(path)
        intraday_info_t = pd.DatetimeIndex(pd.to_datetime(intraday_info_df['time'], format='%Y-%m-%d %H:%M:%S'))
        intraday_info_last_line_time_dt = intraday_info_t[len(intraday_info_t)-1]

        # 
        path = os.path.join(future_price_dir, exchange, variety+'_intraday'+'.csv')
        intraday_fut_df = pd.read_csv(path, header=[0,1])
        intraday_fut_t = pd.DatetimeIndex(pd.to_datetime(intraday_fut_df['time']['time'], format='%Y-%m-%d %H:%M:%S'))

        # 最大成交量
        path = os.path.join(option_price_dir, exchange, variety+'_info'+'.csv')
        if not(os.path.exists(path)):
            return
        info_df = pd.read_csv(path)
        info_t = pd.DatetimeIndex(pd.to_datetime(info_df['time'], format='%Y-%m-%d'))
        maxv_contracts = np.array(info_df['dom1'])

        # 主力
        path = os.path.join(future_price_dir, exchange, variety+'_spot'+'.csv')
        if os.path.exists(path):
            spot_df = pd.read_csv(path)
            spot_t = pd.DatetimeIndex(pd.to_datetime(spot_df['time'], format='%Y-%m-%d'))
            dom_contracts = np.array(spot_df['dominant_contract'])
            spot = True
        else:
            spot = False
            spot_t = info_t
            dom_contracts = maxv_contracts

        path = os.path.join(option_price_dir, exchange, variety+'_intraday_info_detail'+'.csv')
        detail_last_line_time = get_last_line_time(path, '', None, 19, '%Y-%m-%d %H:%M:%S')
        if (detail_last_line_time is not None):
            detail_last_line_time_dt = pd.to_datetime(detail_last_line_time, format='%Y-%m-%d %H:%M:%S')

            if (detail_last_line_time_dt < intraday_info_last_line_time_dt):
                start_idx = np.where(intraday_info_t == detail_last_line_time_dt)[0][0] + 1
            else:
                continue
        else:
            start_idx = 0

        #
        # start_idx = np.where(info_t == detail_last_line_time_dt)[0][0]
        #
        opt_dict = {}
        for i in range(start_idx, len(intraday_info_df)):
            df = pd.DataFrame()
            t = intraday_info_t[i]
            df['time'] = [t.strftime('%Y-%m-%d %H:%M:%S')]
            df['total_put_volume'] = [0]
            df['total_call_volume'] = [0]
            df['total_put_oi'] = [0]
            df['total_call_oi'] = [0]
            df['index_fut_close'] = [0]
            df['total_fut_volume'] = [0]
            df['total_fut_oi'] = [0]

            df['dom'] = [np.nan]
            df['dom_put_volume'] = [np.nan]
            df['dom_call_volume'] = [np.nan]
            df['dom_put_oi'] = [np.nan]
            df['dom_call_oi'] = [np.nan]
            df['dom_fut_close'] = [np.nan]
            df['dom_fut_volume'] = [np.nan]
            df['dom_fut_oi'] = [np.nan]

            df['dom_put_volume_max1'] = [np.nan]
            df['dom_put_volume_max1_strike'] = [np.nan]
            df['dom_put_volume_max2'] = [np.nan]
            df['dom_put_volume_max2_strike'] = [np.nan]
            df['dom_put_volume_max3'] = [np.nan]
            df['dom_put_volume_max3_strike'] = [np.nan]
            df['dom_put_volume_max4'] = [np.nan]
            df['dom_put_volume_max4_strike'] = [np.nan]
            df['dom_put_volume_max5'] = [np.nan]
            df['dom_put_volume_max5_strike'] = [np.nan]

            df['dom_call_volume_max1'] = [np.nan]
            df['dom_call_volume_max1_strike'] = [np.nan]
            df['dom_call_volume_max2'] = [np.nan]
            df['dom_call_volume_max2_strike'] = [np.nan]
            df['dom_call_volume_max3'] = [np.nan]
            df['dom_call_volume_max3_strike'] = [np.nan]
            df['dom_call_volume_max4'] = [np.nan]
            df['dom_call_volume_max4_strike'] = [np.nan]
            df['dom_call_volume_max5'] = [np.nan]
            df['dom_call_volume_max5_strike'] = [np.nan]

            df['dom_put_oi_max1'] = [np.nan]
            df['dom_put_oi_max1_strike'] = [np.nan]
            df['dom_put_oi_max2'] = [np.nan]
            df['dom_put_oi_max2_strike'] = [np.nan]
            df['dom_put_oi_max3'] = [np.nan]
            df['dom_put_oi_max3_strike'] = [np.nan]
            df['dom_put_oi_max4'] = [np.nan]
            df['dom_put_oi_max4_strike'] = [np.nan]
            df['dom_put_oi_max5'] = [np.nan]
            df['dom_put_oi_max5_strike'] = [np.nan]

            df['dom_call_oi_max1'] = [np.nan]
            df['dom_call_oi_max1_strike'] = [np.nan]
            df['dom_call_oi_max2'] = [np.nan]
            df['dom_call_oi_max2_strike'] = [np.nan]
            df['dom_call_oi_max3'] = [np.nan]
            df['dom_call_oi_max3_strike'] = [np.nan]
            df['dom_call_oi_max4'] = [np.nan]
            df['dom_call_oi_max4_strike'] = [np.nan]
            df['dom_call_oi_max5'] = [np.nan]
            df['dom_call_oi_max5_strike'] = [np.nan]


            df['maxv'] = [np.nan]
            df['maxv_put_volume'] = [np.nan]
            df['maxv_call_volume'] = [np.nan]
            df['maxv_put_oi'] = [np.nan]
            df['maxv_call_oi'] = [np.nan]
            df['maxv_fut_close'] = [np.nan]
            df['maxv_fut_volume'] = [np.nan]
            df['maxv_fut_oi'] = [np.nan]

            df['maxv_put_volume_max1'] = [np.nan]
            df['maxv_put_volume_max1_strike'] = [np.nan]
            df['maxv_put_volume_max2'] = [np.nan]
            df['maxv_put_volume_max2_strike'] = [np.nan]
            df['maxv_put_volume_max3'] = [np.nan]
            df['maxv_put_volume_max3_strike'] = [np.nan]
            df['maxv_put_volume_max4'] = [np.nan]
            df['maxv_put_volume_max4_strike'] = [np.nan]
            df['maxv_put_volume_max5'] = [np.nan]
            df['maxv_put_volume_max5_strike'] = [np.nan]

            df['maxv_call_volume_max1'] = [np.nan]
            df['maxv_call_volume_max1_strike'] = [np.nan]
            df['maxv_call_volume_max2'] = [np.nan]
            df['maxv_call_volume_max2_strike'] = [np.nan]
            df['maxv_call_volume_max3'] = [np.nan]
            df['maxv_call_volume_max3_strike'] = [np.nan]
            df['maxv_call_volume_max4'] = [np.nan]
            df['maxv_call_volume_max4_strike'] = [np.nan]
            df['maxv_call_volume_max5'] = [np.nan]
            df['maxv_call_volume_max5_strike'] = [np.nan]

            df['maxv_put_oi_max1'] = [np.nan]
            df['maxv_put_oi_max1_strike'] = [np.nan]
            df['maxv_put_oi_max2'] = [np.nan]
            df['maxv_put_oi_max2_strike'] = [np.nan]
            df['maxv_put_oi_max3'] = [np.nan]
            df['maxv_put_oi_max3_strike'] = [np.nan]
            df['maxv_put_oi_max4'] = [np.nan]
            df['maxv_put_oi_max4_strike'] = [np.nan]
            df['maxv_put_oi_max5'] = [np.nan]
            df['maxv_put_oi_max5_strike'] = [np.nan]

            df['maxv_call_oi_max1'] = [np.nan]
            df['maxv_call_oi_max1_strike'] = [np.nan]
            df['maxv_call_oi_max2'] = [np.nan]
            df['maxv_call_oi_max2_strike'] = [np.nan]
            df['maxv_call_oi_max3'] = [np.nan]
            df['maxv_call_oi_max3_strike'] = [np.nan]
            df['maxv_call_oi_max4'] = [np.nan]
            df['maxv_call_oi_max4_strike'] = [np.nan]
            df['maxv_call_oi_max5'] = [np.nan]
            df['maxv_call_oi_max5_strike'] = [np.nan]


            inst_ids = intraday_info_df.loc[i, 'inst_ids'].split(',')
            inst_ids.remove('')

            L1 = len(info_t)
            L2 = len(spot_t)
            maxv_contract = None
            dom_contract = None
            while (L1 > 0 and L2 > 0):
                if (t >= info_t[L1-1]):
                    maxv_contract = maxv_contracts[L1-1]
                else:
                    L1 = L1 - 1

                if (t >= spot_t[L2-1]):
                    dom_contract = dom_contracts[L2-1]
                else:
                    L2 = L2 - 1

                if (maxv_contract is not None) and (dom_contract is not None):
                    break

            df['dom'] = dom_contract
            df['maxv'] = maxv_contract

            try:
                w = np.where(intraday_fut_t == intraday_info_t[i])[0][0]
            except:
                continue
            fut_temp_df = intraday_fut_df.loc[w,:]
            df['index_fut_close'] = fut_temp_df['index']['close']
            df['total_fut_volume'] = fut_temp_df.loc[pd.IndexSlice[['c1','c2','c3','c4','c5','c6','c7','c8','c9'], 'vol']].sum()
            df['total_fut_oi'] = fut_temp_df.loc[pd.IndexSlice[['c1','c2','c3','c4','c5','c6','c7','c8','c9'], 'oi']].sum()

            for inst_id in inst_ids:
                if (inst_id == ''):
                    continue
                if not(inst_id in opt_dict):
                    try:
                        path3 = os.path.join(option_price_dir, exchange, inst_id+'_intraday'+'.csv')
                    except:
                        continue
                    opt_df = pd.read_csv(path3, header=[0,1,2])
                    opt_t = pd.DatetimeIndex(pd.to_datetime(opt_df['time']['time']['time'], format='%Y-%m-%d %H:%M:%S'))
                    strike = get_full_strike_price(opt_df)
                    opt_dict[inst_id] = [opt_df, opt_t, strike]

                opt_df = opt_dict[inst_id][0]
                opt_t = opt_dict[inst_id][1]
                strike = opt_dict[inst_id][2]

                try:
                    w = np.where(opt_t == intraday_info_t[i])[0][0]
                except:
                    print('opt_t == info_t[i], ', inst_id, intraday_info_t[i])
                    continue
                temp_df = opt_df.loc[w,:]

                # fut
                if (inst_id == dom_contract):
                    for c in ['c1','c2','c3','c4','c5','c6','c7','c8','c9']:
                        if (fut_temp_df[c]['inst_id'] == inst_id):
                            df['dom_fut_close'] = fut_temp_df[c]['close']
                            df['dom_fut_volume'] = fut_temp_df[c]['vol']
                            df['dom_fut_oi'] = fut_temp_df[c]['oi']
                            break
                if (inst_id == maxv_contract):
                    for c in ['c1','c2','c3','c4','c5','c6','c7','c8','c9']:
                        if (fut_temp_df[c]['inst_id'] == inst_id):
                            df['maxv_fut_close'] = fut_temp_df[c]['close']
                            df['maxv_fut_volume'] = fut_temp_df[c]['vol']
                            df['maxv_fut_oi'] = fut_temp_df[c]['oi']
                            break

                # volume
                put_volume = temp_df.loc[pd.IndexSlice['P', :, 'volume']].sum()
                call_volume = temp_df.loc[pd.IndexSlice['C', :, 'volume']].sum()
                df['total_put_volume'] += put_volume
                df['total_call_volume'] += call_volume
                if (inst_id == dom_contract) or (inst_id == maxv_contract):
                    if (inst_id == dom_contract):
                        df['dom_put_volume'] = put_volume
                        df['dom_call_volume'] = call_volume
                    if (inst_id == maxv_contract):
                        df['maxv_put_volume'] = put_volume
                        df['maxv_call_volume'] = call_volume

                    tmp = temp_df.loc[pd.IndexSlice['P', :, 'volume']]
                    P_volume = tmp.replace(np.nan, -1.0)
                    idx = P_volume.index
                    P_volume = np.array(P_volume)
                    sort = np.argsort(P_volume)
                    P_volume = P_volume[sort]
                    idx = idx[sort]
                    if (inst_id == dom_contract) and (len(idx) >= 5):
                        df['dom_put_volume_max1'] = [P_volume[-1]]
                        df['dom_put_volume_max1_strike'] = [idx[-1]]
                        df['dom_put_volume_max2'] = [P_volume[-2]]
                        df['dom_put_volume_max2_strike'] = [idx[-2]]
                        df['dom_put_volume_max3'] = [P_volume[-3]]
                        df['dom_put_volume_max3_strike'] = [idx[-3]]
                        df['dom_put_volume_max4'] = [P_volume[-4]]
                        df['dom_put_volume_max4_strike'] = [idx[-4]]
                        df['dom_put_volume_max5'] = [P_volume[-5]]
                        df['dom_put_volume_max5_strike'] = [idx[-5]]
                    if (inst_id == maxv_contract) and (len(idx) >= 5):
                        df['maxv_put_volume_max1'] = [P_volume[-1]]
                        df['maxv_put_volume_max1_strike'] = [idx[-1]]
                        df['maxv_put_volume_max2'] = [P_volume[-2]]
                        df['maxv_put_volume_max2_strike'] = [idx[-2]]
                        df['maxv_put_volume_max3'] = [P_volume[-3]]
                        df['maxv_put_volume_max3_strike'] = [idx[-3]]
                        df['maxv_put_volume_max4'] = [P_volume[-4]]
                        df['maxv_put_volume_max4_strike'] = [idx[-4]]
                        df['maxv_put_volume_max5'] = [P_volume[-5]]
                        df['maxv_put_volume_max5_strike'] = [idx[-5]]

                    tmp = temp_df.loc[pd.IndexSlice['C', :, 'volume']]
                    C_volume = tmp.replace(np.nan, -1.0)
                    idx = C_volume.index
                    C_volume = np.array(C_volume)
                    sort = np.argsort(C_volume)
                    C_volume = C_volume[sort]
                    idx = idx[sort]
                    if (inst_id == dom_contract) and (len(idx) >= 5):
                        df['dom_call_volume_max1'] = [C_volume[-1]]
                        df['dom_call_volume_max1_strike'] = [idx[-1]]
                        df['dom_call_volume_max2'] = [C_volume[-2]]
                        df['dom_call_volume_max2_strike'] = [idx[-2]]
                        df['dom_call_volume_max3'] = [C_volume[-3]]
                        df['dom_call_volume_max3_strike'] = [idx[-3]]
                        df['dom_call_volume_max4'] = [C_volume[-4]]
                        df['dom_call_volume_max4_strike'] = [idx[-4]]
                        df['dom_call_volume_max5'] = [C_volume[-5]]
                        df['dom_call_volume_max5_strike'] = [idx[-5]]
                    if (inst_id == maxv_contract) and (len(idx) >= 5):
                        df['maxv_call_volume_max1'] = [C_volume[-1]]
                        df['maxv_call_volume_max1_strike'] = [idx[-1]]
                        df['maxv_call_volume_max2'] = [C_volume[-2]]
                        df['maxv_call_volume_max2_strike'] = [idx[-2]]
                        df['maxv_call_volume_max3'] = [C_volume[-3]]
                        df['maxv_call_volume_max3_strike'] = [idx[-3]]
                        df['maxv_call_volume_max4'] = [C_volume[-4]]
                        df['maxv_call_volume_max4_strike'] = [idx[-4]]
                        df['maxv_call_volume_max5'] = [C_volume[-5]]
                        df['maxv_call_volume_max5_strike'] = [idx[-5]]

                # oi
                put_oi = temp_df.loc[pd.IndexSlice['P', :, 'oi']].sum()
                call_oi = temp_df.loc[pd.IndexSlice['C', :, 'oi']].sum()
                df['total_put_oi'] += put_oi
                df['total_call_oi'] += call_oi
                if (inst_id == dom_contract) or (inst_id == maxv_contract):
                    if (inst_id == dom_contract):
                        df['dom_put_oi'] = put_oi
                        df['dom_call_oi'] = call_oi
                    if (inst_id == maxv_contract):
                        df['maxv_put_oi'] = put_oi
                        df['maxv_call_oi'] = call_oi

                    tmp = temp_df.loc[pd.IndexSlice['P', :, 'oi']]
                    P_oi = tmp.replace(np.nan, -1.0)
                    idx = P_oi.index
                    P_oi = np.array(P_oi)
                    sort = np.argsort(P_oi)
                    P_oi = P_oi[sort]
                    idx = idx[sort]
                    if (inst_id == dom_contract) and (len(idx) >= 5):
                        df['dom_put_oi_max1'] = [P_oi[-1]]
                        df['dom_put_oi_max1_strike'] = [idx[-1]]
                        df['dom_put_oi_max2'] = [P_oi[-2]]
                        df['dom_put_oi_max2_strike'] = [idx[-2]]
                        df['dom_put_oi_max3'] = [P_oi[-3]]
                        df['dom_put_oi_max3_strike'] = [idx[-3]]
                        df['dom_put_oi_max4'] = [P_oi[-4]]
                        df['dom_put_oi_max4_strike'] = [idx[-4]]
                        df['dom_put_oi_max5'] = [P_oi[-5]]
                        df['dom_put_oi_max5_strike'] = [idx[-5]]
                    if (inst_id == maxv_contract) and (len(idx) >= 5):
                        df['maxv_put_oi_max1'] = [P_oi[-1]]
                        df['maxv_put_oi_max1_strike'] = [idx[-1]]
                        df['maxv_put_oi_max2'] = [P_oi[-2]]
                        df['maxv_put_oi_max2_strike'] = [idx[-2]]
                        df['maxv_put_oi_max3'] = [P_oi[-3]]
                        df['maxv_put_oi_max3_strike'] = [idx[-3]]
                        df['maxv_put_oi_max4'] = [P_oi[-4]]
                        df['maxv_put_oi_max4_strike'] = [idx[-4]]
                        df['maxv_put_oi_max5'] = [P_oi[-5]]
                        df['maxv_put_oi_max5_strike'] = [idx[-5]]

                    tmp = temp_df.loc[pd.IndexSlice['C', :, 'oi']]
                    C_oi = tmp.replace(np.nan, -1.0)
                    idx = C_oi.index
                    C_oi = np.array(C_oi)
                    sort = np.argsort(C_oi)
                    C_oi = C_oi[sort]
                    idx = idx[sort]
                    if (inst_id == dom_contract) and (len(idx) >= 5):
                        df['dom_call_oi_max1'] = [C_oi[-1]]
                        df['dom_call_oi_max1_strike'] = [idx[-1]]
                        df['dom_call_oi_max2'] = [C_oi[-2]]
                        df['dom_call_oi_max2_strike'] = [idx[-2]]
                        df['dom_call_oi_max3'] = [C_oi[-3]]
                        df['dom_call_oi_max3_strike'] = [idx[-3]]
                        df['dom_call_oi_max4'] = [C_oi[-4]]
                        df['dom_call_oi_max4_strike'] = [idx[-4]]
                        df['dom_call_oi_max5'] = [C_oi[-5]]
                        df['dom_call_oi_max5_strike'] = [idx[-5]]
                    if (inst_id == maxv_contract) and (len(idx) >= 5):
                        df['maxv_call_oi_max1'] = [C_oi[-1]]
                        df['maxv_call_oi_max1_strike'] = [idx[-1]]
                        df['maxv_call_oi_max2'] = [C_oi[-2]]
                        df['maxv_call_oi_max2_strike'] = [idx[-2]]
                        df['maxv_call_oi_max3'] = [C_oi[-3]]
                        df['maxv_call_oi_max3_strike'] = [idx[-3]]
                        df['maxv_call_oi_max4'] = [C_oi[-4]]
                        df['maxv_call_oi_max4_strike'] = [idx[-4]]
                        df['maxv_call_oi_max5'] = [C_oi[-5]]
                        df['maxv_call_oi_max5_strike'] = [idx[-5]]

            path = os.path.join(option_price_dir, exchange, variety+'_intraday_info_detail'+'.csv')
            # print(df)
            if os.path.exists(path):
                old_df = pd.read_csv(path)
                old_df = pd.concat([old_df, df], axis=0)
                old_df.drop_duplicates(subset=['time'], keep='first', inplace=True)
                old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
                old_df.sort_values(by = 'time', inplace=True)
                old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
                old_df.to_csv(path, encoding='utf-8', index=False)
            else:
                df.to_csv(path, encoding='utf-8', index=False)  


def get_option_intraday_atm_price(exchange, inst_id, fut_price):
    path = os.path.join(option_price_dir, exchange, inst_id+'_intraday'+'.csv')
    df = pd.read_csv(path, header=[0,1,2])
    t = pd.DatetimeIndex(pd.to_datetime(df['time']['time']['time'], format='%Y-%m-%d %H:%M:%S'))

    col = df.columns.tolist()
    strike = [(col[i][1]) for i in range(len(col)) if col[i][0] == 'P']

    res = []
    for i in strike:
        if i not in res:
            res.append(i)
    strikes_str = np.array(res, dtype=str)
    strikes = np.array(strikes_str, dtype=float)

    sort = np.argsort(strikes)
    strikes = strikes[sort]
    strikes_str = strikes_str[sort]

    w_call = np.where(strikes >= fut_price)[0][0]
    w_put = np.where(strikes <= fut_price)[0][-1]

    call_strike_str = strikes_str[w_call]
    put_strike_str = strikes_str[w_put]

    call_atm_price = np.array(df.loc[:, pd.IndexSlice['C', call_strike_str, 'close']], dtype=float)
    put_atm_price = np.array(df.loc[:, pd.IndexSlice['P', put_strike_str, 'close']], dtype=float)

    call_atm_price[call_atm_price == 0] = np.nan
    put_atm_price[put_atm_price == 0] = np.nan
    # print(t, call_atm_price, put_atm_price)
    # print(fut_price, call_strike_str, put_strike_str)
    # exit()
    return t, call_atm_price, put_atm_price



def plot_intraday_dominant_option_datas(exchange, variety):
    path3 = os.path.join(option_price_dir, exchange, variety+'_intraday_info_detail'+'.csv')
    if not os.path.exists(path3):
        return
    df = pd.read_csv(path3)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S'))

    index_fut_close = np.array(df['index_fut_close'], dtype=float)

    dom = np.array(df['dom'], dtype=str)
    dom_contract = dom[-1]
    dom_fut_close = np.array(df['dom_fut_close'], dtype=float)

    # t1, dom_call_atm_price, dom_put_atm_price = get_option_intraday_atm_price(exchange, dom_contract, dom_fut_close[-1])

    dom_put_volume_max1= np.array(df['dom_put_volume_max1'], dtype=float)
    dom_put_volume_max1_strike= np.array(df['dom_put_volume_max1_strike'], dtype=float)
    dom_put_volume_max2= np.array(df['dom_put_volume_max2'], dtype=float)
    dom_put_volume_max2_strike= np.array(df['dom_put_volume_max2_strike'], dtype=float)
    dom_put_volume_max3= np.array(df['dom_put_volume_max3'], dtype=float)
    dom_put_volume_max3_strike= np.array(df['dom_put_volume_max3_strike'], dtype=float)
    dom_put_volume_max4= np.array(df['dom_put_volume_max4'], dtype=float)
    dom_put_volume_max4_strike= np.array(df['dom_put_volume_max4_strike'], dtype=float)
    dom_put_volume_max5= np.array(df['dom_put_volume_max5'], dtype=float)
    dom_put_volume_max5_strike= np.array(df['dom_put_volume_max5_strike'], dtype=float)

    dom_call_volume_max1= np.array(df['dom_call_volume_max1'], dtype=float)
    dom_call_volume_max1_strike= np.array(df['dom_call_volume_max1_strike'], dtype=float)
    dom_call_volume_max2= np.array(df['dom_call_volume_max2'], dtype=float)
    dom_call_volume_max2_strike= np.array(df['dom_call_volume_max2_strike'], dtype=float)
    dom_call_volume_max3= np.array(df['dom_call_volume_max3'], dtype=float)
    dom_call_volume_max3_strike= np.array(df['dom_call_volume_max3_strike'], dtype=float)
    dom_call_volume_max4= np.array(df['dom_call_volume_max4'], dtype=float)
    dom_call_volume_max4_strike= np.array(df['dom_call_volume_max4_strike'], dtype=float)
    dom_call_volume_max5= np.array(df['dom_call_volume_max5'], dtype=float)
    dom_call_volume_max5_strike= np.array(df['dom_call_volume_max5_strike'], dtype=float)

    dom_put_oi_max1= np.array(df['dom_put_oi_max1'], dtype=float)
    dom_put_oi_max1_strike= np.array(df['dom_put_oi_max1_strike'], dtype=float)
    dom_put_oi_max2= np.array(df['dom_put_oi_max2'], dtype=float)
    dom_put_oi_max2_strike= np.array(df['dom_put_oi_max2_strike'], dtype=float)
    dom_put_oi_max3= np.array(df['dom_put_oi_max3'], dtype=float)
    dom_put_oi_max3_strike= np.array(df['dom_put_oi_max3_strike'], dtype=float)
    dom_put_oi_max4= np.array(df['dom_put_oi_max4'], dtype=float)
    dom_put_oi_max4_strike= np.array(df['dom_put_oi_max4_strike'], dtype=float)
    dom_put_oi_max5= np.array(df['dom_put_oi_max5'], dtype=float)
    dom_put_oi_max5_strike= np.array(df['dom_put_oi_max5_strike'], dtype=float)

    dom_call_oi_max1= np.array(df['dom_call_oi_max1'], dtype=float)
    dom_call_oi_max1_strike= np.array(df['dom_call_oi_max1_strike'], dtype=float)
    dom_call_oi_max2= np.array(df['dom_call_oi_max2'], dtype=float)
    dom_call_oi_max2_strike= np.array(df['dom_call_oi_max2_strike'], dtype=float)
    dom_call_oi_max3= np.array(df['dom_call_oi_max3'], dtype=float)
    dom_call_oi_max3_strike= np.array(df['dom_call_oi_max3_strike'], dtype=float)
    dom_call_oi_max4= np.array(df['dom_call_oi_max4'], dtype=float)
    dom_call_oi_max4_strike= np.array(df['dom_call_oi_max4_strike'], dtype=float)
    dom_call_oi_max5= np.array(df['dom_call_oi_max5'], dtype=float)
    dom_call_oi_max5_strike= np.array(df['dom_call_oi_max5_strike'], dtype=float)

    # 期权主力 
    maxv = np.array(df['maxv'], dtype=str)
    maxv_contract = maxv[-1]
    maxv_fut_close = np.array(df['maxv_fut_close'], dtype=float)

    # t2, maxv_call_atm_price, maxv_put_atm_price = get_option_intraday_atm_price(exchange, maxv_contract, maxv_fut_close[-1])

    maxv_put_volume_max1= np.array(df['maxv_put_volume_max1'], dtype=float)
    maxv_put_volume_max1_strike= np.array(df['maxv_put_volume_max1_strike'], dtype=float)
    maxv_put_volume_max2= np.array(df['maxv_put_volume_max2'], dtype=float)
    maxv_put_volume_max2_strike= np.array(df['maxv_put_volume_max2_strike'], dtype=float)
    maxv_put_volume_max3= np.array(df['maxv_put_volume_max3'], dtype=float)
    maxv_put_volume_max3_strike= np.array(df['maxv_put_volume_max3_strike'], dtype=float)
    maxv_put_volume_max4= np.array(df['maxv_put_volume_max4'], dtype=float)
    maxv_put_volume_max4_strike= np.array(df['maxv_put_volume_max4_strike'], dtype=float)
    maxv_put_volume_max5= np.array(df['maxv_put_volume_max5'], dtype=float)
    maxv_put_volume_max5_strike= np.array(df['maxv_put_volume_max5_strike'], dtype=float)

    maxv_call_volume_max1= np.array(df['maxv_call_volume_max1'], dtype=float)
    maxv_call_volume_max1_strike= np.array(df['maxv_call_volume_max1_strike'], dtype=float)
    maxv_call_volume_max2= np.array(df['maxv_call_volume_max2'], dtype=float)
    maxv_call_volume_max2_strike= np.array(df['maxv_call_volume_max2_strike'], dtype=float)
    maxv_call_volume_max3= np.array(df['maxv_call_volume_max3'], dtype=float)
    maxv_call_volume_max3_strike= np.array(df['maxv_call_volume_max3_strike'], dtype=float)
    maxv_call_volume_max4= np.array(df['maxv_call_volume_max4'], dtype=float)
    maxv_call_volume_max4_strike= np.array(df['maxv_call_volume_max4_strike'], dtype=float)
    maxv_call_volume_max5= np.array(df['maxv_call_volume_max5'], dtype=float)
    maxv_call_volume_max5_strike= np.array(df['maxv_call_volume_max5_strike'], dtype=float)

    maxv_put_oi_max1= np.array(df['maxv_put_oi_max1'], dtype=float)
    maxv_put_oi_max1_strike= np.array(df['maxv_put_oi_max1_strike'], dtype=float)
    maxv_put_oi_max2= np.array(df['maxv_put_oi_max2'], dtype=float)
    maxv_put_oi_max2_strike= np.array(df['maxv_put_oi_max2_strike'], dtype=float)
    maxv_put_oi_max3= np.array(df['maxv_put_oi_max3'], dtype=float)
    maxv_put_oi_max3_strike= np.array(df['maxv_put_oi_max3_strike'], dtype=float)
    maxv_put_oi_max4= np.array(df['maxv_put_oi_max4'], dtype=float)
    maxv_put_oi_max4_strike= np.array(df['maxv_put_oi_max4_strike'], dtype=float)
    maxv_put_oi_max5= np.array(df['maxv_put_oi_max5'], dtype=float)
    maxv_put_oi_max5_strike= np.array(df['maxv_put_oi_max5_strike'], dtype=float)

    maxv_call_oi_max1= np.array(df['maxv_call_oi_max1'], dtype=float)
    maxv_call_oi_max1_strike= np.array(df['maxv_call_oi_max1_strike'], dtype=float)
    maxv_call_oi_max2= np.array(df['maxv_call_oi_max2'], dtype=float)
    maxv_call_oi_max2_strike= np.array(df['maxv_call_oi_max2_strike'], dtype=float)
    maxv_call_oi_max3= np.array(df['maxv_call_oi_max3'], dtype=float)
    maxv_call_oi_max3_strike= np.array(df['maxv_call_oi_max3_strike'], dtype=float)
    maxv_call_oi_max4= np.array(df['maxv_call_oi_max4'], dtype=float)
    maxv_call_oi_max4_strike= np.array(df['maxv_call_oi_max4_strike'], dtype=float)
    maxv_call_oi_max5= np.array(df['maxv_call_oi_max5'], dtype=float)
    maxv_call_oi_max5_strike= np.array(df['maxv_call_oi_max5_strike'], dtype=float)

    total_put_volume = np.array(df['total_put_volume'], dtype=float)
    total_call_volume = np.array(df['total_call_volume'], dtype=float)
    total_fut_volume = np.array(df['total_fut_volume'], dtype=float)
    total_put_oi = np.array(df['total_put_oi'], dtype=float)
    total_call_oi = np.array(df['total_call_oi'], dtype=float)
    total_fut_oi = np.array(df['total_fut_oi'], dtype=float)

    # t3, volume_ratio = data_div(t, total_put_volume+total_call_volume, t, total_fut_volume)
    # t4, oi_ratio = data_div(t, total_put_oi+total_call_oi, t, total_fut_oi)


    # 主力 volume
    dom_put_volume_max = np.vstack((dom_put_volume_max1, dom_put_volume_max2, dom_put_volume_max3, dom_put_volume_max4, dom_put_volume_max5))
    dom_put_volume_max_strike = np.vstack((dom_put_volume_max1_strike, dom_put_volume_max2_strike, dom_put_volume_max3_strike, dom_put_volume_max4_strike, dom_put_volume_max5_strike))
    sort = np.argsort(dom_put_volume_max_strike, axis=0)
    dom_put_volume_max_strike = np.take_along_axis(dom_put_volume_max_strike, sort, axis=0)
    dom_put_volume_max = np.take_along_axis(dom_put_volume_max, sort, axis=0)
    dom_put_volume_avg_strike = np.sum(dom_put_volume_max[2:5, :]*dom_put_volume_max_strike[2:5, :], axis=0) / np.sum(dom_put_volume_max[2:5, :], axis=0)
    
    dom_call_volume_max = np.vstack((dom_call_volume_max1, dom_call_volume_max2, dom_call_volume_max3, dom_call_volume_max4, dom_call_volume_max5))
    dom_call_volume_max_strike = np.vstack((dom_call_volume_max1_strike, dom_call_volume_max2_strike, dom_call_volume_max3_strike, dom_call_volume_max4_strike, dom_call_volume_max5_strike))
    sort = np.argsort(dom_call_volume_max_strike, axis=0)
    dom_call_volume_max_strike = np.take_along_axis(dom_call_volume_max_strike, sort, axis=0)
    dom_call_volume_max = np.take_along_axis(dom_call_volume_max, sort, axis=0)
    dom_call_volume_avg_strike = np.sum(dom_call_volume_max[0:3, :]*dom_call_volume_max_strike[0:3, :], axis=0) / np.sum(dom_call_volume_max[0:3, :], axis=0)

    # 成交量最大 volume
    maxv_put_volume_max = np.vstack((maxv_put_volume_max1, maxv_put_volume_max2, maxv_put_volume_max3, maxv_put_volume_max4, maxv_put_volume_max5))
    maxv_put_volume_max_strike = np.vstack((maxv_put_volume_max1_strike, maxv_put_volume_max2_strike, maxv_put_volume_max3_strike, maxv_put_volume_max4_strike, maxv_put_volume_max5_strike))
    sort = np.argsort(maxv_put_volume_max_strike, axis=0)
    maxv_put_volume_max_strike = np.take_along_axis(maxv_put_volume_max_strike, sort, axis=0)
    maxv_put_volume_max = np.take_along_axis(maxv_put_volume_max, sort, axis=0)
    maxv_put_volume_avg_strike = np.sum(maxv_put_volume_max[2:5, :]*maxv_put_volume_max_strike[2:5, :], axis=0) / np.sum(maxv_put_volume_max[2:5, :], axis=0)
    
    maxv_call_volume_max = np.vstack((maxv_call_volume_max1, maxv_call_volume_max2, maxv_call_volume_max3, maxv_call_volume_max4, maxv_call_volume_max5))
    maxv_call_volume_max_strike = np.vstack((maxv_call_volume_max1_strike, maxv_call_volume_max2_strike, maxv_call_volume_max3_strike, maxv_call_volume_max4_strike, maxv_call_volume_max5_strike))
    sort = np.argsort(maxv_call_volume_max_strike, axis=0)
    maxv_call_volume_max_strike = np.take_along_axis(maxv_call_volume_max_strike, sort, axis=0)
    maxv_call_volume_max = np.take_along_axis(maxv_call_volume_max, sort, axis=0)
    maxv_call_volume_avg_strike = np.sum(maxv_call_volume_max[0:3, :]*maxv_call_volume_max_strike[0:3, :], axis=0) / np.sum(maxv_call_volume_max[0:3, :], axis=0)
    

    # 主力 oi
    dom_put_oi_max = np.vstack((dom_put_oi_max1, dom_put_oi_max2, dom_put_oi_max3, dom_put_oi_max4, dom_put_oi_max5))
    dom_put_oi_max_strike = np.vstack((dom_put_oi_max1_strike, dom_put_oi_max2_strike, dom_put_oi_max3_strike, dom_put_oi_max4_strike, dom_put_oi_max5_strike))
    sort = np.argsort(dom_put_oi_max_strike, axis=0)
    dom_put_oi_max_strike = np.take_along_axis(dom_put_oi_max_strike, sort, axis=0)
    dom_put_oi_max = np.take_along_axis(dom_put_oi_max, sort, axis=0)
    dom_put_oi_avg_strike = np.sum(dom_put_oi_max[2:5, :]*dom_put_oi_max_strike[2:5, :], axis=0) / np.sum(dom_put_oi_max[2:5, :], axis=0)
    
    dom_call_oi_max = np.vstack((dom_call_oi_max1, dom_call_oi_max2, dom_call_oi_max3, dom_call_oi_max4, dom_call_oi_max5))
    dom_call_oi_max_strike = np.vstack((dom_call_oi_max1_strike, dom_call_oi_max2_strike, dom_call_oi_max3_strike, dom_call_oi_max4_strike, dom_call_oi_max5_strike))
    sort = np.argsort(dom_call_oi_max_strike, axis=0)
    dom_call_oi_max_strike = np.take_along_axis(dom_call_oi_max_strike, sort, axis=0)
    dom_call_oi_max = np.take_along_axis(dom_call_oi_max, sort, axis=0)
    dom_call_oi_avg_strike = np.sum(dom_call_oi_max[0:3, :]*dom_call_oi_max_strike[0:3, :], axis=0) / np.sum(dom_call_oi_max[0:3, :], axis=0)

    # 成交量最大 oi
    maxv_put_oi_max = np.vstack((maxv_put_oi_max1, maxv_put_oi_max2, maxv_put_oi_max3, maxv_put_oi_max4, maxv_put_oi_max5))
    maxv_put_oi_max_strike = np.vstack((maxv_put_oi_max1_strike, maxv_put_oi_max2_strike, maxv_put_oi_max3_strike, maxv_put_oi_max4_strike, maxv_put_oi_max5_strike))
    sort = np.argsort(maxv_put_oi_max_strike, axis=0)
    maxv_put_oi_max_strike = np.take_along_axis(maxv_put_oi_max_strike, sort, axis=0)
    maxv_put_oi_max = np.take_along_axis(maxv_put_oi_max, sort, axis=0)
    maxv_put_oi_avg_strike = np.sum(maxv_put_oi_max[2:5, :]*maxv_put_oi_max_strike[2:5, :], axis=0) / np.sum(maxv_put_oi_max[2:5, :], axis=0)
    
    maxv_call_oi_max = np.vstack((maxv_call_oi_max1, maxv_call_oi_max2, maxv_call_oi_max3, maxv_call_oi_max4, maxv_call_oi_max5))
    maxv_call_oi_max_strike = np.vstack((maxv_call_oi_max1_strike, maxv_call_oi_max2_strike, maxv_call_oi_max3_strike, maxv_call_oi_max4_strike, maxv_call_oi_max5_strike))
    sort = np.argsort(maxv_call_oi_max_strike, axis=0)
    maxv_call_oi_max_strike = np.take_along_axis(maxv_call_oi_max_strike, sort, axis=0)
    maxv_call_oi_max = np.take_along_axis(maxv_call_oi_max, sort, axis=0)
    maxv_call_oi_avg_strike = np.sum(maxv_call_oi_max[0:3, :]*maxv_call_oi_max_strike[0:3, :], axis=0) / np.sum(maxv_call_oi_max[0:3, :], axis=0)
    

    # 
    w1 = np.where(dom_put_oi_max_strike[2,:] <= dom_call_oi_max_strike[0,:])[0]
    w2 = np.where(maxv_put_oi_max_strike[2,:] <= maxv_call_oi_max_strike[0,:])[0]

    datas = [
             [[[t,dom_fut_close,variety+' 主力 '+dom_contract,'color=black, width=4'],
               [t[w1],dom_call_oi_max_strike[0,w1],variety+' 主力 call 持仓量一','color=red,visible=False'],
               [t[w1],dom_call_oi_max_strike[1,w1],variety+' 主力 call 持仓量二','color=orange,visible=False'],
               [t[w1],dom_call_oi_max_strike[2,w1],variety+' 主力 call 持仓量三','color=deeppink'],
               [t[w1],dom_call_oi_avg_strike[w1],'加权平均','color=darkgray'],
               [t[w1],dom_put_oi_max_strike[4,w1],variety+' 主力 put 持仓量一','color=darkgreen,visible=False'],
               [t[w1],dom_put_oi_max_strike[3,w1],variety+' 主力 put 持仓量二','color=blue,visible=False'],
               [t[w1],dom_put_oi_max_strike[2,w1],variety+' 主力 put 持仓量三','color=purple'],
               [t[w1],dom_put_oi_avg_strike[w1],'加权平均','color=darkgray'],],[],''],

             [[[t,maxv_fut_close,variety+' 期权主力 '+maxv_contract,'color=black, width=4'],
               [t[w2],maxv_call_oi_max_strike[0,w2],variety+' 期权主力 call 持仓量一','color=red,visible=False'],
               [t[w2],maxv_call_oi_max_strike[1,w2],variety+' 期权主力 call 持仓量二','color=orange,visible=False'],
               [t[w2],maxv_call_oi_max_strike[2,w2],variety+' 期权主力 call 持仓量三','color=deeppink'],
               [t[w2],maxv_call_oi_avg_strike[w2],'加权平均','color=darkgray'],
               [t[w2],maxv_put_oi_max_strike[4,w2],variety+' 期权主力 put 持仓量一','color=darkgreen,visible=False'],
               [t[w2],maxv_put_oi_max_strike[3,w2],variety+' 期权主力 put 持仓量二','color=blue,visible=False'],
               [t[w2],maxv_put_oi_max_strike[2,w2],variety+' 期权主力 put 持仓量三','color=purple'],
               [t[w2],maxv_put_oi_avg_strike[w2],'加权平均','color=darkgray'],],[],''],

             [[[t,index_fut_close,variety+' 指数','color=black'],
              ],
              [[t,total_put_oi/total_call_oi,'total_put_oi / total_call_oi',''],],''],

            #  [[[t1,dom_call_atm_price,variety+' 主力 期权 ATM CALL PRICE','color=red'],
            #    [t1,dom_put_atm_price,variety+' 主力 期权 ATM PUT PRICE','color=darkgreen'],
            #   ],
            #   [],''],

            #  [[[t2,maxv_call_atm_price,variety+' 期权主力 ATM CALL PRICE','color=red'],
            #    [t2,maxv_put_atm_price,variety+' 期权主力 ATM PUT PRICE','color=darkgreen'],
            #   ],
            #   [],''],

             [[[t,total_call_volume,'total_call_volume','color=red'],
               [t,total_put_volume,'total_put_volume','color=darkgreen'],
              ],
              [[t,total_put_volume-total_call_volume,'total_put_volume - total_call_volume','style=vbar'],],''],

             [[[t,total_call_oi,'total_call_oi','color=red'],
               [t,total_put_oi,'total_put_oi','color=darkgreen'],
              ],
              [[t,total_put_oi-total_call_oi,'total_put_oi - total_call_oi','style=vbar'],],''],

             #####################

             [[[t,dom_fut_close,variety+' 主力 '+dom_contract,'color=black, width=4'],
               [t[w1],dom_call_volume_max_strike[0,w1],variety+' 主力 call 成交量一','color=red,visible=False'],
               [t[w1],dom_call_volume_max_strike[1,w1],variety+' 主力 call 成交量二','color=orange,visible=False'],
               [t[w1],dom_call_volume_max_strike[2,w1],variety+' 主力 call 成交量三','color=deeppink'],
               [t[w1],dom_call_volume_avg_strike[w1],'加权平均','color=darkgray'],
               [t[w1],dom_put_volume_max_strike[4,w1],variety+' 主力 put 成交量一','color=darkgreen,visible=False'],
               [t[w1],dom_put_volume_max_strike[3,w1],variety+' 主力 put 成交量二','color=blue,visible=False'],
               [t[w1],dom_put_volume_max_strike[2,w1],variety+' 主力 put 成交量三','color=purple'],
               [t[w1],dom_put_volume_avg_strike[w1],'加权平均','color=darkgray'],],[],''],

             [[[t,maxv_fut_close,variety+' 期权主力 '+maxv_contract,'color=black, width=4'],
               [t[w2],maxv_call_volume_max_strike[0,w2],variety+' 期权主力 call 成交量一','color=red,visible=False'],
               [t[w2],maxv_call_volume_max_strike[1,w2],variety+' 期权主力 call 成交量二','color=orange,visible=False'],
               [t[w2],maxv_call_volume_max_strike[2,w2],variety+' 期权主力 call 成交量三','color=deeppink'],
               [t[w2],maxv_call_volume_avg_strike[w2],'加权平均','color=darkgray'],
               [t[w2],maxv_put_volume_max_strike[4,w2],variety+' 期权主力 put 成交量一','color=darkgreen,visible=False'],
               [t[w2],maxv_put_volume_max_strike[3,w2],variety+' 期权主力 put 成交量二','color=blue,visible=False'],
               [t[w2],maxv_put_volume_max_strike[2,w2],variety+' 期权主力 put 成交量三','color=purple'],
               [t[w2],maxv_put_volume_avg_strike[w2],'加权平均','color=darkgray'],],[],''],

             ]
    plot_many_figure(datas, max_height=1400)


    # datas = [[t,index_fut_close,variety+' 指数','color=black'], [t,total_put_oi/total_call_oi,'total_put_oi / total_call_oi',''],
    #          [t,index_fut_close,variety+' 指数','color=black'], [t,total_call_oi/total_put_oi,'total_call_oi / total_put_oi','']]
    # compare_two_option_data(datas, start_time='2020-01-01')


def update_intraday_data():
    # init driver
    global dce_driver
    global driver_fut
    global driver_opt

    option = webdriver.FirefoxOptions()
    option.add_argument('--headless')
    # 实例化webdriver对象
    dce_driver = webdriver.Firefox(options=option)
    driver_fut = webdriver.Firefox(options=option)
    driver_opt = webdriver.Firefox(options=option)

    while(1):
        now = datetime.datetime.now()
        hour = now.hour
        minute = now.minute

        t = update_future_intraday_price('shfe')
        if t is not None:
            update_option_intraday_price('shfe', t=t)
            update_option_intraday_info_detail('shfe')

        t = update_future_intraday_price('czce')
        if t is not None:
            update_option_intraday_price('czce', t=t)
            update_option_intraday_info_detail('czce')

        t = update_future_intraday_price_gfex()
        if t is not None:
            update_option_intraday_price_gfex(t=t)
            update_option_intraday_info_detail('gfex')

        now = datetime.datetime.now()
        hour = now.hour
        minute = now.minute
        if (hour == 9 and minute >= 30) or (hour == 10) or (hour == 11 and minute < 40) or ((hour >= 13) and (hour < 15)) or ((hour == 15) and (minute < 10)):
            # CFFEX
            t = update_future_intraday_price_cffex()
            if t is not None:
                update_option_intraday_price_cffex(t=t)
                update_option_intraday_info_detail('cffex')

            # ETF OPTION
            # update_sse_intraday_option_data()

        t = update_future_intraday_price('dce')
        if t is not None:
            update_option_intraday_price_dce(t=t)
            update_option_intraday_info_detail('dce')

        if (hour == 15 and minute >= 10) or (hour == 2 and minute >= 10) or (hour == 12):
            dce_driver.quit()
            driver_fut.quit()
            driver_opt.quit()
            exit()

        time.sleep(60*15)


def get_lastday_option_strike_volume_oi(exchange, variety, inst_id):
    path = os.path.join(option_price_dir, exchange, inst_id+'.csv')
    df = pd.read_csv(path, header=[0,1,2])
    t = pd.DatetimeIndex(pd.to_datetime(df['time']['time']['time'], format='%Y-%m-%d'))

    L = len(t)
    if (L < 2):
        print('L < 2')
        return

    col = df.columns.tolist()
    res = [(col[i][1]) for i in range(len(col)) if col[i][0] == 'P']
    strikes_str = []
    for i in res:
        if i not in strikes_str:
            strikes_str.append(i)

    strike = []
    put_oi_1d = []
    call_oi_1d = []
    put_volume_1d = []
    call_volume_1d = []
    put_oi_2d = []
    call_oi_2d = []

    for strike_str in strikes_str:
        strike.append(float(strike_str))
        put_oi_1d.append(df.loc[L-1, pd.IndexSlice['P', strike_str, 'oi']])
        call_oi_1d.append(df.loc[L-1, pd.IndexSlice['C', strike_str, 'oi']])

        put_volume_1d.append(df.loc[L-1, pd.IndexSlice['P', strike_str, 'volume']])
        call_volume_1d.append(df.loc[L-1, pd.IndexSlice['C', strike_str, 'volume']])

        put_oi_2d.append(df.loc[L-2, pd.IndexSlice['P', strike_str, 'oi']])
        call_oi_2d.append(df.loc[L-2, pd.IndexSlice['C', strike_str, 'oi']])

    strike = np.array(strike, dtype=float)
    put_oi_1d = np.array(put_oi_1d, dtype=float)
    call_oi_1d = np.array(call_oi_1d, dtype=float)
    put_volume_1d = np.array(put_volume_1d, dtype=float)
    call_volume_1d = np.array(call_volume_1d, dtype=float)
    put_oi_2d = np.array(put_oi_2d, dtype=float)
    call_oi_2d = np.array(call_oi_2d, dtype=float)

    path = os.path.join(future_price_dir, exchange, variety+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    row = np.where(fut_t == t[L-1])[0]
    fut_price_1d = None
    if len(row) > 0:
        for c in ['c1','c2','c3','c4','c5','c6','c7','c8','c9']:
            if (fut_df.loc[row[0], pd.IndexSlice[c, 'inst_id']] == inst_id):
                fut_price_1d = fut_df.loc[row[0], pd.IndexSlice[c, 'close']]

    row = np.where(fut_t == t[L-2])[0]
    fut_price_2d = None
    if len(row) > 0:
        for c in ['c1','c2','c3','c4','c5','c6','c7','c8','c9']:
            if (fut_df.loc[row[0], pd.IndexSlice[c, 'inst_id']] == inst_id):
                fut_price_2d = fut_df.loc[row[0], pd.IndexSlice[c, 'close']]

    return strike, put_oi_1d, call_oi_1d, put_volume_1d, call_volume_1d, put_oi_2d, call_oi_2d, \
            fut_price_1d, fut_price_2d



def plot_intraday_option_strike_volume_oi(exchange, variety):
    path3 = os.path.join(option_price_dir, exchange, variety+'_info_detail'+'.csv')
    if not os.path.exists(path3):
        return
    df = pd.read_csv(path3)
    inst_id_opt = df.loc[len(df)-1, 'dom1']

    path3 = os.path.join(future_price_dir, exchange, variety+'.csv')
    if not os.path.exists(path3):
        return
    df = pd.read_csv(path3, header=[0,1])
    inst_id_fut = df.loc[len(df)-1, pd.IndexSlice['dom', 'inst_id']]

    if inst_id_opt == inst_id_fut:
        inst_ids = [inst_id_opt]
    else:
        inst_ids = [inst_id_opt, inst_id_fut]  

    for inst_id in inst_ids:
        path = os.path.join(option_price_dir, exchange, inst_id+'_intraday.csv')
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path, header=[0,1,2])
        t = pd.DatetimeIndex(pd.to_datetime(df['time']['time']['time'], format='%Y-%m-%d %H:%M:%S'))

        L = len(t)
        if (L < 5):
            print('L < 5')
            return

        col = df.columns.tolist()
        res = [(col[i][1]) for i in range(len(col)) if col[i][0] == 'P']
        strikes_str = []
        for i in res:
            if i not in strikes_str:
                strikes_str.append(i)

        strike = []
        put_oi = []
        call_oi = []
        put_vol = []
        call_vol = []

        for strike_str in strikes_str:
            strike.append(float(strike_str))
            put_oi.append(df.loc[L-1, pd.IndexSlice['P', strike_str, 'oi']])
            call_oi.append(df.loc[L-1, pd.IndexSlice['C', strike_str, 'oi']])
            put_vol.append(df.loc[L-1, pd.IndexSlice['P', strike_str, 'volume']])
            call_vol.append(df.loc[L-1, pd.IndexSlice['C', strike_str, 'volume']])

        strike = np.array(strike, dtype=float)
        put_oi = np.array(put_oi, dtype=float)
        call_oi = np.array(call_oi, dtype=float)
        put_vol = np.array(put_vol, dtype=float)
        call_vol = np.array(call_vol, dtype=float)

        path = os.path.join(future_price_dir, exchange, variety+'_intraday.csv')
        fut_df = pd.read_csv(path, header=[0,1])
        fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['time'], format='%Y-%m-%d %H:%M:%S'))
        row = np.where(fut_t == t[L-1])[0]
        fut_price = None
        if len(row) > 0:
            for c in ['c1','c2','c3','c4','c5','c6','c7','c8','c9']:
                if (fut_df.loc[row[0], pd.IndexSlice[c, 'inst_id']] == inst_id):
                    fut_price = fut_df.loc[row[0], pd.IndexSlice[c, 'close']]

        strike_1d_before, put_oi_1d_before, call_oi_1d_before, put_volume_1d_before, call_volume_1d_before,\
        put_oi_2d_before, call_oi_2d_before, fut_price_1d_before, fut_price_2d_before = \
              get_lastday_option_strike_volume_oi(exchange, variety, inst_id)

        put_oi_ld_change = put_oi.copy()
        call_oi_ld_change = call_oi.copy()
        put_oi_2d_change = put_oi.copy()
        call_oi_2d_change = call_oi.copy()
        put_volume_2d = put_vol.copy()
        call_volume_2d = call_vol.copy()

        for i in range(len(strike)):
            for k in range(len(strike_1d_before)):
                if strike[i] == strike_1d_before[k]:
                    put_oi_ld_change[i] = put_oi_ld_change[i] - put_oi_1d_before[k]
                    call_oi_ld_change[i] = call_oi_ld_change[i] - call_oi_1d_before[k]
                    put_oi_2d_change[i] = put_oi_2d_change[i] - put_oi_2d_before[k]
                    call_oi_2d_change[i] = call_oi_2d_change[i] - call_oi_2d_before[k]
                    put_volume_2d[i] = put_vol[i] + put_volume_1d_before[k]
                    call_volume_2d[i] = call_vol[i] + call_volume_1d_before[k]

        strike_sort = np.sort(strike)
        gap = (strike_sort[1]-strike_sort[0]) / 5
        if gap < 0.5:
            gap = 1

        fig1 = figure(frame_width=1400, frame_height=155)
        fig1.quad(left=strike-gap, right=strike, bottom=0, top=put_oi, fill_color='darkgreen')
        fig1.quad(left=strike, right=strike+gap, bottom=0, top=call_oi, fill_color='red')
        fig1.line(x=[fut_price, fut_price], y=[0, np.nanmax(call_oi)], line_width=1, line_color='black', legend_label=inst_id + ' oi')
        fig1.legend.location='top_left'

        net_oi = put_oi - call_oi
        put_idx = np.where(net_oi >= 0)[0]
        call_idx = np.where(net_oi < 0)[0]

        fig11 = figure(frame_width=1400, frame_height=155, x_range=fig1.x_range)
        fig11.quad(left=strike[put_idx]-gap/2, right=strike[put_idx]+gap/2, bottom=0, top=net_oi[put_idx], fill_color='darkgreen')
        fig11.quad(left=strike[call_idx]-gap/2, right=strike[call_idx]+gap/2, bottom=0, top=-net_oi[call_idx], fill_color='red')
        fig11.line(x=[fut_price, fut_price], y=[0, np.nanmax(call_oi)], line_width=1, line_color='black', legend_label=inst_id + ' net oi')
        fig11.legend.location='top_left'

        fig2 = figure(frame_width=1400, frame_height=155, x_range=fig1.x_range)
        fig2.quad(left=strike-gap, right=strike, bottom=0, top=put_vol, fill_color='darkgreen')
        fig2.quad(left=strike, right=strike+gap, bottom=0, top=call_vol, fill_color='red')
        fig2.line(x=[fut_price_1d_before, fut_price_1d_before], y=[0, np.nanmax(call_vol)], line_width=1, line_color='black', legend_label=inst_id + ' 1d volume')
        fig2.legend.location='top_left'
        fig2.background_fill_color = "lightgray"

        fig3 = figure(frame_width=1400, frame_height=155, x_range=fig1.x_range)
        fig3.quad(left=strike-gap, right=strike, bottom=0, top=put_oi_ld_change, fill_color='darkgreen')
        fig3.quad(left=strike, right=strike+gap, bottom=0, top=call_oi_ld_change, fill_color='red')
        fig3.line(x=[fut_price_1d_before, fut_price_1d_before], y=[np.nanmin(call_oi_ld_change), np.nanmax(call_oi_ld_change)], line_width=1, line_color='black', legend_label=inst_id + ' 1d oi change')
        fig3.legend.location='top_left'

        net_oi_1d_change = put_oi_ld_change - call_oi_ld_change
        put_idx = np.where(net_oi_1d_change >= 0)[0]
        call_idx = np.where(net_oi_1d_change < 0)[0]

        fig31 = figure(frame_width=1400, frame_height=155, x_range=fig1.x_range)
        fig31.quad(left=strike[put_idx]-gap/2, right=strike[put_idx]+gap/2, bottom=0, top=net_oi_1d_change[put_idx], fill_color='darkgreen')
        fig31.quad(left=strike[call_idx]-gap/2, right=strike[call_idx]+gap/2, bottom=0, top=-net_oi_1d_change[call_idx], fill_color='red')
        fig31.line(x=[fut_price_1d_before, fut_price_1d_before], y=[0, np.nanmax(call_oi_ld_change)], line_width=1, line_color='black', legend_label=inst_id + ' 1d net oi change')
        fig31.legend.location='top_left'

        fig4 = figure(frame_width=1400, frame_height=155, x_range=fig1.x_range)
        fig4.quad(left=strike-gap, right=strike, bottom=0, top=put_volume_2d, fill_color='darkgreen')
        fig4.quad(left=strike, right=strike+gap, bottom=0, top=call_volume_2d, fill_color='red')
        fig4.line(x=[fut_price_2d_before, fut_price_2d_before], y=[0, np.nanmax(call_volume_2d)], line_width=1, line_color='black', legend_label=inst_id + ' 2d volume')
        fig4.legend.location='top_left'
        fig4.background_fill_color = "lightgray"

        fig5 = figure(frame_width=1400, frame_height=155, x_range=fig1.x_range)
        fig5.quad(left=strike-gap, right=strike, bottom=0, top=put_oi_2d_change, fill_color='darkgreen')
        fig5.quad(left=strike, right=strike+gap, bottom=0, top=call_oi_2d_change, fill_color='red')
        fig5.line(x=[fut_price_2d_before, fut_price_2d_before], y=[np.nanmin(call_oi_2d_change), np.nanmax(call_oi_2d_change)], line_width=1, line_color='black', legend_label=inst_id + ' 2d oi change')
        fig5.legend.location='top_left'


        show(column(fig1,fig11,fig2,fig3,fig31,fig4,fig5))


def plot_intraday_option_data(exchange, variety):
    plot_intraday_dominant_option_datas(exchange, variety)
    time.sleep(0.25)

    plot_intraday_option_strike_volume_oi(exchange, variety)
    time.sleep(0.25)


def plot_some_intraday_option_data(varieties):
    varieties = varieties.replace(' ','')
    s = varieties.split(',')

    for variety in s:
        for exchange in exchange_dict:
            if variety in exchange_dict[exchange]:
                plot_intraday_option_data(exchange, variety)


def update_sse_intraday_option_data():
    SSE_VAR_CODE_DICT = {
        '50ETF': '510050',
        '300ETF': '510300',
        '500ETF': '510500',
        '科创50': '588000',
        '科创板50': '588080',
    }

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "62.push2.eastmoney.com",
        "Proxy-Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    }

    se = requests.session()
    # example
    # URL = 'https://62.push2.eastmoney.com/api/qt/slist/get?secid=1.510300&exti=202309&spt=9&fltt=2&invt=2&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fields=f334,f14,f2,f5,f108,f249,f341,f344,f345,f346&fid=f161&pn=1&pz=20&po=0&wbp2u=|0|0|0|web'

    URL = 'https://62.push2.eastmoney.com/api/qt/slist/get?secid={}.{}&exti={}&spt=9&fltt=2&invt=2&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fields=f334,f14,f2,f5,f108,f249,f341,f344,f345,f346&fid=f161&pn=1&pz=20&po=0&wbp2u=|0|0|0|web'
    EXPIRY_URL = "http://stock.finance.sina.com.cn/futures/api/openapi.php/StockOptionService.getStockName"
    varieties = list(SSE_VAR_CODE_DICT.keys())

    now = datetime.datetime.now()
    if now.hour >= 15:
        t = datetime.datetime(year=now.year, month=now.month, day=now.day, hour=15, minute=0, second=0)
        daily_data = True
    else:
        t = now.strftime('%Y-%m-%d %H:%M:%S')
        daily_data = False

    for variety in varieties:
        code = SSE_VAR_CODE_DICT[variety]

        # 合约到期月份
        params = {"exchange": f"{'null'}", "cate": f"{variety}"}
        while (1):
            try:
                r = se.get(EXPIRY_URL, params=params)
                break
            except Exception as e:
                print(e)
                time.sleep(5)

        data_json = r.json()
        expiry_list = data_json["result"]["data"]["contractMonth"]
        expiry_list = ["".join(i.split("-")) for i in expiry_list][1:]
        volume_list = []

        print(variety, expiry_list)
        for expiry_time in expiry_list:
            url = URL.format('1', code, expiry_time)
            while (1):
                try:
                    r = se.get(url, headers=headers)
                    break
                except Exception as e:
                    print(e)
                    time.sleep(5)

            data_json = r.json()
            data_df = pd.DataFrame(data_json['data']['diff'])
            
            data_df.rename(columns={'f334':'etf_price', 'f14':'opt_inst_id', 
                                    'f2':'call_price', 'f5': 'call_volume', 'f108': 'call_oi', 'f249': 'call_iv',
                                    'f341':'put_price', 'f344': 'put_volume', 'f345': 'put_oi', 'f346': 'put_iv'}, inplace=True)

            data_df = data_df[['opt_inst_id', 'etf_price', 
                               'call_price', 'call_volume', 'call_oi', 'call_iv',
                               'put_price', 'put_volume', 'put_oi', 'put_iv',]]
            data_df.replace('-',0,inplace=True)
            call_volume = np.array(data_df['call_volume'], dtype=float)
            put_volume = np.array(data_df['put_volume'], dtype=float)
            volume_list.append(call_volume.sum()+put_volume.sum())

            col1 = ['time', 'etf_price']
            col2 = ['time', 'etf_price']
            col3 = ['time', 'etf_price']
            row = [t, data_df.loc[0, 'etf_price']]
            for i in range(len(data_df)):
                s = data_df.loc[i, 'opt_inst_id']
                s = s.split('购')[1].split('月')
                strike = str(round(float(s[1])/1000, 3))

                col1 += ['C','C','C','C','P','P','P','P']
                col2 += [strike,strike,strike,strike,strike,strike,strike,strike]
                col3 += ['close','volume','oi','iv','close','volume','oi','iv']

            row += data_df[['call_price', 'call_volume', 'call_oi', 'call_iv',
                            'put_price', 'put_volume', 'put_oi', 'put_iv']].values.flatten().tolist()
            
            df = pd.DataFrame(columns=[col1, col2, col3], data=[row])
            ym = expiry_time[2:]
            if daily_data == True:
                path = os.path.join(option_price_dir, 'sse', variety+ym+'_volume_oi'+'.csv')
            else:
                path = os.path.join(option_price_dir, 'sse', variety+ym+'_intraday_volume_oi'+'.csv')
            if os.path.exists(path):
                old_df = pd.read_csv(path, header=[0,1,2])
                old_df = pd.concat([old_df, df], axis=0)
                old_df.drop_duplicates(subset=[('time','time','time')], keep='last', inplace=True) # last
                old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
                old_df.sort_values(by = ('time','time','time'), inplace=True)
                old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
                old_df.to_csv(path, encoding='utf-8', index=False)
            else:
                df.to_csv(path, encoding='utf-8', index=False)
        
        volume_list = np.array(volume_list, dtype=float)
        sort = np.argsort(volume_list)[::-1]
        inst_ids = ''
        for i in range(len(volume_list)):
            inst_ids += (variety + expiry_list[sort[i]][2:])
            inst_ids += ','

        info_df = pd.DataFrame(columns=['time', 'inst_ids'])
        info_df.loc[0, 'time'] = t
        info_df.loc[0, 'inst_ids'] = inst_ids
        if daily_data == True:
            path = os.path.join(option_price_dir, 'sse', variety+'_volume_oi_info'+'.csv')
        else:
            path = os.path.join(option_price_dir, 'sse', variety+'_intraday_volume_oi_info'+'.csv')

        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, info_df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
            old_df.sort_values(by = 'time', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            info_df.to_csv(path, encoding='utf-8', index=False)  


def get_sse_lastday_option_strike_volume_oi(exchange, variety, inst_id):
    path = os.path.join(option_price_dir, exchange, inst_id+'_volume_oi'+'.csv')
    df = pd.read_csv(path, header=[0,1,2])
    t = pd.DatetimeIndex(pd.to_datetime(df['time']['time']['time'], format='%Y-%m-%d %H:%M:%S'))

    L = len(t)
    if (L < 1):
        print('L < 1')
        return

    col = df.columns.tolist()
    res = [(col[i][1]) for i in range(len(col)) if col[i][0] == 'P']
    strikes_str = []
    for i in res:
        if i not in strikes_str:
            strikes_str.append(i)

    strike = []
    put_oi = []
    call_oi = []

    for strike_str in strikes_str:
        strike.append(float(strike_str))
        put_oi.append(df.loc[L-1, pd.IndexSlice['P', strike_str, 'oi']])
        call_oi.append(df.loc[L-1, pd.IndexSlice['C', strike_str, 'oi']])


    strike = np.array(strike, dtype=float)
    put_oi = np.array(put_oi, dtype=float)
    call_oi = np.array(call_oi, dtype=float)
  
    etf_price = df.loc[L-1, pd.IndexSlice['etf_price', 'etf_price', 'etf_price']]

    return strike, put_oi, call_oi, etf_price


def plot_see_intraday_option_strike_volume_oi(exchange, variety):
    path = os.path.join(option_price_dir, exchange, variety+'_intraday_volume_oi_info'+'.csv')
    if not os.path.exists(path):
        return
    df = pd.read_csv(path)
    inst_ids = df.loc[len(df)-1, 'inst_ids'].split(',')

    # dom1, dom2
    for inst_id in [inst_ids[0], inst_ids[1]]:
        path = os.path.join(option_price_dir, exchange, inst_id+'_intraday_volume_oi.csv')
        df = pd.read_csv(path, header=[0,1,2])
        t = pd.DatetimeIndex(pd.to_datetime(df['time']['time']['time'], format='%Y-%m-%d %H:%M:%S'))

        L = len(t)
        if (L < 2):
            print('L < 2')
            return

        col = df.columns.tolist()
        res = [(col[i][1]) for i in range(len(col)) if col[i][0] == 'P']
        strikes_str = []
        for i in res:
            if i not in strikes_str:
                strikes_str.append(i)

        strike = []
        put_oi = []
        call_oi = []
        put_vol = []
        call_vol = []

        for strike_str in strikes_str:
            strike.append(float(strike_str))
            put_oi.append(df.loc[L-1, pd.IndexSlice['P', strike_str, 'oi']])
            call_oi.append(df.loc[L-1, pd.IndexSlice['C', strike_str, 'oi']])
            put_vol.append(df.loc[L-1, pd.IndexSlice['P', strike_str, 'volume']])
            call_vol.append(df.loc[L-1, pd.IndexSlice['C', strike_str, 'volume']])

        strike = np.array(strike, dtype=float)
        put_oi = np.array(put_oi, dtype=float)
        call_oi = np.array(call_oi, dtype=float)
        put_vol = np.array(put_vol, dtype=float)
        call_vol = np.array(call_vol, dtype=float)

        etf_price = df.loc[L-1, pd.IndexSlice['etf_price', 'etf_price', 'etf_price']]

        strike_last_day, put_oi_last_day, call_oi_last_day, etf_price_last_day = get_sse_lastday_option_strike_volume_oi(exchange, variety, inst_id)
        put_oi_ld_change = put_oi.copy()
        call_oi_ld_change = call_oi.copy()
        for i in range(len(strike)):
            for k in range(len(strike_last_day)):
                if strike[i] == strike_last_day[k]:
                    put_oi_ld_change[i] = put_oi_ld_change[i] - put_oi_last_day[k]
                    call_oi_ld_change[i] = call_oi_ld_change[i] - call_oi_last_day[k]

        # print(strike)
        # exit()
        strike_sort = np.sort(strike)
        gap = float((strike_sort[1]-strike_sort[0]) / 5)
        if gap == 0:
            gap = 1

        fig1 = figure(frame_width=1300, frame_height=150)
        fig1.quad(left=strike-gap, right=strike, bottom=0, top=put_oi, fill_color='darkgreen')
        fig1.quad(left=strike, right=strike+gap, bottom=0, top=call_oi, fill_color='red')
        fig1.line(x=[etf_price, etf_price], y=[0, np.nanmax(call_oi)], line_width=1, line_color='black', legend_label=inst_id + ' oi')
        fig1.legend.location='top_left'

        fig2 = figure(frame_width=1300, frame_height=160, x_range=fig1.x_range)
        fig2.quad(left=strike-gap, right=strike, bottom=0, top=put_vol, fill_color='darkgreen')
        fig2.quad(left=strike, right=strike+gap, bottom=0, top=call_vol, fill_color='red')
        fig2.line(x=[etf_price_last_day, etf_price_last_day], y=[0, np.nanmax(call_vol)], line_width=1, line_color='black', legend_label=inst_id + ' 1d volume')
        fig2.legend.location='top_left'
        fig2.background_fill_color = "lightgray"

        fig3 = figure(frame_width=1300, frame_height=160, x_range=fig1.x_range)
        fig3.quad(left=strike-gap, right=strike, bottom=0, top=put_oi_ld_change, fill_color='darkgreen')
        fig3.quad(left=strike, right=strike+gap, bottom=0, top=call_oi_ld_change, fill_color='red')
        fig3.line(x=[etf_price_last_day, etf_price_last_day], y=[np.nanmin(call_oi_ld_change), np.nanmax(call_oi_ld_change)], line_width=1, line_color='black', legend_label=inst_id + ' 1d oi change')
        fig3.legend.location='top_left'

        show(column(fig1,fig2,fig3))



if __name__=="__main__":
    update_intraday_data()

    # plot_some_intraday_option_data('zn')
    # plot_some_intraday_option_data('i, SM, SF')
    # plot_some_intraday_option_data('ag, v, eg, TA, i')
    # plot_some_intraday_option_data('au,ag,cu,al,zn,sc')
    # plot_some_intraday_option_data('p, y, OI')
    # plot_some_intraday_option_data('a,b,m,RM')


    # plot_some_intraday_option_data('au, ag, cu, al, zn, sc, rb, i, c, a, b, m, RM, TA, PF, SR, CF, SA')
    # plot_some_intraday_option_data('SM, SF, AP, PK, si, lc, IF, IH, IM')
    # plot_some_intraday_option_data('ru, br, MA, UR, eg, eb, p, y, OI, pg, pp, PX, SH, v, l')
    # plot_some_intraday_option_data('AP')


    # plot_see_intraday_option_strike_volume_oi('sse', '50ETF')
    # plot_see_intraday_option_strike_volume_oi('sse', '300ETF')
    # plot_see_intraday_option_strike_volume_oi('sse', '500ETF')
    pass
