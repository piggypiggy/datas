import json
import os
import time
import numpy as np
import pandas as pd
import datetime
import calendar
from utils import *
import requests
import calendar


# RMBS条件早偿率指数
def get_rmbs_data():
    print('RMBS')
    url = (f"https://www.chinamoney.com.cn/ags/ms/cm-u-bk-bond/RMBSIndexHis")
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
        "Host": "www.chinamoney.com.cn",
    }
    r = requests.get(url, headers=headers)
    try:
        context = json.loads(r.text)
    except:
        print('error')
        return {}

    df = pd.DataFrame(context["records"])
    df.rename(columns={'date':'time'}, inplace=True)
    df.drop(['name'], axis=1, inplace=True)
    df = df.iloc[::-1]
    path = os.path.join(data_dir, 'RMBS条件早偿率指数'+'.csv')
    df.to_csv(path, encoding='utf-8', index=False)


def plot_rmbs():
    path = os.path.join(data_dir, 'RMBS条件早偿率指数'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    data = np.array(df['rate'], dtype=float)

    datas = [[t, data, 'RMBS条件早偿率指数']]
    plot_one_figure(datas)


def get_last_day(dt_str):
    dt = pd.to_datetime(dt_str, format='%Y-%m-%d')
    last_day = calendar.monthrange(dt.year, dt.month)[-1]
    dt_last_day = datetime.datetime(dt.year, dt.month, last_day)
    return dt_last_day.strftime('%Y-%m-%d')

def get_next_day(dt_str):
    dt = pd.to_datetime(dt_str, format='%Y-%m-%d')
    dt_next_day = dt + pd.Timedelta(days=1)
    return dt_next_day.strftime('%Y-%m-%d')


# CDS
def get_CFETS_SHCH_CDS_data(name, _url, columns):
    print(name)
    start_time = '2022-06-01'
    path = os.path.join(data_dir, name+'.csv')
    if not os.path.exists(path):
        file_exists = False
    else:
        file_exists = True
        # 最后一行的时间
        with open(path, 'rb') as f:
            f.seek(0, os.SEEK_END)
            pos = f.tell() - 1  # 不算最后一个字符'\n'
            while pos > 0:
                pos -= 1
                f.seek(pos, os.SEEK_SET)
                if f.read(1) == b'\n':
                    break
            last_line = f.readline().decode().strip()
            print('LAST TIME: ', last_line[:10])
            data_time_dt = pd.to_datetime(last_line[:10], format='%Y-%m-%d')
            data_time_dt += pd.Timedelta(days=1)
            start_time = data_time_dt.strftime('%Y-%m-%d')

    start_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')
    df_new = pd.DataFrame(columns=columns)
    data_columns = columns[1:]
    k = 0
    while(datetime.datetime.now() >= start_time_dt):
        end_time = get_last_day(start_time)
        print('DATA TIME: ', start_time, end_time)

        url = _url+'&startDate='+start_time+'&endDate='+end_time+'&pageNum=1&pageSize=180'
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
            "Host": "www.chinamoney.com.cn",
        }
        r = requests.get(url, headers=headers)
        try:
            context = json.loads(r.text)
        except:
            print('error')
            return
        
        df = pd.DataFrame(context["records"])
        df.rename(columns={'date':'time'}, inplace=True)
    
        L = len(df)
        L1 = len(data_columns)
        for i in range(L//L1):
            data = df.loc[L-(i+1)*L1:L-(i+1)*L1+L1-1, 'cdIndxVl'].values.flatten().tolist()
            df_new.loc[k, 'time'] = df.loc[L-(i+1)*L1, 'time']
            df_new.loc[k, data_columns] = data
            k = k + 1

        start_time = get_next_day(end_time)
        start_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')

    if file_exists == False:
        df_new.to_csv(path, encoding='utf-8', index=False)
    else:
        df_new.to_csv(path, mode='a', encoding='utf-8', index=False, header=None)   


def plot_china_cds():
    path = os.path.join(data_dir, 'CFETS-SHCH-GTJA高等级CDS指数'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    _3m = np.array(df['3M'], dtype=float)
    _6m = np.array(df['6M'], dtype=float)
    _1y = np.array(df['1Y'], dtype=float)
    _2y = np.array(df['2Y'], dtype=float)
    _3y = np.array(df['3Y'], dtype=float)
    _4y = np.array(df['4Y'], dtype=float)
    _5y = np.array(df['5Y'], dtype=float)
    datas = [[t, _3m, '3M'],
             [t, _6m, '6M'],
             [t, _1y, '1Y'],
             [t, _2y, '2Y'],
             [t, _3y, '3Y'],
             [t, _4y, '4Y'],
             [t, _5y, '5Y']]
    plot_one_figure(datas, 'CFETS-SHCH-GTJA高等级CDS指数')

    time.sleep(0.5)
    path = os.path.join(data_dir, 'CFETS-SHCH-CBR长三角区域CDS指数'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    _3m = np.array(df['3M'], dtype=float)
    _6m = np.array(df['6M'], dtype=float)
    _9m = np.array(df['9M'], dtype=float)
    _1y = np.array(df['1Y'], dtype=float)
    _2y = np.array(df['2Y'], dtype=float)
    _3y = np.array(df['3Y'], dtype=float)
    datas = [[t, _3m, '3M'],
             [t, _6m, '6M'],
             [t, _9m, '9M'],
             [t, _1y, '1Y'],
             [t, _2y, '2Y'],
             [t, _3y, '3Y']]
    plot_one_figure(datas, 'CFETS-SHCH-CBR长三角区域CDS指数')

    time.sleep(0.5)
    path = os.path.join(data_dir, 'CFETS-SHCH民企CDS指数'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    _3m = np.array(df['3M'], dtype=float)
    _6m = np.array(df['6M'], dtype=float)
    _9m = np.array(df['9M'], dtype=float)
    _1y = np.array(df['1Y'], dtype=float)
    _2y = np.array(df['2Y'], dtype=float)
    _3y = np.array(df['3Y'], dtype=float)
    datas = [[t, _3m, '3M'],
             [t, _6m, '6M'],
             [t, _9m, '9M'],
             [t, _1y, '1Y'],
             [t, _2y, '2Y'],
             [t, _3y, '3Y']]
    plot_one_figure(datas, 'CFETS-SHCH民企CDS指数')


def update_shibor_data():
    print('shibor')
    start_time = '2017-01-01'
    path = os.path.join(interest_rate_dir, 'shibor'+'.csv')
    if not os.path.exists(path):
        df = pd.DataFrame(columns=['time','ON','1W','2W','1M','3M','6M','9M','1Y'])
        df.to_csv(path, encoding='utf-8', index=False)
        print('SHIBOR CREATE ' + path)
    else:
        # 最后一行的时间
        with open(path, 'rb') as f:
            f.seek(0, os.SEEK_END)
            pos = f.tell() - 1  # 不算最后一个字符'\n'
            while pos > 0:
                pos -= 1
                f.seek(pos, os.SEEK_SET)
                if f.read(1) == b'\n':
                    break
            last_line = f.readline().decode().strip()
            print('LAST TIME: ', last_line[:10])
            data_time_dt = pd.to_datetime(last_line[:10], format='%Y-%m-%d')
            data_time_dt += pd.Timedelta(days=1)
            start_time = data_time_dt.strftime('%Y-%m-%d')

    start_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')
    order = ['time','ON','1W','2W','1M','3M','6M','9M','1Y']
    while(datetime.datetime.now() >= start_time_dt):
        end_time = get_last_day(start_time)
        print('DATA TIME: ', start_time, end_time)
        url = f'https://www.chinamoney.com.cn/ags/ms/cm-u-bk-shibor/ShiborHis?lang=cn&startDate={start_time}&endDate={end_time}'
        # url = _url+'&startDate='+start_time+'&endDate='+end_time+'&pageNum=1&pageSize=180'
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
            "Host": "www.chinamoney.com.cn",
        }
        r = requests.get(url, headers=headers)
        try:
            context = json.loads(r.text)
        except:
            print('error')
            return
        
        df = pd.DataFrame(context["records"])
        if not (df.empty):
            df.rename(columns={'showDateCN':'time'}, inplace=True)
            df['time'] = pd.to_datetime(df['time'])
            df.sort_values(by='time', axis=0, ascending=True, inplace=True)
            df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            df.drop('showDateEN', axis=1, inplace=True)
            df = df[order]
            df.to_csv(path, mode='a', encoding='utf-8', index=False, header=None)   

        start_time = get_next_day(end_time)
        start_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')


# 人民币汇率中间价
def update_cny_middle():
    earlist_time = '2006-01-04'
    # earlist_time = '2022-01-04'
    MIDDLE_URL = 'https://www.chinamoney.com.cn/ags/ms/cm-u-bk-ccpr/CcprHisNew?startDate={}&endDate={}&currency=USD/CNY,EUR/CNY,100JPY/CNY,HKD/CNY,GBP/CNY,AUD/CNY,NZD/CNY,SGD/CNY,CHF/CNY,CAD/CNY,CNY/MYR,CNY/RUB,CNY/ZAR,CNY/KRW,CNY/AED,CNY/SAR,CNY/HUF,CNY/PLN,CNY/DKK,CNY/SEK,CNY/NOK,CNY/TRY,CNY/MXN,CNY/THB&pageNum=1&pageSize=180'

    print('CNY中间价')
    path = os.path.join(fx_dir, 'CNY中间价'+'.csv')
    last_line_time = get_last_line_time(path, 'CNY中间价', earlist_time, 10, '%Y-%m-%d')

    start_time_dt = pd.to_datetime(last_line_time, format='%Y-%m-%d') + pd.Timedelta(days=1)
    now_dt = datetime.datetime.now()
    while(now_dt >= start_time_dt):
        end_time_dt = start_time_dt + pd.Timedelta(days=180)
        if (end_time_dt > now_dt):
            end_time = now_dt

        start_time = start_time_dt.strftime('%Y-%m-%d')
        end_time = end_time_dt.strftime('%Y-%m-%d')
        print('DATA TIME: ', start_time, end_time)
        url = MIDDLE_URL.format(start_time, end_time)
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
            "Host": "www.chinamoney.com.cn",
        }
        r = requests.get(url, headers=headers)
        try:
            z = json.loads(r.text)
        except:
            print('error')
            return

        df = pd.DataFrame(columns=['time','USDCNY','EURCNY','100JPYCNY','HKDCNY','GBPCNY','AUDCNY','NZDCNY','SGDCNY','CHFCNY','CADCNY','CNYMYR','CNYRUB','CNYZAR','CNYKRW','CNYAED','CNYSAR','CNYHUF','CNYPLN','CNYDKK','CNYSEK','CNYNOK','CNYTRY','CNYMXN','CNYTHB'])
        for i in range(len(z["records"])):
            df.loc[i, 'time'] = z["records"][i]['date']
            df.loc[i, ['USDCNY','EURCNY','100JPYCNY','HKDCNY','GBPCNY','AUDCNY','NZDCNY','SGDCNY','CHFCNY','CADCNY','CNYMYR','CNYRUB','CNYZAR','CNYKRW','CNYAED','CNYSAR','CNYHUF','CNYPLN','CNYDKK','CNYSEK','CNYNOK','CNYTRY','CNYMXN','CNYTHB']] = \
                z["records"][i]['values']

        df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
        df.sort_values(by = 'time', inplace=True)
        df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
        df.replace('---','',inplace=True)

        if not(os.path.exists(path)):
            df.to_csv(path, encoding='utf-8', index=False)
        else:
            df.to_csv(path, mode='a', encoding='utf-8', index=False, header=None)

        start_time_dt = end_time_dt + pd.Timedelta(days=1)

# 人民币汇率远期
def update_cny_forward():
    earlist_time = '2015-01-01'
    # earlist_time = '2022-01-01'
    FORWARD_3M_URL = 'https://www.chinamoney.com.cn/ags/ms/cm-u-bk-fx/SddsExchRateSwpHis?dataType=3&startDate={}&endDate={}'
    FORWARD_6M_URL = 'https://www.chinamoney.com.cn/ags/ms/cm-u-bk-fx/SddsExchRateSwpHis?dataType=6&startDate={}&endDate={}'

    print('CNY FORWARD')
    path = os.path.join(fx_dir, 'CNY FORWARD'+'.csv')
    last_line_time = get_last_line_time(path, 'CNY FORWARD', earlist_time, 10, '%Y-%m-%d')

    start_time_dt = pd.to_datetime(last_line_time, format='%Y-%m-%d') + pd.Timedelta(days=1)
    now_dt = datetime.datetime.now()
    while(now_dt >= start_time_dt):
        end_time_dt = start_time_dt + pd.Timedelta(days=180)
        if (end_time_dt > now_dt):
            end_time = now_dt

        start_time = start_time_dt.strftime('%Y-%m-%d')
        end_time = end_time_dt.strftime('%Y-%m-%d')
        print('DATA TIME: ', start_time, end_time)

        df = pd.DataFrame(columns=['time','USDCNY_3M','EURCNY_3M','100JPYCNY_3M','HKDCNY_3M','GBPCNY_3M','AUDCNY_3M','NZDCNY_3M','SGDCNY_3M','CHFCNY_3M','CADCNY_3M','CNYMOP_3M','CNYMYR_3M','CNYRUB_3M','CNYZAR_3M','CNYKRW_3M','CNYAED_3M','CNYSAR_3M','CNYHUF_3M','CNYPLN_3M','CNYDKK_3M','CNYSEK_3M','CNYNOK_3M','CNYTRY_3M','CNYMXN_3M','CNYTHB_3M',
                                          'USDCNY_6M','EURCNY_6M','100JPYCNY_6M','HKDCNY_6M','GBPCNY_6M','AUDCNY_6M','NZDCNY_6M','SGDCNY_6M','CHFCNY_6M','CADCNY_6M','CNYMOP_6M','CNYMYR_6M','CNYRUB_6M','CNYZAR_6M','CNYKRW_6M','CNYAED_6M','CNYSAR_6M','CNYHUF_6M','CNYPLN_6M','CNYDKK_6M','CNYSEK_6M','CNYNOK_6M','CNYTRY_6M','CNYMXN_6M','CNYTHB_6M'])

        # 3M
        url = FORWARD_3M_URL.format(start_time, end_time)
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
            "Host": "www.chinamoney.com.cn",
        }
        while (1):
            try:
                r = requests.get(url, headers=headers)
                z = json.loads(r.text)
                break
            except Exception as e:
                print(e)
                time.sleep(30)

        for i in range(len(z["records"])):
            df.loc[i, 'time'] = z["records"][i]['dateString']
            df.loc[i, ['USDCNY_3M','EURCNY_3M','100JPYCNY_3M','HKDCNY_3M','GBPCNY_3M','AUDCNY_3M','NZDCNY_3M','SGDCNY_3M','CHFCNY_3M','CADCNY_3M','CNYMOP_3M','CNYMYR_3M','CNYRUB_3M','CNYZAR_3M','CNYKRW_3M','CNYAED_3M','CNYSAR_3M','CNYHUF_3M','CNYPLN_3M','CNYDKK_3M','CNYSEK_3M','CNYNOK_3M','CNYTRY_3M','CNYMXN_3M','CNYTHB_3M']] = \
                z["records"][i]['dateMapNew']
            
        # 6M
        url = FORWARD_6M_URL.format(start_time, end_time)
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
            "Host": "www.chinamoney.com.cn",
        }
        r = requests.get(url, headers=headers)
        try:
            z = json.loads(r.text)
        except:
            print('error')
            return

        for i in range(len(z["records"])):
            df.loc[i, ['USDCNY_6M','EURCNY_6M','100JPYCNY_6M','HKDCNY_6M','GBPCNY_6M','AUDCNY_6M','NZDCNY_6M','SGDCNY_6M','CHFCNY_6M','CADCNY_6M','CNYMOP_6M','CNYMYR_6M','CNYRUB_6M','CNYZAR_6M','CNYKRW_6M','CNYAED_6M','CNYSAR_6M','CNYHUF_6M','CNYPLN_6M','CNYDKK_6M','CNYSEK_6M','CNYNOK_6M','CNYTRY_6M','CNYMXN_6M','CNYTHB_6M']] = \
                z["records"][i]['dateMapNew']


        df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
        df.sort_values(by = 'time', inplace=True)
        df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
        df.replace('---','',inplace=True)

        if not(os.path.exists(path)):
            df.to_csv(path, encoding='utf-8', index=False)
        else:
            df.to_csv(path, mode='a', encoding='utf-8', index=False, header=None)
    
        start_time_dt = end_time_dt + pd.Timedelta(days=1)


###########################################################################################
time_dict = {
    '0.019': '1W',
    '0.038': '2W',
    '0.058': '3W',
    "0.083": '1M',
    '0.125': '6W',
    '0.167': '2M',
    '0.208': '10W',
    '0.25': '3M',
    '0.333': '4M',
    '0.417': '5M',
    '0.5': '6M',
    '0.583': '7M',
    '0.667': '8M',
    '0.75': '9M',
    '0.833': '10M',
    '0.917': '11M',
}

CHINA_IR_CODE_NAME = {
    # file_name: [[code1, name1], [code2, name2], ...]

    '国债收益率': [['CYCC000', '']],
    '政策性金融债': [['CYCC021', '国开'], ['CYCC024', '进出口行'], ['CYCC023', '农发行']],
    '同业存单': [['CYCC41B', 'AAA'], ['CYCC41D', 'AA'], ['CYCC41M', 'A']],
    '地方政府债': [['CYCC84A', 'AAA'], ['CYCC84B', 'AAA-']],
    '中短期票据': [['CYCC82A', 'AAA+'],['CYCC82B', 'AAA'],['CYCC82E', 'AA'],['CYCC82H', 'A'],],
    '企业债': [['CYCC80A', '有担保AAA+'], ['CYCC80B', '有担保AAA'], ['CYCC80E', '有担保AA'],
              ['CYCC83A', '无担保AAA'], ['CYCC83E', '无担保AA',],
              ['CYCC834', 'AAA-'], ['CYCC837', 'A'], ['CYCC830', 'BBB'], ['CYCC83C', 'B']],
    '证券公司短期融资券': [['CYCC87A', 'AAA'], ['CYCC87D', 'AA']],
    '商业银行普通金融债': [['CYCC85A', 'AAA'], ['CYCC85D', 'AA'],['CYCC85G', 'A']],

    '商业银行二级资本债': [['CYCC862', 'AAA-'], ['CYCC864', 'AA'],['CYCC861', 'A']],
    '资产支持证券': [['CYCC50A', 'AAA'], ['CYCC50C', 'AA+']],
}

def update_china_interest_rate():
    se = requests.session()
    # EXAMPLE
    # URL = 'https://www.chinamoney.com.cn/ags/ms/cm-u-bk-currency/ClsYldCurvHis?lang=CN&reference=1&bondType=CYCC000&startDate=2023-10-10&endDate=2023-10-27&termId=1&pageNum=1&pageSize=15'

    URL = 'https://www.chinamoney.com.cn/ags/ms/cm-u-bk-currency/ClsYldCurvHis?lang=CN&reference=1&bondType={}&startDate={}&endDate={}&termId=1&pageNum={}&pageSize={}'
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
        "Host": "www.chinamoney.com.cn",
    }

    #####
    for file_name in CHINA_IR_CODE_NAME:
        code_name = CHINA_IR_CODE_NAME[file_name]
        path = os.path.join(interest_rate_dir, file_name+'.csv')
        if not os.path.exists(path):
            print('path not exists, ', path)
            continue
        else:
            old_df = pd.read_csv(path)
            t = pd.DatetimeIndex(pd.to_datetime(old_df['time'], format='%Y-%m-%d'))
            start_time = t[-1].strftime('%Y-%m-%d')
            end_time = datetime.datetime.now().strftime('%Y-%m-%d')
            if start_time == end_time:
                print(file_name, 'no update')
                continue

        df = pd.DataFrame()
        print('-------', file_name, '-------')
        for k in range(len(code_name)):
            page = 1
            page_size = 500
            remain = 0

            data_dict = {}
            code = code_name[k][0]
            name = code_name[k][1]
            while (1):
                time.sleep(0.25)
                print(name, start_time + ' - ' + end_time)
                url = URL.format(code, start_time, end_time, str(page), str(page_size))
                while (1):
                    try:
                        r = se.get(url, headers=headers, verify=False)
                        data_json = r.json()
                        break
                    except Exception as e:
                        print(e)
                        time.sleep(15)

                if page == 1:
                    remain = data_json['data']['total']

                for i in range(len(data_json['records'])):
                    t_str = data_json['records'][i]['newDateValueCN']
                    term = data_json['records'][i]['yearTermStr']
                    rate = data_json['records'][i]['maturityYieldStr']

                    if not(t_str in data_dict):
                        data_dict[t_str] = [[term], [rate]]
                    else:
                        if not(term in data_dict[t_str][0]):
                            data_dict[t_str][0].append(term)
                            data_dict[t_str][1].append(rate)

                remain -= page_size
                page += 1
                if remain <= 0:
                    break

            _df = pd.DataFrame()
            for t_str in data_dict:
                terms = data_dict[t_str][0]
                rates = data_dict[t_str][1]

                for i in range(len(terms)):
                    term = terms[i]
                    if term in time_dict:
                        # 小于1年
                        term = time_dict[term]
                    else:
                        # 大于等于1年
                        term = str(int(float(term))) + 'Y'
                    if name != '':
                        terms[i] = name + ':' + term
                    else:
                        terms[i] = term

                col = ['time'] + terms
                data = [t_str] + rates
                temp_df = pd.DataFrame(columns=col, data=[data])

                _df = pd.concat([_df, temp_df], axis=0)

            if (df.empty):
                df = _df.copy()
            else:
                df = pd.merge(df, _df, on='time', how='outer')

        path = os.path.join(interest_rate_dir, file_name+'.csv')
        if os.path.exists(path):
            # old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            old_df.sort_values(by = 'time', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            df.sort_values(by = 'time', inplace=True)
            df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            df.to_csv(path, encoding='utf-8', index=False)


#############
# 回购定盘利率
def update_fixing_repo_rate():
    se = requests.session()
    URL = 'https://www.chinamoney.com.cn/ags/ms/cm-u-bk-currency/FrrHis?lang=CN&startDate={}&endDate={}'
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
        "Host": "www.chinamoney.com.cn",
    }

    earlist_time = '2000-01-01'
    path = os.path.join(interest_rate_dir, '回购定盘利率.csv')
    if os.path.exists(path):
        df = pd.read_csv(path)
        t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
        start_time_dt = t[-1] + pd.Timedelta(days=1)
    else:
        start_time_dt = pd.to_datetime(earlist_time, format='%Y-%m-%d')

    now = datetime.datetime.now()

    print('回购定盘利率')
    while (start_time_dt <= now):
        end_time_dt = start_time_dt + pd.Timedelta(days=180)
        url = URL.format(start_time_dt.strftime('%Y-%m-%d'), end_time_dt.strftime('%Y-%m-%d'))
        print(start_time_dt.strftime('%Y-%m-%d') + ' - ' + end_time_dt.strftime('%Y-%m-%d'))

        start_time_dt = end_time_dt + pd.Timedelta(days=1)
        while (1):
            try:
                r = se.get(url, headers=headers, verify=False)
                break
            except Exception as e:
                print(e)
                time.sleep(5)

        data_json = r.json()
        df = pd.DataFrame()

        if (len(data_json['records']) == 0):
            continue

        for i in range(len(data_json['records'])):
            temp_df = pd.DataFrame(data_json['records'][i]['frValueMap'], index=[i])
            df = pd.concat([df, temp_df], axis=0)

        df.rename(columns={"date":"time"}, inplace=True)
        df = df[['time', 'FR001', 'FR007', 'FR014', 'FDR001', 'FDR007', 'FDR014']]
        df.replace('---', '', inplace=True)

        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            old_df.sort_values(by = 'time', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            df.sort_values(by = 'time', inplace=True)
            df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            df.to_csv(path, encoding='utf-8', index=False)


# LPR
def update_lpr():
    se = requests.session()
    URL = 'https://www.chinamoney.com.cn/ags/ms/cm-u-bk-currency/LprHis?lang=CN&strStartDate={}&strEndDate={}'
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
        "Host": "www.chinamoney.com.cn",
    }

    earlist_time = '2014-01-01'
    path = os.path.join(interest_rate_dir, 'LPR.csv')
    if os.path.exists(path):
        df = pd.read_csv(path)
        t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
        start_time_dt = t[-1] + pd.Timedelta(days=1)
    else:
        start_time_dt = pd.to_datetime(earlist_time, format='%Y-%m-%d')

    now = datetime.datetime.now()

    print('LPR')
    while (start_time_dt <= now):
        end_time_dt = start_time_dt + pd.Timedelta(days=180)
        url = URL.format(start_time_dt.strftime('%Y-%m-%d'), end_time_dt.strftime('%Y-%m-%d'))
        print(start_time_dt.strftime('%Y-%m-%d') + ' - ' + end_time_dt.strftime('%Y-%m-%d'))

        start_time_dt = end_time_dt + pd.Timedelta(days=1)
        while (1):
            try:
                r = se.get(url, headers=headers, verify=False)
                data_json = r.json()
                break
            except Exception as e:
                print(e)
                time.sleep(15)

        df = pd.DataFrame()

        if (len(data_json['records']) == 0):
            continue

        for i in range(len(data_json['records'])):
            temp_df = pd.DataFrame(data_json['records'][i], index=[i])
            df = pd.concat([df, temp_df], axis=0)

        df.drop(['showDateEN'], axis=1, inplace=True)
        df.rename(columns={'showDateCN':'time'}, inplace=True)
        col = df.columns.tolist()
        if '5Y' in col:
            df = df[['time', '1Y', '5Y']]
        else:
            df = df[['time', '1Y']]
        
        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            old_df.sort_values(by = 'time', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            df.sort_values(by = 'time', inplace=True)
            df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            df.to_csv(path, encoding='utf-8', index=False)


# 境内外币同业拆放参考利率
def update_ciror():
    se = requests.session()
    URL = 'https://www.chinamoney.com.cn/ags/ms/cm-u-bk-ciror/USDHis?lang=CN&startDate={}&endDate={}'
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
        "Host": "www.chinamoney.com.cn",
    }

    earlist_time = '2018-10-01'
    path = os.path.join(interest_rate_dir, 'ciror.csv')
    if os.path.exists(path):
        df = pd.read_csv(path)
        t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
        start_time_dt = t[-1] + pd.Timedelta(days=1)
    else:
        start_time_dt = pd.to_datetime(earlist_time, format='%Y-%m-%d')

    now = datetime.datetime.now()

    while (start_time_dt <= now):
        end_time_dt = start_time_dt + pd.Timedelta(days=180)
        url = URL.format(start_time_dt.strftime('%Y-%m-%d'), end_time_dt.strftime('%Y-%m-%d'))
        print('CIROR', start_time_dt.strftime('%Y-%m-%d') + ' - ' + end_time_dt.strftime('%Y-%m-%d'))

        start_time_dt = end_time_dt + pd.Timedelta(days=1)
        while (1):
            try:
                r = se.get(url, headers=headers, verify=False)
                break
            except Exception as e:
                print(e)
                time.sleep(5)

        data_json = r.json()
        if (len(data_json['records']) == 0):
            continue

        df = pd.DataFrame(data_json['records'])
        df.rename(columns={"showDateCN":"time"}, inplace=True)
        df = df[['time', 'ON', '1W', '2W', '1M', '3M', '6M', '9M', '1Y']]
        df.replace('---', '', inplace=True)

        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            old_df.sort_values(by = 'time', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            df.sort_values(by = 'time', inplace=True)
            df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            df.to_csv(path, encoding='utf-8', index=False)




def update_all_CFETS_SHCH_CDS_data():
    get_CFETS_SHCH_CDS_data('CFETS-SHCH-GTJA高等级CDS指数',
                            f"https://www.chinamoney.com.cn/ags/ms/cm-u-bk-bond/CDSIndexHis?indxName=CFETS-SHCH-GTJA%E9%AB%98%E7%AD%89%E7%BA%A7CDS%E6%8C%87%E6%95%B0",
                            ['time','3M','6M','1Y','2Y','3Y','4Y','5Y'])

    get_CFETS_SHCH_CDS_data('CFETS-SHCH民企CDS指数',
                            f"https://www.chinamoney.com.cn/ags/ms/cm-u-bk-bond/CDSIndexHis?indxName=CFETS-SHCH%E6%B0%91%E4%BC%81CDS%E6%8C%87%E6%95%B0",
                            ['time','3M','6M','9M','1Y','2Y','3Y'])

    get_CFETS_SHCH_CDS_data('CFETS-SHCH-CBR长三角区域CDS指数',
                            f"https://www.chinamoney.com.cn/ags/ms/cm-u-bk-bond/CDSIndexHis?indxName=CFETS-SHCH-CBR%E9%95%BF%E4%B8%89%E8%A7%92%E5%8C%BA%E5%9F%9FCDS%E6%8C%87%E6%95%B0",
                            ['time','3M','6M','9M','1Y','2Y','3Y'])


def update_all_china_rate():
    get_rmbs_data()
    update_all_CFETS_SHCH_CDS_data()
    update_shibor_data()

    # 人民币中间价
    update_cny_middle()
    # 人民币远期
    update_cny_forward()

    # see CHINA_IR_CODE_NAME
    update_china_interest_rate()

    # LPR
    update_lpr()

    # 境内外币同业拆放参考利率
    update_ciror()

    # 回购定盘利率
    update_fixing_repo_rate()


if __name__=="__main__":
    update_all_china_rate()

    pass
