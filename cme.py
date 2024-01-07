import os
import time
import zipfile
import numpy as np
import pandas as pd
import datetime
import requests
import pdfplumber
from utils import *


def download_cme_daily_bulletin():
    se = requests.session()
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Host": "www.cmegroup.com",
        'Sec-Fetch-Dest': 'document',
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36",
    }

    proxy = {'https':'127.0.0.1:8889'}
    url = 'https://www.cmegroup.com/ftp/bulletin/'
    r = se.get(url, headers=headers, proxies=proxy)

    temp_df = pd.read_html(r.text)[0]
    name_list = []
    for i in range(len(temp_df)):
        name = temp_df.loc[i, 'Name']
        if type(name) == str:
            if 'zip' in name:
                name_list.append(name)

    for file_name in name_list:
        path = os.path.join(cme_data_dir, file_name)
        if os.path.exists(path):
            continue

        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "Host": "www.cmegroup.com",
            'Sec-Fetch-Dest': 'document',
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36",
        }
        url = 'https://www.cmegroup.com/ftp/bulletin/' + file_name
        proxy = {'https':'127.0.0.1:8889'}
        while (1):
            try:
                r = se.get(url, verify=False, headers=headers, proxies=proxy, stream=True, timeout=(10,20))
                break
            except Exception as e:
                print(e)

        # 记录下载文件的大小
        total_length = int(r.headers.get('content-length'))
        temp_size = 0
        print(file_name + ' total_length: ', total_length)

        fd = None
        step_size = 2*1024*1024  # 一次下载 2MB
        while (1):
            if temp_size >= total_length:
                fd.close()
                break

            try:
                if os.path.exists(path):
                    temp_size = os.path.getsize(path)  # 本地已经下载的文件大小
                else:
                    temp_size = 0

                if (temp_size + step_size) > total_length:
                    headers['Range'] = f'bytes={temp_size}-{total_length}'
                else:
                    headers['Range'] = f'bytes={temp_size}-{temp_size + step_size}'

                print('downloaded: ', temp_size, ' Bytes')
                r = se.get(url, headers=headers, stream=True, verify=False, timeout=(30,40))

                if fd is None:
                    fd = open(path, 'ab')

                temp_size += len(r.content)
                fd.write(r.content)
                fd.flush()

            except Exception as e:
                print(e)
                time.sleep(1)


def get_cme_daily_bulletin_zip_names():
    zip_names = []
    zip_time = []
    for _, _, files in os.walk(os.path.join(data_dir, 'cme_daily_bulletin')):
        for file in files:
            if 'zip' in file:
                zip_names.append(file)
                zip_time.append(file[18:26])

    zip_time = pd.to_datetime(zip_time, format='%Y%m%d')
    zip_names = np.array(zip_names)
    zip_time = np.array(zip_time)

    sort = np.argsort(zip_time)
    zip_names = zip_names[sort]
    zip_time = zip_time[sort]

    return zip_names, zip_time


def update_cme_30d_fed_fund_fut_data():
    zip_names, zip_time = get_cme_daily_bulletin_zip_names()

    variety = 'ZQ'
    path = os.path.join(future_price_dir, 'cme', variety+'.csv')
    last_line_time = get_last_line_time(path, '', None, 10, '%Y-%m-%d')
    if last_line_time is None:
        start_file_idx = 0
    else:
        last_line_time_dt = pd.to_datetime(last_line_time, format='%Y-%m-%d')
        w = np.where(zip_time > last_line_time_dt)[0]
        if len(w) > 0:
            start_file_idx = w[0]
        else:
            return

    for zip_name in zip_names[start_file_idx:]:
        path = os.path.join(data_dir, 'cme_daily_bulletin', zip_name)
        z = zipfile.ZipFile(path, "r")
        fp = z.open('Section59_30DFedfund_10yrSwap_5yrSwap_Options.pdf', mode='r')
        pdf = pdfplumber.open(fp)
        page = pdf.pages[0]
        table = page.extract_text_lines()

        t = zip_name[18:26]
        t = t[:4] + '-' + t[4:6] + '-' + t[6:]
        print(variety, t)
        expiry_read = False
        i = 0
        while (i < (len(table)-1)):
            s = table[i]['text']

            if 'EXPIRATION:' in s and expiry_read == False:
                ym = s.split(' ')
                ym.pop(0)
                for k in range(len(ym)):
                    ym[k] = ym[k][3:] + month_dict[ym[k][:3]]

                i += 1
                s = table[i]['text']
                s = s.replace('30D FED FUND FUT ', '')
                s = s.replace('/', '-')
                expiry_time = s.split(' ')
                for k in range(len(expiry_time)):
                    expiry_time[k] = '20' + ym[k][:2] + '-' + expiry_time[k]

                expiry_read = True

            if '30D FED FD FUT' in s:
                variety = 'ZQ'
                col1 = ['time']
                col2 = ['time'] 
                data = [t]
                n = 0
                count = 0
                data_idx = [1,2,3,4,9,10,10,11]
                while (1):
                    i += 1
                    s = table[i]['text']
                    if 'TOTAL' in s:
                        break

                    s = s.replace('UNCH', 'UNCH 0')
                    s = s.replace('NEW', 'UNCH 0')
                    s = s.replace('*', '')
                    row = [s[:5]]
                    s = s[5:]
                    s = s.replace('A', '')
                    s = s.replace('B', '')
                    sp = s.split(' ')
                    row += [sp[k] for k in data_idx]
                    row[0] = variety + row[0][3:] + month_dict[row[0][:3]]
                    # OPEN
                    if row[1] == '----':
                        row[1] = ''
                    # HIGH
                    if row[2] == '----':
                        row[2] = ''
                    # LOW
                    if row[3] == '----':
                        row[3] = ''
                    # RTH VOLUME
                    if row[5] == '----':
                        row[5]  = '0'
                    # GLOBEX VOLUME
                    if row[6] == '----':
                        row[6]  = '0'
                    # RTH VOLUME + GLOBEX VOLUME
                    row[7] = str(int(row[5]) + int(row[6]))
                    # OPEN INTEREST
                    if row[8] == '----':
                        row[8]  = '0'

                    count += 1
                    # exclude expired contract
                    if (count == 1):
                        if row[6] == '0':
                            continue

                    n += 1
                    cn = 'c' + str(n)
                    col1 += [cn, cn, cn, cn, cn, cn, cn, cn, cn]
                    col2 += ['inst_id', 'open', 'high', 'low', 'settle', 'rth vloume', 'globex volume', 'volume', 'oi']
                    data += row

                df = pd.DataFrame(columns=[col1,col2], data=[data])
                path = os.path.join(future_price_dir, 'cme', variety+'.csv')
                if os.path.exists(path):
                    old_df = pd.read_csv(path, header=[0,1])
                    old_df = pd.concat([old_df, df], axis=0)
                    old_df.drop_duplicates(subset=[('time','time')], keep='last', inplace=True) # last
                    old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
                    old_df.sort_values(by = ('time','time'), inplace=True)
                    old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
                    old_df.to_csv(path, encoding='utf-8', index=False)
                else:
                    df.to_csv(path, encoding='utf-8', index=False)

                expiry_df = pd.DataFrame(columns=['year_month', 'expiry_time'])
                expiry_df['year_month'] = ym
                expiry_df['expiry_time'] = expiry_time
                path = os.path.join(future_price_dir, 'cme', variety+'_expiry_time'+'.csv')
                if os.path.exists(path):
                    old_df = pd.read_csv(path, dtype=str)
                    old_df = pd.concat([old_df, expiry_df], axis=0)
                    old_df.drop_duplicates(subset=['year_month'], keep='last', inplace=True)
                    old_df['year_month'] = old_df['year_month'].apply(lambda x:pd.to_datetime('20'+str(x), format='%Y%m'))
                    old_df.sort_values(by = 'year_month', inplace=True)
                    old_df['year_month'] = old_df['year_month'].apply(lambda x:datetime.datetime.strftime(x,'%Y%m')[2:])
                    old_df.to_csv(path, encoding='utf-8', index=False)
                else:
                    expiry_df.to_csv(path, encoding='utf-8', index=False)  

                z.close()
                break

            i += 1


if __name__=="__main__":
    download_cme_daily_bulletin()
    
    update_cme_30d_fed_fund_fut_data()

    # TODO: SR3 



    pass
