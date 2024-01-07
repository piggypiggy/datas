import os
import numpy as np
import pandas as pd
import datetime
from utils import *
import requests
from bs4 import BeautifulSoup
import re
from io import StringIO, BytesIO

##### 人民银行 #####

# 
def plot_financing_data():
    path = os.path.join(pboc_dir, '社会融资规模'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))
    name_list = ['社会融资规模增量',
                 '社会融资规模增量:企业债券',
                 '社会融资规模增量:政府债券',
                 ]
    for name in name_list:
        data = np.array(df[name], dtype=float)
        plot_seasonality(t, data, start_year=2015, title=name)


    path = os.path.join(pboc_dir, '金融机构人民币信贷收支表'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))

    # 贷款
    name_list = ['各项贷款:境内:住户',
                 '各项贷款:境内:住户:中长期',
                 '各项贷款:境内:住户:中长期:消费',
                 '各项贷款:境内:住户:中长期:经营',
                 '各项贷款:境内:住户:短期',
                 '各项贷款:境内:住户:短期:消费',
                 '各项贷款:境内:住户:短期:经营',
                 '各项贷款:境内:企事业单位:中长期',
                 '各项贷款:境内:企事业单位:短期',
                 ]
    for name in name_list:
        data = np.array(df[name], dtype=float)
        plot_seasonality(t[1:], data[1:]-data[:-1], start_year=2015, title=name)


    # 存款
    name_list = ['各项存款:境内:住户:活期',
                 '各项存款:境内:住户:定期及其他',
                 '各项存款:境内:非金融企业:活期',
                 '各项存款:境内:非金融企业:定期及其他',
                 '各项存款:境内:非银行业金融机构',
                 '各项存款:境内:政府',
                 '各项存款:境内:财政性',
                 '各项存款:境内:机关团体',
                 ]
    for name in name_list:
        data = np.array(df[name], dtype=float)
        plot_seasonality(t[1:], data[1:]-data[:-1], start_year=2015, title=name)


# # 
# def test1():
#     path = os.path.join(data_dir, '社会融资规模'+'.csv') 
#     df = pd.read_csv(path)

#     t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))
#     sf_total = np.array(df['社会融资增量:当月值'], dtype=float)
#     resident1 = np.array(df['金融机构:新增人民币贷款:居民户:中长期:当月值'], dtype=float)
#     resident2 = np.array(df['金融机构:新增人民币贷款:居民户:短期:当月值'], dtype=float)
#     z1 = np.array(df['社会融资增量:企业债券融资:当月值'], dtype=float)
#     z2 = np.array(df['社会融资增量:政府债券:当月值'], dtype=float)
#     z3 = np.array(df['金融机构:新增人民币贷款:非金融性公司及其他部门:中长期:当月值'], dtype=float)
#     z4 = np.array(df['金融机构:新增人民币贷款:非金融性公司及其他部门:短期:当月值'], dtype=float)

#     plot_seasonality(t, sf_total, start_year=2015, title='社会融资增量:当月值 (亿元)')
#     plot_seasonality(t, resident1, start_year=2015, title='新增人民币贷款:居民户:中长期:当月值 (亿元)')
#     plot_seasonality(t, resident2, start_year=2015, title='新增人民币贷款:居民户:短期:当月值 (亿元)')
#     plot_seasonality(t, z1, start_year=2015, title='社会融资增量:企业债券融资:当月值')
#     plot_seasonality(t, z2, start_year=2015, title='社会融资增量:政府债券:当月值')
#     plot_seasonality(t, z3, start_year=2015, title='新增人民币贷款:非金融性公司及其他部门:中长期:当月值')
#     plot_seasonality(t, z4, start_year=2015, title='新增人民币贷款:非金融性公司及其他部门:短期:当月值')


def update_pboc_url():
    se = requests.session()
    PBOC_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                    'Host': 'www.pbc.gov.cn'}
    # 调查统计司
    PBOC_ROOT_URL = 'http://www.pbc.gov.cn/diaochatongjisi/116219/index.html'

    path = os.path.join(pboc_dir, 'pboc_url'+'.csv')
    if os.path.exists(path):
        old_df = pd.read_csv(path)
        old_t = pd.DatetimeIndex(pd.to_datetime(old_df['time'], format='%Y'))
        old_df.drop_duplicates(subset=['time'], keep='last', inplace=True) # last
        start_year = old_t[-1].year
    else:
        old_df = pd.DataFrame()
        start_year = 2015

    r = se.post(PBOC_ROOT_URL, headers=PBOC_HEADERS)
    s = r.content.decode('utf-8')
    soup = BeautifulSoup(s, 'html.parser')
    z = soup.find_all(name='a', string=re.compile('年统计数据'))

    for a1 in z:
        col = ['time']
        # 年统计数据的 url
        year = a1.get_text()[:4]
        data = [int(year)]
        year_url = 'http://www.pbc.gov.cn' + a1['href']
        if (int(year) >= start_year):
            print('==============', 'YEAR', year, '==============')
            r = se.post(year_url, headers=PBOC_HEADERS)
            s = r.content.decode('utf-8')
            soup = BeautifulSoup(s, 'html.parser')

            name_list = ['社会融资规模', '货币统计概览', '金融机构信贷收支统计']
            for name in name_list:
                # 各类统计数据的 url
                a2 = soup.find_all(name='a', string=re.compile(name))[0]
                name_url = 'http://www.pbc.gov.cn' + a2['href']

                r = se.post(name_url, headers=PBOC_HEADERS)
                s = r.content.decode('utf-8')
                soup2 = BeautifulSoup(s, 'html.parser')
                tmp_col = []
                tmp_data = []
                if name == '社会融资规模':
                    div = soup2.find_all(name='div', class_ = "titp20")
                    a3 = soup2.find_all(name='a', string=re.compile('htm'))
                    a4 = soup2.find_all(name='a', string=re.compile('Q'))

                    # 社会融资规模增量
                    tmp_col.append(div[0].get_text().split('统计表')[0].strip())
                    tmp_data.append('http://www.pbc.gov.cn' + a3[0]['href'])
                    
                    # 社会融资规模存量
                    tmp_col.append(div[1].get_text().split('统计表')[0].strip())
                    tmp_data.append('http://www.pbc.gov.cn' + a3[1]['href'])

                    # 地区社会融资规模增量
                    s = div[2].get_text().split('统计表')[0].strip()
                    for k in range(4):
                        tmp_col.append(s+'Q'+str(k+1))
                        try:
                            region_url = 'http://www.pbc.gov.cn' + a4[k]['href']
                            r = se.post(region_url, headers=PBOC_HEADERS)
                            rs = r.content.decode('utf-8')
                            soup3 = BeautifulSoup(rs, 'html.parser')
                            ra = soup3.find_all(name='a', string=re.compile('地区社会融资规模'))[0]
                            tmp_data.append('http://www.pbc.gov.cn' + ra['href'])
                        except Exception as e:
                            tmp_data.append('')

                if (name == '货币统计概览') or (name == '金融机构信贷收支统计'):
                    div = soup2.find_all(name='div', class_ = "titp20")
                    a3 = soup2.find_all(name='a', string=re.compile('htm'))

                    for k in range(len(a3)):
                        tmp_col.append(div[k].get_text().strip().split(' ')[0].strip())
                        tmp_data.append('http://www.pbc.gov.cn' + a3[k]['href'])

                print('----------', name, '----------')
                print(tmp_col)
                print(tmp_data)

                col += tmp_col
                data += tmp_data

            df = pd.DataFrame(columns=col, data=[data])
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True) # last
            old_df['time'] = pd.to_datetime(old_df['time'], format='%Y')
            old_df.sort_values(by='time', axis=0, ascending=True, inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y'))
            old_df.to_csv(path, encoding='utf-8', index=False)


# 社会融资规模 增量
def get_afre_data1(se, url):
    if se is None:
        se = requests.session()
    PBOC_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                    'Host': 'www.pbc.gov.cn'}
    # url = 'http://www.pbc.gov.cn/diaochatongjisi/resource/cms/2018/04/2018041118034695174.htm'

    r = se.post(url, headers=PBOC_HEADERS)
    try:
        s = r.content.decode('gbk')
        temp_df = pd.read_html(StringIO(s))[0]
    except:
        s = r.content
        temp_df = pd.read_html(s)[0]
    temp_df.dropna(how='all', subset=[2], inplace=True)
    temp_df.reset_index(inplace=True, drop=True)

    for i in range(len(temp_df)):
        s = temp_df.loc[i,1]
        if (type(s) == str) and (s == '社会融资规模增量'):
            col = temp_df.loc[i,:].values.tolist()
            col[0] = 'time'
            n = 0
            for k in range(len(col)):
                if type(col[k]) == str:
                    n += 1

                if col[k] == '地方政府专项债券':
                    col[k] = '政府债券'

                if type(col[k]) == str and col[k] != 'time' and col[k] != '社会融资规模增量':
                    col[k] = '社会融资规模增量:' + col[k]

            col = col[:n]
            start_idx = i + 2

        s = temp_df.loc[i,0]
        if (type(s) == str) and ('注' in s):
            end_idx = i - 1

    df = temp_df.loc[start_idx:end_idx, 0:n-1]
    df.columns = col

    df['time'] = pd.to_datetime(df['time'], format='%Y.%m')
    df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m'))
    # print(df)
    return df


# 社会融资规模 存量
def get_afre_data2(se, url):
    if se is None:
        se = requests.session()
    se = requests.session()
    PBOC_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                    'Host': 'www.pbc.gov.cn'}

    r = se.post(url, headers=PBOC_HEADERS)
    try:
        s = r.content.decode('gbk')
        temp_df = pd.read_html(StringIO(s))[0]
    except:
        s = r.content
        temp_df = pd.read_html(s)[0]

    temp_df = temp_df.T
    temp_df.drop([0,1,2,3,5,6,7], axis=1, inplace=True)

    col = temp_df.loc[0].values.tolist()
    col[0] = 'time'
    n = 0
    z = [4]
    for k in range(len(col)):
        if type(col[k]) != str:
            break
        n += 1
        col[k] = col[k].split(' ')[0]
        col[k] = col[k].replace('其中:', '')

        if col[k] != 'time' and col[k] != '社会融资规模存量':
            col[k] = '社会融资规模存量:' + col[k]

        z.append(8+k)

    col = col[:n]

    temp_df = temp_df.loc[:, z[:len(z)-1]]
    temp_df.dropna(how='all', subset=[8], inplace=True)
    temp_df = temp_df.loc[1:, ]
    temp_df.columns = col
    temp_df.reset_index(inplace=True, drop=True)

    new_col = col.copy()
    for k in range(1, len(col)):
        new_col.append(col[k]+' 同比')

    df = pd.DataFrame(columns=new_col)
    for k in range(0, len(temp_df), 2):
        data = temp_df.loc[k, :].values.tolist()
        data += temp_df.loc[k+1, col[1:]].values.tolist()
        df.loc[k//2] = data

    df['time'] = pd.to_datetime(df['time'], format='%Y.%m')
    df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m'))

    for c in new_col:
        if (c != 'time') and not('同比' in c):
            z = np.array(df[c], dtype=float)
            df[c] = z*10000

    # print(df)
    return df


# Aggregate Financing to the Real Economy
# 社会融资规模
def update_afre_data():
    se = requests.session()
    current_year = datetime.datetime.now().year
    path = os.path.join(pboc_dir, '社会融资规模'+'.csv')
    if os.path.exists(path):
        old_df = pd.read_csv(path)
        old_t = pd.DatetimeIndex(pd.to_datetime(old_df['time'], format='%Y-%m'))
        month = old_t[-1].month
        start_year = old_t[-1].year
        if month == 12:
            start_year += 1
    else:
        old_df = pd.DataFrame()
        start_year = 2015


    path = os.path.join(pboc_dir, 'pboc_url'+'.csv')
    url_df = pd.read_csv(path)
    url_t = np.array(url_df['time'], dtype=str)

    df = pd.DataFrame()
    while (start_year <= current_year):
        print('社会融资规模', start_year)
        idx = np.where(url_t == str(start_year))[0][0]
        url1 = url_df.loc[idx, '社会融资规模增量']
        url2 = url_df.loc[idx, '社会融资规模存量']

        print(url1)
        df1 = get_afre_data1(se, url1)
        if start_year > 2015:
            df2 = get_afre_data2(se, url2)
            df1 = pd.merge(df1, df2, on='time', how='outer')

        df = pd.concat([df, df1], axis=0)
        start_year += 1

    path = os.path.join(pboc_dir, '社会融资规模'+'.csv')
    old_df = pd.concat([old_df, df], axis=0)
    old_df.drop_duplicates(subset=['time'], keep='last', inplace=True) # last
    old_df['time'] = pd.to_datetime(old_df['time'], format='%Y-%m')
    old_df.sort_values(by='time', axis=0, ascending=True, inplace=True)
    old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m'))
    old_df.to_csv(path, encoding='utf-8', index=False)


# 金融机构人民币信贷收支表
def get_summary_of_sources_and_uses(se=None, url=None, name=None):
    if se is None:
        se = requests.session()
    PBOC_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                    'Host': 'www.pbc.gov.cn'}

    r = se.post(url, headers=PBOC_HEADERS)
    try:
        s = r.content.decode('gbk')
        df = pd.read_html(StringIO(s))[0]
    except:
        s = r.content
        df = pd.read_html(s)[0]

    start_idx = 0
    end_idx = 0
    remove_idx = 0
    time_idx = 0
    for i in range(len(df)):
        s = df.loc[i, 0]
        if type(s) == str:
            s = s.strip()
            if ('项目' in s) and ('Item' in s):
                time_idx = i
            if '来源方项目' in s:
                start_idx = i+1
            if '运用方项目' in s:
                remove_idx = i
            if '注：' in s:
                end_idx = i-2

    reserve = [time_idx]
    for i in range(start_idx, end_idx+1):
        reserve.append(i)
    df = df.loc[reserve,]
    df.drop(remove_idx, axis=0, inplace=True)
    df.dropna(how='all', axis=1, inplace=True)
    df = df.T

    col = df.loc[0,].values.tolist()
    prefix = []
    for i in range(len(col)):
        ss = re.findall('[\u4e00-\u9fa5]', col[i])
        s = ''
        for c in ss:
            if c in ['一', '二', '三' , '四', '五' , '六', '七']:
                continue
            s += c

        if '各项存款' in s:
            prefix.append('各项存款:')

        if '境内存款' in s:
            prefix.append('境内:')

        if '住户存款' in s:
            prefix.append('住户:')

        if '非金融企业存款' in s:
            del prefix[-1]
            prefix.append('非金融企业:')

        if '政府存款' in s:
            del prefix[-1]

        if ('机关团体存款' in s) or ('财政性存款' in s):
            if '非金融企业:' in prefix:
                del prefix[-1]
      
        if ('境外存款' in s):
            del prefix[-1]   

        if ('金融债券' in s):
            del prefix[-1]

        ################################
        if '各项贷款' in s:
            prefix.append('各项贷款:')

        if '境内贷款' in s:
            prefix.append('境内:')
   
        if '住户贷款' in s:
            prefix.append('住户:')

        if ('短期贷款' in s) and ('住户:' in prefix):
            prefix.append('短期:')

        if ('中长期贷款' in s) and ('住户:' in prefix):
            del prefix[-1]
            prefix.append('中长期:')

        if ('企事业单位贷款' in s) or ('非金融企业及机关团体贷款'in s):
            s = '企事业单位贷款'
            del prefix[-1]
            del prefix[-1]
            prefix.append('企事业单位:')

        if '非银行业金融机构贷款' in s:
            del prefix[-1]

        if '境外贷款' in s:
            del prefix[-1]

        if '债券投资' in s:
            del prefix[-1]

        if '外汇买卖' in s:
            s = '中央银行外汇占款'

        if len(prefix) > 0 and not((s == '各项存款') or (s == '各项贷款')):
            s = s.replace('存款', '')
            s = s.replace('贷款', '')
            p = ''
            for c in prefix:
                if not(s in c):
                    p = p + c
            s = p + s
    
        col[i] = s

    col[0] = 'time'
    df = df.loc[1:, ]
    df.columns = col
    df.reset_index(inplace=True, drop=True)
    df.dropna(how='any', axis=0, inplace=True)
    df['time'] = pd.to_datetime(df['time'], format='%Y.%m')
    df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m'))
    return df


def update_summary_of_sources_and_uses(name):
    se = requests.session()
    current_year = datetime.datetime.now().year
    path = os.path.join(pboc_dir, name+'.csv')
    if os.path.exists(path):
        old_df = pd.read_csv(path)
        old_t = pd.DatetimeIndex(pd.to_datetime(old_df['time'], format='%Y-%m'))
        month = old_t[-1].month
        start_year = old_t[-1].year
        if month == 12:
            start_year += 1
    else:
        old_df = pd.DataFrame()
        start_year = 2015


    path = os.path.join(pboc_dir, 'pboc_url'+'.csv')
    url_df = pd.read_csv(path)
    url_t = np.array(url_df['time'], dtype=str)

    df = pd.DataFrame()
    while (start_year <= current_year):
        print(name, start_year)
        idx = np.where(url_t == str(start_year))[0][0]
        url = url_df.loc[idx, name]

        df1 = get_summary_of_sources_and_uses(se, url, name)
        df = pd.concat([df, df1], axis=0)
        start_year += 1

    path = os.path.join(pboc_dir, name+'.csv')
    old_df = pd.concat([old_df, df], axis=0)
    old_df.drop_duplicates(subset=['time'], keep='last', inplace=True) # last
    old_df['time'] = pd.to_datetime(old_df['time'], format='%Y-%m')
    old_df.sort_values(by='time', axis=0, ascending=True, inplace=True)
    old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m'))
    old_df.to_csv(path, encoding='utf-8', index=False)


# 存款性公司概览
def update_depository_corporationss_survey():
    se = requests.session()
    PBOC_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                    'Host': 'www.pbc.gov.cn'}

    current_year = datetime.datetime.now().year
    name = '存款性公司概览'
    path = os.path.join(pboc_dir, name+'.csv')
    if os.path.exists(path):
        old_df = pd.read_csv(path)
        old_t = pd.DatetimeIndex(pd.to_datetime(old_df['time'], format='%Y-%m'))
        month = old_t[-1].month
        start_year = old_t[-1].year
        if month == 12:
            start_year += 1
    else:
        old_df = pd.DataFrame()
        start_year = 2015


    path = os.path.join(pboc_dir, 'pboc_url'+'.csv')
    url_df = pd.read_csv(path)
    url_t = np.array(url_df['time'], dtype=str)

    df = pd.DataFrame()
    while (start_year <= current_year):
        print(name, start_year)
        idx = np.where(url_t == str(start_year))[0][0]
        url = url_df.loc[idx, name]


        r = se.post(url, headers=PBOC_HEADERS)
        try:
            s = r.content.decode('gbk')
            temp_df = pd.read_html(StringIO(s))[0]
        except:
            s = r.content
            temp_df = pd.read_html(s)[0]

        i = 0
        while (1):
            s = temp_df.loc[i, 0]
            if type(s) == str and '国外净资产' in s:
                break
            i += 1

        k = 0
        while (1):
            s = temp_df.loc[k, 0]
            if type(s) == str and '其他（净）' in s:
                break
            k += 1

        reserve = [i-2]
        for i in range(i, k+1):
            reserve.append(i)
        temp_df = temp_df.loc[reserve,]
        temp_df.dropna(how='any', axis=1, inplace=True)
        temp_df = temp_df.T
        temp_df.dropna(how='any', axis=1, inplace=True)

        
        col = temp_df.loc[0].values.tolist()
        for i in range(len(col)):
            if (i == 0):
                col[i] = 'time'
                continue

            ss = col[i]
            c = re.findall('[A-Z]', col[i])
            ss = ss.replace(' ', '')
            w = ss.find(c[0])
            ss = ss[:w]
            if '个人存款' in ss:
                ss = '个人存款'
            col[i] = ss

        print(col)
        # exit()
    #     exit()
        temp_df.columns = col
        temp_df.drop(0, inplace=True)
        temp_df['time'] = temp_df['time'].apply(lambda x:x.replace('.', '-'))


        df = pd.concat([df, temp_df], axis=0)
        start_year += 1

    path = os.path.join(pboc_dir, name+'.csv')
    old_df = pd.concat([old_df, df], axis=0)
    old_df.drop_duplicates(subset=['time'], keep='last', inplace=True) # last
    old_df['time'] = pd.to_datetime(old_df['time'], format='%Y-%m')
    old_df.sort_values(by='time', axis=0, ascending=True, inplace=True)
    old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m'))
    old_df.to_csv(path, encoding='utf-8', index=False)


if __name__=="__main__":
    # update_pboc_url()
    update_depository_corporationss_survey()

    # update_pboc_url()

    # # 社会融资规模
    # update_afre_data()

    # # 信贷收支表
    # update_summary_of_sources_and_uses('金融机构本外币信贷收支表')
    # update_summary_of_sources_and_uses('金融机构人民币信贷收支表')
    # update_summary_of_sources_and_uses('金融机构外汇信贷收支表')
    # update_summary_of_sources_and_uses('存款类金融机构本外币信贷收支表')
    # update_summary_of_sources_and_uses('存款类金融机构人民币信贷收支表')
    # update_summary_of_sources_and_uses('存款类金融机构外汇信贷收支表')
    

    # plot_financing_data()

    pass
