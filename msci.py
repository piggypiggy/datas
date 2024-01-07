import os
import time
import numpy as np
import pandas as pd
import datetime
import requests
from utils import *
from cftc import *

msci_code_name = {
    '301700': 'AC ASIA',
    '899800': 'AC ASIA ex JAPAN',

    # china
    '302400': 'CHINA',
    '105890': 'CHINA GROWTH',
    '105891': 'CHINA VALUE',
    '136057': 'CHINA A ONSHORE GROWTH',
    '136056': 'CHINA A ONSHORE VALUE',
    '735577': 'CHINA A 50 CONNECT',

    # FM COUNTIES
    '136641': 'UKRAINE',
    '700873': 'ZIMBABWE',
    '958600': 'PAKISTAN',
    '136647': 'VIETNAM',
    '903200': 'ARGENTINA',

    ## EM COUNTRIES
    # EUROPE
    '920000': 'CZECH REPUBLIC',
    '930000': 'GREECE',
    '934800': 'HUNGARY',
    '961600': 'POLAND',

    # AFRICA
    '971000': 'SOUTH AFRICA',

    # ASIA
    '935600': 'INDIA',
    '105767': 'INDONESIA',
    '941000': 'KOREA',
    '105768': 'MALAYSIA',
    '860800': 'PHILIPPINES',
    '915800': 'TAIWAN',
    '105769': 'THAILAND',

    # NORTH AMERICA
    '848400': 'MEXICO',

    # MIDDLE EAST
    '105766': 'EGYPT',
    '133713': 'KUWAIT',
    '133715': 'QATAR',
    '705405': 'SAUDI ARABIA',
    '979200': 'UNITED ARAB EMIRATES',

    # SOUTH AMERICA
    '907600': 'BRAZIL',
    '700403': 'BRAZIL ADR',
    '915200': 'CHILE',
    '917000': 'COLOMBIA',
    '960400': 'PERU',

    ## DM COUNTRIES
    # EUROPE
    '904000': 'AUSTRIA',
    '905600': 'BELGIUM',
    '920800': 'DENMARK',
    '924600': 'FINLAND',
    '925000': 'FRANCE',
    '105785': 'FRANCE GROWTH',
    '105786': 'FRANCE VALUE',
    '928000': 'GERMANY',
    '105787': 'GERMANY GROWTH',
    '105788': 'GERMANY VALUE',
    '937200': 'IRELAND',
    '938000': 'ITALY',
    '952800': 'NETHERLANDS',
    '957800': 'NORWAY',
    '962000': 'PORTUGAL',
    '972400': 'SPAIN',
    '975200': 'SWEDEN',
    '975600': 'SWITZERLAND',
    '982600': 'UNITED KINGDOM',
    '105823': 'UNITED KINGDOM GROWTH',
    '105824': 'UNITED KINGDOM VALUE',

    # ASIA PACIFIC
    '903600': 'AUSTRALIA',
    '934400': 'HONG KONG',
    '939200': 'JAPAN',
    '105795': 'JAPAN GROWTH', 
    '105796': 'JAPAN VALUE', 
    '955400': 'NEW ZEALAND',
    '998100': 'SINGAPORE',

    # NORTH AMERICA
    '912400': 'CANADA',
    '984000': 'USA',
    '105825': 'USA GROWTH',
    '105826': 'USA VALUE',

    # MIDDLE EAST
    '300400': 'ISRAEL',

    ######################################################################
    # https://www.msci.com/our-solutions/indexes/acwi
    '892400': 'ACWI',  # WORLD + EM

    # EM
    '711886': 'ASEAN',   # Indonesia, Malaysia, Philippines, Thailand, Singapore, Vietnam.
    '139921': 'ASEAN GROWTH',
    '139922': 'ASEAN VALUE',

    '127300': 'BRIC',
    '891800': 'EM',
    '106062': 'EM GRWOTH',
    '106063': 'EM VALUE',
    '713021': 'EM ex CHINA',
    '711885': 'EM ASEAN',
    '899700': 'EM ASIA',  # China, India, Indonesia, Korea, Malaysia, Philippines, Taiwan and Thailand.
    '106066': 'EM ASIA GROWTH',
    '106067': 'EM ASIA VALUE',
    '121659': 'EM ex ASIA',
    '892000': 'EM LATIN AMERICA',
    '146305': 'EM GCC COUNTRIES',
    '139899': 'ANDEAN',  # Argentina, Chile, Colombia and Peru.
    '700098': 'EM EASTERN EUROPE ex RUSSIA',

    # DM
    '990100': 'WORLD',   # 24 DM COUNTIES
    '105867': 'WORLD GRWOTH',
    '105868': 'WORLD VALUE',
    '105873': 'WORLD ex USA GRWOTH',
    '105874': 'WORLD ex USA VALUE',
    '990300': 'EAFE',    # 22 DM COUNTIES, WORLD EX US&CANADA
    '990200': 'NORTH AMERICA',  # US&CANADA
    '990500': 'EUROPE',
    '105843': 'EUROPE GRWOTH',
    '105844': 'EUROPE VALUE',
    '990800': 'PACIFIC',  # Australia, Hong Kong, New Zealand, Singapore, Japan
    '748764': 'EAFE Expanded ADR',
    '113647': 'G7',
    '991200': 'WORLD ex JAPAN',
    '990700': 'NORDIC COUNTRIES',  # Denmark, Finland, Norway and Sweden.

    # GCC
    '707258': 'GCC COUNTRIES',  # Bahrain, Kuwait, Oman, Qatar, Saudi Arabia, United Arab Emirates
}

# example : "https://app2.msci.com/products/service/index/indexmaster/getLevelDataForGraph?currency_symbol=USD&index_variant=STRD&start_date=20000101&end_date=20040101&data_frequency=DAILY&index_codes=718708"
MSCI_URL = "https://app2.msci.com/products/service/index/indexmaster/getLevelDataForGraph?currency_symbol=USD&index_variant=STRD&start_date={}&end_date={}&data_frequency=DAILY&index_codes={}"
MSCI_LATEST_WORLD_DATE_URL = "https://app2.msci.com/products/service/index/indexmaster/getLatestWorldDate"

headers = {
    "Accept": "application/json, */*; q=0.01",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Host": "app2.msci.com",
    "Proxy-Connection": "keep-alive",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
}

def update_all_msci_index():
    earlist_time = '2000-01-01'
    se = requests.session()

    r = se.get(MSCI_LATEST_WORLD_DATE_URL, headers=headers)
    if (r.status_code == 200):
        # print(r.content)
        latest_msci_time_dt = pd.to_datetime(r.text, format='%Y%m%d')
        cookies = r.cookies
        print('MSCI LATEST TIME: ', r.text)
    else:
        print('status_code ==', r.status_code)
        return

    for code in msci_code_name:
        name = msci_code_name[code]
        path = os.path.join(msci_dir, name+'.csv')
        if os.path.exists(path):
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

                try:
                    last_line_dt = pd.to_datetime(last_line[:10], format='%Y-%m-%d')
                    start_time_dt = last_line_dt + pd.Timedelta(days=1)
                    start_time = start_time_dt.strftime('%Y%m%d')
                except:
                    start_time = earlist_time
                    start_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')
        else:
            df = pd.DataFrame(columns=['time', 'level_eod'])
            df.to_csv(path, encoding='utf-8', index=False)
            start_time = earlist_time
            start_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')

        while (start_time_dt < latest_msci_time_dt):
            weekday = start_time_dt.weekday()
            if (weekday >= 5):
                start_time_dt = start_time_dt + pd.Timedelta(7-weekday) # 下周一
                if (start_time_dt >= latest_msci_time_dt):
                    break

            end_time_dt = datetime.datetime(start_time_dt.year+4, 1, 1) # 一次拿4年的数据
            if (end_time_dt > latest_msci_time_dt):
                end_time_dt = latest_msci_time_dt
            start_time = start_time_dt.strftime('%Y%m%d')

            weekday = end_time_dt.weekday()
            if (weekday >= 5):
                end_time_dt = end_time_dt + pd.Timedelta(days=2)
                if (end_time_dt > latest_msci_time_dt):
                    end_time_dt = latest_msci_time_dt
            end_time = end_time_dt.strftime('%Y%m%d')

            url = MSCI_URL.format(start_time, end_time, code)
            print(name, start_time + ' - ' + end_time)
            while (1):
                try:
                    r = se.get(url, verify=False, cookies=cookies, headers=headers)
                    if (r.status_code == 200):
                        break
                    else:
                        print('status_code ==', r.status_code)
                        time.sleep(15)
                except:
                    time.sleep(15)
                    continue

            data_json = r.json()
            # print(data_json)
            df = pd.DataFrame(data_json["indexes"]["INDEX_LEVELS"])
            if (len(df) > 0):
                df.rename(columns={"calc_date": "time"}, inplace=True)
                df = df[['time','level_eod']]
                df['time'] = pd.to_datetime(df['time'], format='%Y%m%d')
                df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))

                if os.path.exists(path):
                    old_df = pd.read_csv(path)
                    old_df = pd.concat([old_df, df], axis=0)
                    old_df.drop_duplicates(subset=['time'], keep='last', inplace=True) # last
                    old_df['time'] = pd.to_datetime(old_df['time'], format='%Y-%m-%d')
                    old_df.sort_values(by='time', axis=0, ascending=True, inplace=True)
                    old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
                    old_df.to_csv(path, encoding='utf-8', index=False)
                else:
                    df.to_csv(path, encoding='utf-8', index=False)

                # temp_df['time'] = temp_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
                # temp_df.to_csv(path, mode='a', encoding='utf-8', index=False, header=None)

            start_time_dt = end_time_dt + pd.Timedelta(days=1)


def read_csv_data(path, names, header=[0], start_time='2000-01-01', end_time='2100-01-01'):
    df = pd.read_csv(path, header=header)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    datas = []
    for i in range(names):
        data = np.array(df[names[i]], dtype=float)
        t1, data1 = get_period_data(t, data, start=start_time, end=end_time)
        datas.append([t1, data1])

    return datas

# CHINA INDIA EM
def test1():
    path1 = os.path.join(msci_dir, 'CHINA'+'.csv')
    path11 = os.path.join(msci_dir, 'CHINA A 50 CONNECT'+'.csv')
    path2 = os.path.join(msci_dir, 'INDIA'+'.csv')
    path3 = os.path.join(msci_dir, 'VIETNAM'+'.csv')
    path9 = os.path.join(msci_dir, 'EM ASIA'+'.csv')

    df1 = pd.read_csv(path1)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    data1 = np.array(df1['level_eod'], dtype=float)

    df11 = pd.read_csv(path11)
    t11 = pd.DatetimeIndex(pd.to_datetime(df11['time'], format='%Y-%m-%d'))
    data11 = np.array(df11['level_eod'], dtype=float)

    df2 = pd.read_csv(path2)
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
    data2 = np.array(df2['level_eod'], dtype=float)

    df3 = pd.read_csv(path3)
    t3 = pd.DatetimeIndex(pd.to_datetime(df3['time'], format='%Y-%m-%d'))
    data3 = np.array(df3['level_eod'], dtype=float)


    df9 = pd.read_csv(path9)
    t9 = pd.DatetimeIndex(pd.to_datetime(df9['time'], format='%Y-%m-%d'))
    data9 = np.array(df9['level_eod'], dtype=float)

    # rebase to start_time
    start_time = '2015-01-01'
    t1, data1 = get_period_data(t1, data1, start=start_time)
    t11, data11 = get_period_data(t11, data11, start=start_time)
    t2, data2 = get_period_data(t2, data2, start=start_time)
    t3, data3 = get_period_data(t3, data3, start=start_time)
    t9, data9 = get_period_data(t9, data9, start=start_time)

    data1 = data1*100 / data1[0]
    data11 = data11*100 / data11[0]
    data2 = data2*100 / data2[0]
    data3 = data3*100 / data3[0]
    data9 = data9*100 / data9[0]

   
    datas = [[[[t1,data1,'MSCI CHINA 2015=100',''],
               [t1,data11,'MSCI CHINA A50 CONNECT 2015=100',''],
               ],[],''],]
    plot_many_figure(datas)
   
    datas = [[[[t1,data1,'MSCI CHINA 2015=100',''],
               [t1,data2,'MSCI INDIA 2015=100',''],
               [t1,data9,'MSCI EM ASIA 2015=100',''],
               ],[],''],

               [[[t1, data1-data9, 'MSCI CHINA - EM ASIA',''],
                 [t1, data2-data9, 'MSCI INDIA - EM ASIA','']],[],''],]
    plot_many_figure(datas)

    datas = [[[[t1,data11,'MSCI CHINA A50 CONNECT 2015=100',''],
               [t1,data2,'MSCI INDIA 2015=100',''],
               [t1,data9,'MSCI EM ASIA 2015=100',''],
               ],[],''],

               [[[t1, data11-data9, 'MSCI CHINA A50 CONNECT - EM ASIA',''],
                 [t1, data2-data9, 'MSCI INDIA - EM ASIA','']],[],''],]
    plot_many_figure(datas)

    datas = [[[[t1,data1,'MSCI CHINA 2015=100',''],
               [t1,data3,'MSCI VIETNAM 2015=100',''],
               [t1,data9,'MSCI EM ASIA 2015=100',''],
               ],[],''],

               [[[t1, data1-data9, 'MSCI CHINA - EM ASIA',''],
                 [t1, data3-data9, 'MSCI VIETNAM - EM ASIA','']],[],''],]
    plot_many_figure(datas)


#  EM DM
def test2():
    path1 = os.path.join(msci_dir, 'EM'+'.csv')
    path2 = os.path.join(msci_dir, 'EM ASIA'+'.csv')
    path3 = os.path.join(msci_dir, 'EM ex CHINA'+'.csv')
    path4 = os.path.join(msci_dir, 'EM EASTERN EUROPE ex RUSSIA'+'.csv')
    path5 = os.path.join(msci_dir, 'WORLD'+'.csv')
    path6 = os.path.join(msci_dir, 'CHINA'+'.csv')
    path9 = os.path.join(msci_dir, 'ACWI'+'.csv')

    df1 = pd.read_csv(path1)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    data1 = np.array(df1['level_eod'], dtype=float)

    df2 = pd.read_csv(path2)
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
    data2 = np.array(df2['level_eod'], dtype=float)

    df3 = pd.read_csv(path3)
    t3 = pd.DatetimeIndex(pd.to_datetime(df3['time'], format='%Y-%m-%d'))
    data3 = np.array(df3['level_eod'], dtype=float)

    df4 = pd.read_csv(path4)
    t4 = pd.DatetimeIndex(pd.to_datetime(df4['time'], format='%Y-%m-%d'))
    data4 = np.array(df4['level_eod'], dtype=float)

    df5 = pd.read_csv(path5)
    t5 = pd.DatetimeIndex(pd.to_datetime(df5['time'], format='%Y-%m-%d'))
    data5 = np.array(df5['level_eod'], dtype=float)

    df6 = pd.read_csv(path6)
    t6 = pd.DatetimeIndex(pd.to_datetime(df6['time'], format='%Y-%m-%d'))
    data6 = np.array(df6['level_eod'], dtype=float)

    df9 = pd.read_csv(path9)
    t9 = pd.DatetimeIndex(pd.to_datetime(df9['time'], format='%Y-%m-%d'))
    data9 = np.array(df9['level_eod'], dtype=float)

    # rebase to start_time
    start_time = '2015-01-01'
    t1, data1 = get_period_data(t1, data1, start=start_time)
    t2, data2 = get_period_data(t2, data2, start=start_time)
    t3, data3 = get_period_data(t3, data3, start=start_time)
    t4, data4 = get_period_data(t4, data4, start=start_time)
    t5, data5 = get_period_data(t5, data5, start=start_time)
    t6, data6 = get_period_data(t6, data6, start=start_time)
    t9, data9 = get_period_data(t9, data9, start=start_time)

    data1 = data1*100 / data1[0]
    data2 = data2*100 / data2[0]
    data3 = data3*100 / data3[0]
    data4 = data4*100 / data4[0]
    data5 = data5*100 / data5[0]
    data6 = data6*100 / data6[0]
    data9 = data9*100 / data9[0]

   
    datas = [[[[t1,data1,'MSCI EM 2015=100',''],
               [t1,data2,'MSCI WORLD 2015=100',''],
               [t1,data4,'MSCI EM EASTERN EUROPE ex RUSSIA 2015=100',''],
               [t1,data9,'MSCI ACWI 2015=100',''],
               ],[],''],
            [[[t1, data1-data4, 'MSCI EM - EM EASTERN EUROPE ex RUSSIA',''],
              ],[],''],]
    plot_many_figure(datas)
   

    datas = [[[[t1,data5,'MSCI WORLD',''],
               [t1,data6,'MSCI CHINA',''],
               ],[],''],
            [[[t1, data5-data6, 'MSCI WORLD - CHINA',''],
              ],[],''],]
    plot_many_figure(datas)



# CHINA GROWTH VALUE
def test3():
    path1 = os.path.join(msci_dir, 'CHINA GROWTH'+'.csv')
    path2 = os.path.join(msci_dir, 'CHINA VALUE'+'.csv')
    path3 = os.path.join(msci_dir, 'CHINA A ONSHORE GROWTH'+'.csv')
    path4 = os.path.join(msci_dir, 'CHINA A ONSHORE value'+'.csv')
    path5 = os.path.join(msci_dir, 'CHINA'+'.csv')
    path6 = os.path.join(msci_dir, 'CHINA A 50 CONNECT'+'.csv')

    df1 = pd.read_csv(path1)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    data1 = np.array(df1['level_eod'], dtype=float)

    df2 = pd.read_csv(path2)
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
    data2 = np.array(df2['level_eod'], dtype=float)

    df3 = pd.read_csv(path3)
    t3 = pd.DatetimeIndex(pd.to_datetime(df3['time'], format='%Y-%m-%d'))
    data3 = np.array(df3['level_eod'], dtype=float)

    df4 = pd.read_csv(path4)
    t4 = pd.DatetimeIndex(pd.to_datetime(df4['time'], format='%Y-%m-%d'))
    data4 = np.array(df4['level_eod'], dtype=float)

    df5 = pd.read_csv(path5)
    t5 = pd.DatetimeIndex(pd.to_datetime(df5['time'], format='%Y-%m-%d'))
    data5 = np.array(df5['level_eod'], dtype=float)

    df6 = pd.read_csv(path6)
    t6 = pd.DatetimeIndex(pd.to_datetime(df6['time'], format='%Y-%m-%d'))
    data6 = np.array(df6['level_eod'], dtype=float)


    # rebase to start_time
    start_time = '2015-01-01'
    t1, data1 = get_period_data(t1, data1, start=start_time)
    t2, data2 = get_period_data(t2, data2, start=start_time)
    t3, data3 = get_period_data(t3, data3, start=start_time)
    t4, data4 = get_period_data(t4, data4, start=start_time)
    t5, data5 = get_period_data(t5, data5, start=start_time)
    t6, data6 = get_period_data(t6, data6, start=start_time)

    data1 = data1*100 / data1[0]
    data2 = data2*100 / data2[0]
    data3 = data3*100 / data3[0]
    data4 = data4*100 / data4[0]
    data5 = data5*100 / data5[0]
    data6 = data6*100 / data6[0]

   
    datas = [[[[t1,data1,'CHINA GROWTH',''],
               [t1,data2,'CHINA VALUE',''],
               [t1,data5,'CHINA',''],
               ],[],''],
            [[[t1, data1-data5, 'MSCI CHINA GROWTH - CHINA',''],
              [t1, data2-data5, 'MSCI CHINA VALUE - CHINA',''],
              ],[],''],]
    plot_many_figure(datas)
   
    datas = [[[[t1,data3,'CHINA ONSHORE GROWTH',''],
               [t1,data4,'CHINA ONSHORE VALUE',''],
               [t1,data5,'CHINA',''],
               ],[],''],
            [[[t1, data3-data5, 'MSCI CHINA ONSHORE GROWTH - CHINA',''],
              [t1, data4-data5, 'MSCI CHINA ONSHORE VALUE - CHINA',''],
              ],[],''],]
    plot_many_figure(datas)
   

# CHINA TAIWAN KOREA JAPAN SINGAPORE
def test5():
    path1 = os.path.join(msci_dir, 'CHINA'+'.csv')
    path11 = os.path.join(msci_dir, 'CHINA A 50 CONNECT'+'.csv')
    path2 = os.path.join(msci_dir, 'TAIWAN'+'.csv')
    path3 = os.path.join(msci_dir, 'KOREA'+'.csv')
    path4 = os.path.join(msci_dir, 'JAPAN'+'.csv')
    path5 = os.path.join(msci_dir, 'SINGAPORE'+'.csv')
    path6 = os.path.join(msci_dir, 'INDIA'+'.csv')
    path7 = os.path.join(msci_dir, 'VIETNAM'+'.csv')
    path9 = os.path.join(msci_dir, 'AC ASIA'+'.csv')

    df1 = pd.read_csv(path1)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    data1 = np.array(df1['level_eod'], dtype=float)

    df11 = pd.read_csv(path11)
    t11 = pd.DatetimeIndex(pd.to_datetime(df11['time'], format='%Y-%m-%d'))
    data11 = np.array(df11['level_eod'], dtype=float)

    df2 = pd.read_csv(path2)
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
    data2 = np.array(df2['level_eod'], dtype=float)

    df3 = pd.read_csv(path3)
    t3 = pd.DatetimeIndex(pd.to_datetime(df3['time'], format='%Y-%m-%d'))
    data3 = np.array(df3['level_eod'], dtype=float)

    df4 = pd.read_csv(path4)
    t4 = pd.DatetimeIndex(pd.to_datetime(df4['time'], format='%Y-%m-%d'))
    data4 = np.array(df4['level_eod'], dtype=float)

    df5 = pd.read_csv(path5)
    t5 = pd.DatetimeIndex(pd.to_datetime(df5['time'], format='%Y-%m-%d'))
    data5 = np.array(df5['level_eod'], dtype=float)

    df6 = pd.read_csv(path6)
    t6 = pd.DatetimeIndex(pd.to_datetime(df6['time'], format='%Y-%m-%d'))
    data6 = np.array(df6['level_eod'], dtype=float)

    df7 = pd.read_csv(path7)
    t7 = pd.DatetimeIndex(pd.to_datetime(df7['time'], format='%Y-%m-%d'))
    data7 = np.array(df7['level_eod'], dtype=float)

    df9 = pd.read_csv(path9)
    t9 = pd.DatetimeIndex(pd.to_datetime(df9['time'], format='%Y-%m-%d'))
    data9 = np.array(df9['level_eod'], dtype=float)

    # rebase to start_time
    start_time = '2019-01-01'
    year = start_time[:4]
    t1, data1 = get_period_data(t1, data1, start=start_time)
    t11, data11 = get_period_data(t11, data11, start=start_time)
    t2, data2 = get_period_data(t2, data2, start=start_time)
    t3, data3 = get_period_data(t3, data3, start=start_time)
    t4, data4 = get_period_data(t4, data4, start=start_time)
    t5, data5 = get_period_data(t5, data5, start=start_time)
    t6, data6 = get_period_data(t6, data6, start=start_time)
    t7, data7 = get_period_data(t7, data7, start=start_time)
    t9, data9 = get_period_data(t9, data9, start=start_time)

    data1 = data1*100 / data1[0]
    data11 = data11*100 / data11[0]
    data2 = data2*100 / data2[0]
    data3 = data3*100 / data3[0]
    data4 = data4*100 / data4[0]
    data5 = data5*100 / data5[0]
    data6 = data6*100 / data6[0]
    data7 = data7*100 / data7[0]
    data9 = data9*100 / data9[0]

   
    datas = [[[[t1,data1,'MSCI CHINA '+year+'=100',''],
               [t1,data11,'MSCI CHINA A50 CONNECT '+year+'=100',''],
               [t2,data2,'MSCI TAIWAN '+year+'=100',''],
               [t4,data3,'MSCI KOREA '+year+'=100',''],
               [t4,data4,'MSCI JAPAN '+year+'=100',''],
               [t5,data5,'MSCI SINGAPORE '+year+'=100',''],
               [t6,data6,'MSCI INDIA '+year+'=100',''],
               [t7,data7,'MSCI VIETNAM '+year+'=100',''],
               [t9,data9,'MSCI AC ASIA '+year+'=100',''],
               ],[],''],]
    plot_many_figure(datas)
   

    datas = [[[[t1, data1-data9, 'MSCI CHINAT - AC ASIA',''],
               [t1, data11-data9, 'MSCI CHINA A50 CONNECT - AC ASIA',''],
               [t1, data2-data9, 'MSCI TAIWAN - AC ASIA',''],
               [t1, data3-data9, 'MSCI KOREA - AC ASIA',''],
               [t1, data4-data9, 'MSCI JAPAN - AC ASIA',''],
               [t1, data5-data9, 'MSCI SINGAPORE - AC ASIA',''],
               [t1, data6-data9, 'MSCI INDIA - AC ASIA',''],
               [t1, data7-data9, 'MSCI VIETNAM - AC ASIA',''],],[],''],]
    plot_many_figure(datas)


# CHINA BRAZIL MEXICO EU
def test6():
    path1 = os.path.join(msci_dir, 'CHINA'+'.csv')
    path11 = os.path.join(msci_dir, 'CHINA A 50 CONNECT'+'.csv')
    path2 = os.path.join(msci_dir, 'BRAZIL'+'.csv')
    path3 = os.path.join(msci_dir, 'MEXICO'+'.csv')
    path4 = os.path.join(msci_dir, 'GERMANY'+'.csv')
    path5 = os.path.join(msci_dir, 'FRANCE'+'.csv')
    path6 = os.path.join(msci_dir, 'UNITED KINGDOM'+'.csv')
    path7 = os.path.join(msci_dir, 'ARGENTINA'+'.csv')
    path9 = os.path.join(msci_dir, 'ACWI'+'.csv')

    df1 = pd.read_csv(path1)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    data1 = np.array(df1['level_eod'], dtype=float)

    df11 = pd.read_csv(path11)
    t11 = pd.DatetimeIndex(pd.to_datetime(df11['time'], format='%Y-%m-%d'))
    data11 = np.array(df11['level_eod'], dtype=float)

    df2 = pd.read_csv(path2)
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
    data2 = np.array(df2['level_eod'], dtype=float)

    df3 = pd.read_csv(path3)
    t3 = pd.DatetimeIndex(pd.to_datetime(df3['time'], format='%Y-%m-%d'))
    data3 = np.array(df3['level_eod'], dtype=float)

    df4 = pd.read_csv(path4)
    t4 = pd.DatetimeIndex(pd.to_datetime(df4['time'], format='%Y-%m-%d'))
    data4 = np.array(df4['level_eod'], dtype=float)

    df5 = pd.read_csv(path5)
    t5 = pd.DatetimeIndex(pd.to_datetime(df5['time'], format='%Y-%m-%d'))
    data5 = np.array(df5['level_eod'], dtype=float)

    df6 = pd.read_csv(path6)
    t6 = pd.DatetimeIndex(pd.to_datetime(df6['time'], format='%Y-%m-%d'))
    data6 = np.array(df6['level_eod'], dtype=float)

    df7 = pd.read_csv(path7)
    t7 = pd.DatetimeIndex(pd.to_datetime(df7['time'], format='%Y-%m-%d'))
    data7 = np.array(df7['level_eod'], dtype=float)

    df9 = pd.read_csv(path9)
    t9 = pd.DatetimeIndex(pd.to_datetime(df9['time'], format='%Y-%m-%d'))
    data9 = np.array(df9['level_eod'], dtype=float)

    # rebase to start_time
    start_time = '2019-01-01'
    year = start_time[:4]
    t1, data1 = get_period_data(t1, data1, start=start_time)
    t11, data11 = get_period_data(t11, data11, start=start_time)
    t2, data2 = get_period_data(t2, data2, start=start_time)
    t3, data3 = get_period_data(t3, data3, start=start_time)
    t4, data4 = get_period_data(t4, data4, start=start_time)
    t5, data5 = get_period_data(t5, data5, start=start_time)
    t6, data6 = get_period_data(t6, data6, start=start_time)
    t7, data7 = get_period_data(t7, data7, start=start_time)
    t9, data9 = get_period_data(t9, data9, start=start_time)

    data1 = data1*100 / data1[0]
    data11 = data11*100 / data11[0]
    data2 = data2*100 / data2[0]
    data3 = data3*100 / data3[0]
    data4 = data4*100 / data4[0]
    data5 = data5*100 / data5[0]
    data6 = data6*100 / data6[0]
    data7 = data7*100 / data7[0]
    data9 = data9*100 / data9[0]

   
    datas = [[[[t1,data1,'MSCI CHINA '+year+'=100',''],
               [t1,data11,'MSCI CHINA A50 CONNECT '+year+'=100',''],
               [t2,data2,'MSCI BRAZIL '+year+'=100',''],
               [t4,data3,'MSCI MEXICO '+year+'=100',''],
               [t4,data4,'MSCI GERMANY '+year+'=100',''],
               [t5,data5,'MSCI FRANCE '+year+'=100',''],
               [t6,data6,'MSCI UNITED KINGDOM '+year+'=100',''],
               [t7,data7,'MSCI ARGENTINA '+year+'=100',''],
               [t9,data9,'MSCI ACWI '+year+'=100',''],
               ],[],''],]
    plot_many_figure(datas)
   

    datas = [[[[t1, data1-data9, 'MSCI CHINA - ACWI',''],
               [t1, data11-data9, 'MSCI CHINA A50 CONNECT - ACWI',''],
               [t1, data2-data9, 'MSCI BRAZIL - ACWI',''],
               [t1, data3-data9, 'MSCI MEXICO - ACWI',''],
               [t1, data4-data9, 'MSCI GERMANY - ACWI',''],
               [t1, data5-data9, 'MSCI FRANCE - ACWI',''],
               [t1, data7-data9, 'MSCI ARGENTINA - ACWI',''],
               [t1, data6-data9, 'MSCI UNITED KINGDOM - ACWI',''],],[],''],]
    plot_many_figure(datas)



# 持仓
def test4():
    start_time = '2012-1-1'
    end_time = '2099-12-31'

    # MSCI EM INDEX
    path = os.path.join(msci_dir, 'EM'+'.csv')
    fut_df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time'], format='%Y-%m-%d'))
    price = np.array(fut_df['level_eod'])
    t0, price = get_period_data(t, price, start_time, end_time, remove_nan=True)
    cftc_plot_financial(t0, price, 'MSCI EM', code='244042', inst_name='ICUS:MSCI EM INDEX')

    # MSCI EAFE
    path = os.path.join(msci_dir, 'EAFE'+'.csv')
    fut_df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time'], format='%Y-%m-%d'))
    price = np.array(fut_df['level_eod'])
    t0, price = get_period_data(t, price, start_time, end_time, remove_nan=True)
    cftc_plot_financial(t0, price, 'MSCI EAFE', code='244041', inst_name='ICUS:MSCI EAFE INDEX')


def plot_saudi_vs_oil():
    path = os.path.join(msci_dir, 'SAUDI ARABIA'+'.csv')
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    price = np.array(df['level_eod'])

    path = os.path.join(cfd_dir, 'WTI_CFD'+'.csv')
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    wti = np.array(df['close'])

    t2, ratio = data_div(t, price, t1, wti)

    datas = [[[[t, price, 'SAUDI', '']],
              [[t1, wti, 'WTI', '']],''],

             [[[t2, ratio, 'SAUDI / WTI', '']],
              [],''],
            ]
    plot_many_figure(datas)


if __name__=="__main__":
    update_all_msci_index()

    plot_saudi_vs_oil()

    # CHINA INDIA VIETNAM EM
    test1()

    # EM DM
    test2()

    # CHINA GROWTH VALUE
    test3()

    # # 持仓
    test4()

    # CHINA TAIWAN KOREA JAPAN SINGAPORE
    test5()

    # CHINA BRAZIL MEXICO EU
    test6()

