import os
import requests
import pandas as pd
import datetime
import numpy as np
from utils import *

headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Host": "top.100ppi.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
        "Sec-Fetch-Site": "same-origin"
    }

spot_code_category = {
    '11': '能源',
    '12': '有色',
    '13': '钢铁',
    '14': '化工',
    '15': '橡塑',
    '16': '纺织',
    '17': '建材',
    '18': '农副',
}

def update_all_spot_price():
    earlist_time = '2018-01-01'
    se = requests.session()
    URL = 'https://top.100ppi.com/zdb/detail-day-{}-{}.html'

    for code in spot_code_category:
        path = os.path.join(spot_dir, spot_code_category[code]+'.csv')
        last_line_time = get_last_line_time(path, '', earlist_time, 10, '%Y-%m-%d')
        start_time_dt = pd.to_datetime(last_line_time, format='%Y-%m-%d')
        current_time_dt = datetime.datetime.now()

        df = pd.DataFrame()
        while (start_time_dt <= current_time_dt):
            time.sleep(0.5)
            start_time_dt = start_time_dt + pd.Timedelta(days=1)
            if start_time_dt.weekday() == 0:
                query_time = (start_time_dt - pd.Timedelta(days=3)).strftime('%Y-%m%d')
            elif start_time_dt.weekday() <= 4:
                query_time = (start_time_dt - pd.Timedelta(days=1)).strftime('%Y-%m%d')
            else:
                continue

            url = URL.format(query_time, code)
            print(url)
            while (1):
                try:
                    r = se.get(url, headers=headers, timeout=5)
                    break
                except:
                    print('SPOT GET DATA ERROR')
                    if len(df) > 0:
                        if os.path.exists(path):
                            old_df = pd.read_csv(path)
                            old_df = pd.concat([old_df, df], axis=0)
                            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
                            old_df.to_csv(path, encoding='utf-8', index=False)
                        else:
                            df.to_csv(path, encoding='utf-8', index=False)
                        df = pd.DataFrame()
                    time.sleep(30)

            print('生意社 '+spot_code_category[code]+'现货', start_time_dt.strftime('%Y-%m-%d'))
            if len(r.text) < 100:
                continue
            try:
                temp_df = pd.read_html(r.text)[0]
            except:
                print('SPOT PARSE DATA ERROR')
                return

            commodity = ['time'] + temp_df.loc[1:,0].tolist()
            price = [start_time_dt.strftime('%Y-%m-%d')] + temp_df.loc[1:,3].tolist()
            temp_df = pd.DataFrame(columns=commodity, data=[price])

            df = pd.concat([df, temp_df], axis=0)

            if len(df) >= 50:
                if os.path.exists(path):
                    old_df = pd.read_csv(path)
                    old_df = pd.concat([old_df, df], axis=0)
                    old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
                    old_df.to_csv(path, encoding='utf-8', index=False)
                else:
                    df.to_csv(path, encoding='utf-8', index=False)
                df = pd.DataFrame()              

        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df.to_csv(path, encoding='utf-8', index=False)


PK_CODE_NAME_DICT = {
    '118': '河南南阳',
    '119': '河南正阳',
    '120': '山东莒南',
    '121': '山东新泰',
    '122': '辽宁锦州',
}

_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Host": "",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36",
}

def update_pk_spot_price():
    earlist_time = '2021-02-01'
    se = requests.session()
    # URL = 'http://www.huasheng7.com/zs/search.php?catid=118&fromdate=20230901&todate=20230925'
    URL = 'http://www.huasheng7.com/zs/search.php?catid={}&fromdate={}&todate={}'

    path = os.path.join(spot_dir, 'PK_spot'+'.csv')
    last_line_time = get_last_line_time(path, '', earlist_time, 10, '%Y-%m-%d')
    start_time_dt = pd.to_datetime(last_line_time, format='%Y-%m-%d')
    current_time_dt = datetime.datetime.now()

    while (start_time_dt < current_time_dt):
        diff = current_time_dt - start_time_dt
        if diff.days >= 180:
            end_time_dt = start_time_dt + pd.Timedelta(days=180)
        else:
            end_time_dt = current_time_dt

        df = pd.DataFrame()
        price = None
        n = 0
        for code in PK_CODE_NAME_DICT:
            print('PK: ' + PK_CODE_NAME_DICT[code])
            url = URL.format(code, start_time_dt.strftime('%Y%m%d'), end_time_dt.strftime('%Y%m%d'))
            while (1):
                try:
                    r = se.get(url, headers=_HEADERS, timeout=10)
                    break
                except Exception as e:
                    print(e)
                    time.sleep(5)
            s = r.text
            # time
            w = s.find('categories')
            if w == -1:
                return
            w1 = s[w:].find('[')
            w2 = s[w:].find(']')
            t_str = s[w+w1+1 : w+w2].replace('\'', '')
            t_str = t_str.replace('\r\n', '')
            ts = t_str.split(',')
            ts = ts[0:len(ts)-1]
            for i in range(len(ts)):
                t = pd.to_datetime(ts[i], format='%Y/%m/%d').strftime('%Y-%m-%d')
                ts[i] = t

            # data
            w = s.find('data')
            if w == -1:
                return
            w1 = s[w:].find('[')
            w2 = s[w:].find(']')
            data_str = s[w+w1+1 : w+w2].replace('\r\n', '')
            data_str = data_str.replace(' ', '')
            data = data_str.split(',')
            data = data[0:len(data)-1]
            data = np.array(data, dtype=float)*2000  # 斤 -> 吨

            temp_df = pd.DataFrame()
            temp_df['time'] = ts
            temp_df[PK_CODE_NAME_DICT[code]] = data

            if (df.empty):
                df = temp_df.copy()
            else:
                df = pd.merge(df, temp_df, on='time', how='outer')

        for i in range(len(df)):
            price = 0
            n = 0
            for code in PK_CODE_NAME_DICT:
                if not np.isnan(df.loc[i, PK_CODE_NAME_DICT[code]]):
                    price += df.loc[i, PK_CODE_NAME_DICT[code]]
                    n = n + 1
            df.loc[i, 'avg_price'] = price/n

        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df.to_csv(path, encoding='utf-8', index=False)

        start_time_dt = end_time_dt


AP_CODE_NAME_DICT = {
    '199': '产区苹果（红富士）价格指数',
    '207': '烟台苹果（一、二级红富士）价格指数',
    '215': '批发市场苹果价格指数',
}

SPOT_INFO_DICT = {
    # variety: [CODE_NAME_DICT, URL, host, earlist_time, adjust]
    'AP': [AP_CODE_NAME_DICT, 'http://www.pingguo7.cn/zs/search.php?catid={}&fromdate={}&todate={}', 'www.pingguo7.cn', '2017-10-01', 1],
    'PK': [PK_CODE_NAME_DICT, 'http://www.huasheng7.com/zs/search.php?catid={}&fromdate={}&todate={}', 'www.huasheng7.com', '2021-02-01', 2000],
}

def update_other_spot_price(variety):
    se = requests.session()
    # URL = 'http://www.huasheng7.com/zs/search.php?catid=118&fromdate=20230901&todate=20230925'
    code_name_dict = SPOT_INFO_DICT[variety][0]
    URL = SPOT_INFO_DICT[variety][1]
    host = SPOT_INFO_DICT[variety][2]
    earlist_time = SPOT_INFO_DICT[variety][3]
    adjust = SPOT_INFO_DICT[variety][4]

    _HEADERS['Host'] = host

    path = os.path.join(spot_dir, variety+'_spot'+'.csv')
    last_line_time = get_last_line_time(path, '', earlist_time, 10, '%Y-%m-%d')
    start_time_dt = pd.to_datetime(last_line_time, format='%Y-%m-%d')
    current_time_dt = datetime.datetime.now()

    while (start_time_dt < current_time_dt):
        diff = current_time_dt - start_time_dt
        if diff.days >= 180:
            end_time_dt = start_time_dt + pd.Timedelta(days=180)
        else:
            end_time_dt = current_time_dt

        df = pd.DataFrame()
        price = None
        n = 0
        for code in code_name_dict:
            print(variety + ': ' + code_name_dict[code], start_time_dt.strftime('%Y%m%d'), end_time_dt.strftime('%Y%m%d'))
            url = URL.format(code, start_time_dt.strftime('%Y%m%d'), end_time_dt.strftime('%Y%m%d'))
            while (1):
                try:
                    r = se.get(url, headers=_HEADERS, timeout=10)
                    break
                except Exception as e:
                    print(e)
                    time.sleep(5)
            s = r.text
            # time
            w = s.find('categories')
            if w == -1:
                return
            w1 = s[w:].find('[')
            w2 = s[w:].find(']')
            t_str = s[w+w1+1 : w+w2].replace('\'', '')
            t_str = t_str.replace('\r\n', '')
            ts = t_str.split(',')
            ts = ts[0:len(ts)-1]
            for i in range(len(ts)):
                t = pd.to_datetime(ts[i], format='%Y/%m/%d').strftime('%Y-%m-%d')
                ts[i] = t

            # data
            w = s.find('data')
            if w == -1:
                return
            w1 = s[w:].find('[')
            w2 = s[w:].find(']')
            data_str = s[w+w1+1 : w+w2].replace('\r\n', '')
            data_str = data_str.replace(' ', '')
            data = data_str.split(',')
            data = data[0:len(data)-1]
            data = np.array(data, dtype=float)*adjust  # PK: 斤 -> 吨

            temp_df = pd.DataFrame()
            temp_df['time'] = ts
            temp_df[code_name_dict[code]] = data

            if (df.empty):
                df = temp_df.copy()
            else:
                df = pd.merge(df, temp_df, on='time', how='outer')

        for i in range(len(df)):
            price = 0
            n = 0
            for code in code_name_dict:
                if not np.isnan(df.loc[i, code_name_dict[code]]):
                    price += df.loc[i, code_name_dict[code]]
                    n = n + 1
            df.loc[i, 'avg_price'] = price/n

        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df.to_csv(path, encoding='utf-8', index=False)

        start_time_dt = end_time_dt


def update_cotton_spot_price():
    se = requests.session()
    url = 'http://www.cottonchina.org.cn/sdata_new/TrendLine_data.php'
    CF_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Host": "www.cottonchina.org.cn",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36",
    }
    payload = {
        'lname': 'cci',
        "enddate": datetime.datetime.now().strftime('%Y-%m-%d'),
    }

    r = se.post(url, headers=CF_HEADERS, data=payload)
    s = r.content.decode("unicode-escape").replace('"', '')

    w1 = s.find('[')
    w2 = s.find(']')
    t = s[w1+1:w2].split(',')

    w0 = s.find('sn1')
    w1 = s[w0:].find(',')
    name1 = s[w0+4:w0+w1]
    w1 = s[w0:].find('[')
    w2 = s[w0:].find(']')
    data1 = s[w0+w1+1:w0+w2].replace('-', '').split(',')

    w0 = s.find('sn2')
    w1 = s[w0:].find(',')
    name2 = s[w0+4:w0+w1]
    w1 = s[w0:].find('[')
    w2 = s[w0:].find(']')
    data2 = s[w0+w1+1:w0+w2].replace('-', '').split(',')

    w0 = s.find('sn3')
    w1 = s[w0:].find(',')
    name3 = s[w0+4:w0+w1]
    w1 = s[w0:].find('[')
    w2 = s[w0:].find(']')
    data3 = s[w0+w1+1:w0+w2].replace('-', '').split(',')

    df = pd.DataFrame()
    df['time'] = t
    df[name1] = data1
    df[name2] = data2
    df[name3] = data3

    path = os.path.join(spot_dir, 'CF_spot'+'.csv')
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


def plot_pk_basis():
    path1 = os.path.join(future_price_dir, 'czce', 'PK'+'.csv')
    df1 = pd.read_csv(path1, header=[0,1])
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    index_close = np.array(df1.loc[:, pd.IndexSlice['index', 'close']], dtype=float)
    c1_close = np.array(df1.loc[:, pd.IndexSlice['c1', 'close']], dtype=float)
    c2_close = np.array(df1.loc[:, pd.IndexSlice['c2', 'close']], dtype=float)
    c3_close = np.array(df1.loc[:, pd.IndexSlice['c3', 'close']], dtype=float)
    dom = np.array(df1.loc[:, pd.IndexSlice['dom', 'close']], dtype=float)
    start_time='2005-01-01'
    end_time='2099-01-01'

    try:
        path2 = os.path.join(spot_dir, 'PK_spot'+'.csv')
        df2 = pd.read_csv(path2)
        t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
        spot = np.array(df2['avg_price'], dtype=float)
    except:
        return
    
    name = 'PK'
    datas = [[[[t1,c1_close,name+' c1'],[t2,spot,name+' 现货']],[],''],
             [[[t1,c2_close,name+' c2'],[t2,spot,name+' 现货']],[],''],
             [[[t1,c3_close,name+' c3'],[t2,spot,name+' 现货']],[],''],
             [[[t1,dom,name+' 主力'],[t2,spot,name+' 现货']],[],''],
             [[[t1,index_close,name+' 指数'],[t2,spot,name+' 现货']],[],'']]

    L1 = len(datas)
    fig_list = list()

    for i in range(L1):
        left_list = list()
        if (i==0):
            fig_list.append(figure(frame_width=1400, frame_height=700//L1, tools=TOOLS, title=datas[i][2], x_axis_type = "datetime"))
        else:
            fig_list.append(figure(frame_width=1400, frame_height=700//L1, tools=TOOLS, title=datas[i][2], x_range=fig_list[0].x_range, x_axis_type = "datetime"))


        t, v = data_sub(datas[i][0][1][0], datas[i][0][1][1], datas[i][0][0][0], datas[i][0][0][1])
        y_column2_name = 'y2'
        fig_list[i].extra_y_ranges = {
            y_column2_name: Range1d(
                start=np.nanmin(v) - abs(np.nanmin(v))*0.05,
                end=np.nanmax(v) + abs(np.nanmax(v))*0.05,
            ),
        }
        fig_list[i].varea(x=t, y1=0, y2=v, fill_color='gray', legend_label='现货-期货', y_range_name='y2')

        c = 0
        for k in range(len(datas[i][0])):
            left_list.append(get_period_data(datas[i][0][k][0], datas[i][0][k][1], start_time, end_time, remove_nan=True))
            fig_list[i].line(left_list[k][0], left_list[k][1], line_width=2, line_color=many_colors[c], legend_label=datas[i][0][k][2])
            c = c + 1
        fig_list[i].y_range = Range1d(np.nanmin(left_list[0][1]) - abs(np.nanmin(left_list[0][1]))*0.05, np.nanmax(left_list[1][1]) + abs(np.nanmax(left_list[1][1]))*0.05)

        fig_list[i].add_layout(LinearAxis(y_range_name="y2"), 'right')
        fig_list[i].xaxis[0].ticker.desired_num_ticks = 20
        fig_list[i].legend.click_policy="hide"
        fig_list[i].legend.location='top_left'

    show(column(fig_list))


def plot_cf_spot_price():
    path = os.path.join(spot_dir, 'CF_spot'+'.csv')
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    d1 = np.array(df['CCIndex3128'], dtype=float)
    d2 = np.array(df['FCIndexM 1%关税'], dtype=float)
    d3 = np.array(df['FCIndexM 滑准税'], dtype=float)
    diff = d2 - d1

    path = os.path.join(future_price_dir, 'czce', 'CF'+'.csv')
    df = pd.read_csv(path, header=[0,1])
    fut_t = pd.DatetimeIndex(pd.to_datetime(df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    index_close = np.array(df['index']['close'], dtype=float)

    datas = [
             [[[t,d1,'CCIndex3128','color=orange'],
               [t,d2,'FCIndexM 1%关税','color=red'],
               [t,d3,'FCIndexM 滑准税','color=blue'],
               [fut_t,index_close,'CF指数','color=black'],
              ],
              [[t,diff,'FCIndexM 滑准税 - CCIndex3128','style=vbar']],''],
    ]
    plot_many_figure(datas)


    ### 棉花加工品价格
    path = os.path.join(spot_dir, '棉花加工品价格'+'.csv')
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    d1 = np.array(df['棉纱'], dtype=float)
    d2 = np.array(df['棉油'], dtype=float)
    d3 = np.array(df['棉短绒'], dtype=float)
    d4 = np.array(df['棉粕'], dtype=float)
    d5 = np.array(df['棉壳'], dtype=float)

    datas = [
             [[[t,d1,'棉纱','color=orange'],
              ],
              [[fut_t,index_close,'CF指数','color=black'],],''],

             [[
               [t,d2,'棉油','color=red'],
              ],
              [[fut_t,index_close,'CF指数','color=black'],],''],

             [[
               [t,d3,'棉短绒','color=blue'],
              ],
              [[fut_t,index_close,'CF指数','color=black'],],''],

             [[
               [t,d4,'棉粕','color=darkgreen'],
              ],
              [[fut_t,index_close,'CF指数','color=black'],],''],

             [[
               [t,d5,'棉壳','color=purple'],
              ],
              [[fut_t,index_close,'CF指数','color=black'],],''],
    ]
    plot_many_figure(datas, max_height=1000)


# 开工率 毛利润
def plot_pk_production_profit():
    path1 = os.path.join(future_price_dir, 'czce', 'PK'+'.csv')
    df1 = pd.read_csv(path1, header=[0,1])
    t0 = pd.DatetimeIndex(pd.to_datetime(df1['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    index_close = np.array(df1.loc[:, pd.IndexSlice['index', 'close']], dtype=float)
    c1_close = np.array(df1.loc[:, pd.IndexSlice['c1', 'close']], dtype=float)
    c3_close = np.array(df1.loc[:, pd.IndexSlice['c3', 'close']], dtype=float)

    path = os.path.join(spot_dir, '花生压榨开工率'+'.csv')
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    rate = np.array(df['压榨开工率'])

    path = os.path.join(spot_dir, '花生压榨毛利润'+'.csv')
    df = pd.read_csv(path)
    t2 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    profit = np.array(df['压榨毛利润'])

    datas = [
             [[[t0,index_close,'PK指数','color=black'],
              ],
              [[t1,rate,'花生压榨开工率','']],''],

             [[[t0,index_close,'PK指数','color=black'],
              ],
              [[t2,profit,'花生压榨毛利润','']],''],
    ]
    plot_many_figure(datas, start_time='2020-01-01')


def plot_soybean_production_profit():
    path1 = os.path.join(future_price_dir, 'dce', 'a'+'.csv')
    df1 = pd.read_csv(path1, header=[0,1])
    t0 = pd.DatetimeIndex(pd.to_datetime(df1['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    a_index_close = np.array(df1.loc[:, pd.IndexSlice['index', 'close']], dtype=float)

    path1 = os.path.join(future_price_dir, 'dce', 'b'+'.csv')
    df1 = pd.read_csv(path1, header=[0,1])
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    b_index_close = np.array(df1.loc[:, pd.IndexSlice['index', 'close']], dtype=float)

    path = os.path.join(spot_dir, '大豆压榨毛利润'+'.csv')
    df = pd.read_csv(path)
    t2 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    profit = np.array(df['压榨毛利润'])

    datas = [
             [[[t0,a_index_close,'a指数','color=black'],
              ],
              [[t2,profit,'大豆压榨毛利润','']],''],

             [[[t1,b_index_close,'b指数','color=black'],
              ],
              [[t2,profit,'大豆压榨毛利润','']],''],
    ]
    plot_many_figure(datas, start_time='2018-01-01')





if __name__=="__main__":
    # update_pk_spot_price()
    # update_all_spot_price()

    # plot_pk_basis()
    # update_other_spot_price('PK')

    # update_cotton_spot_price()

    plot_cf_spot_price()

    # plot_pk_production_profit()
    # plot_soybean_production_profit()

    pass

