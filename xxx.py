import os
import requests
import pandas as pd
import datetime
import numpy as np
import sys
from utils import *
from bokeh.io import output_file, show
from bokeh.layouts import column, row, gridplot, layout
from bokeh.plotting import figure
from bokeh.models import LinearAxis, Range1d, VBar, DatetimeTickFormatter
from scipy.stats import linregress

# [inst_id, auction]
inst_ids_info = [['sc2402', 1], ['sc2403', 1],
                 ['au2402', 1], ['au2404', 1],
                 ['ag2402', 1], ['ag2404', 1], 
                 ['zn2402', 1], ['zn2403', 1],
                 ['al2402', 1], ['al2403', 1], 
                 ['cu2402', 1], ['cu2403', 1],
                 ['CF405', 0],
                ]

sina_usd_symbol_dict = {
    'sc': 'OIL',
    'au': 'GC',
    'ag': 'SI',
    'cu': 'CAD',
    'zn': 'ZSD',
    'al': 'AHD',
}

headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Host": "gu.sina.cn",
    "Proxy-Connection": "keep-alive",
    'Sec-Fetch-Site': 'same-site',
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
}

def get_usd_price():
    se = requests.session()
    URL = 'https://gu.sina.cn/ft/api/jsonp.php/var_DATA=/GlobalService.getMink?symbol={}&type=5'

    for variety in sina_usd_symbol_dict:
        print(variety)

        usd_symbol = sina_usd_symbol_dict[variety]
        path = os.path.join(data_dir, usd_symbol+'_5min'+'.csv')
        url = URL.format(usd_symbol)
        r = se.get(url, verify=False, headers=headers)
        s = r.text
        s=s.replace('},', '')
        s=s.replace('"', '')
        s=s.replace('}]);', '')
        z = s.split('{')[1:]
        datas = []
        for i in range(len(z)):
            data = z[i].split(',')[:7]
            data[0] = data[0][2:]
            for k in range(1, len(data)):
                data[k] = data[k].split(':')[1]

            datas.append(data)

        df = pd.DataFrame(columns=['time','open','high','low','close','v','p'], data=datas)
        df.to_csv(path, encoding='utf-8', index=False)

def get_usd_cny():
    se = requests.session()
    url = 'https://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/DATA=/NewForexService.getMinKline?symbol=fx_susdcny&scale=5&datalen=1440'
    print('USDCNY')

    path = os.path.join(data_dir, 'USDCNY_5min'+'.csv')
    r = se.get(url, verify=False, headers=headers)
    s = r.text
    s=s.replace('},', '')
    s=s.replace('"', '')
    s=s.replace('}]);', '')
    z = s.split('{')[1:]
    datas = []
    for i in range(len(z)):
        data = z[i].split(',')[:7]
        data[0] = data[0][2:]
        for k in range(1, len(data)):
            data[k] = data[k].split(':')[1]

        datas.append(data)

    df = pd.DataFrame(columns=['time','open','high','low','close'], data=datas)
    df.to_csv(path, encoding='utf-8', index=False)


def update_au_ag_td_intraday_data():
    se = requests.session()
    url = 'https://api-ddc-wscn.awtmt.com/market/kline?prod_code=AUTD.SGE&tick_count=360&period_type=60&adjust_price_type=forward&fields=tick_at,open_px,close_px,high_px,low_px,turnover_volume'

    r = se.get(url)
    data_json = r.json()
    df = pd.DataFrame(data_json['data']['candle']['AUTD.SGE']['lines'])
    df.columns = ['open', 'close', 'high', 'low', 'volume', 'time']
    df = df[['time', 'open', 'high', 'low', 'close', 'volume']]

    df['time'] = df['time'].apply(lambda x:datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
    print(df)



def xxx_price(display):
    now = datetime.datetime.now()

    df = pd.DataFrame()
    for inst_id_info in inst_ids_info:
        time.sleep(0.25)
        inst_id = inst_id_info[0]
        auction = inst_id_info[1]
        if inst_id[1].isdigit():
            variety = inst_id[0]
        else:
            variety = inst_id[0:2]
        for exchange in exchange_dict:
            if variety in exchange_dict[exchange]:
                break

        path = os.path.join(future_price_dir, exchange, variety+'.csv')
        if not os.path.exists(path):
            print("error", path)
            exit()
        fut_df = pd.read_csv(path, header=[0,1])
        fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))

        path = os.path.join(option_price_dir, exchange, inst_id+'.csv')
        if not os.path.exists(path):
            print("error", path)
            exit()
        opt_df = pd.read_csv(path, header=[0,1,2])
        opt_t = pd.DatetimeIndex(pd.to_datetime(opt_df['time']['time']['time'], format='%Y-%m-%d'))

        # usd price
        if (auction == 1):
            path = os.path.join(data_dir, sina_usd_symbol_dict[variety]+'_5min'+'.csv')
            usd_df = pd.read_csv(path)
            usd_t = pd.DatetimeIndex(pd.DatetimeIndex(pd.to_datetime(usd_df['time'], format='%Y-%m-%d %H:%M:%S')))

            # usdcny
            path = os.path.join(data_dir, 'USDCNY_5min'+'.csv')
            usdcny_df = pd.read_csv(path)
            usdcny_t = pd.DatetimeIndex(pd.to_datetime(usdcny_df['time'], format='%Y-%m-%d %H:%M:%S'))

        if (now.strftime('%Y-%m-%d') != opt_t[-1].strftime('%Y-%m-%d')):
            print('wrong time 2,', variety)
            exit()
        if (now.strftime('%Y-%m-%d') != fut_t[-1].strftime('%Y-%m-%d')):
            print('wrong time 3,', variety)
            exit()

        price_chg = 0
        if (auction == 1):
            if (now.strftime('%Y-%m-%d') != usd_t[-1].strftime('%Y-%m-%d')):
                print('wrong time 1,', variety)
                exit()
            if (now.strftime('%Y-%m-%d') != usdcny_t[-1].strftime('%Y-%m-%d')):
                print('wrong time 4,', variety)
                exit()

            if now.hour >= 15:
                usd_start_time = usd_t[-1].strftime('%Y-%m-%d') + ' 15:00:00'
            else:
                print('wrong time 5,', variety)
                exit()

            usd_start_time_dt = pd.to_datetime(usd_start_time, format='%Y-%m-%d %H:%M:%S')
            w = np.where(usd_t == usd_start_time_dt)[0]
            if (len(w) == 0):
                usd_start_time = usd_t[-1].strftime('%Y-%m-%d') + ' 14:55:00'
                usd_start_time_dt = pd.to_datetime(usd_start_time, format='%Y-%m-%d %H:%M:%S')
                w = np.where(usd_t == usd_start_time_dt)[0]
                if (len(w) == 0):
                    print('wrong time 6,', variety)
                    exit()

            w = w[0]
            start_price = (usd_df.loc[w, 'open'] + usd_df.loc[w, 'high'] + \
                        usd_df.loc[w, 'low'] + usd_df.loc[w, 'close']) / 4
            end_price = (usd_df.loc[len(usd_df)-1, 'open'] + usd_df.loc[len(usd_df)-1, 'high'] + \
                        usd_df.loc[len(usd_df)-1, 'low'] + usd_df.loc[len(usd_df)-1, 'close']) / 4

            w = np.where(usdcny_t == usd_start_time_dt)[0]
            if (len(w) == 0):
                print('wrong time 7,', variety)
                exit()

            w = w[0]
            start_fx = (usdcny_df.loc[w, 'open'] + usdcny_df.loc[w, 'high'] + \
                        usdcny_df.loc[w, 'low'] + usdcny_df.loc[w, 'close']) / 4
            end_fx = (usdcny_df.loc[len(usdcny_df)-1, 'open'] + usdcny_df.loc[len(usdcny_df)-1, 'high'] + \
                        usdcny_df.loc[len(usdcny_df)-1, 'low'] + 3*usdcny_df.loc[len(usdcny_df)-1, 'close']) / 6

            price_chg = (end_price*end_fx - start_price*start_fx) / (start_price*start_fx)

        cny_price_start = None
        temp_fut_df = fut_df.loc[len(fut_df)-1, :]
        cs = ['c1','c2','c3','c4','c5','c6','c7','c8','c9']
        for c in cs:
            if (temp_fut_df[c]['inst_id'] == inst_id):
                cny_price_start = temp_fut_df[c]['close']
                break
        if cny_price_start is None:
            print('cny_price_start is None')
            exit()

        if (auction == 1):
            cny_price_end = cny_price_start * (1 + price_chg)
        else:
            cny_price_end = cny_price_start
        print(inst_id, cny_price_end, str(round(price_chg*100,2))+'%')

        col = opt_df.columns.tolist()
        res = [(col[i][1]) for i in range(len(col)) if col[i][0] == 'P']
        strikes_str = []
        for i in res:
            if i not in strikes_str:
                strikes_str.append(i)

        strike = []
        call_price = []
        call_delta = []
        put_price = []
        put_delta = []

        for strike_str in strikes_str:
            strike.append(float(strike_str))
            call_price.append(opt_df.loc[len(opt_df)-1, pd.IndexSlice['C', strike_str, 'close']])
            call_delta.append(opt_df.loc[len(opt_df)-1, pd.IndexSlice['C', strike_str, 'delta_c']])
            put_price.append(opt_df.loc[len(opt_df)-1, pd.IndexSlice['P', strike_str, 'close']])
            put_delta.append(opt_df.loc[len(opt_df)-1, pd.IndexSlice['P', strike_str, 'delta_c']])

        strike = np.array(strike, dtype=float)
        call_price = np.array(call_price, dtype=float)
        call_delta = np.array(call_delta, dtype=float)
        put_price = np.array(put_price, dtype=float)
        put_delta = np.array(put_delta, dtype=float)


        col = ['inst_id', 'price', 'delta']
        data = [[inst_id, cny_price_end, 0]]
        for i in range(len(strike)):
            if (strike[i] <= cny_price_end*1.15) and (strike[i] >= cny_price_end*0.85):
                # call
                if exchange == 'shfe' or exchange == 'czce':
                    opt_inst_id = inst_id + 'C' + strikes_str[i]
                else:
                    opt_inst_id = inst_id + '-C-' + strikes_str[i]
                price = call_price[i] + call_delta[i]*(cny_price_end - cny_price_start)
                data.append([opt_inst_id, price, call_delta[i]])
  
                # put
                if exchange == 'shfe' or exchange == 'czce':
                    opt_inst_id = inst_id + 'P' + strikes_str[i]
                else:
                    opt_inst_id = inst_id + '-P-' + strikes_str[i]
                price = put_price[i] + put_delta[i]*(cny_price_end - cny_price_start)
                data.append([opt_inst_id, price, put_delta[i]])

        temp_df = pd.DataFrame(columns=col, data=data)
        df = pd.concat([df, temp_df], axis=0)

        if (display):
            # plot
            opt_inst_id = np.array(temp_df.loc[1:, 'inst_id'], dtype=str)
            price = np.array(temp_df.loc[1:, 'price'], dtype=float)
            delta = np.array(temp_df.loc[1:, 'delta'], dtype=float)
            w = np.where(np.logical_not(np.isnan(price)))[0]
            opt_inst_id = opt_inst_id[w]
            price = price[w]
            delta = delta[w]

            L = len(temp_df.loc[0, 'inst_id'])
            c_strike = []
            c_price = []
            c_delta = []

            p_strike = []
            p_price = []
            p_delta = []
            for i in range(len(opt_inst_id)):
                if 'C' in opt_inst_id[i][L:]:
                    if ('-' in opt_inst_id[i][L:]):
                        c_strike.append(float(opt_inst_id[i][L+3:]))
                    else:
                        c_strike.append(float(opt_inst_id[i][L+1:]))
                    c_price.append(price[i])
                    c_delta.append(delta[i])

                if 'P' in opt_inst_id[i][L:]:
                    if ('-' in opt_inst_id[i][L:]):
                        p_strike.append(float(opt_inst_id[i][L+3:]))
                    else:
                        p_strike.append(float(opt_inst_id[i][L+1:]))
                    p_price.append(price[i])
                    p_delta.append(delta[i])

            c_strike = np.array(c_strike, dtype=float)
            c_price = np.array(c_price, dtype=float)
            c_delta = np.array(c_delta, dtype=float)

            sort = np.argsort(c_strike)
            c_strike = c_strike[sort]
            c_price = c_price[sort]
            c_delta = c_delta[sort]

            p_strike = np.array(p_strike, dtype=float)
            p_price = np.array(p_price, dtype=float)
            p_delta = np.array(p_delta, dtype=float)

            sort = np.argsort(p_strike)
            p_strike = p_strike[sort]
            p_price = p_price[sort]
            p_delta = p_delta[sort]

            fig1 = figure(frame_width=1400, frame_height=300, tools=TOOLS, title=temp_df.loc[0, 'inst_id'])
            fig1.line(c_strike, c_price, line_width=2, line_color='red', legend_label='CALL PRICE')
            fig1.circle(x=c_strike, y=c_price, color='red', legend_label='CALL PRICE')
            fig1.line(p_strike, p_price, line_width=2, line_color='darkgreen', legend_label='PUT PRICE')
            fig1.circle(x=p_strike, y=p_price, color='darkgreen', legend_label='PUT PRICE')

            fig2 = figure(frame_width=1400, frame_height=300, tools=TOOLS, x_range=fig1.x_range)
            fig2.line(c_strike, c_delta, line_width=2, line_color='red', legend_label='CALL DELTA')
            fig2.circle(x=c_strike, y=c_delta, color='red', legend_label='CALL DELTA')
            fig2.line(p_strike, p_delta, line_width=2, line_color='darkgreen', legend_label='PUT DELTA')
            fig2.circle(x=p_strike, y=p_delta, color='darkgreen', legend_label='PUT DELTA')

            show(column(fig1, fig2))


    path = os.path.join('D:\CTP', 'opt_price'+'.csv')
    df.to_csv(path, encoding='utf-8', index=False)


if __name__=="__main__":
    get_usd_price()
    get_usd_cny()
    # TODO: 早盘盘前竞价
    xxx_price(len(sys.argv) == 1)


    # update_au_ag_td_intraday_data()

    pass
