import os
import requests
import pandas as pd
import datetime
import numpy as np
from utils import *
import json
import math as m
from position import *
# from cn_fut_opt import *
import calendar

# 分钟
period = 60

def get_deribit_file_name(ss):
    if 'PERPETUAL' in ss:
        return ss

    month_dict = {'JAN':'01','FEB':'02','MAR':'03','APR':'04','MAY':'05','JUN':'06',
                  'JUL':'07','AUG':'08','SEP':'09','OCT':'10','NOV':'11','DEC':'12'}
    s = ss.split('-')

    if len(s[1]) == 6:
        # 4AUG23
        y = s[1][4:]
        m = month_dict[s[1][1:4]]
        d = '0' + s[1][0]
    else:
        # 25AUG23
        y = s[1][5:]
        m = month_dict[s[1][2:5]]
        d = s[1][0:2]     

    name = s[0] + y + m + d
    # name example: BTC230825
    return name   

def crypto_implied_volatility(S0, K, T, r, price, Otype):
    e = 1e-3
    x0 = 3 # 300%
    sqrt_T = m.sqrt(T)

    def newtons_method(S0, K, T, sqrt_T, r, price, Otype, x0, e):
        k=0
        delta = call_bsm(S0, K, r, T, sqrt_T, Otype, x0) - price
        while delta > e:
            k=k+1
            if (k > 40):
                return np.nan
            _vega = vega(S0, K, r, T, sqrt_T, x0)
            if (_vega == 0.0):
                return np.nan
            x0 = (x0 - (call_bsm(S0, K, r, T, sqrt_T, Otype, x0) - price)/_vega)
            delta = abs(call_bsm(S0, K, r, T, sqrt_T, Otype, x0) - price)
        return x0
    iv = newtons_method(S0, K, T, sqrt_T, r, price, Otype, x0, e)   
    return iv


def calculate_crypto_greeks(S0, K, T, r, price, Otype):
    if (np.isnan(S0) or np.isnan(K) or np.isnan(price) or S0==0.0):
        return [0, 0]
    
    if (Otype == 'C' and S0/K > 3):
        return [0, 0]

    if (Otype == 'P' and K/S0 > 3):
        return [0, 0]

    sqrt_T = m.sqrt(T)

    # print(S0, K, T, r, price, Otype)

    # imp_vol
    iv = crypto_implied_volatility(S0, K, T, r, price, Otype)
    d1 = ((m.log(S0/K)) + (r + (iv*iv)/2)*T)/(iv*sqrt_T)

    # delta
    if Otype == 'C':
        delta = ss.norm.cdf(d1)
    else:
        delta = ss.norm.cdf(d1) - 1

    return [round(iv,5), round(delta,5)]


def get_deribit_future_inst_id_data(fut_df, inst_id):
    t1 = np.array(fut_df['time']['time'])

    cs = ['c7','c6','c5','c4','c3','c2','c1']
    k = 0
    start = 0
    end = 0
    for i in range(len(cs)):
        c = cs[i]
        tmp = (fut_df[c]['inst_id'] == inst_id)
        ii = tmp[tmp == True].index
        if (len(ii) > 0):
            if (k == 0):
                start = ii[0]
                end = ii[len(ii)-1]
                close = np.array(fut_df.loc[ii, pd.IndexSlice[c, 'close']], dtype=float)
            else:
                close = np.concatenate((close, np.array(fut_df.loc[ii, pd.IndexSlice[c, 'close']], dtype=float)), axis=0)
                end = ii[len(ii)-1]

            k = k + 1

    if (start == 0 and end == 0):
        return None, None

    t = t1[start:end+1]
    # print(t)
    # print(len(t), len(open))
    return t, close


def update_deribit_option_data():
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "www.deribit.com",
        "Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    }

    SUMMARY_URL = 'https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency={}&kind=option'
    INST_URL = 'https://www.deribit.com/api/v2/public/get_instruments?currency={}&kind=option'
    DATA_URL = 'https://www.deribit.com/api/v2/public/get_tradingview_chart_data?instrument_name={}&start_timestamp={}&end_timestamp={}&resolution={}'
    # session = requests.session()
    proxy = {'https':'127.0.0.1:8889'}

    t = datetime.datetime.now()
    if t.hour >= 16:
        earlist_time_dt = datetime.datetime(year=t.year, month=t.month, day=t.day, hour=16, minute=0, second=0)
    else:
        earlist_time_dt = datetime.datetime(year=t.year, month=t.month, day=t.day, hour=0, minute=0, second=0)
    earlist_time = earlist_time_dt.strftime('%Y-%m-%d %H:%M:%S')

    for variety in ['BTC', 'ETH']:
    # for variety in ['ETH']:
        inst_url = INST_URL.format(variety)
        se = requests.session()
        while (1):
            try:
                r = se.get(inst_url, verify=False, headers=headers, proxies=proxy)
                break
            except Exception as e:
                print(e)
                time.sleep(5)
        cookies = r.cookies
        data_json = json.loads(r.content)
        # print(data_json)
        inst_df = pd.DataFrame(data_json["result"])
        inst_df = inst_df[['instrument_name', 'expiration_timestamp']]
        inst_dict = {}
        for i in range(len(inst_df)):
            s = inst_df.loc[i, 'instrument_name'].split("-")
            ss = s[0] + '-' + s[1]
            if not(ss in inst_dict):
                inst_dict[ss] = [inst_df.loc[i, 'expiration_timestamp'], inst_df.loc[i, 'instrument_name']]
            else:
                inst_dict[ss].append(inst_df.loc[i, 'instrument_name'])

        # BOOK SUMMARY
        url = SUMMARY_URL.format(variety)
        while(1):
            try:
                r = se.get(url, verify=False, headers=headers, cookies=cookies, proxies=proxy)
                data_json = json.loads(r.content)
                break
            except:
                print(variety, 'BOOK SUMMARY GET ERROR')
                time.sleep(5)
        # print(data_json)
        summary_df = pd.DataFrame(data_json["result"])
        summary_dict = {}
        for i in range(len(summary_df)):
            summary_dict[summary_df.loc[i, 'instrument_name']] = [summary_df.loc[i, 'volume'], summary_df.loc[i, 'open_interest']]

        # BTC FUTURE PRICE
        path = os.path.join(future_price_dir, 'deribit', variety+'.csv')
        fut_df = pd.read_csv(path, header=[0,1])

        info_df = pd.DataFrame()
        for ss in inst_dict:
            df = pd.DataFrame()
            name = get_deribit_file_name(ss)
            opt_path = os.path.join(option_price_dir, 'deribit', name+'.csv')
            last_line_time = get_last_line_time(opt_path, name, '2023-07-22 00:00:00', 19, '%Y-%m-%d %H:%M:%S')
            if last_line_time == '2023-07-22 00:00:00':
                last_line_time = earlist_time
            tmp = time.strptime(last_line_time, '%Y-%m-%d %H:%M:%S')
            last_line_time_ts = int(time.mktime(tmp))*1000
            tmp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            tmp = time.strptime(tmp, '%Y-%m-%d %H:%M:%S')
            current_time_ts = int(time.mktime(tmp))*1000

            inst_ids = inst_dict[ss]
            expiration_timestamp = inst_ids[0]
            inst_ids.pop(0)

            fut_t, fut_price = get_deribit_future_inst_id_data(fut_df, name)
            if fut_t is None:
                continue
            
            for inst_id in inst_ids:
                print(inst_id)
                start_time_ts = last_line_time_ts + 100 # +0.1s
                temp_df = pd.DataFrame()
                while ((start_time_ts+1000*60*period) < current_time_ts):
                    if (current_time_ts - start_time_ts) > (1000*60*60*24*20): # 20d
                        end_time_ts = start_time_ts + 1000*60*60*24*20
                    else:
                        end_time_ts = current_time_ts
                    url = DATA_URL.format(inst_id, str(start_time_ts), str(end_time_ts), str(period))
                    while (1):
                        try:
                            r = se.get(url, verify=False, headers=headers, cookies=cookies, proxies=proxy)
                            break
                        except:
                            print('DERIBIT OPTION GET ERROR')
                            time.sleep(10)

                    year_milliseconds = float(365*24*60*60*1000)
                    data_json = json.loads(r.content)
                    temp_df1 = pd.DataFrame(data_json["result"])
                    temp_df1['time'] = temp_df1['ticks'].apply(lambda x:time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(x)/1000)))
                    temp_df1['oi'] = np.nan

                    # inst_id example : BTC-01AUG23-30000-C
                    K = float((inst_id.split('-'))[2])
                    strike = str(int(K))
                    opt_type = inst_id[-1]
                    rate = 0
                    for i in range(len(temp_df1)):
                        data_t = temp_df1.loc[i, 'time']
                        w = np.where(fut_t == data_t)[0]
                        if len(w) > 0:
                            S0 = fut_price[w[0]]
                            T = (float(expiration_timestamp) - float(temp_df1.loc[i, 'ticks'])) / year_milliseconds
                            # print(S0, K, T, r, temp_df1.loc[i, 'close'])
                            imp_vol, delta = calculate_crypto_greeks(S0, K, T, rate, temp_df1.loc[i, 'close']*S0, opt_type)
                        else:
                            imp_vol = np.nan
                            delta = np.nan

                        temp_df1.loc[i, 'imp_vol'] = imp_vol
                        temp_df1.loc[i, 'delta'] = delta
                    
                    col1 = ['time', opt_type, opt_type, opt_type, opt_type, opt_type]
                    col2 = ['time', strike, strike, strike, strike, strike]
                    col3 = ['time', 'close', 'volume', 'oi', 'imp_vol', 'delta']
                    if (len(temp_df1) > 0):
                        temp_df1 = temp_df1[['time', 'close', 'volume', 'oi', 'imp_vol', 'delta']]
                        temp_df1.columns = [col1, col2, col3]

                        if temp_df.empty:
                            temp_df = temp_df1.copy()
                        else:
                            temp_df = pd.concat([temp_df, temp_df1], axis=0)
                            temp_df.drop_duplicates(subset=[('time','time','time')], keep='last', inplace=True)

                    start_time_ts = end_time_ts

                if len(temp_df) > 0:
                    # 最后一行数据是截至到当前的数据, 会被下一次拿的数据替换掉, 所以把 oi 放在倒数第二个数据里
                    if (len(temp_df) > 1):
                        temp_df.loc[len(temp_df)-1-1, pd.IndexSlice[opt_type, strike, 'oi']] = summary_dict[inst_id][1]
                    else:
                        temp_df.loc[len(temp_df)-1, pd.IndexSlice[opt_type, strike, 'oi']] = summary_dict[inst_id][1]

                    if (df.empty):
                        df = temp_df.copy()
                    else:
                        df = pd.merge(df, temp_df, on=[('time','time','time')], how='outer')

            if len(df) > 0:
                temp_info_df = pd.DataFrame()
                temp_info_df['time'] = df['time']['time']['time']
                temp_info_df[name] = name
                if info_df.empty:
                    info_df = temp_info_df.copy()
                else:
                    info_df = pd.merge(info_df, temp_info_df, on='time', how='outer')

                if not(os.path.exists(opt_path)):
                    df.to_csv(opt_path, encoding='utf-8', index=False)
                else:
                    old_df = pd.read_csv(opt_path, header=[0,1,2])
                    old_df = pd.concat([old_df, df], axis=0)
                    old_df.drop_duplicates(subset=[('time','time','time')], keep='last', inplace=True) # last
                    old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
                    old_df.sort_values(by = ('time','time','time'), inplace=True)
                    old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
                    old_df.to_csv(opt_path, encoding='utf-8', index=False)

        if len(info_df) > 0:
            print(info_df)
            cols = info_df.columns.tolist()
            cols.remove('time')
            for i in range(len(info_df)):
                s = ''
                for col in cols:
                    if type(info_df.loc[i, col]) == str:
                        s += info_df.loc[i, col]
                        s += ','
                info_df.loc[i, 'inst_ids'] = s

            info_df = info_df[['time', 'inst_ids']]
            path = os.path.join(option_price_dir, 'deribit', variety+'_info'+'.csv')
            if not(os.path.exists(path)):
                info_df.to_csv(path, encoding='utf-8', index=False)
            else:
                old_info_df = pd.read_csv(path)
                old_info_df = pd.concat([old_info_df, info_df], axis=0)
                old_info_df.drop_duplicates(subset='time', keep='last', inplace=True)
                old_info_df['time'] = old_info_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
                old_info_df.sort_values(by = 'time', inplace=True)
                old_info_df['time'] = old_info_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
                old_info_df.to_csv(path, encoding='utf-8', index=False)   


def update_deribit_future_data():
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "www.deribit.com",
        "Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    }

    SUMMARY_URL = 'https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency={}&kind=future'
    INST_URL = 'https://www.deribit.com/api/v2/public/get_instruments?currency={}&kind=future'
    DATA_URL = 'https://www.deribit.com/api/v2/public/get_tradingview_chart_data?instrument_name={}&start_timestamp={}&end_timestamp={}&resolution={}'
    # session = requests.session()
    proxy = {'https':'127.0.0.1:8889'}
    se = requests.session()

    for variety in ['BTC', 'ETH']:
        inst_url = INST_URL.format(variety)
        while (1):
            try:
                r = se.get(inst_url, verify=False, headers=headers, proxies=proxy)
                break
            except Exception as e:
                print(e)
                time.sleep(5)

        cookies = r.cookies
        data_json = json.loads(r.content)
        # print(data_json)
        inst_df = pd.DataFrame(data_json["result"])
        inst_df = inst_df[['instrument_name']]
        print(inst_df)

        # BOOK SUMMARY
        url = SUMMARY_URL.format(variety)
        r = se.get(url, verify=False, headers=headers, cookies=cookies, proxies=proxy)
        data_json = json.loads(r.content)
        # print(data_json)
        summary_df = pd.DataFrame(data_json["result"])
        summary_dict = {}
        for i in range(len(summary_df)):
            summary_dict[summary_df.loc[i, 'instrument_name']] = summary_df.loc[i, 'open_interest']

        fut_path = os.path.join(future_price_dir, 'deribit', variety+'.csv')
        last_line_time = get_last_line_time(fut_path, variety+' future', '2023-07-22 00:00:00', 19, '%Y-%m-%d %H:%M:%S')
        tmp = time.strptime(last_line_time, '%Y-%m-%d %H:%M:%S')
        last_line_time_ts = int(time.mktime(tmp))*1000

        tmp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        tmp = time.strptime(tmp, '%Y-%m-%d %H:%M:%S')
        current_time_ts = int(time.mktime(tmp))*1000
        
        df = pd.DataFrame()
        n = 1
        for i in range(len(inst_df)):
            inst_id = inst_df.loc[i, 'instrument_name']
            name = get_deribit_file_name(inst_id)

            if not('PERPETUAL' in name):
                cn = 'c' + str(n)
                n = n + 1
            else:
                cn = 'PERPETUAL'

            print(cn)

            start_time_ts = last_line_time_ts + 100 # +0.1s
            temp_df = pd.DataFrame()
            while ((start_time_ts+1000*60*period) < current_time_ts):
                if (current_time_ts - start_time_ts) > (1000*60*60*24*20): # 20d
                    end_time_ts = start_time_ts + 1000*60*60*24*20
                else:
                    end_time_ts = current_time_ts
                url = DATA_URL.format(inst_id, str(start_time_ts), str(end_time_ts), str(period))
                try:
                    r = se.get(url, verify=False, headers=headers, cookies=cookies, proxies=proxy)
                except:
                    print('GET ERROR')
                    time.sleep(5)

                data_json = json.loads(r.content)
                # print(data_json)
                temp_df1 = pd.DataFrame(data_json["result"])

                temp_df1['time'] = temp_df1['ticks'].apply(lambda x:time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(x)/1000)))
                temp_df1['inst_id'] = name
                temp_df1['oi'] = np.nan
                temp_df1 = temp_df1[['time', 'inst_id', 'close', 'volume', 'oi']]
                col1 = ['time', cn, cn, cn, cn]
                col2 = ['time', 'inst_id', 'close', 'volume', 'oi']
                temp_df1.columns = [col1, col2]
                if temp_df.empty:
                    temp_df = temp_df1.copy()
                else:
                    temp_df = pd.concat([temp_df, temp_df1], axis=0)
                    temp_df.drop_duplicates(subset=[('time','time')], keep='last', inplace=True)
                
                start_time_ts = end_time_ts

            if (temp_df.empty):
                return
            
            if (len(temp_df) > 1):
                temp_df.loc[len(temp_df)-1-1, pd.IndexSlice[cn,'oi']] = summary_dict[inst_id]
            elif (len(temp_df) == 1):
                temp_df.loc[len(temp_df)-1, pd.IndexSlice[cn,'oi']] = summary_dict[inst_id]

            if (df.empty):
                df = temp_df.copy()
            else:
                df = pd.merge(df, temp_df, on=[('time','time')], how='outer')

            # print(df)

        if not(os.path.exists(fut_path)):
            df.to_csv(fut_path, encoding='utf-8', index=False)
        else:
            old_df = pd.read_csv(fut_path, header=[0,1])
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=[('time','time')], keep='last', inplace=True) # last
            old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
            old_df.sort_values(by = ('time','time'), inplace=True)
            old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
            old_df.to_csv(fut_path, encoding='utf-8', index=False)


def get_last_friday_expiry_time(year, month):
    last_day = calendar.monthrange(year, month)[-1]
    dt = datetime.datetime(year=year, month=month, day=last_day)
    while (1):
        weekday = dt.weekday()
        if weekday == 4: # friday
            break
        dt = dt - pd.Timedelta(days=1)
    return datetime.datetime(year=dt.year, month=dt.month, day=dt.day, hour=16)


def get_crypto_full_strike_price(df):
    col = df.columns.tolist()

    put_strike = [(col[i][1]) for i in range(len(col)) if col[i][0] == 'P']
    call_strike = [(col[i][1]) for i in range(len(col)) if col[i][0] == 'C']

    res = []
    for i in put_strike:
        if i not in res:
            res.append(i)
    put_strike = np.array(res, dtype=float)

    res = []
    for i in call_strike:
        if i not in res:
            res.append(i)
    call_strike = np.array(res, dtype=float)

    return put_strike, call_strike


def crypto_put_call_delta_volatility(df, delta, price, put_strike, call_strike):
    # CALL CLOSE
    tmp = df.loc[pd.IndexSlice['C', :, 'delta']]
    idx1, idx2, delta1, delta2 = column_index_delta(tmp, delta)
    o_price = df.loc[pd.IndexSlice['C', :, 'close']]
    iv = df.loc[pd.IndexSlice['C', :, 'imp_vol']]

    if delta1 == delta:
        _25d_call_price = o_price[idx1]
        _25d_call_iv = iv[idx1]
    else:
        w1 = (delta2 - delta)/(delta2 - delta1)
        w2 = (delta - delta1)/(delta2 - delta1)
        _25d_call_price = w1*o_price[idx1] + w2*o_price[idx2]
        _25d_call_iv = w1*iv[idx1] + w2*iv[idx2]


    if (price > 0.25):
        # print(df)
        # print(strike)
        # print(price)
        idx1, idx2, price1, price2 = column_index_price(call_strike, price)
        if price1 == price:
            _atm_call_iv = iv[idx1]
        else:
            w1 = (price2 - price)/(price2 - price1)
            w2 = (price - price1)/(price2 - price1)
            _atm_call_iv = w1*iv[idx1] + w2*iv[idx2]

    else:
        _atm_call_iv = np.nan


    ############################################################################
    delta = -delta

    # PUT CLOSE
    tmp = df.loc[pd.IndexSlice['P', :, 'delta']]
    idx1, idx2, delta1, delta2 = column_index_delta(tmp, delta)
    o_price = df.loc[pd.IndexSlice['P', :, 'close']]
    iv = df.loc[pd.IndexSlice['P', :, 'imp_vol']]

    if delta1 == delta:
        _25d_put_price = o_price[idx1]
        _25d_put_iv = iv[idx1]
    else:
        w1 = (delta2 - delta)/(delta2 - delta1)
        w2 = (delta - delta1)/(delta2 - delta1)
        _25d_put_price = w1*o_price[idx1] + w2*o_price[idx2]
        _25d_put_iv = w1*iv[idx1] + w2*iv[idx2]


    if (price > 0.25):
        idx1, idx2, price1, price2 = column_index_price(put_strike, price)
        if price1 == price:
            _atm_put_iv = iv[idx1]
        else:
            w1 = (price2 - price)/(price2 - price1)
            w2 = (price - price1)/(price2 - price1)
            _atm_put_iv = w1*iv[idx1] + w2*iv[idx2]
    else:
        _atm_put_iv = np.nan

    return [_25d_put_price, _25d_put_iv,\
            _25d_call_price, _25d_call_iv,\
            _atm_put_iv,\
            _atm_call_iv
            ]


def update_deribit_option_info_detail():
    for variety in ['BTC', 'ETH']:
        print(variety + ' info details')
        info_path = os.path.join(option_price_dir, 'deribit', variety+'_info'+'.csv')
        info_df = pd.read_csv(info_path)
        info_t = pd.DatetimeIndex(pd.to_datetime(info_df['time'], format='%Y-%m-%d %H:%M:%S'))

        earlist_time = '2023-07-22 00:00:00'
        path = os.path.join(option_price_dir, 'deribit', variety+'_info_detail'+'.csv')
        last_line_time = get_last_line_time(path, variety+'_info_detail', earlist_time, 19, '%Y-%m-%d %H:%M:%S')
        last_line_time_dt = pd.to_datetime(last_line_time, format='%Y-%m-%d %H:%M:%S')
        if (last_line_time == earlist_time):
            start_idx = 0
        else:
            start_idx = np.where(info_t == last_line_time_dt)[0][0] + 1

        fut_path = os.path.join(future_price_dir, 'deribit', variety+'.csv')
        fut_df = pd.read_csv(fut_path, header=[0,1])
        fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['time'], format='%Y-%m-%d %H:%M:%S'))
        fut_cols = ['c1','c2','c3','c4','c5','c6','c7']


        opt_dict = {}
        cols = ['time', 'total_put_volume', 'total_call_volume', 'total_put_oi', 'total_call_oi', 
                # 本周末到期的期权合约
                'total_put_volume_1w', 'total_call_volume_1w', 'total_put_oi_1w', 'total_call_oi_1w', 
                'atm_put_iv_1w', 'atm_call_iv_1w', '25d_put_iv_1w', '25d_call_iv_1w', 
                '10d_put_iv_1w', '10d_call_iv_1w', '5d_put_iv_1w', '5d_call_iv_1w',
                'put_volume_max1_1w', 'put_volume_max1_strike_1w', 'put_volume_max2_1w', 'put_volume_max2_strike_1w',
                'put_volume_max3_1w', 'put_volume_max3_strike_1w', 'put_volume_max4_1w', 'put_volume_max4_strike_1w',
                'put_volume_max5_1w', 'put_volume_max5_strike_1w',
                'call_volume_max1_1w', 'call_volume_max1_strike_1w', 'call_volume_max2_1w', 'call_volume_max2_strike_1w',
                'call_volume_max3_1w', 'call_volume_max3_strike_1w', 'call_volume_max4_1w', 'call_volume_max4_strike_1w',
                'call_volume_max5_1w', 'call_volume_max5_strike_1w',
                'put_oi_max1_1w', 'put_oi_max1_strike_1w', 'put_oi_max2_1w', 'put_oi_max2_strike_1w',
                'put_oi_max3_1w', 'put_oi_max3_strike_1w', 'put_oi_max4_1w', 'put_oi_max4_strike_1w',
                'put_oi_max5_1w', 'put_oi_max5_strike_1w',
                'call_oi_max1_1w', 'call_oi_max1_strike_1w', 'call_oi_max2_1w', 'call_oi_max2_strike_1w',
                'call_oi_max3_1w', 'call_oi_max3_strike_1w', 'call_oi_max4_1w', 'call_oi_max4_strike_1w',
                'call_oi_max5_1w', 'call_oi_max5_strike_1w',
                # 下周末到期的期权合约
                'total_put_volume_2w', 'total_call_volume_2w', 'total_put_oi_2w', 'total_call_oi_2w', 
                'atm_put_iv_2w', 'atm_call_iv_2w', '25d_put_iv_2w', '25d_call_iv_2w', 
                '10d_put_iv_2w', '10d_call_iv_2w', '5d_put_iv_2w', '5d_call_iv_2w',
                'put_volume_max1_2w', 'put_volume_max1_strike_2w', 'put_volume_max2_2w', 'put_volume_max2_strike_2w',
                'put_volume_max3_2w', 'put_volume_max3_strike_2w', 'put_volume_max4_2w', 'put_volume_max4_strike_2w',
                'put_volume_max5_2w', 'put_volume_max5_strike_2w',
                'call_volume_max1_2w', 'call_volume_max1_strike_2w', 'call_volume_max2_2w', 'call_volume_max2_strike_2w',
                'call_volume_max3_2w', 'call_volume_max3_strike_2w', 'call_volume_max4_2w', 'call_volume_max4_strike_2w',
                'call_volume_max5_2w', 'call_volume_max5_strike_2w',
                'put_oi_max1_2w', 'put_oi_max1_strike_2w', 'put_oi_max2_2w', 'put_oi_max2_strike_2w',
                'put_oi_max3_2w', 'put_oi_max3_strike_2w', 'put_oi_max4_2w', 'put_oi_max4_strike_2w',
                'put_oi_max5_2w', 'put_oi_max5_strike_2w',
                'call_oi_max1_2w', 'call_oi_max1_strike_2w', 'call_oi_max2_2w', 'call_oi_max2_strike_2w',
                'call_oi_max3_2w', 'call_oi_max3_strike_2w', 'call_oi_max4_2w', 'call_oi_max4_strike_2w',
                'call_oi_max5_2w', 'call_oi_max5_strike_2w',
                # 本月末到期的期权合约
                'total_put_volume_1m', 'total_call_volume_1m', 'total_put_oi_1m', 'total_call_oi_1m', 
                'atm_put_iv_1m', 'atm_call_iv_1m', '25d_put_iv_1m', '25d_call_iv_1m', 
                '10d_put_iv_1m', '10d_call_iv_1m', '5d_put_iv_1m', '5d_call_iv_1m',
                'put_volume_max1_1m', 'put_volume_max1_strike_1m', 'put_volume_max2_1m', 'put_volume_max2_strike_1m',
                'put_volume_max3_1m', 'put_volume_max3_strike_1m', 'put_volume_max4_1m', 'put_volume_max4_strike_1m',
                'put_volume_max5_1m', 'put_volume_max5_strike_1m',
                'call_volume_max1_1m', 'call_volume_max1_strike_1m', 'call_volume_max2_1m', 'call_volume_max2_strike_1m',
                'call_volume_max3_1m', 'call_volume_max3_strike_1m', 'call_volume_max4_1m', 'call_volume_max4_strike_1m',
                'call_volume_max5_1m', 'call_volume_max5_strike_1m',
                'put_oi_max1_1m', 'put_oi_max1_strike_1m', 'put_oi_max2_1m', 'put_oi_max2_strike_1m',
                'put_oi_max3_1m', 'put_oi_max3_strike_1m', 'put_oi_max4_1m', 'put_oi_max4_strike_1m',
                'put_oi_max5_1m', 'put_oi_max5_strike_1m',
                'call_oi_max1_1m', 'call_oi_max1_strike_1m', 'call_oi_max2_1m', 'call_oi_max2_strike_1m',
                'call_oi_max3_1m', 'call_oi_max3_strike_1m', 'call_oi_max4_1m', 'call_oi_max4_strike_1m',
                'call_oi_max5_1m', 'call_oi_max5_strike_1m',
                # 本季度末到期的期权合约
                'total_put_volume_1q', 'total_call_volume_1q', 'total_put_oi_1q', 'total_call_oi_1q', 
                'atm_put_iv_1q', 'atm_call_iv_1q', '25d_put_iv_1q', '25d_call_iv_1q', 
                '10d_put_iv_1q', '10d_call_iv_1q', '5d_put_iv_1q', '5d_call_iv_1q',
                'put_volume_max1_1q', 'put_volume_max1_strike_1q', 'put_volume_max2_1q', 'put_volume_max2_strike_1q',
                'put_volume_max3_1q', 'put_volume_max3_strike_1q', 'put_volume_max4_1q', 'put_volume_max4_strike_1q',
                'put_volume_max5_1q', 'put_volume_max5_strike_1q',
                'call_volume_max1_1q', 'call_volume_max1_strike_1q', 'call_volume_max2_1q', 'call_volume_max2_strike_1q',
                'call_volume_max3_1q', 'call_volume_max3_strike_1q', 'call_volume_max4_1q', 'call_volume_max4_strike_1q',
                'call_volume_max5_1q', 'call_volume_max5_strike_1q',
                'put_oi_max1_1q', 'put_oi_max1_strike_1q', 'put_oi_max2_1q', 'put_oi_max2_strike_1q',
                'put_oi_max3_1q', 'put_oi_max3_strike_1q', 'put_oi_max4_1q', 'put_oi_max4_strike_1q',
                'put_oi_max5_1q', 'put_oi_max5_strike_1q',
                'call_oi_max1_1q', 'call_oi_max1_strike_1q', 'call_oi_max2_1q', 'call_oi_max2_strike_1q',
                'call_oi_max3_1q', 'call_oi_max3_strike_1q', 'call_oi_max4_1q', 'call_oi_max4_strike_1q',
                'call_oi_max5_1q', 'call_oi_max5_strike_1q',
                # 年末到期的期权合约
                'total_put_volume_1y', 'total_call_volume_1y', 'total_put_oi_1y', 'total_call_oi_1y', 
                'atm_put_iv_1y', 'atm_call_iv_1y', '25d_put_iv_1y', '25d_call_iv_1y', 
                '10d_put_iv_1y', '10d_call_iv_1y', '5d_put_iv_1y', '5d_call_iv_1y',
                'put_volume_max1_1y', 'put_volume_max1_strike_1y', 'put_volume_max2_1y', 'put_volume_max2_strike_1y',
                'put_volume_max3_1y', 'put_volume_max3_strike_1y', 'put_volume_max4_1y', 'put_volume_max4_strike_1y',
                'put_volume_max5_1y', 'put_volume_max5_strike_1y',
                'call_volume_max1_1y', 'call_volume_max1_strike_1y', 'call_volume_max2_1y', 'call_volume_max2_strike_1y',
                'call_volume_max3_1y', 'call_volume_max3_strike_1y', 'call_volume_max4_1y', 'call_volume_max4_strike_1y',
                'call_volume_max5_1y', 'call_volume_max5_strike_1y',
                'put_oi_max1_1y', 'put_oi_max1_strike_1y', 'put_oi_max2_1y', 'put_oi_max2_strike_1y',
                'put_oi_max3_1y', 'put_oi_max3_strike_1y', 'put_oi_max4_1y', 'put_oi_max4_strike_1y',
                'put_oi_max5_1y', 'put_oi_max5_strike_1y',
                'call_oi_max1_1y', 'call_oi_max1_strike_1y', 'call_oi_max2_1y', 'call_oi_max2_strike_1y',
                'call_oi_max3_1y', 'call_oi_max3_strike_1y', 'call_oi_max4_1y', 'call_oi_max4_strike_1y',
                'call_oi_max5_1y', 'call_oi_max5_strike_1y',
                ]

        df = pd.DataFrame(columns=cols)
        for i in range(start_idx, len(info_t)):
            t = info_t[i]
            df.loc[i, 'time'] = t.strftime('%Y-%m-%d %H:%M:%S')
            df.loc[i, 'total_put_volume'] = 0
            df.loc[i, 'total_call_volume'] = 0
            df.loc[i, 'total_put_oi'] = 0
            df.loc[i, 'total_call_oi'] = 0
            keys = ['1w', '2w', '1m', '1q', '1y']
            for key in keys:
                df.loc[i, key] = ''

            inst_ids = info_df.loc[i, 'inst_ids'].split(',')
            inst_ids.remove('')
            # 到期日dict
            expiry_dict = {}
            for inst_id in inst_ids:
                if not(inst_id in opt_dict):
                    opt_path = os.path.join(option_price_dir, 'deribit', inst_id+'.csv')
                    opt_df = pd.read_csv(opt_path, header=[0,1,2])
                    opt_t = pd.DatetimeIndex(pd.to_datetime(opt_df['time']['time']['time'], format='%Y-%m-%d %H:%M:%S'))
                    put_strike, call_strike = get_crypto_full_strike_price(opt_df)
                    opt_dict[inst_id] = [opt_t, opt_df, put_strike, call_strike]

            # 星期五下午 16:00:00 到期
            ymd_dt = pd.to_datetime(t.strftime('%Y-%m-%d'), format='%Y-%m-%d')
            weekday = t.weekday()
            hour = t.hour
            # 1w
            if (weekday < 4) or (weekday == 4 and hour < 16):
                # 本周 星期五下午 16:00:00 之前
                expiry_dt = ymd_dt + pd.Timedelta(days=(4-weekday)) # to friday
                expiry = expiry_dt.strftime('%Y%m%d')
            else:
                # 之后
                expiry_dt = ymd_dt + pd.Timedelta(days=(11-weekday)) # to next friday
                expiry = expiry_dt.strftime('%Y%m%d')
            expiry_dict['1w'] = expiry[2:]

            # 2w
            expiry_dt = expiry_dt + pd.Timedelta(days=7)
            expiry = expiry_dt.strftime('%Y%m%d')
            expiry_dict['2w'] = expiry[2:]

            # 1m
            expiry_dt = get_last_friday_expiry_time(t.year, t.month)
            if (t < expiry_dt):
                expiry = expiry_dt.strftime('%Y%m%d')
            else:
                if t.month == 12:
                    expiry_dt = get_last_friday_expiry_time(t.year+1, 1)
                    expiry = expiry_dt.strftime('%Y%m%d')
                else:
                    expiry_dt = get_last_friday_expiry_time(t.year, t.month+1)
                    expiry = expiry_dt.strftime('%Y%m%d')
            expiry_dict['1m'] = expiry[2:]

            # 1q
            month = t.month
            # 本季度末的那个月
            month = (month//3 + 1)*3
            expiry_dt = get_last_friday_expiry_time(t.year, month)
            if (t < expiry_dt):
                expiry = expiry_dt.strftime('%Y%m%d')
            else:
                if t.month == 12:
                    expiry_dt = get_last_friday_expiry_time(t.year+1, 3)
                    expiry = expiry_dt.strftime('%Y%m%d')
                else:
                    expiry_dt = get_last_friday_expiry_time(t.year, month+3)
                    expiry = expiry_dt.strftime('%Y%m%d')
            expiry_dict['1q'] = expiry[2:]            

            # 1y
            expiry_dt = get_last_friday_expiry_time(t.year, 12)
            if (t < expiry_dt):
                expiry = expiry_dt.strftime('%Y%m%d')
            else:
                expiry_dt = get_last_friday_expiry_time(t.year+1, 12)
                expiry = expiry_dt.strftime('%Y%m%d')
            expiry_dict['1y'] = expiry[2:]

            print(t, expiry_dict)

            for key in keys:
                # inst_id example: BTC230825
                inst_id = variety + expiry_dict[key]
                if inst_id in opt_dict:
                    temp_t = opt_dict[inst_id][0]
                    try:
                        w = np.where(temp_t == t)[0][0]
                    except:
                        continue
                    temp_df = opt_dict[inst_id][1].loc[w,:]
                    put_strike = opt_dict[inst_id][2]
                    call_strike = opt_dict[inst_id][3]

                    put_volume = temp_df.loc[pd.IndexSlice['P', :, 'volume']].sum()
                    call_volume = temp_df.loc[pd.IndexSlice['C', :, 'volume']].sum()
                    df.loc[i, 'total_put_volume_'+key] = put_volume
                    df.loc[i, 'total_call_volume_'+key] = call_volume
                    df.loc[i, 'total_put_volume'] = df.loc[i, 'total_put_volume'] + put_volume
                    df.loc[i, 'total_call_volume'] = df.loc[i, 'total_call_volume'] + call_volume

                    put_oi = temp_df.loc[pd.IndexSlice['P', :, 'oi']].sum()
                    call_oi = temp_df.loc[pd.IndexSlice['C', :, 'oi']].sum()
                    df.loc[i, 'total_put_oi_'+key] = put_oi
                    df.loc[i, 'total_call_oi_'+key] = call_oi
                    df.loc[i, 'total_put_oi'] = df.loc[i, 'total_put_oi'] + put_oi
                    df.loc[i, 'total_call_oi'] = df.loc[i, 'total_call_oi'] + call_oi

                    ## put volume sort
                    tmp = temp_df.loc[pd.IndexSlice['P', :, 'volume']]
                    P_volume = tmp.replace(np.nan, -1.0)
                    idx = P_volume.index
                    P_volume = np.array(P_volume)
                    sort = np.argsort(P_volume)
                    P_volume = P_volume[sort]
                    idx = idx[sort]
                    df.loc[i, 'put_volume_max1_'+key] = P_volume[-1]
                    df.loc[i, 'put_volume_max1_strike_'+key] = idx[-1]
                    df.loc[i, 'put_volume_max2_'+key] = P_volume[-2]
                    df.loc[i, 'put_volume_max2_strike_'+key] = idx[-2]
                    df.loc[i, 'put_volume_max3_'+key] = P_volume[-3]
                    df.loc[i, 'put_volume_max3_strike_'+key] = idx[-3]
                    df.loc[i, 'put_volume_max4_'+key] = P_volume[-4]
                    df.loc[i, 'put_volume_max4_strike_'+key] = idx[-4]
                    df.loc[i, 'put_volume_max5_'+key] = P_volume[-5]
                    df.loc[i, 'put_volume_max5_strike_'+key] = idx[-5]
                    ## call volume sort
                    tmp = temp_df.loc[pd.IndexSlice['C', :, 'volume']]
                    C_volume = tmp.replace(np.nan, -1.0)
                    idx = C_volume.index
                    C_volume = np.array(C_volume)
                    sort = np.argsort(C_volume)
                    C_volume = C_volume[sort]
                    idx = idx[sort]
                    df.loc[i, 'call_volume_max1_'+key] = C_volume[-1]
                    df.loc[i, 'call_volume_max1_strike_'+key] = idx[-1]
                    df.loc[i, 'call_volume_max2_'+key] = C_volume[-2]
                    df.loc[i, 'call_volume_max2_strike_'+key] = idx[-2]
                    df.loc[i, 'call_volume_max3_'+key] = C_volume[-3]
                    df.loc[i, 'call_volume_max3_strike_'+key] = idx[-3]
                    df.loc[i, 'call_volume_max4_'+key] = C_volume[-4]
                    df.loc[i, 'call_volume_max4_strike_'+key] = idx[-4]
                    df.loc[i, 'call_volume_max5_'+key] = C_volume[-5]
                    df.loc[i, 'call_volume_max5_strike_'+key] = idx[-5]


                    ## put oi sort
                    tmp = temp_df.loc[pd.IndexSlice['P', :, 'oi']]
                    P_oi = tmp.replace(np.nan, -1.0)
                    idx = P_oi.index
                    P_oi = np.array(P_oi)
                    sort = np.argsort(P_oi)
                    P_oi = P_oi[sort]
                    idx = idx[sort]
                    df.loc[i, 'put_oi_max1_'+key] = P_oi[-1]
                    df.loc[i, 'put_oi_max1_strike_'+key] = idx[-1]
                    df.loc[i, 'put_oi_max2_'+key] = P_oi[-2]
                    df.loc[i, 'put_oi_max2_strike_'+key] = idx[-2]
                    df.loc[i, 'put_oi_max3_'+key] = P_oi[-3]
                    df.loc[i, 'put_oi_max3_strike_'+key] = idx[-3]
                    df.loc[i, 'put_oi_max4_'+key] = P_oi[-4]
                    df.loc[i, 'put_oi_max4_strike_'+key] = idx[-4]
                    df.loc[i, 'put_oi_max5_'+key] = P_oi[-5]
                    df.loc[i, 'put_oi_max5_strike_'+key] = idx[-5]
                    ## call oi sort
                    tmp = temp_df.loc[pd.IndexSlice['C', :, 'oi']]
                    C_oi = tmp.replace(np.nan, -1.0)
                    idx = C_oi.index
                    C_oi = np.array(C_oi)
                    sort = np.argsort(C_oi)
                    C_oi = C_oi[sort]
                    idx = idx[sort]
                    df.loc[i, 'call_oi_max1_'+key] = C_oi[-1]
                    df.loc[i, 'call_oi_max1_strike_'+key] = idx[-1]
                    df.loc[i, 'call_oi_max2_'+key] = C_oi[-2]
                    df.loc[i, 'call_oi_max2_strike_'+key] = idx[-2]
                    df.loc[i, 'call_oi_max3_'+key] = C_oi[-3]
                    df.loc[i, 'call_oi_max3_strike_'+key] = idx[-3]
                    df.loc[i, 'call_oi_max4_'+key] = C_oi[-4]
                    df.loc[i, 'call_oi_max4_strike_'+key] = idx[-4]
                    df.loc[i, 'call_oi_max5_'+key] = C_oi[-5]
                    df.loc[i, 'call_oi_max5_strike_'+key] = idx[-5]

                    try:
                        w = np.where(fut_t == t)[0][0]
                    except:
                        continue
                    fut_temp_df = fut_df.loc[w,:]
                    for c in fut_cols:
                        if (fut_temp_df[c]['inst_id'] == inst_id):
                            fut_price = fut_temp_df[c]['close']
                            break

                    df.loc[i, key] = inst_id
                    ret = crypto_put_call_delta_volatility(temp_df, 0.25, fut_price, put_strike, call_strike)
                    df.loc[i, '25d_put_iv_'+key] = ret[1]
                    df.loc[i, '25d_call_iv_'+key] = ret[3]
                    df.loc[i, 'atm_put_iv_'+key] = ret[4]
                    df.loc[i, 'atm_call_iv_'+key] = ret[5]

                    ret = crypto_put_call_delta_volatility(temp_df, 0.1, fut_price, put_strike, call_strike)
                    df.loc[i, '10d_put_iv_'+key] = ret[1]
                    df.loc[i, '10d_call_iv_'+key] = ret[3]

                    ret = crypto_put_call_delta_volatility(temp_df, 0.05, fut_price, put_strike, call_strike)
                    df.loc[i, '5d_put_iv_'+key] = ret[1]
                    df.loc[i, '5d_call_iv_'+key] = ret[3]

        path = os.path.join(option_price_dir, 'deribit', variety+'_info_detail'+'.csv')
        if os.path.exists(path):
            df.to_csv(path, mode='a', encoding='utf-8', index=False, header=None)
        else:
            df.to_csv(path, encoding='utf-8', index=False)  


def plot_deribit_option_datas():
    for variety in ['BTC', 'ETH']:
        path = os.path.join(option_price_dir, 'deribit', variety+'_info_detail'+'.csv')
        df = pd.read_csv(path)
        t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S'))

        cols = df.columns.tolist()
        cols.remove('time')
        data_dict = {}
        for col in cols:
            if len(col) == 2:
                data_dict[col] = np.array(df[col], dtype=str)
            else:
                data_dict[col] = np.array(df[col], dtype=float)

            if 'oi' in col:
                tmp = data_dict[col]
                tmp[tmp == 0] = np.nan
                data_dict[col] = tmp

        fut_path = os.path.join(future_price_dir, 'deribit', variety+'.csv')
        fut_df = pd.read_csv(fut_path, header=[0,1])
        fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['time'], format='%Y-%m-%d %H:%M:%S'))
        fut_price = np.array(fut_df['PERPETUAL']['close'], dtype=float)


        contract_1w = data_dict['1w'][-1]
        # fut_price_1w = np.array(fut_df['c1']['close'], dtype=float)
        # put_oi_max1_strike_1w = np.array(df['put_oi_max1_strike_1w'], dtype=float)
        # put_oi_max2_strike_1w = np.array(df['put_oi_max2_strike_1w'], dtype=float)
        # put_oi_max3_strike_1w = np.array(df['put_oi_max3_strike_1w'], dtype=float)
        # call_oi_max1_strike_1w = np.array(df['call_oi_max1_strike_1w'], dtype=float)
        # call_oi_max2_strike_1w = np.array(df['call_oi_max2_strike_1w'], dtype=float)
        # call_oi_max3_strike_1w = np.array(df['call_oi_max3_strike_1w'], dtype=float)
        # put_oi_max_strike_1w = np.vstack((put_oi_max1_strike_1w, put_oi_max2_strike_1w, put_oi_max3_strike_1w))
        # put_oi_max_strike_1w = np.sort(put_oi_max_strike_1w, axis=0)
        # call_oi_max_strike_1w = np.vstack((call_oi_max1_strike_1w, call_oi_max2_strike_1w, call_oi_max3_strike_1w))
        # call_oi_max_strike_1w = np.sort(call_oi_max_strike_1w, axis=0)

        # put_oi_max = np.array(df['put_oi_max1_1w'], dtype=float)
        # w = np.where(put_oi_max > 0)[0]

        # datas = [
        #         [[[t,data_dict['5d_put_iv_1w'],'1week '+contract_1w+' 5d_put_iv','color=darkgreen'],
        #           [t,data_dict['5d_call_iv_1w'],'1week '+contract_1w+' 5d_call_iv','color=red'],
        #         ],
        #         [],''],            

        #         [[[t,data_dict['10d_put_iv_1w'],'1week '+contract_1w+' 10d_put_iv','color=darkgreen'],
        #           [t,data_dict['10d_call_iv_1w'],'1week '+contract_1w+' 10d_call_iv','color=red'],
        #         ],
        #         [],''],                

        #         [[
        #           [t,data_dict['5d_put_iv_1w']-data_dict['5d_call_iv_1w'],'1week '+contract_1w+'  5d_skew',''],
        #           [t,data_dict['10d_put_iv_1w']-data_dict['10d_call_iv_1w'],'1week '+contract_1w+' 10d_skew',''],
        #           [t,data_dict['25d_put_iv_1w']-data_dict['25d_call_iv_1w'],'1week '+contract_1w+' 25d_skew',''], 
        #           [t,data_dict['atm_put_iv_1w']-data_dict['atm_call_iv_1w'],'1week '+contract_1w+' atm_skew',''],
        #          ],
        #         [],''],

        #         [[[fut_t,fut_price_1w,variety,'color=black, width=4'],
        #           [t[w],call_oi_max_strike_1w[0,:][w],'call 持仓量一','color=red'],
        #           [t[w],call_oi_max_strike_1w[1,:][w],'call 持仓量二','color=orange'],
        #           [t[w],put_oi_max_strike_1w[2,:][w],'put 持仓量一','color=darkgreen'],
        #           [t[w],put_oi_max_strike_1w[1,:][w],'put 持仓量二','color=purple'],],[],''],

        #         [[[t,data_dict['25d_put_iv_1w'],'1week '+contract_1w+' 25d_put_iv','color=darkgreen'],
        #           [t,data_dict['25d_call_iv_1w'],'1week '+contract_1w+' 25d_call_iv','color=red'],
        #         ],
        #         [],''],

        #         [[[t,data_dict['atm_put_iv_1w'],'1week '+contract_1w+' atm_put_iv','color=darkgreen'],
        #           [t,data_dict['atm_call_iv_1w'],'1week '+contract_1w+' atm_call_iv','color=red'],
        #         ],
        #         [],''],
        #         ]
        # plot_many_figure(datas, max_height=900)


        contract_2w = data_dict['2w'][-1]
        # fut_price_2w = np.array(fut_df['c2']['close'], dtype=float)
        # put_oi_max1_strike_2w = np.array(df['put_oi_max1_strike_2w'], dtype=float)
        # put_oi_max2_strike_2w = np.array(df['put_oi_max2_strike_2w'], dtype=float)
        # put_oi_max3_strike_2w = np.array(df['put_oi_max3_strike_2w'], dtype=float)
        # call_oi_max1_strike_2w = np.array(df['call_oi_max1_strike_2w'], dtype=float)
        # call_oi_max2_strike_2w = np.array(df['call_oi_max2_strike_2w'], dtype=float)
        # call_oi_max3_strike_2w = np.array(df['call_oi_max3_strike_2w'], dtype=float)
        # put_oi_max_strike_2w = np.vstack((put_oi_max1_strike_2w, put_oi_max2_strike_2w, put_oi_max3_strike_2w))
        # put_oi_max_strike_2w = np.sort(put_oi_max_strike_2w, axis=0)
        # call_oi_max_strike_2w = np.vstack((call_oi_max1_strike_2w, call_oi_max2_strike_2w, call_oi_max3_strike_2w))
        # call_oi_max_strike_2w = np.sort(call_oi_max_strike_2w, axis=0)

        # put_oi_max = np.array(df['put_oi_max1_2w'], dtype=float)
        # w = np.where(put_oi_max > 0)[0]

        # datas = [
        #         [[[t,data_dict['5d_put_iv_2w'],'2week '+contract_2w+' 5d_put_iv','color=darkgreen'],
        #           [t,data_dict['5d_call_iv_2w'],'2week '+contract_2w+' 5d_call_iv','color=red'],
        #         ],
        #         [],''],            

        #         [[[t,data_dict['10d_put_iv_2w'],'2week '+contract_2w+' 10d_put_iv','color=darkgreen'],
        #           [t,data_dict['10d_call_iv_2w'],'2week '+contract_2w+' 10d_call_iv','color=red'],
        #         ],
        #         [],''],                

        #         [[
        #           [t,data_dict['5d_put_iv_2w']-data_dict['5d_call_iv_2w'],'2week '+contract_2w+'  5d_skew',''],
        #           [t,data_dict['10d_put_iv_2w']-data_dict['10d_call_iv_2w'],'2week '+contract_2w+' 10d_skew',''],
        #           [t,data_dict['25d_put_iv_2w']-data_dict['25d_call_iv_2w'],'2week '+contract_2w+' 25d_skew',''], 
        #           [t,data_dict['atm_put_iv_2w']-data_dict['atm_call_iv_2w'],'2week '+contract_2w+' atm_skew',''],
        #          ],
        #         [],''],

        #         [[[fut_t,fut_price_2w,variety,'color=black, width=4'],
        #           [t[w],call_oi_max_strike_2w[0,:][w],'call 持仓量一','color=red'],
        #           [t[w],call_oi_max_strike_2w[1,:][w],'call 持仓量二','color=orange'],
        #           [t[w],put_oi_max_strike_2w[2,:][w],'put 持仓量一','color=darkgreen'],
        #           [t[w],put_oi_max_strike_2w[1,:][w],'put 持仓量二','color=purple'],],[],''],

        #         [[[t,data_dict['25d_put_iv_2w'],'2week '+contract_2w+' 25d_put_iv','color=darkgreen'],
        #           [t,data_dict['25d_call_iv_2w'],'2week '+contract_2w+' 25d_call_iv','color=red'],
        #         ],
        #         [],''],

        #         [[[t,data_dict['atm_put_iv_2w'],'2week '+contract_2w+' atm_put_iv','color=darkgreen'],
        #           [t,data_dict['atm_call_iv_2w'],'2week '+contract_2w+' atm_call_iv','color=red'],
        #         ],
        #         [],''],
        #         ]
        # plot_many_figure(datas, max_height=900)


        contract_1m = data_dict['1m'][-1]
        fut_price_1m = np.array(fut_df['c3']['close'], dtype=float)
        put_oi_max1_strike_1m = np.array(df['put_oi_max1_strike_1m'], dtype=float)
        put_oi_max2_strike_1m = np.array(df['put_oi_max2_strike_1m'], dtype=float)
        put_oi_max3_strike_1m = np.array(df['put_oi_max3_strike_1m'], dtype=float)
        call_oi_max1_strike_1m = np.array(df['call_oi_max1_strike_1m'], dtype=float)
        call_oi_max2_strike_1m = np.array(df['call_oi_max2_strike_1m'], dtype=float)
        call_oi_max3_strike_1m = np.array(df['call_oi_max3_strike_1m'], dtype=float)
        put_oi_max_strike_1m = np.vstack((put_oi_max1_strike_1m, put_oi_max2_strike_1m, put_oi_max3_strike_1m))
        put_oi_max_strike_1m = np.sort(put_oi_max_strike_1m, axis=0)
        call_oi_max_strike_1m = np.vstack((call_oi_max1_strike_1m, call_oi_max2_strike_1m, call_oi_max3_strike_1m))
        call_oi_max_strike_1m = np.sort(call_oi_max_strike_1m, axis=0)

        put_oi_max = np.array(df['put_oi_max1_1m'], dtype=float)
        w = np.where(put_oi_max > 0)[0]

        datas = [
                [[[t,data_dict['5d_put_iv_1m'],'1month '+contract_1m+' 5d_put_iv','color=darkgreen'],
                  [t,data_dict['5d_call_iv_1m'],'1month '+contract_1m+' 5d_call_iv','color=red'],
                ],
                [],''],            

                [[[t,data_dict['10d_put_iv_1m'],'1month '+contract_1m+' 10d_put_iv','color=darkgreen'],
                  [t,data_dict['10d_call_iv_1m'],'1month '+contract_1m+' 10d_call_iv','color=red'],
                ],
                [],''],                

                [[
                  [t,data_dict['5d_put_iv_1m']-data_dict['5d_call_iv_1m'],'1month '+contract_1m+'  5d_skew',''],
                  [t,data_dict['10d_put_iv_1m']-data_dict['10d_call_iv_1m'],'1month '+contract_1m+' 10d_skew',''],
                  [t,data_dict['25d_put_iv_1m']-data_dict['25d_call_iv_1m'],'1month '+contract_1m+' 25d_skew',''], 
                  [t,data_dict['atm_put_iv_1m']-data_dict['atm_call_iv_1m'],'1month '+contract_1m+' atm_skew',''],
                 ],
                [],''],

                [[[fut_t,fut_price_1m,variety,'color=black, width=4'],
                  [t[w],call_oi_max_strike_1m[0,:][w],'call 持仓量一','color=red'],
                  [t[w],call_oi_max_strike_1m[1,:][w],'call 持仓量二','color=orange'],
                  [t[w],put_oi_max_strike_1m[2,:][w],'put 持仓量一','color=darkgreen'],
                  [t[w],put_oi_max_strike_1m[1,:][w],'put 持仓量二','color=purple'],],[],''],

                [[[t,data_dict['25d_put_iv_1m'],'1month '+contract_1m+' 25d_put_iv','color=darkgreen'],
                  [t,data_dict['25d_call_iv_1m'],'1month '+contract_1m+' 25d_call_iv','color=red'],
                ],
                [],''],

                [[[t,data_dict['atm_put_iv_1m'],'1month '+contract_1m+' atm_put_iv','color=darkgreen'],
                  [t,data_dict['atm_call_iv_1m'],'1month '+contract_1m+' atm_call_iv','color=red'],
                ],
                [],''],
                ]
        plot_many_figure(datas, max_height=900)


        contract_1q = data_dict['1q'][-1]
        # fut_price_1q = np.array(fut_df['c4']['close'], dtype=float)
        # put_oi_max1_strike_1q = np.array(df['put_oi_max1_strike_1q'], dtype=float)
        # put_oi_max2_strike_1q = np.array(df['put_oi_max2_strike_1q'], dtype=float)
        # put_oi_max3_strike_1q = np.array(df['put_oi_max3_strike_1q'], dtype=float)
        # call_oi_max1_strike_1q = np.array(df['call_oi_max1_strike_1q'], dtype=float)
        # call_oi_max2_strike_1q = np.array(df['call_oi_max2_strike_1q'], dtype=float)
        # call_oi_max3_strike_1q = np.array(df['call_oi_max3_strike_1q'], dtype=float)
        # put_oi_max_strike_1q = np.vstack((put_oi_max1_strike_1q, put_oi_max2_strike_1q, put_oi_max3_strike_1q))
        # put_oi_max_strike_1q = np.sort(put_oi_max_strike_1q, axis=0)
        # call_oi_max_strike_1q = np.vstack((call_oi_max1_strike_1q, call_oi_max2_strike_1q, call_oi_max3_strike_1q))
        # call_oi_max_strike_1q = np.sort(call_oi_max_strike_1q, axis=0)

        # put_oi_max = np.array(df['put_oi_max1_1q'], dtype=float)
        # w = np.where(put_oi_max > 0)[0]

        # datas = [
        #         [[[t,data_dict['5d_put_iv_1q'],'1quarter '+contract_1q+' 5d_put_iv','color=darkgreen'],
        #           [t,data_dict['5d_call_iv_1q'],'1quarter '+contract_1q+' 5d_call_iv','color=red'],
        #         ],
        #         [],''],            

        #         [[[t,data_dict['10d_put_iv_1q'],'1quarter '+contract_1q+' 10d_put_iv','color=darkgreen'],
        #           [t,data_dict['10d_call_iv_1q'],'1quarter '+contract_1q+' 10d_call_iv','color=red'],
        #         ],
        #         [],''],                

        #         [[
        #           [t,data_dict['5d_put_iv_1q']-data_dict['5d_call_iv_1q'],'1quarter '+contract_1q+'  5d_skew',''],
        #           [t,data_dict['10d_put_iv_1q']-data_dict['10d_call_iv_1q'],'1quarter '+contract_1q+' 10d_skew',''],
        #           [t,data_dict['25d_put_iv_1q']-data_dict['25d_call_iv_1q'],'1quarter '+contract_1q+' 25d_skew',''], 
        #           [t,data_dict['atm_put_iv_1q']-data_dict['atm_call_iv_1q'],'1quarter '+contract_1q+' atm_skew',''],
        #          ],
        #         [],''],

        #         [[[fut_t,fut_price_1q,variety,'color=black, width=4'],
        #           [t[w],call_oi_max_strike_1q[0,:][w],'call 持仓量一','color=red'],
        #           [t[w],call_oi_max_strike_1q[1,:][w],'call 持仓量二','color=orange'],
        #           [t[w],put_oi_max_strike_1q[2,:][w],'put 持仓量一','color=darkgreen'],
        #           [t[w],put_oi_max_strike_1q[1,:][w],'put 持仓量二','color=purple'],],[],''],

        #         [[[t,data_dict['25d_put_iv_1q'],'1quarter '+contract_1q+' 25d_put_iv','color=darkgreen'],
        #           [t,data_dict['25d_call_iv_1q'],'1quarter '+contract_1q+' 25d_call_iv','color=red'],
        #         ],
        #         [],''],

        #         [[[t,data_dict['atm_put_iv_1q'],'1quarter '+contract_1q+' atm_put_iv','color=darkgreen'],
        #           [t,data_dict['atm_call_iv_1q'],'1quarter '+contract_1q+' atm_call_iv','color=red'],
        #         ],
        #         [],''],
        #         ]
        # plot_many_figure(datas, max_height=900)


        contract_1y = data_dict['1y'][-1]
        # fut_price_1y = np.array(fut_df['c5']['close'], dtype=float)
        # put_oi_max1_strike_1y = np.array(df['put_oi_max1_strike_1y'], dtype=float)
        # put_oi_max2_strike_1y = np.array(df['put_oi_max2_strike_1y'], dtype=float)
        # put_oi_max3_strike_1y = np.array(df['put_oi_max3_strike_1y'], dtype=float)
        # call_oi_max1_strike_1y = np.array(df['call_oi_max1_strike_1y'], dtype=float)
        # call_oi_max2_strike_1y = np.array(df['call_oi_max2_strike_1y'], dtype=float)
        # call_oi_max3_strike_1y = np.array(df['call_oi_max3_strike_1y'], dtype=float)
        # put_oi_max_strike_1y = np.vstack((put_oi_max1_strike_1y, put_oi_max2_strike_1y, put_oi_max3_strike_1y))
        # put_oi_max_strike_1y = np.sort(put_oi_max_strike_1y, axis=0)
        # call_oi_max_strike_1y = np.vstack((call_oi_max1_strike_1y, call_oi_max2_strike_1y, call_oi_max3_strike_1y))
        # call_oi_max_strike_1y = np.sort(call_oi_max_strike_1y, axis=0)

        # put_oi_max = np.array(df['put_oi_max1_1y'], dtype=float)
        # w = np.where(put_oi_max > 0)[0]

        # datas = [
        #         [[[t,data_dict['5d_put_iv_1y'],'1year '+contract_1y+' 5d_put_iv','color=darkgreen'],
        #           [t,data_dict['5d_call_iv_1y'],'1year '+contract_1y+' 5d_call_iv','color=red'],
        #         ],
        #         [],''],            

        #         [[[t,data_dict['10d_put_iv_1y'],'1year '+contract_1y+' 10d_put_iv','color=darkgreen'],
        #           [t,data_dict['10d_call_iv_1y'],'1year '+contract_1y+' 10d_call_iv','color=red'],
        #         ],
        #         [],''],                

        #         [[
        #           [t,data_dict['5d_put_iv_1y']-data_dict['5d_call_iv_1y'],'1year '+contract_1y+'  5d_skew',''],
        #           [t,data_dict['10d_put_iv_1y']-data_dict['10d_call_iv_1y'],'1year '+contract_1y+' 10d_skew',''],
        #           [t,data_dict['25d_put_iv_1y']-data_dict['25d_call_iv_1y'],'1year '+contract_1y+' 25d_skew',''], 
        #           [t,data_dict['atm_put_iv_1y']-data_dict['atm_call_iv_1y'],'1year '+contract_1y+' atm_skew',''],
        #          ],
        #         [],''],

        #         [[[fut_t,fut_price_1y,variety,'color=black, width=4'],
        #           [t[w],call_oi_max_strike_1y[0,:][w],'call 持仓量一','color=red'],
        #           [t[w],call_oi_max_strike_1y[1,:][w],'call 持仓量二','color=orange'],
        #           [t[w],put_oi_max_strike_1y[2,:][w],'put 持仓量一','color=darkgreen'],
        #           [t[w],put_oi_max_strike_1y[1,:][w],'put 持仓量二','color=purple'],],[],''],

        #         [[[t,data_dict['25d_put_iv_1y'],'1year '+contract_1y+' 25d_put_iv','color=darkgreen'],
        #           [t,data_dict['25d_call_iv_1y'],'1year '+contract_1y+' 25d_call_iv','color=red'],
        #         ],
        #         [],''],

        #         [[[t,data_dict['atm_put_iv_1y'],'1year '+contract_1y+' atm_put_iv','color=darkgreen'],
        #           [t,data_dict['atm_call_iv_1y'],'1year '+contract_1y+' atm_call_iv','color=red'],
        #         ],
        #         [],''],
        #         ]
        # plot_many_figure(datas, max_height=900)


        datas = [
                [[[t,data_dict['total_call_volume_1w'],'1week '+contract_1w+' total_call_volume','color=red'],
                [t,data_dict['total_put_volume_1w'],'1week '+contract_1w+' total_put_volume','color=darkgreen'],
                ],
                [[t,data_dict['total_put_volume_1w']-data_dict['total_call_volume_1w'],'1week '+contract_1w+' total_put_volume - total_call_volume','style=vbar'],],''],

                [[[t,data_dict['total_call_volume_2w'],'2week '+contract_2w+' total_call_volume','color=red'],
                [t,data_dict['total_put_volume_2w'],'2week '+contract_2w+' total_put_volume','color=darkgreen'],
                ],
                [[t,data_dict['total_put_volume_2w']-data_dict['total_call_volume_2w'],'2week '+contract_2w+' total_put_volume - total_call_volume','style=vbar'],],''],

                [[[t,data_dict['total_call_volume_1m'],'1month '+contract_1m+' total_call_volume','color=red'],
                [t,data_dict['total_put_volume_1m'],'1month '+contract_1m+' total_put_volume','color=darkgreen'],
                ],
                [[t,data_dict['total_put_volume_1m']-data_dict['total_call_volume_1m'],'1month '+contract_1m+' total_put_volume - total_call_volume','style=vbar'],],''],

                [[[fut_t,fut_price,variety,'color=black']],[],''],

                [[[t,data_dict['total_call_volume_1q'],'1quarter '+contract_1q+' total_call_volume','color=red'],
                [t,data_dict['total_put_volume_1q'],'1quarter '+contract_1q+' total_put_volume','color=darkgreen'],
                ],
                [[t,data_dict['total_put_volume_1q']-data_dict['total_call_volume_1q'],'1quarter '+contract_1q+' total_put_volume - total_call_volume','style=vbar'],],''],

                [[[t,data_dict['total_call_volume_1y'],'1year '+contract_1y+' total_call_volume','color=red'],
                [t,data_dict['total_put_volume_1y'],'1year '+contract_1y+' total_put_volume','color=darkgreen'],
                ],
                [[t,data_dict['total_put_volume_1y']-data_dict['total_call_volume_1y'],'1year '+contract_1y+' total_put_volume - total_call_volume','style=vbar'],],''],

                [[[t,data_dict['total_call_volume'],'total_call_volume','color=red'],
                [t,data_dict['total_put_volume'],'total_put_volume','color=darkgreen'],
                ],
                [[t,data_dict['total_put_volume']-data_dict['total_call_volume'],'total_put_volume - total_call_volume','style=vbar'],],''],

                ]
        plot_many_figure(datas, max_height=800)


        datas = [
                [[[t,data_dict['total_call_oi_1m'],'1month '+contract_1m+' total_call_oi','color=red'],
                [t,data_dict['total_put_oi_1m'],'1month '+contract_1m+' total_put_oi','color=darkgreen'],
                ],
                [[t,data_dict['total_put_oi_1m']-data_dict['total_call_oi_1m'],'1month '+contract_1m+' total_put_oi - total_call_oi','style=vbar'],],''],

                [[[t,data_dict['total_call_oi_1q'],'1quarter '+contract_1q+' total_call_oi','color=red'],
                [t,data_dict['total_put_oi_1q'],'1quarter '+contract_1q+' total_put_oi','color=darkgreen'],
                ],
                [[t,data_dict['total_put_oi_1q']-data_dict['total_call_oi_1q'],'1quarter '+contract_1q+' total_put_oi - total_call_oi','style=vbar'],],''],

                [[[t,data_dict['total_call_oi'],'total_call_oi','color=red'],
                [t,data_dict['total_put_oi'],'total_put_oi','color=darkgreen'],
                ],
                [[t,data_dict['total_put_oi']-data_dict['total_call_oi'],' total_put_oi - total_call_oi','style=vbar'],],''],

                [[[fut_t,fut_price,variety,'color=black']],[],''],

                [[[t,data_dict['total_put_volume']/data_dict['total_call_volume'],' total_put_volume / total_call_volume lhs',''],
                ],
                [[t,data_dict['total_put_oi']/data_dict['total_call_oi'],' total_put_oi / total_call_oi rhs',''],],''],
                ]
        plot_many_figure(datas, max_height=1000)


if __name__=="__main__":
    update_deribit_future_data()
    update_deribit_option_data()
    update_deribit_option_info_detail()

    plot_deribit_option_datas()


