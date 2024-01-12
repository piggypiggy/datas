import os
import requests
import pandas as pd
import datetime
import numpy as np
from utils import *
import json
from deribit import crypto_put_call_delta_volatility


# ETF 期权数据
NASDAQ_ETF_OPTION_DICT = {
    'GLD': 'SPDR追踪金价ETF',
    'SLV': '白银ETF-iShares',
    'TLT': 'iShares 20+ Year Treasury Bond ETF',
    'USO': '美国石油指数基金',
    'XLE': '能源ETF-SPDR (主要是石油行业)',
    'XOP': '油气开采',
    'XLY': 'SPDR可选消费品ETF',
    'QQQ': '纳斯达克100ETF-ProShares',
    'SPY': '标普500ETF-SPDR',
    'DIA': 'SPDR追踪道指ETF',
    'IWM': '罗素2000指数ETF-iShares',
    'KWEB': 'KraneShares中概互联网指数ETF',
    'LQD': 'iShares iBoxx投资级公司债',
    'HYG': 'iShares iBoxx高收益公司债',
    'EEM': 'iShares MSCI Emerging Markets ETF',
    'FXI': 'iShares China Large-Cap ETF, 富时25中国',
    'VXX': '短期期货恐慌指数ETF',
}

# ETF 历史价格数据
NASDAQ_ETF_DICT = {
    'XLE': '能源ETF-SPDR (主要是石油行业)',
    'XOP': '油气开采',
    'USO': '美国石油指数基金',
    'QQQ': '纳斯达克100ETF-ProShares',
    'SPY': '标普500ETF-SPDR',
    'DIA': 'SPDR追踪道指ETF',
    'IWM': '罗素2000指数ETF-iShares',
    'KWEB': 'KraneShares中概互联网指数ETF',
    'VXX': '短期期货恐慌指数ETF',

    'BIL': 'SPDR Bloomberg 1-3 Month T-Bill ETF',
    'SHY': 'iShares 1-3 Year Treasury Bond ETF',
    'IEF': 'iShares 7-10 Year Treasury Bond ETF',
    'TLT': 'iShares 20+ Year Treasury Bond ETF',
    'GOVT': 'iShares U.S. Treasury Bond ETF',
    'LQD': 'iShares iBoxx投资级公司债',
    'HYG': 'iShares iBoxx高收益公司债',
    'TIP': '通胀债券指数ETF',

    'GLD': 'SPDR追踪金价ETF',
    'SLV': '白银ETF-iShares',

    'EEM': 'iShares MSCI Emerging Markets ETF',
    # 'VWO': 'Vanguard FTSE Emerging Markets ETF',
    'VNM': '越南',
    'INDA': '印度',
    'EWJ': 'iShares MSCI Japan ETF',
    'FXI': 'iShares China Large-Cap ETF, 富时25中国',

    'XBI': 'SPDR生物科技ETF',
    'XLY': 'SPDR可选消费品ETF',
    'XLP': 'SPDR必需消费品ETF',
    'XLF': 'SPDR金融ETF',
    'XLV': 'SPDR医疗保健ETF',
    'XLI': 'SPDR工业ETF',
    'XLB': 'SPDR原材料ETF',
    'XLC': 'SPDR通信服务ETF',
    'XLK': 'SPDR科技ETF',
    'XLU': 'SPDR公用事业ETF',
    'XLRE': 'SPDR房地产ETF',
    'SOXX': 'iShares Semiconductor ETF',
    'SMH': 'VanEck Semiconductor ETF',

    'EMB': 'iShares J.P. Morgan USD Emerging Markets Bond ETF',
    'EMLC': 'VanEck J.P. Morgan EM Local Currency Bond ETF, 新兴市场本币主权债ETF',

    # BTC
    'ARKB': 'ARK 21Shares Bitcoin ETF',
    'BITB': 'Bitwise Bitcoin ETP Trust',
    'FBTC': 'Fidelity Wise Origin Bitcoin Trust',
    'EZBC': 'Franklin Bitcoin ETF',
    'GBTC': 'Grayscale Bitcoin Trust',
    'DEFI': 'Hashdex Bitcoin ETF',
    'BTCO': 'Invesco Galaxy Bitcoin ETF',
    'IBIT': 'iShares Bitcoin Trust',
    'BRRR': 'Valkyrie Bitcoin Fund',
    'HODL': 'VanEck Bitcoin Trust',
    'BTCW': 'WisdomTree Bitcoin Trust',

}


headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Host": "api.nasdaq.com",
    'Sec-Fetch-Dest': 'document',
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36",
}

def keep_str(s):
    return s

def parse_last_trade(se, variety, last_trade):
    tmp = last_trade.split('$')[1].split('(')

    price = tmp[0].strip(' ')

    tmp = tmp[1].split('AS OF ')[1].split(' ET')[0].split(', ')
    md = tmp[0].split(' ')
    month = int(month_dict[md[0]])
    day = int(md[1])

    yhm = tmp[1].split(' ')
    if len(yhm) == 3:
        year = int(yhm[0])
        hour = int(yhm[1].split(':')[0])
        minute = int(yhm[1].split(':')[1])

        if yhm[2] == 'PM':
            if hour != 12:
                hour = hour + 12
    else:
        URL = 'https://api.nasdaq.com/api/quote/{}/info?assetclass=etf'
        url = URL.format(variety)
        while (1):
            try:
                r = se.get(url, verify=False, headers=headers)
                z = json.loads(r.content)

                if (z['data']['marketStatus']) == 'Closed':
                    t = z['data']['primaryData']['lastTradeTimestamp']
                    tmp = t.split(', ')
                    month = int(month_dict[tmp[0].split(' ')[0].upper()])
                    day = int(tmp[0].split(' ')[1])
                    year = int(tmp[1])
                    hour = 16
                    minute = 0
                if (z['data']['marketStatus']) == 'Pre-Market':
                    try:
                        t = z['data']['secondaryData']['lastTradeTimestamp']
                        if 'Closed at ' in t:
                            t = t.replace('Closed at ', '')
                            tmp = t.split(', ')
                            month = int(month_dict[tmp[0].split(' ')[0].upper()])
                            day = int(tmp[0].split(' ')[1])
                            tmp = tmp[1].split(' ')
                            year = int(tmp[0])
                            hour = int(tmp[1].split(':')[0]) + 12 # PM
                            minute = 0
                    except:
                        t = z['data']['primaryData']['lastTradeTimestamp']
                        tmp = t.split(', ')
                        month = int(month_dict[tmp[0].split(' ')[0].upper()])
                        day = int(tmp[0].split(' ')[1])
                        year = int(tmp[1])
                        hour = 16
                        minute = 0
                break
            except:
                print('ERROR')
                time.sleep(5)
                pass

    time_dt = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=0)

    time_str = time_dt.strftime('%Y-%m-%d %H:%M:%S')
    return price, time_dt, time_str

def update_nasdaq_etf_option_data(variety):
    # example
    # URL = 'https://api.nasdaq.com/api/quote/XLE/option-chain?assetclass=etf&limit=60&offset=60&fromdate=2023-08-11&todate=2023-08-25&excode=oprac&callput=callput&money=at&type=all'

    PRICE_URL = 'https://api.nasdaq.com/api/quote/{}/option-chain?assetclass=etf&fromdate={}&todate={}&excode=oprac&callput=callput&money=at&type=all'
    GREEKS_URL = 'https://api.nasdaq.com/api/quote/{}/option-chain/greeks?assetclass=etf&date={}'

    se = requests.session()

    start_time_dt = datetime.datetime.now()
    # +3 month
    if start_time_dt.month <= 9:
        year = start_time_dt.year
        month = start_time_dt.month+3
        day = calendar.monthrange(year, month)[-1]
        end_time_dt = datetime.datetime(year=year, month=month, day=day)
    else:
        year = start_time_dt.year+1
        month = start_time_dt.month-9
        day = calendar.monthrange(year, month)[-1]
        end_time_dt = datetime.datetime(year=year, month=month, day=day)

    start_time = start_time_dt.strftime('%Y-%m-%d')
    end_time = end_time_dt.strftime('%Y-%m-%d')
    url = PRICE_URL.format(variety, start_time, end_time)

    expiry_dict = {}
    cookies = None
    while (1):
        try:
            if cookies is None:
                r = se.get(url, verify=False, headers=headers, timeout=15)
            else:
                r = se.get(url, verify=False, cookies=cookies, headers=headers, timeout=15)
            z = json.loads(r.content)
            if cookies is None:
                cookies = r.cookies
            last_trade = z['data']['lastTrade']
            print(last_trade)
            price, time_dt, time_str = parse_last_trade(se, variety, last_trade)
            print(price, time_dt, time_str)
            hour = time_dt.hour
            minute = time_dt.minute

            datas = z['data']['table']['rows']
            for i in range(len(datas)):
                data = datas[i]
                s = data['expirygroup']
                if s != '':
                    continue

                s = data['drillDownURL'].split('/')
                s = s[len(s)-1][6:12]
                # key example XLE230825
                key = variety + s
                if not(key in expiry_dict):
                    expiry_dict[key] = {}
                    expiry_dict[key+'volume'] = 0

                # [option price last, volume, oi]
                expiry_dict[key]['C'+data['strike']] = [data['c_Last'], data['c_Volume'], data['c_Openinterest'], None, None, None, None, None, None]
                expiry_dict[key]['P'+data['strike']] = [data['p_Last'], data['p_Volume'], data['p_Openinterest'], None, None, None, None, None, None]
                if data['c_Volume'] == '--':
                    pass
                else:
                    expiry_dict[key+'volume'] = expiry_dict[key+'volume'] + float(data['c_Volume'])

                if data['p_Volume'] == '--':
                    pass
                else:
                    expiry_dict[key+'volume'] = expiry_dict[key+'volume'] + float(data['p_Volume'])

            print('PRICE ' + key)
            break
        except Exception as e:
            print(e, 'NASDAQ PRICE URL ERROR ' + variety)
            time.sleep(15)

    # PRICE
    path = os.path.join(option_price_dir, 'nasdaq', variety+'.csv')
    df = pd.DataFrame(columns=['time', 'price'], data=[[time_str, price]])
    if os.path.exists(path):
        old_df = pd.read_csv(path)
        old_df = pd.concat([old_df, df], axis=0)
        old_df.drop_duplicates(subset=['time'], keep='last', inplace=True) # last
        old_df['time'] = pd.to_datetime(old_df['time'], format='%Y-%m-%d %H:%M:%S')
        old_df.sort_values(by='time', axis=0, ascending=True, inplace=True)
        old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
        old_df.to_csv(path, encoding='utf-8', index=False)
    else:
        df.to_csv(path, encoding='utf-8', index=False)

    # GREEKS
    if (hour == 16 and minute == 0):
        for key in expiry_dict:
            if 'volume' in key:
                continue
            t = key.replace(variety, '')
            t = '20'+t

            t_str = pd.to_datetime(t, format='%Y%m%d').strftime('%Y-%m-%d')
            url = GREEKS_URL.format(variety, t_str)

            while (1):
                try:
                    r = se.get(url, verify=False, headers=headers, timeout=15)
                    z = json.loads(r.content, parse_float=keep_str)
                    datas = z['data']['table']['rows']
                    for i in range(len(datas)):
                        data = datas[i]
                        strike = data['strike']

                        d = expiry_dict[key]
                        if ('C'+strike) in d:
                            d['C'+strike][3] = data['cDelta']
                            d['C'+strike][4] = data['cIV']
                            d['C'+strike][5] = data['cGamma']
                            d['C'+strike][6] = data['cRho']
                            d['C'+strike][7] = data['cTheta']
                            d['C'+strike][8] = data['cVega']

                        if ('P'+strike) in d:
                            d['P'+strike][3] = data['pDelta']
                            d['P'+strike][4] = data['pIV']
                            d['P'+strike][5] = data['pGamma']
                            d['P'+strike][6] = data['pRho']
                            d['P'+strike][7] = data['pTheta']
                            d['P'+strike][8] = data['pVega']

                    print('GREEKS ' + key)
                    break
                except Exception as e:
                    print(e, 'NASDAQ GREEKS URL ERROR ' + variety)
                    time.sleep(15)

    inst_ids_list = []
    volume_list = []
    col3_add = ['close', 'volume', 'oi', 'delta', 'imp_vol', 'gamma', 'rho', 'theta', 'vega']
    for key in expiry_dict:
        if 'volume' in key:
            continue
        inst_ids_list.append(key)
        volume_list.append(expiry_dict[key+'volume'])

        d = expiry_dict[key]
        col1 = ['time']
        col2 = ['time']
        col3 = ['time']
        datas = [time_str]
        for h in d:
            opt_type = h[0]
            strike = h[1:]
            col1 += [opt_type for _ in range(len(col3_add))]
            col2 += [strike for _ in range(len(col3_add))]
            col3 += col3_add
            datas += d[h]

        df = pd.DataFrame(columns=[col1, col2, col3], data=[datas])
        df.replace('--', '', inplace=True)

        path = os.path.join(option_price_dir, 'nasdaq', key+'.csv')
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

    # INFO
    inst_ids_list = np.array(inst_ids_list, dtype=str)
    volume_list = np.array(volume_list, dtype=float)
    sort = np.argsort(volume_list)[::-1]
    inst_ids = ''
    for i in range(len(volume_list)):
        inst_ids += inst_ids_list[sort[i]]
        inst_ids += ','

    info_df = pd.DataFrame(columns=['time', 'inst_ids'], data=[[time_str, inst_ids]])
    path = os.path.join(option_price_dir, 'nasdaq', variety+'_info'+'.csv')
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


def update_nasdaq_etf_option_info_detail(variety):
    print(variety + ' info detail')
    info_path = os.path.join(option_price_dir, 'nasdaq', variety+'_info'+'.csv')
    info_df = pd.read_csv(info_path)
    info_t = pd.DatetimeIndex(pd.to_datetime(info_df['time'], format='%Y-%m-%d %H:%M:%S'))

    earlist_time = '2023-07-22 00:00:00'
    path = os.path.join(option_price_dir, 'nasdaq', variety+'_info_detail'+'.csv')
    last_line_time = get_last_line_time(path, variety+'_info_detail', earlist_time, 19, '%Y-%m-%d %H:%M:%S')
    last_line_time_dt = pd.to_datetime(last_line_time, format='%Y-%m-%d %H:%M:%S')
    if (last_line_time == earlist_time):
        start_idx = 0
    else:
        start_idx = np.where(info_t == last_line_time_dt)[0][0] + 1

    etf_path = os.path.join(option_price_dir, 'nasdaq', variety+'.csv')
    etf_df = pd.read_csv(etf_path)
    etf_t = pd.DatetimeIndex(pd.to_datetime(etf_df['time'], format='%Y-%m-%d %H:%M:%S'))
    etf_price = np.array(etf_df['price'], dtype=float)

    opt_dict = {}
    COL = ['inst_id', 'total_put_volume', 'total_call_volume', 'total_put_oi', 'total_call_oi', 
           'atm_put_iv', 'atm_call_iv', '25d_put_iv', '25d_call_iv', 
           '10d_put_iv', '10d_call_iv', '5d_put_iv', '5d_call_iv',
           'put_volume_max1', 'put_volume_max1_strike', 'put_volume_max2', 'put_volume_max2_strike',
           'put_volume_max3', 'put_volume_max3_strike', 'put_volume_max4', 'put_volume_max4_strike',
           'put_volume_max5', 'put_volume_max5_strike',
           'call_volume_max1', 'call_volume_max1_strike', 'call_volume_max2', 'call_volume_max2_strike',
           'call_volume_max3', 'call_volume_max3_strike', 'call_volume_max4', 'call_volume_max4_strike',
           'call_volume_max5', 'call_volume_max5_strike',
           'put_oi_max1', 'put_oi_max1_strike', 'put_oi_max2', 'put_oi_max2_strike',
           'put_oi_max3', 'put_oi_max3_strike', 'put_oi_max4', 'put_oi_max4_strike',
           'put_oi_max5', 'put_oi_max5_strike',
           'call_oi_max1', 'call_oi_max1_strike', 'call_oi_max2', 'call_oi_max2_strike',
           'call_oi_max3', 'call_oi_max3_strike', 'call_oi_max4', 'call_oi_max4_strike',
           'call_oi_max5', 'call_oi_max5_strike',]
    
    df = pd.DataFrame()
    for i in range(start_idx, len(info_t)):
        t = info_t[i]
        inst_ids = info_df.loc[i, 'inst_ids'].split(',')
        inst_ids.remove('')

        n = 0
        cols = ['time', 'total_put_volume', 'total_call_volume', 'total_put_oi', 'total_call_oi']
        for inst_id in inst_ids:
            n = n + 1
            cn = 'c'+str(n)
            cols += [c+'_'+cn for c in COL]

            if not(inst_id in opt_dict):
                opt_path = os.path.join(option_price_dir, 'nasdaq', inst_id+'.csv')
                opt_df = pd.read_csv(opt_path, header=[0,1,2])
                opt_t = pd.DatetimeIndex(pd.to_datetime(opt_df['time']['time']['time'], format='%Y-%m-%d %H:%M:%S'))
                strike = get_full_strike_price(opt_df)
                opt_dict[inst_id] = [opt_t, opt_df, strike]
    
        _df = pd.DataFrame(columns=cols)
        _df.loc[i, 'time'] = t.strftime('%Y-%m-%d %H:%M:%S')
        _df.loc[i, 'total_put_volume'] = 0
        _df.loc[i, 'total_call_volume'] = 0
        _df.loc[i, 'total_put_oi'] = 0
        _df.loc[i, 'total_call_oi'] = 0

        n = 0
        for inst_id in inst_ids:
            n = n + 1
            cn = 'c'+str(n)
            temp_t = opt_dict[inst_id][0]
            try:
                w = np.where(temp_t == t)[0][0]
            except:
                continue
            temp_df = opt_dict[inst_id][1].loc[w,:]
            strike = opt_dict[inst_id][2]

            put_volume = temp_df.loc[pd.IndexSlice['P', :, 'volume']].sum()
            call_volume = temp_df.loc[pd.IndexSlice['C', :, 'volume']].sum()
            _df.loc[i, 'total_put_volume_'+cn] = put_volume
            _df.loc[i, 'total_call_volume_'+cn] = call_volume
            _df.loc[i, 'total_put_volume'] = _df.loc[i, 'total_put_volume'] + put_volume
            _df.loc[i, 'total_call_volume'] = _df.loc[i, 'total_call_volume'] + call_volume

            put_oi = temp_df.loc[pd.IndexSlice['P', :, 'oi']].sum()
            call_oi = temp_df.loc[pd.IndexSlice['C', :, 'oi']].sum()
            _df.loc[i, 'total_put_oi_'+cn] = put_oi
            _df.loc[i, 'total_call_oi_'+cn] = call_oi
            _df.loc[i, 'total_put_oi'] = _df.loc[i, 'total_put_oi'] + put_oi
            _df.loc[i, 'total_call_oi'] = _df.loc[i, 'total_call_oi'] + call_oi

            ## put volume sort
            tmp = temp_df.loc[pd.IndexSlice['P', :, 'volume']]
            P_volume = tmp.replace(np.nan, -1.0)
            idx = P_volume.index
            P_volume = np.array(P_volume)
            sort = np.argsort(P_volume)
            P_volume = P_volume[sort]
            idx = idx[sort]
            if len(idx) >= 5:
                _df.loc[i, 'put_volume_max1_'+cn] = P_volume[-1]
                _df.loc[i, 'put_volume_max1_strike_'+cn] = idx[-1]
                _df.loc[i, 'put_volume_max2_'+cn] = P_volume[-2]
                _df.loc[i, 'put_volume_max2_strike_'+cn] = idx[-2]
                _df.loc[i, 'put_volume_max3_'+cn] = P_volume[-3]
                _df.loc[i, 'put_volume_max3_strike_'+cn] = idx[-3]
                _df.loc[i, 'put_volume_max4_'+cn] = P_volume[-4]
                _df.loc[i, 'put_volume_max4_strike_'+cn] = idx[-4]
                _df.loc[i, 'put_volume_max5_'+cn] = P_volume[-5]
                _df.loc[i, 'put_volume_max5_strike_'+cn] = idx[-5]
            ## call volume sort
            tmp = temp_df.loc[pd.IndexSlice['C', :, 'volume']]
            C_volume = tmp.replace(np.nan, -1.0)
            idx = C_volume.index
            C_volume = np.array(C_volume)
            sort = np.argsort(C_volume)
            C_volume = C_volume[sort]
            idx = idx[sort]
            if len(idx) >= 5:
                _df.loc[i, 'call_volume_max1_'+cn] = C_volume[-1]
                _df.loc[i, 'call_volume_max1_strike_'+cn] = idx[-1]
                _df.loc[i, 'call_volume_max2_'+cn] = C_volume[-2]
                _df.loc[i, 'call_volume_max2_strike_'+cn] = idx[-2]
                _df.loc[i, 'call_volume_max3_'+cn] = C_volume[-3]
                _df.loc[i, 'call_volume_max3_strike_'+cn] = idx[-3]
                _df.loc[i, 'call_volume_max4_'+cn] = C_volume[-4]
                _df.loc[i, 'call_volume_max4_strike_'+cn] = idx[-4]
                _df.loc[i, 'call_volume_max5_'+cn] = C_volume[-5]
                _df.loc[i, 'call_volume_max5_strike_'+cn] = idx[-5]


            ## put oi sort
            tmp = temp_df.loc[pd.IndexSlice['P', :, 'oi']]
            P_oi = tmp.replace(np.nan, -1.0)
            idx = P_oi.index
            P_oi = np.array(P_oi)
            sort = np.argsort(P_oi)
            P_oi = P_oi[sort]
            idx = idx[sort]
            if len(idx) >= 5:
                _df.loc[i, 'put_oi_max1_'+cn] = P_oi[-1]
                _df.loc[i, 'put_oi_max1_strike_'+cn] = idx[-1]
                _df.loc[i, 'put_oi_max2_'+cn] = P_oi[-2]
                _df.loc[i, 'put_oi_max2_strike_'+cn] = idx[-2]
                _df.loc[i, 'put_oi_max3_'+cn] = P_oi[-3]
                _df.loc[i, 'put_oi_max3_strike_'+cn] = idx[-3]
                _df.loc[i, 'put_oi_max4_'+cn] = P_oi[-4]
                _df.loc[i, 'put_oi_max4_strike_'+cn] = idx[-4]
                _df.loc[i, 'put_oi_max5_'+cn] = P_oi[-5]
                _df.loc[i, 'put_oi_max5_strike_'+cn] = idx[-5]
            ## call oi sort
            tmp = temp_df.loc[pd.IndexSlice['C', :, 'oi']]
            C_oi = tmp.replace(np.nan, -1.0)
            idx = C_oi.index
            C_oi = np.array(C_oi)
            sort = np.argsort(C_oi)
            C_oi = C_oi[sort]
            idx = idx[sort]
            # print(idx)
            # print(C_oi)
            if len(idx) >= 5:
                _df.loc[i, 'call_oi_max1_'+cn] = C_oi[-1]
                _df.loc[i, 'call_oi_max1_strike_'+cn] = idx[-1]
                _df.loc[i, 'call_oi_max2_'+cn] = C_oi[-2]
                _df.loc[i, 'call_oi_max2_strike_'+cn] = idx[-2]
                _df.loc[i, 'call_oi_max3_'+cn] = C_oi[-3]
                _df.loc[i, 'call_oi_max3_strike_'+cn] = idx[-3]
                _df.loc[i, 'call_oi_max4_'+cn] = C_oi[-4]
                _df.loc[i, 'call_oi_max4_strike_'+cn] = idx[-4]
                _df.loc[i, 'call_oi_max5_'+cn] = C_oi[-5]
                _df.loc[i, 'call_oi_max5_strike_'+cn] = idx[-5]

            w = np.where(etf_t == t)[0][0]
            price = etf_price[w]

            _df.loc[i, 'inst_id_'+cn] = inst_id
            # if (np.isnan(temp_df.loc[pd.IndexSlice['P', :, 'delta']].tolist()[0])):
            #     _df.loc[i, '25d_put_iv_'+cn] = np.nan
            #     _df.loc[i, '25d_call_iv_'+cn] = np.nan
            #     _df.loc[i, 'atm_put_iv_'+cn] = np.nan
            #     _df.loc[i, 'atm_call_iv_'+cn] = np.nan
            #     _df.loc[i, '10d_put_iv_'+cn] = np.nan
            #     _df.loc[i, '10d_call_iv_'+cn] = np.nan
            #     _df.loc[i, '5d_put_iv_'+cn] = np.nan
            #     _df.loc[i, '5d_call_iv_'+cn] = np.nan
            # else:
            ret = crypto_put_call_delta_volatility(temp_df, 0.25, price, strike, strike)
            _df.loc[i, '25d_put_iv_'+cn] = ret[1]
            _df.loc[i, '25d_call_iv_'+cn] = ret[3]
            _df.loc[i, 'atm_put_iv_'+cn] = ret[4]
            _df.loc[i, 'atm_call_iv_'+cn] = ret[5]

            ret = crypto_put_call_delta_volatility(temp_df, 0.1, price, strike, strike)
            _df.loc[i, '10d_put_iv_'+cn] = ret[1]
            _df.loc[i, '10d_call_iv_'+cn] = ret[3]

            ret = crypto_put_call_delta_volatility(temp_df, 0.05, price, strike, strike)
            _df.loc[i, '5d_put_iv_'+cn] = ret[1]
            _df.loc[i, '5d_call_iv_'+cn] = ret[3]

        df = pd.concat([df, _df], axis=0)

    if (len(df) > 0):
        path = os.path.join(option_price_dir, 'nasdaq', variety+'_info_detail'+'.csv')
        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
            old_df.sort_values(by = 'time', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df.to_csv(path, encoding='utf-8', index=False)


def plot_nasdaq_option_data(t, df, data_dict, etf_t, etf_price, variety, cn):
    contract = data_dict[cn][-1]

    put_oi_max1 = np.array(df['put_oi_max1_'+cn], dtype=float)
    put_oi_max2 = np.array(df['put_oi_max2_'+cn], dtype=float)
    put_oi_max3 = np.array(df['put_oi_max3_'+cn], dtype=float)
    put_oi_max4 = np.array(df['put_oi_max4_'+cn], dtype=float)
    put_oi_max5 = np.array(df['put_oi_max5_'+cn], dtype=float)
    call_oi_max1 = np.array(df['call_oi_max1_'+cn], dtype=float)
    call_oi_max2 = np.array(df['call_oi_max2_'+cn], dtype=float)
    call_oi_max3 = np.array(df['call_oi_max3_'+cn], dtype=float)
    call_oi_max4 = np.array(df['call_oi_max4_'+cn], dtype=float)
    call_oi_max5 = np.array(df['call_oi_max5_'+cn], dtype=float)

    put_oi_max1_strike = np.array(df['put_oi_max1_strike_'+cn], dtype=float)
    put_oi_max2_strike = np.array(df['put_oi_max2_strike_'+cn], dtype=float)
    put_oi_max3_strike = np.array(df['put_oi_max3_strike_'+cn], dtype=float)
    put_oi_max4_strike = np.array(df['put_oi_max4_strike_'+cn], dtype=float)
    put_oi_max5_strike = np.array(df['put_oi_max5_strike_'+cn], dtype=float)
    call_oi_max1_strike = np.array(df['call_oi_max1_strike_'+cn], dtype=float)
    call_oi_max2_strike = np.array(df['call_oi_max2_strike_'+cn], dtype=float)
    call_oi_max3_strike = np.array(df['call_oi_max3_strike_'+cn], dtype=float)
    call_oi_max4_strike = np.array(df['call_oi_max4_strike_'+cn], dtype=float)
    call_oi_max5_strike = np.array(df['call_oi_max5_strike_'+cn], dtype=float)

    put_oi_max = np.vstack((put_oi_max1, put_oi_max2, put_oi_max3, put_oi_max4, put_oi_max5))
    put_oi_max_strike = np.vstack((put_oi_max1_strike, put_oi_max2_strike, put_oi_max3_strike, put_oi_max4_strike, put_oi_max5_strike))
    sort = np.argsort(put_oi_max_strike, axis=0)
    put_oi_max_strike = np.take_along_axis(put_oi_max_strike, sort, axis=0)
    put_oi_max = np.take_along_axis(put_oi_max, sort, axis=0)
    put_oi_avg_strike = np.sum(put_oi_max[2:5, :]*put_oi_max_strike[2:5, :], axis=0) / np.sum(put_oi_max[2:5, :], axis=0)

    call_oi_max = np.vstack((call_oi_max1, call_oi_max2, call_oi_max3, call_oi_max4, call_oi_max5))
    call_oi_max_strike = np.vstack((call_oi_max1_strike, call_oi_max2_strike, call_oi_max3_strike, call_oi_max4_strike, call_oi_max5_strike))
    sort = np.argsort(call_oi_max_strike, axis=0)
    call_oi_max_strike = np.take_along_axis(call_oi_max_strike, sort, axis=0)
    call_oi_max = np.take_along_axis(call_oi_max, sort, axis=0)
    call_oi_avg_strike = np.sum(call_oi_max[0:3, :]*call_oi_max_strike[0:3, :], axis=0) / np.sum(call_oi_max[0:3, :], axis=0)

    put_oi_max = np.array(df['put_oi_max1_'+cn], dtype=float)
    w = np.where(put_oi_max > 0)[0]

    datas = [
            [[[t,data_dict['5d_put_iv_'+cn],cn+' '+contract+' 5d_put_iv','color=darkgreen'],
                [t,data_dict['5d_call_iv_'+cn],cn+' '+contract+' 5d_call_iv','color=red'],
            ],
            [],''],            

            [[[t,data_dict['10d_put_iv_'+cn],cn+' '+contract+' 10d_put_iv','color=darkgreen'],
                [t,data_dict['10d_call_iv_'+cn],cn+' '+contract+' 10d_call_iv','color=red'],
            ],
            [],''],                

            [[
                [t,data_dict['5d_put_iv_'+cn]-data_dict['5d_call_iv_'+cn],cn+' '+contract+'  5d_skew',''],
                [t,data_dict['10d_put_iv_'+cn]-data_dict['10d_call_iv_'+cn],cn+' '+contract+' 10d_skew',''],
                [t,data_dict['25d_put_iv_'+cn]-data_dict['25d_call_iv_'+cn],cn+' '+contract+' 25d_skew',''], 
                [t,data_dict['atm_put_iv_'+cn]-data_dict['atm_call_iv_'+cn],cn+' '+contract+' atm_skew',''],
                ],
            [],''],

            [[[etf_t,etf_price,variety,'color=black, width=4'],
                [t[w],call_oi_max_strike[0,:][w],'call 持仓量一','color=red'],
                [t[w],call_oi_max_strike[1,:][w],'call 持仓量二','color=orange'],
                [t[w],call_oi_avg_strike[w],'加权平均','color=darkgray'],
                [t[w],put_oi_max_strike[4,:][w],'put 持仓量一','color=darkgreen'],
                [t[w],put_oi_max_strike[3,:][w],'put 持仓量二','color=purple'],
                [t[w],put_oi_avg_strike[w],'加权平均','color=darkgray'],],[],''],

            [[[t,data_dict['25d_put_iv_'+cn],cn+' '+contract+' 25d_put_iv','color=darkgreen'],
                [t,data_dict['25d_call_iv_'+cn],cn+' '+contract+' 25d_call_iv','color=red'],
            ],
            [],''],

            [[[t,data_dict['atm_put_iv_'+cn],cn+' '+contract+' atm_put_iv','color=darkgreen'],
                [t,data_dict['atm_call_iv_'+cn],cn+' '+contract+' atm_call_iv','color=red'],
            ],
            [],''],
            ]
    plot_many_figure(datas, max_height=900)


def get_nasdaq_lastday_option_strike_volume_oi(exchange, variety, inst_id):
    path = os.path.join(option_price_dir, exchange, inst_id+'.csv')
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

    i = L-2
    while (i >= 0):
        hour = t[i].hour
        if hour == 16:
            t1 = t[i]
            break
        i = i - 1

    for strike_str in strikes_str:
        strike.append(float(strike_str))
        put_oi.append(df.loc[i, pd.IndexSlice['P', strike_str, 'oi']])
        call_oi.append(df.loc[i, pd.IndexSlice['C', strike_str, 'oi']])
      

    strike = np.array(strike, dtype=float)
    put_oi = np.array(put_oi, dtype=float)
    call_oi = np.array(call_oi, dtype=float)
  
    path = os.path.join(option_price_dir, exchange, variety+'.csv')
    if not os.path.exists(path):
        return
    price_df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(price_df['time'], format='%Y-%m-%d %H:%M:%S'))
    idx = np.where(t == t1)[0]

    if len(idx) > 0:
        price = price_df.loc[idx, 'price']
    else:
        price = None

    return strike, put_oi, call_oi, price


def plot_nasdaq_intraday_option_strike_volume_oi(exchange, variety):
    exchange = 'nasdaq'
    path = os.path.join(option_price_dir, exchange, variety+'_info'+'.csv')
    if not os.path.exists(path):
        return
    df = pd.read_csv(path)
    inst_ids = df.loc[len(df)-1, 'inst_ids'].split(',')

    # dom1, dom2
    for inst_id in [inst_ids[0], inst_ids[1], inst_ids[2]]:
        path = os.path.join(option_price_dir, exchange, inst_id+'.csv')
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

        path = os.path.join(option_price_dir, exchange, variety+'.csv')
        if not os.path.exists(path):
            return
        price_df = pd.read_csv(path)
        etf_price = price_df.loc[len(price_df)-1, 'price']

        strike_last_day, put_oi_last_day, call_oi_last_day, etf_price_last_day = get_nasdaq_lastday_option_strike_volume_oi(exchange, variety, inst_id)
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
        gap = float((strike_sort[len(strike_sort)//2]-strike_sort[len(strike_sort)//2-1]) / 5)
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


def plot_nasdaq_option_datas(variety):
    path = os.path.join(option_price_dir, 'nasdaq', variety+'_info_detail'+'.csv')
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S'))

    cols = df.columns.tolist()
    cols.remove('time')
    data_dict = {}
    for col in cols:
        if 'inst_id' in col:
            data_dict[col[8:]] = np.array(df[col], dtype=str)
        else:
            data_dict[col] = np.array(df[col], dtype=float)

        # if 'oi' in col:
        #     tmp = data_dict[col]
        #     tmp[tmp == 0] = np.nan
        #     data_dict[col] = tmp

    etf_path = os.path.join(option_price_dir, 'nasdaq', variety+'.csv')
    etf_df = pd.read_csv(etf_path)
    etf_t = pd.DatetimeIndex(pd.to_datetime(etf_df['time'], format='%Y-%m-%d %H:%M:%S'))
    etf_price = np.array(etf_df['price'], dtype=float)

    # plot_nasdaq_option_data(t, df, data_dict, etf_t, etf_price, variety, 'c1')
    # plot_nasdaq_option_data(t, df, data_dict, etf_t, etf_price, variety, 'c2')
    # plot_nasdaq_option_data(t, df, data_dict, etf_t, etf_price, variety, 'c3')
    # plot_nasdaq_option_data(t, df, data_dict, etf_t, etf_price, variety, 'c4')
    # plot_nasdaq_option_data(t, df, data_dict, etf_t, etf_price, variety, 'c5')
    # plot_nasdaq_option_data(t, df, data_dict, etf_t, etf_price, variety, 'c6')

    contract_c1 = data_dict['c1'][-1]
    contract_c2 = data_dict['c2'][-1]
    contract_c3 = data_dict['c3'][-1]
    contract_c4 = data_dict['c4'][-1]
    contract_c5 = data_dict['c5'][-1]
    contract_c6 = data_dict['c6'][-1]

    datas = [
            [[[t,data_dict['total_call_volume_c1'],'c1 '+contract_c1+' total_call_volume','color=red'],
            [t,data_dict['total_put_volume_c1'],'c1 '+contract_c1+' total_put_volume','color=darkgreen'],
            ],
            [[t,data_dict['total_put_volume_c1']-data_dict['total_call_volume_c1'],'c1 '+contract_c1+' total_put_volume - total_call_volume','style=vbar'],],''],

            [[[t,data_dict['total_call_volume_c2'],'c2 '+contract_c2+' total_call_volume','color=red'],
            [t,data_dict['total_put_volume_c2'],'c2 '+contract_c2+' total_put_volume','color=darkgreen'],
            ],
            [[t,data_dict['total_put_volume_c2']-data_dict['total_call_volume_c2'],'c2 '+contract_c2+' total_put_volume - total_call_volume','style=vbar'],],''],

            [[[t,data_dict['total_call_volume_c3'],'c3 '+contract_c3+' total_call_volume','color=red'],
            [t,data_dict['total_put_volume_c3'],'c3 '+contract_c3+' total_put_volume','color=darkgreen'],
            ],
            [[t,data_dict['total_put_volume_c3']-data_dict['total_call_volume_c3'],'c3 '+contract_c3+' total_put_volume - total_call_volume','style=vbar'],],''],

            [[[etf_t,etf_price,variety,'color=black']],
             [[t,data_dict['total_put_oi']/data_dict['total_call_oi'],' total_put_oi / total_call_oi',''],],''],

            [[[t,data_dict['total_call_volume'],'total_call_volume','color=red'],
            [t,data_dict['total_put_volume'],'total_put_volume','color=darkgreen'],
            ],
            [[t,data_dict['total_put_volume']-data_dict['total_call_volume'],'total_put_volume - total_call_volume','style=vbar'],],''],

            [[[t,data_dict['total_call_oi'],'total_call_oi','color=red'],
            [t,data_dict['total_put_oi'],'total_put_oi','color=darkgreen'],
            ],
            [[t,data_dict['total_put_oi']-data_dict['total_call_oi'],' total_put_oi - total_call_oi','style=vbar'],],''],

            [[[t,data_dict['25d_call_iv_c1'],'c1 '+contract_c1+' 25d_call_iv','color=red'],
            [t,data_dict['25d_put_iv_c1'],'c1 '+contract_c1+' 25d_put_iv','color=darkgreen'],
            ],
            [[t,data_dict['25d_put_iv_c1']-data_dict['25d_call_iv_c1'],'c1 '+contract_c1+' 25d_put_iv - 25d_call_iv','style=vbar'],],''],

            [[[t,data_dict['25d_call_iv_c2'],'c2 '+contract_c2+' 25d_call_iv','color=red'],
            [t,data_dict['25d_put_iv_c2'],'c2 '+contract_c2+' 25d_put_iv','color=darkgreen'],
            ],
            [[t,data_dict['25d_put_iv_c2']-data_dict['25d_call_iv_c2'],'c2 '+contract_c2+' 25d_put_iv - 25d_call_iv','style=vbar'],],''],

            [[[t,data_dict['total_call_volume_c4'],'c4 '+contract_c4+' total_call_volume','color=red'],
            [t,data_dict['total_put_volume_c4'],'c4 '+contract_c4+' total_put_volume','color=darkgreen'],
            ],
            [[t,data_dict['total_put_volume_c4']-data_dict['total_call_volume_c4'],'c4 '+contract_c4+' total_put_volume - total_call_volume','style=vbar'],],''],

            # [[[t,data_dict['total_call_volume_c5'],'c5 '+contract_c5+' total_call_volume','color=red'],
            # [t,data_dict['total_put_volume_c5'],'c5 '+contract_c5+' total_put_volume','color=darkgreen'],
            # ],
            # [[t,data_dict['total_put_volume_c5']-data_dict['total_call_volume_c5'],'c5 '+contract_c5+' total_put_volume - total_call_volume','style=vbar'],],''],

            # [[[t,data_dict['total_call_volume_c6'],'c6 '+contract_c6+' total_call_volume','color=red'],
            # [t,data_dict['total_put_volume_c6'],'c6 '+contract_c6+' total_put_volume','color=darkgreen'],
            # ],
            # [[t,data_dict['total_put_volume_c6']-data_dict['total_call_volume_c6'],'c6 '+contract_c6+' total_put_volume - total_call_volume','style=vbar'],],''],

            ]
    plot_many_figure(datas, max_height=1200)



    # datas = [
    #         [[[t,data_dict['total_call_oi_1m'],'1month '+contract_1m+' total_call_oi','color=red'],
    #         [t,data_dict['total_put_oi_1m'],'1month '+contract_1m+' total_put_oi','color=darkgreen'],
    #         ],
    #         [[t,data_dict['total_put_oi_1m']-data_dict['total_call_oi_1m'],'1month '+contract_1m+' total_put_oi - total_call_oi','style=vbar'],],''],

    #         [[[t,data_dict['total_call_oi_1q'],'1quarter '+contract_1q+' total_call_oi','color=red'],
    #         [t,data_dict['total_put_oi_1q'],'1quarter '+contract_1q+' total_put_oi','color=darkgreen'],
    #         ],
    #         [[t,data_dict['total_put_oi_1q']-data_dict['total_call_oi_1q'],'1quarter '+contract_1q+' total_put_oi - total_call_oi','style=vbar'],],''],

    #         [[[t,data_dict['total_call_oi'],'total_call_oi','color=red'],
    #         [t,data_dict['total_put_oi'],'total_put_oi','color=darkgreen'],
    #         ],
    #         [[t,data_dict['total_put_oi']-data_dict['total_call_oi'],' total_put_oi - total_call_oi','style=vbar'],],''],

    #         [[[etf_t,etf_price,variety,'color=black']],[],''],

    #         [[[t,data_dict['total_put_volume']/data_dict['total_call_volume'],' total_put_volume / total_call_volume lhs',''],
    #         ],
    #         [[t,data_dict['total_put_oi']/data_dict['total_call_oi'],' total_put_oi / total_call_oi rhs',''],],''],
    #         ]
    # plot_many_figure(datas, max_height=1000)


def update_all_nasdaq_etf_option_data():
    for variety in NASDAQ_ETF_OPTION_DICT:
        print(variety)
        update_nasdaq_etf_option_data(variety)

    for variety in NASDAQ_ETF_OPTION_DICT:
        update_nasdaq_etf_option_info_detail(variety)


def compute_total_dividend(etf_path, dividend_path):
    etf_df = pd.read_csv(etf_path)
    etf_t = pd.DatetimeIndex(pd.to_datetime(etf_df['time'], format='%Y-%m-%d'))

    plus = np.zeros(len(etf_t), dtype=float)
    if os.path.exists(dividend_path):
        try:
            div_df = pd.read_csv(dividend_path)
            div_t = pd.DatetimeIndex(pd.to_datetime(div_df['exOrEffDate'], format='%Y-%m-%d'))
            amount = np.array(div_df['amount'], dtype=float)
            div_t, amount = get_period_data(div_t, amount, start=etf_t[0].strftime('%Y-%m-%d'), end=etf_t[-1].strftime('%Y-%m-%d'))

            for i in range(len(div_t)):
                w = np.where(etf_t >= div_t[i])[0]
                plus[w] += amount[i]
        except:
            pass

    etf_df['div_plus'] = plus
    etf_df.to_csv(etf_path, encoding='utf-8', index=False)


def convert_time_format(x):
    try:
        y = pd.to_datetime(x, format='%m/%d/%Y')
        z = datetime.datetime.strftime(y,'%Y-%m-%d')
    except:
        z = None
    return z


def update_nasdaq_etf_dividend(variety, se=None):
    if se is None:
        se = requests.session()

    NASDAQ_ETF_DIVIDEND_URL = 'https://api.nasdaq.com/api/quote/{}/dividends?assetclass=etf'
    url = NASDAQ_ETF_DIVIDEND_URL.format(variety)
    print(variety + ' DIVIDEND')

    path = os.path.join(option_price_dir, 'nasdaq', variety+'_dividend'+'.csv')
    while (1):
        try:
            r = se.get(url, verify=False, headers=headers)
            z = json.loads(r.content)
            df = pd.DataFrame(z['data']['dividends']['rows'])
            if (len(df) > 0):
                df.drop(['declarationDate'], axis=1, inplace=True)
                df.replace('\$', '', regex=True, inplace=True)

                df['exOrEffDate'] = df['exOrEffDate'].apply(lambda x:pd.to_datetime(x, format='%m/%d/%Y'))
                df.sort_values(by = 'exOrEffDate', inplace=True)
                df['exOrEffDate'] = df['exOrEffDate'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))

                df['recordDate'] = df['recordDate'].apply(convert_time_format)
                df['paymentDate'] = df['paymentDate'].apply(convert_time_format)
            
            df.to_csv(path, encoding='utf-8', index=False)
            break
        except Exception as e:
            print(e, ' NASDAQ ETF DIVIDEND')
            # time.sleep(5)
            break


def update_all_nasdaq_etf_data():
    se = requests.session()
    current_time_dt = datetime.datetime.now()
    current_time = current_time_dt.strftime('%Y-%m-%d')

    # 10 year
    earlist_time_dt = datetime.datetime(year=current_time_dt.year-10, month=current_time_dt.month, day=current_time_dt.day) + pd.Timedelta(days=4)
    earlist_time = earlist_time_dt.strftime('%Y-%m-%d')

    NASDAQ_ETF_URL = 'https://api.nasdaq.com/api/quote/{}/historical?assetclass=etf&limit={}&fromdate={}&todate={}'

    for variety in NASDAQ_ETF_DICT:
        path = os.path.join(option_price_dir, 'nasdaq', variety+'_daily'+'.csv')

        last_line_time = get_last_line_time(path, variety, earlist_time, 10, '%Y-%m-%d')
        start_time_dt = pd.to_datetime(last_line_time, format='%Y-%m-%d') + pd.Timedelta(days=1)
        start_time = start_time_dt.strftime('%Y-%m-%d')
        if (start_time_dt > current_time_dt):
            continue

        while (1):
            try:
                url = NASDAQ_ETF_URL.format(variety, 10, start_time, current_time)
                print(url)
                r = se.get(url, verify=False, headers=headers)
                if (r.status_code == 200):
                    z = json.loads(r.content)
                    try:
                        total_records = z['data']['totalRecords']
                    except:
                        if z['data'] is None:
                            print("z['data'] is None")
                            total_records = 0
                    break
            except Exception as e:
                print(e)
                time.sleep(5)

        if total_records == 0:
            continue

        while (total_records > 0):
            print(variety, total_records)
            start_time = start_time_dt.strftime('%Y-%m-%d')
            offset = total_records - 120
            if offset < 0:
                offset = 0
            url = NASDAQ_ETF_URL.format(variety, 120, start_time, current_time) + '&offset=' + str(offset)
            total_records = total_records - 120

            while (1):
                try:
                    r = se.get(url, verify=False, headers=headers)
                    if (r.status_code == 200):
                        z = json.loads(r.content)
                        df = pd.DataFrame(z['data']['tradesTable']['rows'])
                        df.replace(',', '', regex=True, inplace=True)
                        df.rename(columns={"date":"time"}, inplace=True)
                        df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%m/%d/%Y'))
                        df.sort_values(by = 'time', inplace=True)
                        df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
                        df = df[['time','open','high','low','close','volume']]

                        if not(os.path.exists(path)):
                            df.to_csv(path, encoding='utf-8', index=False)
                        else:
                            df.to_csv(path, mode='a', encoding='utf-8', index=False, header=None)

                    break
                except Exception as e:
                    print(e, 'NASDAQ ETF DATA GET ERROR: ' + variety)
                    time.sleep(5)

        # DIVIDEND
        # update_nasdaq_etf_dividend(variety, se)
        div_path = os.path.join(option_price_dir, 'nasdaq', variety+'_dividend'+'.csv')
        if not(os.path.exists(div_path)):
            update_nasdaq_etf_dividend(variety, se)
        else:
            try:
                df = pd.read_csv(div_path)
                ex_t = pd.DatetimeIndex(pd.to_datetime(df['exOrEffDate'], format='%Y-%m-%d'))
                if (current_time_dt - ex_t[-1]).days >= 30:
                    update_nasdaq_etf_dividend(variety, se)
            except:
                pass

        etf_path = os.path.join(option_price_dir, 'nasdaq', variety+'_daily'+'.csv')
        compute_total_dividend(etf_path, div_path)


def read_us_etf_data(variety, fq=True):
    path = os.path.join(option_price_dir, 'nasdaq', variety+'_daily'+'.csv')
    df = pd.read_csv(path)
    df.drop_duplicates('time', keep='last', inplace=True)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    data = np.array(df['close'], dtype=float) + np.array(df['div_plus'], dtype=float)
    if (fq == True):
        start_time = '2017-01-01'
        t, data = get_period_data(t, data, start=start_time)
        data = data*100 / data[0]
    return [t, data]

def plot_us_etf_data():
    
    def read_msci_data(variety):
        path = os.path.join(msci_dir, variety+'.csv')
        df = pd.read_csv(path)
        df.drop_duplicates('time', keep='last', inplace=True)
        t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
        data = np.array(df['level_eod'], dtype=float)
        start_time = '2017-01-01'
        t, data = get_period_data(t, data, start=start_time)
        data = data*100 / data[0]
        return [t, data]

    data_dict = {}
    for variety in NASDAQ_ETF_DICT:
        data_dict[variety] = read_us_etf_data(variety)
    data_dict['VIETNAM'] = read_msci_data('VIETNAM')

    t0, data0 = data_div(data_dict['XLE'][0], data_dict['XLE'][1], data_dict['USO'][0], data_dict['USO'][1])
    t1, data1 = data_div(data_dict['XLE'][0], data_dict['XLE'][1], data_dict['XLK'][0], data_dict['XLK'][1])
    t2, data2 = data_div(data_dict['VNM'][0], data_dict['VNM'][1], data_dict['EEM'][0], data_dict['EEM'][1])
    t3, data3 = data_div(data_dict['INDA'][0], data_dict['INDA'][1], data_dict['EEM'][0], data_dict['EEM'][1])
    t4, data4 = data_div(data_dict['FXI'][0], data_dict['FXI'][1], data_dict['EEM'][0], data_dict['EEM'][1])
    # datas = [[[[data_dict['XLE'][0], data_dict['XLE'][1], 'XLE', ''],
    #            [data_dict['XLK'][0], data_dict['XLK'][1], 'XLK', ''],],
    #           [],''],

    #          [[[t1, data1, 'XLE / XLK', 'color=black'],
    #            ],
    #           [],''],
    #         ]
    # plot_many_figure(datas)

    datas = [[[[data_dict['INDA'][0], data_dict['INDA'][1], 'INDA', ''],
               [data_dict['VNM'][0], data_dict['VNM'][1], 'VNM', ''],
               [data_dict['EEM'][0], data_dict['EEM'][1], 'EEM', ''],],
              [],''],

             [[[data_dict['VNM'][0], data_dict['VNM'][1], 'VNM', ''],
               [data_dict['VIETNAM'][0], data_dict['VIETNAM'][1], 'MSCI VIETNAM', ''],
               [data_dict['XLY'][0], data_dict['XLY'][1], 'XLY 可选消费', ''],
               ],
              [[data_dict['XLK'][0], data_dict['XLK'][1], 'XLK', ''],],''],

             [[[data_dict['XLE'][0], data_dict['XLE'][1], 'XLE', ''],
               [data_dict['XLK'][0], data_dict['XLK'][1], 'XLK', ''],
               [data_dict['KWEB'][0], data_dict['KWEB'][1], 'KWEB', ''],
               [data_dict['FXI'][0], data_dict['FXI'][1], 'FXI', ''],],
              [],''],

             [[[t2, data2, 'VNM / EEM', ''],
               [t3, data3, 'INDA / EEM', ''],
               [t4, data4, 'FXI / EEM', ''],
               ],
              [],''],
            ]
    plot_many_figure(datas)


    datas = [[[[data_dict['XLE'][0], data_dict['XLE'][1], 'XLE', ''],
               [data_dict['XLK'][0], data_dict['XLK'][1], 'XLK', ''],
               [data_dict['USO'][0], data_dict['USO'][1], 'USO', ''],],
              [],''],
             [[[t0, data0, 'XLE - USO', ''],],
              [],''],
             [[[t1, data1, 'XLE - XLK', ''],],
              [],''],
            ]
    plot_many_figure(datas)


    datas = [[[[data_dict['SHY'][0], data_dict['SHY'][1], 'iShares 1-3 Year Treasury Bond ETF', ''],
               [data_dict['IEF'][0], data_dict['IEF'][1], 'iShares 7-10 Year Treasury Bond ETF', ''],
               [data_dict['TLT'][0], data_dict['TLT'][1], 'iShares 20+ Year Treasury Bond ETF', ''],],
               [],''],

             [[[data_dict['LQD'][0], data_dict['LQD'][1], 'iShares iBoxx投资级公司债', ''],
               [data_dict['HYG'][0], data_dict['HYG'][1], 'iShares iBoxx高收益公司债', ''],],
              [],''],
            ]
    plot_many_figure(datas)


    datas = [[[[data_dict['EMB'][0], data_dict['EMB'][1], 'USD Emerging Markets Bond ETF', ''],
               [data_dict['EMLC'][0], data_dict['EMLC'][1], 'EM Local Currency Bond ETF', ''],],
               [],''],
            ]
    plot_many_figure(datas)


def plot_gold_vs_tlt():
    path = os.path.join(cfd_dir, 'GOLD_CFD'+'.csv') 
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    gold_usd = np.array(df['close'], dtype=float)

    path = os.path.join(future_price_dir, 'shfe', 'au'+'.csv') 
    df = pd.read_csv(path, header=[0,1])
    t2 = pd.DatetimeIndex(pd.to_datetime(df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    gold_cny = np.array(df['index']['close'], dtype=float)

    t3, tlt = read_us_etf_data('TLT')

    path = os.path.join(interest_rate_dir, 'us_yield_curve'+'.csv') 
    df = pd.read_csv(path)
    t4 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    us2y = np.array(df['2Y'], dtype=float)

    path = os.path.join(interest_rate_dir, 'us_real_rate'+'.csv') 
    df = pd.read_csv(path)
    t5 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    us_real_5y = np.array(df['5Y'], dtype=float)
    us_real_10y = np.array(df['10Y'], dtype=float)

    datas = [[[[t1, gold_usd, 'gold_usd', ''],],
              [[t3, tlt, 'TLT', ''],],''],

             [[[t2, gold_cny, 'gold_cny', ''],],
              [[t3, tlt, 'TLT', ''],],''],

             [[[t1, gold_usd, 'gold_usd', ''],],
              [[t4, us2y, 'US2Y', ''],],''],

             [[[t1, gold_usd, 'gold_usd', ''],],
              [[t5, us_real_5y, 'US REAL 5Y', ''],
               [t5, us_real_10y, 'US REAL 10Y', ''],],''],
            ]
    plot_many_figure(datas, start_time='2017-01-01')


if __name__=="__main__":
    # for variety in NASDAQ_ETF_OPTION_DICT:
        # plot_nasdaq_option_datas(variety)

    # plot_nasdaq_option_datas('GLD')
    # plot_nasdaq_option_datas('SLV')
    # plot_nasdaq_option_datas('KWEB')
    # plot_nasdaq_option_datas('FXI')
    # plot_nasdaq_option_datas('EEM')
    # plot_nasdaq_option_datas('SPY')
    # plot_nasdaq_option_datas('QQQ')
    # plot_nasdaq_option_datas('DIA')
    # plot_nasdaq_option_datas('IWM')
    # plot_nasdaq_option_datas('USO')
    # plot_nasdaq_option_datas('XOP')
    # plot_nasdaq_option_datas('XLE')
    # plot_nasdaq_option_datas('XLY')
    # plot_gold_vs_tlt()

    update_all_nasdaq_etf_data()
    update_all_nasdaq_etf_option_data()

    # while (1):
    #     update_all_nasdaq_etf_option_data()
    #     time.sleep(60*30)

    # plot_us_etf_data()

    # plot_nasdaq_intraday_option_strike_volume_oi('nasdaq', 'XLE')
    # plot_nasdaq_intraday_option_strike_volume_oi('nasdaq', 'USO')

    # plot_nasdaq_intraday_option_strike_volume_oi('nasdaq', 'GLD')
    # plot_nasdaq_intraday_option_strike_volume_oi('nasdaq', 'SLV')

    # plot_nasdaq_intraday_option_strike_volume_oi('nasdaq', 'SPY')
    # plot_nasdaq_intraday_option_strike_volume_oi('nasdaq', 'QQQ')

    # plot_nasdaq_intraday_option_strike_volume_oi('nasdaq', 'EEM')
    # plot_nasdaq_intraday_option_strike_volume_oi('nasdaq', 'KWEB')
    # plot_nasdaq_intraday_option_strike_volume_oi('nasdaq', 'FXI')

    pass

