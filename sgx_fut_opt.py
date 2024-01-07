import os
import requests
import pandas as pd
import zipfile
import datetime
import numpy as np
from utils import *
from io import StringIO, BytesIO
from us_rate import update_treasury_yield
from compare import compare_price_in_different_currency


SGX_FUTURE_CODE_NAME = {
    'FEF': 'SGX TSI Iron Ore CFR China (62% FE Fines) Index Futures Contract',
    'UC': 'SGX USD/CNH (Full-Sized) Futures',
    'CN': 'SGX FTSE China A50 Index Futures',
    'TF': 'SICOM TSR20 Rubber Futures',
}

SGX_OPTION_CODE_NAME = {
    'FEF': 'SGX TSI Iron Ore CFR China (62% FE Fines) Index Futures Contract',
    'UC': 'SGX USD/CNH (Full-Sized) Futures',
}


def update_sgx_future_data():
    se = requests.session()
    SGX_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Host': 'links.sgx.com'}
    # csv 格式
    start_number = 5340
    path = os.path.join(future_price_dir, 'sgx', 'number.csv')
    if os.path.exists(path):
        number_df = pd.read_csv(path)
        start_number = number_df.loc[len(number_df)-1, 'number'] + 1
    now = datetime.datetime.now()
    now = pd.to_datetime(now.strftime('%Y-%m-%d'), format='%Y-%m-%d')

    # url = 'https://links.sgx.com/1.0.0/derivatives-daily/6829/FUTURE.zip'
    URL = 'https://links.sgx.com/1.0.0/derivatives-daily/{}/FUTURE.zip'

    err_count = 0
    while (1):
        time.sleep(0.25)
        start_number_str = str(start_number)
        start_number = start_number + 1
        url = URL.format(start_number_str)

        while (1):
            try:
                r = se.get(url, headers=SGX_HEADERS, timeout=10)
                if r.status_code == 200 or r.status_code == 302:
                    break
                time.sleep(5)
            except Exception as e:
                print(e)
                time.sleep(5)

        if r.status_code == 302:
            return

        null_data = [None,None,None,None,None,None,None,None]
        col1 = ['time']
        col2 = ['time']
        n = 0
        for i in range(9):
            n = n + 1
            cn  = 'c'+str(n)
            # inst_id, open, high low, close, settle, volume, oi
            col1 += [cn, cn, cn, cn, cn, cn, cn, cn]
            col2 += ['inst_id', 'open', 'high', 'low', 'close', 'settle', 'volume', 'oi']

        try:
            z = zipfile.ZipFile(BytesIO(r.content), "r")
            err_count = 0
        except Exception as e:
            print(e)
            err_count = err_count + 1
            if err_count >= 5:
                return
            continue

        file_name = z.namelist()[0]
        temp_df = pd.read_table(z.open(file_name), header=[0], delimiter=',', dtype=str)
        temp_df.replace(' ', '', regex=True, inplace=True)
        if len(temp_df) == 0:
            continue

        t = temp_df.loc[0, 'DATE']
        # YYYY-MM-DD
        t = t[0:4] + '-' + t[4:6] + '-' + t[6:8]

        for code in SGX_FUTURE_CODE_NAME:
            data = [t]

            com = np.array(temp_df['COM'], dtype=str)
            idx = np.where(com == code)[0]
            L = len(idx)
            if L == 0:
                continue

            if (L >= 9):
                for i in range(9):
                    inst_id = code + temp_df.loc[idx[i], 'COM_YY'][2:]
                    if len(temp_df.loc[idx[i], 'COM_MM']) == 1:
                        inst_id = inst_id + '0' + temp_df.loc[idx[i], 'COM_MM']
                    else:
                        inst_id = inst_id + temp_df.loc[idx[i], 'COM_MM']
                    
                    data += [inst_id]
                    data += temp_df.loc[idx[i], ['OPEN','HIGH','LOW','CLOSE','SETTLE','VOLUME','OINT']].values.tolist()
            else:
                for i in range(L):
                    inst_id = code + temp_df.loc[idx[i], 'COM_YY'][2:]
                    if len(temp_df.loc[idx[i], 'COM_MM']) == 1:
                        inst_id = inst_id + '0' + temp_df.loc[idx[i], 'COM_MM']
                    else:
                        inst_id = inst_id + temp_df.loc[idx[i], 'COM_MM']

                    data += [inst_id]
                    data += temp_df.loc[idx[i], ['OPEN','HIGH','LOW','CLOSE','SETTLE','VOLUME','OINT']].values.tolist()

                for i in range(9-L):
                    data += null_data

            df = pd.DataFrame(columns=[col1, col2], data=[data])
            path = os.path.join(future_price_dir, 'sgx', code+'.csv')
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

        number_df = pd.DataFrame(columns=['time', 'number'], data=[[t, start_number-1]])
        path = os.path.join(future_price_dir, 'sgx', 'number.csv')
        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, number_df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            old_df.sort_values(by = 'time', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            number_df.to_csv(path, encoding='utf-8', index=False)  

        print('SGX FUTURE: ', t, start_number-1)
        dt = pd.to_datetime(t, format='%Y-%m-%d')
        if dt >= now:
            return


def update_sgx_fef_3pm_price():
    # URL = 'https://api2.sgx.com/sites/default/files/reports/prices-reports/2023/08/wcm@sgx_en@iron_fef@08-Aug-2023@Iron_Ore_FEF.csv'
    URL = 'https://api2.sgx.com/sites/default/files/reports/prices-reports/{}/{}/wcm@sgx_en@iron_fef@{}@Iron_Ore_FEF.csv'
    se = requests.session()
    SGX_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Host': 'api2.sgx.com'}

    now = datetime.datetime.now()
    earlist_time = '2021-02-22'
    path = os.path.join(future_price_dir, 'sgx', 'FEF_3PM'+'.csv')
    if os.path.exists(path):
        old_df = pd.read_csv(path, header=[0,1])
        old_t = pd.to_datetime(old_df['time']['time'], format='%Y-%m-%d')
        start_time_dt = old_t[len(old_t)-1] + pd.Timedelta(days=1)
    else:
        old_df = pd.DataFrame()
        start_time_dt = pd.to_datetime(earlist_time, format='%Y-%m-%d')

    data_days = 0
    df = pd.DataFrame()
    while (start_time_dt <= now):
        time.sleep(0.3)
        start_time = start_time_dt.strftime('%Y-%m-%d')
        col1 = ['time']
        col2 = ['time']
        data = [start_time]
        print('FEF 3PM PRICE:', start_time)
        year = start_time[0:4]
        month = start_time[5:7]
        start_time = start_time_dt.strftime('%d-%b-%Y')
        url = URL.format(year, month, start_time)
        start_time_dt += pd.Timedelta(days=1)


        while (1):
            try:
                r = se.get(url, headers=SGX_HEADERS)
                break
            except Exception as e:
                print(e)
                time.sleep(5)

        try:
            temp_df = pd.read_csv(BytesIO(r.content))
            s = temp_df.loc[0, 'Series Month']
        except:
            continue

        n = 0
        for i in range(4):  # len(temp_df)
            s = temp_df.loc[i, 'Series Month']
            inst_id = 'FEF' + s[4:6] + character_month_dict[s[3]]
            price = temp_df.loc[i, temp_df.columns.tolist()[1]]

            n += 1
            cn = 'c'+str(n)

            col1 += [cn, cn]
            col2 += ['inst_id', 'price']
            data += [inst_id, price]

        df = pd.concat([df, pd.DataFrame(columns=[col1, col2], data=[data])], axis=0)
        if os.path.exists(path):
            data_days += 1
            if (data_days > 10):
                old_df = pd.read_csv(path, header=[0,1])
                old_df = pd.concat([old_df, df], axis=0)
                old_df.drop_duplicates(subset=[('time','time')], keep='last', inplace=True) # last
                old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
                old_df.sort_values(by = ('time','time'), inplace=True)
                old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
                old_df.to_csv(path, encoding='utf-8', index=False)
                data_days = 0
                df = pd.DataFrame()
        else:
            df.to_csv(path, encoding='utf-8', index=False)
            df = pd.DataFrame()

    if not(df.empty):
        old_df = pd.read_csv(path, header=[0,1])
        old_df = pd.concat([old_df, df], axis=0)
        old_df.drop_duplicates(subset=[('time','time')], keep='last', inplace=True) # last
        old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
        old_df.sort_values(by = ('time','time'), inplace=True)
        old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
        old_df.to_csv(path, encoding='utf-8', index=False)


def sgx_expiry_time(variety, year, month):
    if variety == 'FEF':
        last_day = calendar.monthrange(year, month)[-1]
        last_day = datetime.datetime(year, month, last_day)
        while(last_day.weekday() >= 5):
            last_day = last_day - pd.Timedelta(days=1)
        return last_day - pd.Timedelta(hours=9)  # 最后一天交易时间要短一些
    
    if variety == 'UC':
        day = datetime.datetime(year, month, 1)
        count = 0
        while (1):
            if (day.weekday()) == 2:
                count += 1
            if count == 3:
                break
            day = day + pd.Timedelta(days=1)
        day = day - pd.Timedelta(days=2)
        return day - pd.Timedelta(hours=18)
    
    return None


def update_sgx_option_data():
    se = requests.session()
    SGX_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Host': 'links.sgx.com'}
    # csv 格式
    start_number = 5340
    # start_number = 6828
    path = os.path.join(option_price_dir, 'sgx', 'number.csv')
    if os.path.exists(path):
        number_df = pd.read_csv(path)
        start_number = number_df.loc[len(number_df)-1, 'number'] + 1
    now = datetime.datetime.now()
    now = pd.to_datetime(now.strftime('%Y-%m-%d'), format='%Y-%m-%d')

    # url = 'https://links.sgx.com/1.0.0/derivatives-daily/6829/FUTURE.zip'
    URL = 'https://links.sgx.com/1.0.0/derivatives-daily/{}/OPTION.zip'
    err_count = 0

    path = os.path.join(interest_rate_dir, 'us_yield_curve'+'.csv') 
    rate_df = pd.read_csv(path)
    rate_t = pd.DatetimeIndex(pd.to_datetime(rate_df['time'], format='%Y-%m-%d'))
    us2y = np.array(rate_df['2Y'], dtype=float)

    future_df_dict = {}
    for code in SGX_OPTION_CODE_NAME:
        path = os.path.join(future_price_dir, 'sgx', code+'.csv')
        future_df_dict[code] = pd.read_csv(path, header=[0,1])

    while (1):
        time.sleep(0.25)
        start_number_str = str(start_number)
        start_number = start_number + 1
        url = URL.format(start_number_str)

        while (1):
            try:
                r = se.get(url, headers=SGX_HEADERS, timeout=10)
                if r.status_code == 200 or r.status_code == 302:
                    break
                time.sleep(5)
            except Exception as e:
                print(e)
                time.sleep(5)

        if r.status_code == 302:
            return

        try:
            z = zipfile.ZipFile(BytesIO(r.content), "r")
            err_count = 0
        except Exception as e:
            print(e)
            err_count = err_count + 1
            if err_count >= 5:
                return
            continue

        file_name = z.namelist()[0]
        temp_df = pd.read_table(z.open(file_name), header=[0], delimiter=',', dtype=str)
        temp_df.replace(' ', '', regex=True, inplace=True)
        if len(temp_df) == 0:
            continue
        t = temp_df.loc[0, 'DATE']
        # YYYY-MM-DD
        t = t[0:4] + '-' + t[4:6] + '-' + t[6:8]
        dt = pd.to_datetime(t, format='%Y-%m-%d')

        com = np.array(temp_df['COM'], dtype=str)
        month_array = np.array(temp_df['COM_MM'], dtype=str)
        year_array = np.array(temp_df['COM_YY'], dtype=str)
        opt_type_array = np.array(temp_df['OPT'], dtype=str)
        strike_array = np.array(temp_df['STRIKE'], dtype=str)
        settle_array = np.array(temp_df['SETTLE'], dtype=float)
        volume_array = np.array(temp_df['VOLUME'], dtype=float)
        oi_array = np.array(temp_df['OINT'], dtype=float)

        for code in SGX_OPTION_CODE_NAME:
            idx = np.where(com == code)[0]
            L = len(idx)

            i = idx[0]
            current_month = month_array[0]
            if len(current_month) == 1:
                current_month = '0' + current_month
            year = year_array[i]
            data_dict = {}
            cs = np.array(['c1','c2','c3','c4','c5','c6','c7','c8','c9'])
            contract_count = 0
            inst_ids = ''
            while (1):
                month = month_array[i]
                if len(month) == 1:
                    month = '0' + month
                year = year_array[i]

                opt_type = opt_type_array[i]
                strike = strike_array[i]
                if not(strike in data_dict):
                    data_dict[strike] = [strike, 1, [opt_type, settle_array[i], volume_array[i], oi_array[i]]]
                else:
                    data_dict[strike][1] = data_dict[strike][1] + 1
                    data_dict[strike].append([opt_type, settle_array[i], volume_array[i], oi_array[i]])

                i = i + 1
                if (i >= (idx[0]+L)):
                    print('break')
                    break
                #
                if month_array[i] != current_month:
                    current_month = month_array[i]
                    col1 = ['time']
                    col2 = ['time']
                    col3 = ['time']
                    data = [t]

                    # T
                    dt = pd.to_datetime(t, format='%Y-%m-%d')
                    expiry_time = sgx_expiry_time(code, int(year), int(month))
                    diff = expiry_time - dt
                    T = (float(diff.days) + float(diff.seconds)/86400) / 365

                    # rate
                    good = False
                    idx2 = rate_df[rate_df['time'] == t].index
                    if len(idx2) > 0:
                        idx2 = idx2[0]
                        if not np.isnan(us2y[idx2]):
                            rate = us2y[idx2] / 100
                            good = True
                    if not good:
                        idx2 = np.where(dt >= rate_t)[0]
                        k = -1
                        while (1):
                            rate = us2y[idx2[k]] / 100
                            if not(np.isnan(rate)):
                                break
                            k -= 1

                    # future settle
                    inst_id = code + year[2:] + month
                    future_df = future_df_dict[code]
                    tmp = future_df[future_df['time']['time'] == t]
                    if (len(tmp) == 0):
                        continue
                    idx2 = (tmp.loc[:, pd.IndexSlice[cs, 'inst_id']] == inst_id).values[0]
                    if (len(cs[idx2]) == 0):
                        continue
                    c = cs[idx2][0]
                    settle_fut = tmp.loc[tmp.index[0], pd.IndexSlice[c, 'settle']]

                    #
                    for strike in data_dict:
                        d = data_dict[strike]
                        if d[1] != 2:
                            continue
                        else:
                            if (d[2][3] + d[3][3]) <= 0:
                                continue

                        for k in [2,3]:
                            opt_type =  d[k][0]
                            col1 += [opt_type,opt_type,opt_type,opt_type,opt_type]
                            col2 += [strike,strike,strike,strike,strike]
                            col3 += ['settle','volume','oi','imp_vol','delta']
                            data += [d[k][1],d[k][2],d[k][3]]
                            # greeks
                            # print(settle_fut, float(strike), T, rate, float(d[k][1]), opt_type)
                            ret = calculate_greeks(settle_fut, float(strike), T, rate, float(d[k][1]), opt_type)
                            data += ret

                    data_dict = {}

                    # write data to file
                    if len(data) > 1:
                        df = pd.DataFrame(columns=[col1,col2,col3], data=[data])
                        inst_ids += inst_id
                        inst_ids += ','
                        path = os.path.join(option_price_dir, 'sgx', inst_id+'.csv')
                        if os.path.exists(path):
                            old_df = pd.read_csv(path, header=[0,1,2])
                            old_df = pd.concat([old_df, df], axis=0)
                            old_df.drop_duplicates(subset=[('time','time','time')], keep='last', inplace=True) # last
                            old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
                            old_df.sort_values(by = ('time','time','time'), inplace=True)
                            old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
                            old_df.to_csv(path, encoding='utf-8', index=False)
                        else:
                            df.to_csv(path, encoding='utf-8', index=False)

                    contract_count += 1
                    if contract_count >= 4:
                        break

            if len(inst_ids) > 1:
                info_df = pd.DataFrame(columns=['time', 'inst_ids'])
                info_df.loc[0, 'time'] = t
                info_df.loc[0, 'inst_ids'] = inst_ids
                path = os.path.join(option_price_dir, 'sgx', code+'_info'+'.csv')
                # print(df)
                if os.path.exists(path):
                    old_df = pd.read_csv(path)
                    old_df = pd.concat([old_df, info_df], axis=0)
                    old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
                    old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
                    old_df.sort_values(by = 'time', inplace=True)
                    old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
                    old_df.to_csv(path, encoding='utf-8', index=False)
                else:
                    info_df.to_csv(path, encoding='utf-8', index=False)  

        print('SGX OPTION: ', t, start_number-1)
        number_df = pd.DataFrame(columns=['time', 'number'], data=[[t, start_number-1]])
        path = os.path.join(option_price_dir, 'sgx', 'number.csv')
        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, number_df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            old_df.sort_values(by = 'time', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            number_df.to_csv(path, encoding='utf-8', index=False)  


def sgx_put_call_delta_volatility(df, delta, price, strike):
    # CALL
    tmp = df.loc[pd.IndexSlice['C', :, 'delta']]
    idx1, idx2, delta1, delta2 = column_index_delta(tmp, delta)
    o_price = df.loc[pd.IndexSlice['C', :, 'settle']]
    iv = df.loc[pd.IndexSlice['C', :, 'imp_vol']]

    if delta1 == delta:
        _25d_call_price = o_price[idx1]
        _25d_call_iv = iv[idx1]
    else:
        w1 = (delta2 - delta)/(delta2 - delta1)
        w2 = (delta - delta1)/(delta2 - delta1)
        _25d_call_price = w1*o_price[idx1] + w2*o_price[idx2]
        _25d_call_iv = w1*iv[idx1] + w2*iv[idx2]

    if (price > 0.01):
        # print(df)
        # print(strike)
        # print(price)
        idx1, idx2, price1, price2 = column_index_price(strike, price)
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

    # PUT
    tmp = df.loc[pd.IndexSlice['P', :, 'delta']]
    idx1, idx2, delta1, delta2 = column_index_delta(tmp, delta)
    o_price = df.loc[pd.IndexSlice['P', :, 'settle']]
    iv = df.loc[pd.IndexSlice['P', :, 'imp_vol']]

    if delta1 == delta:
        _25d_put_price = o_price[idx1]
        _25d_put_iv = iv[idx1]
    else:
        w1 = (delta2 - delta)/(delta2 - delta1)
        w2 = (delta - delta1)/(delta2 - delta1)
        _25d_put_price = w1*o_price[idx1] + w2*o_price[idx2]
        _25d_put_iv = w1*iv[idx1] + w2*iv[idx2]

    if (price > 0.01):
        idx1, idx2, price1, price2 = column_index_price(strike, price)
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


def update_sgx_fut_option_info_detail(variety):
    print(variety + ' info detail')
    info_path = os.path.join(option_price_dir, 'sgx', variety+'_info'+'.csv')
    info_df = pd.read_csv(info_path)
    info_t = pd.DatetimeIndex(pd.to_datetime(info_df['time'], format='%Y-%m-%d'))

    earlist_time = info_t[0].strftime('%Y-%m-%d')
    path = os.path.join(option_price_dir, 'sgx', variety+'_info_detail'+'.csv')
    last_line_time = get_last_line_time(path, variety+'_info_detail', earlist_time, 10, '%Y-%m-%d')
    last_line_time_dt = pd.to_datetime(last_line_time, format='%Y-%m-%d')
    if (last_line_time == earlist_time):
        start_idx = 0
    else:
        start_idx = np.where(info_t == last_line_time_dt)[0][0] + 1

    fut_path = os.path.join(future_price_dir, 'sgx', variety+'.csv')
    fut_df = pd.read_csv(fut_path, header=[0,1])
    fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['time'], format='%Y-%m-%d'))

    cs = ['c1','c2','c3','c4','c5','c6','c7','c8','c9']

    opt_dict = {}
    COL = ['inst_id', 'settle',
           'total_put_volume', 'total_call_volume', 'total_put_oi', 'total_call_oi', 
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
                opt_path = os.path.join(option_price_dir, 'sgx', inst_id+'.csv')
                opt_df = pd.read_csv(opt_path, header=[0,1,2])
                opt_t = pd.DatetimeIndex(pd.to_datetime(opt_df['time']['time']['time'], format='%Y-%m-%d'))
                strike = get_full_strike_price(opt_df)
                opt_dict[inst_id] = [opt_t, opt_df, strike]
    
        _df = pd.DataFrame(columns=cols)
        _df.loc[i, 'time'] = t.strftime('%Y-%m-%d')
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

            w = np.where(fut_t == t)[0][0]
            fut_settle = 0
            try:
                w = np.where(fut_t == info_t[i])[0][0]
            except:
                print('fut_t == info_t[i], ', inst_id, info_t[i])
                continue
            temp_df2 = fut_df.loc[w,:]
            for c in cs:
                if (temp_df2[c]['inst_id'] == inst_id):
                    fut_settle = temp_df2[c]['settle']
                    break

            _df.loc[i, 'inst_id_'+cn] = inst_id
            _df.loc[i, 'settle_'+cn] = fut_settle
            ret = sgx_put_call_delta_volatility(temp_df, 0.25, fut_settle, strike)
            _df.loc[i, '25d_put_iv_'+cn] = ret[1]
            _df.loc[i, '25d_call_iv_'+cn] = ret[3]
            _df.loc[i, 'atm_put_iv_'+cn] = ret[4]
            _df.loc[i, 'atm_call_iv_'+cn] = ret[5]

            ret = sgx_put_call_delta_volatility(temp_df, 0.1, fut_settle, strike)
            _df.loc[i, '10d_put_iv_'+cn] = ret[1]
            _df.loc[i, '10d_call_iv_'+cn] = ret[3]

            ret = sgx_put_call_delta_volatility(temp_df, 0.05, fut_settle, strike)
            _df.loc[i, '5d_put_iv_'+cn] = ret[1]
            _df.loc[i, '5d_call_iv_'+cn] = ret[3]

        df = pd.concat([df, _df], axis=0)

    if (len(df) > 0):
        path = os.path.join(option_price_dir, 'sgx', variety+'_info_detail'+'.csv')
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



def plot_sgx_option_iv(variety):
    path = os.path.join(option_price_dir, 'sgx', variety+'_info_detail'+'.csv')
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))

    cols = df.columns.tolist()
    cols.remove('time')
    data_dict = {}
    for col in cols:
        if 'inst_id' in col:
            data_dict[col] = np.array(df[col], dtype=str)
        else:
            data_dict[col] = np.array(df[col], dtype=float)

    contract_c1 = data_dict['inst_id_c1'][-1]
    contract_c2 = data_dict['inst_id_c2'][-1]

    w = np.where(data_dict['settle_c1'] > 0)[0]

    datas = [
            [[[t,data_dict['total_call_volume'],'total_call_volume','color=red'],
             [t,data_dict['total_put_volume'],'total_put_volume','color=darkgreen'],
            ],
            [[t,data_dict['total_put_volume']-data_dict['total_call_volume'],'total_put_volume - total_call_volume','style=vbar'],],''],

            [[[t,data_dict['25d_call_iv_c2'],'25d_call_iv_c2','color=red'],
              [t,data_dict['25d_put_iv_c2'],'25d_put_iv_c2','color=darkgreen'],
            ],
            [[t,data_dict['25d_put_iv_c2']-data_dict['25d_call_iv_c2'],'25d_put_iv_c2 - 25d_call_iv_c2','style=vbar'],],''],

            [[[t,data_dict['25d_call_iv_c1'],'25d_call_iv_c1','color=red'],
              [t,data_dict['25d_put_iv_c1'],'25d_put_iv_c1','color=darkgreen'],
            ],
            [[t,data_dict['25d_put_iv_c1']-data_dict['25d_call_iv_c1'],'25d_put_iv_c1 - 25d_call_iv_c1','style=vbar'],],''],

            [[[t[w],data_dict['settle_c1'][w],variety,'color=black']],
             [[t,data_dict['total_put_oi']/data_dict['total_call_oi'],'total_put_oi / total_call_oi',''],],''],

            [[[t,data_dict['total_call_oi'],'total_call_oi','color=red'],
             [t,data_dict['total_put_oi'],'total_put_oi','color=darkgreen'],
            ],
            [[t,data_dict['total_put_oi']-data_dict['total_call_oi'],'total_put_oi - total_call_oi','style=vbar'],],''],

            [[[t,data_dict['atm_call_iv_c2'],'atm_call_iv_c2','color=red'],
              [t,data_dict['atm_put_iv_c2'],'atm_put_iv_c2','color=darkgreen'],
            ],
            [[t,data_dict['atm_put_iv_c2']-data_dict['atm_call_iv_c2'],'atm_put_iv_c2 - atm_call_iv_c2','style=vbar'],],''],

            [[[t,data_dict['atm_call_iv_c1'],'atm_call_iv_c1','color=red'],
              [t,data_dict['atm_put_iv_c1'],'atm_put_iv_c1','color=darkgreen'],
            ],
            [[t,data_dict['atm_put_iv_c1']-data_dict['atm_call_iv_c1'],'atm_put_iv_c1 - atm_call_iv_c1','style=vbar'],],''],

            ]
    plot_many_figure(datas, max_height=1000)


def plot_sgx_option_strike_volume_oi(variety):
    path3 = os.path.join(option_price_dir, 'sgx', variety+'_info_detail'+'.csv')
    if not os.path.exists(path3):
        return
    df = pd.read_csv(path3)

    path3 = os.path.join(option_price_dir, 'sgx', variety+'_info.csv')
    if not os.path.exists(path3):
        return
    df = pd.read_csv(path3)
    inst_id_fut = df.loc[len(df)-1, 'inst_ids']

    inst_ids = inst_id_fut.split(',')[:2]

    for inst_id in inst_ids:
        path = os.path.join(option_price_dir, 'sgx', inst_id+'.csv')
        df = pd.read_csv(path, header=[0,1,2])
        t = pd.DatetimeIndex(pd.to_datetime(df['time']['time']['time'], format='%Y-%m-%d'))

        L = len(t)
        if (L < 7):
            print('L < 7')
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

        put_oi_1d = []
        call_oi_1d = []
        put_vol_1d = []
        call_vol_1d = []

        put_oi_2d = []
        call_oi_2d = []
        put_vol_2d = []
        call_vol_2d = []

        put_vol_3d = []
        call_vol_3d = []

        put_vol_4d = []
        call_vol_4d = []

        put_oi_5d = []
        call_oi_5d = []
        put_vol_5d = []
        call_vol_5d = []
        for strike_str in strikes_str:
            strike.append(float(strike_str))
            put_oi.append(df.loc[L-1, pd.IndexSlice['P', strike_str, 'oi']])
            call_oi.append(df.loc[L-1, pd.IndexSlice['C', strike_str, 'oi']])
            put_vol.append(df.loc[L-1, pd.IndexSlice['P', strike_str, 'volume']])
            call_vol.append(df.loc[L-1, pd.IndexSlice['C', strike_str, 'volume']])

            put_oi_1d.append(df.loc[L-2, pd.IndexSlice['P', strike_str, 'oi']])
            call_oi_1d.append(df.loc[L-2, pd.IndexSlice['C', strike_str, 'oi']])
            put_vol_1d.append(df.loc[L-2, pd.IndexSlice['P', strike_str, 'volume']])
            call_vol_1d.append(df.loc[L-2, pd.IndexSlice['C', strike_str, 'volume']])

            put_oi_2d.append(df.loc[L-3, pd.IndexSlice['P', strike_str, 'oi']])
            call_oi_2d.append(df.loc[L-3, pd.IndexSlice['C', strike_str, 'oi']])
            put_vol_2d.append(df.loc[L-3, pd.IndexSlice['P', strike_str, 'volume']])
            call_vol_2d.append(df.loc[L-3, pd.IndexSlice['C', strike_str, 'volume']])

            put_vol_3d.append(df.loc[L-4, pd.IndexSlice['P', strike_str, 'volume']])
            call_vol_3d.append(df.loc[L-4, pd.IndexSlice['C', strike_str, 'volume']])

            put_vol_4d.append(df.loc[L-5, pd.IndexSlice['P', strike_str, 'volume']])
            call_vol_4d.append(df.loc[L-5, pd.IndexSlice['C', strike_str, 'volume']])

            put_oi_5d.append(df.loc[L-6, pd.IndexSlice['P', strike_str, 'oi']])
            call_oi_5d.append(df.loc[L-6, pd.IndexSlice['C', strike_str, 'oi']])
            put_vol_5d.append(df.loc[L-6, pd.IndexSlice['P', strike_str, 'volume']])
            call_vol_5d.append(df.loc[L-6, pd.IndexSlice['C', strike_str, 'volume']])

        strike = np.array(strike, dtype=float)
        put_oi = np.array(put_oi, dtype=float)
        call_oi = np.array(call_oi, dtype=float)
        put_vol = np.array(put_vol, dtype=float)
        call_vol = np.array(call_vol, dtype=float)

        put_oi_1d = np.array(put_oi_1d, dtype=float)
        call_oi_1d = np.array(call_oi_1d, dtype=float)
        put_vol_1d = np.array(put_vol_1d, dtype=float)
        call_vol_1d = np.array(call_vol_1d, dtype=float)

        put_oi_2d = np.array(put_oi_2d, dtype=float)
        call_oi_2d = np.array(call_oi_2d, dtype=float)
        put_vol_2d = np.array(put_vol_2d, dtype=float)
        call_vol_2d = np.array(call_vol_2d, dtype=float)

        put_vol_3d = np.array(put_vol_3d, dtype=float)
        call_vol_3d = np.array(call_vol_3d, dtype=float)

        put_vol_4d = np.array(put_vol_4d, dtype=float)
        call_vol_4d = np.array(call_vol_4d, dtype=float)

        put_oi_5d = np.array(put_oi_5d, dtype=float)
        call_oi_5d = np.array(call_oi_5d, dtype=float)
        put_vol_5d = np.array(put_vol_5d, dtype=float)
        call_vol_5d = np.array(call_vol_5d, dtype=float)


        path = os.path.join(future_price_dir, 'sgx', variety+'.csv')
        fut_df = pd.read_csv(path, header=[0,1])
        fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['time'], format='%Y-%m-%d'))
        row = np.where(fut_t == t[L-1])[0]
        if len(row) > 0:
            for c in ['c1','c2','c3','c4','c5','c6','c7','c8','c9']:
                if (fut_df.loc[row[0], pd.IndexSlice[c, 'inst_id']] == inst_id):
                    fut_price = fut_df.loc[row[0], pd.IndexSlice[c, 'settle']]

        row = np.where(fut_t == t[L-2])[0]
        if len(row) > 0:
            for c in ['c1','c2','c3','c4','c5','c6','c7','c8','c9']:
                if (fut_df.loc[row[0], pd.IndexSlice[c, 'inst_id']] == inst_id):
                    fut_price_1d = fut_df.loc[row[0], pd.IndexSlice[c, 'settle']]

        row = np.where(fut_t == t[L-3])[0]
        if len(row) > 0:
            for c in ['c1','c2','c3','c4','c5','c6','c7','c8','c9']:
                if (fut_df.loc[row[0], pd.IndexSlice[c, 'inst_id']] == inst_id):
                    fut_price_2d = fut_df.loc[row[0], pd.IndexSlice[c, 'settle']]

        row = np.where(fut_t == t[L-6])[0]
        if len(row) > 0:
            for c in ['c1','c2','c3','c4','c5','c6','c7','c8','c9']:
                if (fut_df.loc[row[0], pd.IndexSlice[c, 'inst_id']] == inst_id):
                    fut_price_5d = fut_df.loc[row[0], pd.IndexSlice[c, 'settle']]

        strike_sort = np.sort(strike)
        diff = strike_sort[1:] - strike_sort[:-1]
        bar_width = (np.min(diff)) / 5

        fig1 = figure(frame_width=1300, frame_height=155)
        fig1.quad(left=strike-bar_width, right=strike, bottom=0, top=put_oi, fill_color='darkgreen')
        fig1.quad(left=strike, right=strike+bar_width, bottom=0, top=call_oi, fill_color='red')
        fig1.line(x=[fut_price, fut_price], y=[0, np.nanmax(call_oi)], line_width=1, line_color='black', legend_label=inst_id + ' oi')
        fig1.legend.location='top_left'

        net_oi = put_oi - call_oi
        put_idx = np.where(net_oi >= 0)[0]
        call_idx = np.where(net_oi < 0)[0]

        fig11 = figure(frame_width=1300, frame_height=155, x_range=fig1.x_range)
        fig11.quad(left=strike[put_idx]-bar_width/2, right=strike[put_idx]+bar_width/2, bottom=0, top=net_oi[put_idx], fill_color='darkgreen')
        fig11.quad(left=strike[call_idx]-bar_width/2, right=strike[call_idx]+bar_width/2, bottom=0, top=-net_oi[call_idx], fill_color='red')
        fig11.line(x=[fut_price, fut_price], y=[0, np.nanmax(call_oi)], line_width=1, line_color='black', legend_label=inst_id + ' net oi')
        fig11.legend.location='top_left'

        fig2 = figure(frame_width=1300, frame_height=155, x_range=fig1.x_range)
        fig2.quad(left=strike-bar_width, right=strike, bottom=0, top=put_vol, fill_color='darkgreen')
        fig2.quad(left=strike, right=strike+bar_width, bottom=0, top=call_vol, fill_color='red')
        fig2.line(x=[fut_price_1d, fut_price_1d], y=[0, np.nanmax(call_vol)], line_width=1, line_color='black', legend_label=inst_id + ' 1d volume')
        fig2.legend.location='top_left'
        fig2.background_fill_color = "lightgray"

        fig21 = figure(frame_width=1300, frame_height=155, x_range=fig1.x_range)
        fig21.quad(left=strike-bar_width, right=strike, bottom=0, top=put_oi-put_oi_1d, fill_color='darkgreen')
        fig21.quad(left=strike, right=strike+bar_width, bottom=0, top=call_oi-call_oi_1d, fill_color='red')
        fig21.line(x=[fut_price_1d, fut_price_1d], y=[np.nanmin(call_oi-call_oi_1d), np.nanmax(call_oi-call_oi_1d)], line_width=1, line_color='black', legend_label=inst_id + ' oi 1d change')
        fig21.legend.location='top_left'

        fig3 = figure(frame_width=1300, frame_height=155, x_range=fig1.x_range)
        fig3.quad(left=strike-bar_width, right=strike, bottom=0, top=put_vol+put_vol_1d, fill_color='darkgreen')
        fig3.quad(left=strike, right=strike+bar_width, bottom=0, top=call_vol+call_vol_1d, fill_color='red')
        fig3.line(x=[fut_price_2d, fut_price_2d], y=[0, np.nanmax(call_vol+call_vol_1d)], line_width=1, line_color='black', legend_label=inst_id + ' 2d volume')
        fig3.legend.location='top_left'
        fig3.background_fill_color = "lightgray"

        fig31 = figure(frame_width=1300, frame_height=155, x_range=fig1.x_range)
        fig31.quad(left=strike-bar_width, right=strike, bottom=0, top=put_oi-put_oi_2d, fill_color='darkgreen')
        fig31.quad(left=strike, right=strike+bar_width, bottom=0, top=call_oi-call_oi_2d, fill_color='red')
        fig31.line(x=[fut_price_2d, fut_price_2d], y=[np.nanmin(call_oi-call_oi_2d), np.nanmax(call_oi-call_oi_2d)], line_width=1, line_color='black', legend_label=inst_id + ' oi 2d change')
        fig31.legend.location='top_left'

        fig4 = figure(frame_width=1300, frame_height=155, x_range=fig1.x_range)
        fig4.quad(left=strike-bar_width, right=strike, bottom=0, top=put_vol+put_vol_1d+put_vol_2d+put_vol_3d+put_vol_4d, fill_color='darkgreen')
        fig4.quad(left=strike, right=strike+bar_width, bottom=0, top=call_vol+call_vol_1d+call_vol_1d+call_vol_3d+call_vol_4d, fill_color='red')
        fig4.line(x=[fut_price_5d, fut_price_5d], y=[0, np.nanmax(call_vol+call_vol_1d+call_vol_1d+call_vol_3d+call_vol_4d)], line_width=1, line_color='black', legend_label=inst_id + ' 5d volume')
        fig4.legend.location='top_left'
        fig4.background_fill_color = "lightgray"

        fig41 = figure(frame_width=1300, frame_height=155, x_range=fig1.x_range)
        fig41.quad(left=strike-bar_width, right=strike, bottom=0, top=put_oi-put_oi_5d, fill_color='darkgreen')
        fig41.quad(left=strike, right=strike+bar_width, bottom=0, top=call_oi-call_oi_5d, fill_color='red')
        fig41.line(x=[fut_price_5d, fut_price_5d], y=[np.nanmin(call_oi-call_oi_5d), np.nanmax(call_oi-call_oi_5d)], line_width=1, line_color='black', legend_label=inst_id + ' oi 5d change')
        fig41.legend.location='top_left'

        show(column(fig1,fig11,fig2,fig21,fig3,fig31,fig4,fig41))


def plot_fef_3pm():
    path = os.path.join(future_price_dir, 'dce', 'i'+'.csv')
    df1 = pd.read_csv(path, header=[0,1])
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    c2_dce_id = np.array(df1['c2']['inst_id'], dtype=str)
    c2_dce = np.array(df1['c2']['close'], dtype=float)
    c3_dce_id = np.array(df1['c3']['inst_id'], dtype=str)
    c3_dce = np.array(df1['c3']['close'], dtype=float)

    path = os.path.join(future_price_dir, 'sgx', 'FEF_3PM'+'.csv')
    df2 = pd.read_csv(path, header=[0,1])
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time']['time'], format='%Y-%m-%d'))
    c2_sgx_id = np.array(df2['c2']['inst_id'], dtype=str)
    c2_sgx = np.array(df2['c2']['price'], dtype=float)
    c3_sgx_id = np.array(df2['c3']['inst_id'], dtype=str)
    c3_sgx = np.array(df2['c3']['price'], dtype=float)
    c4_sgx = np.array(df2['c4']['price'], dtype=float)


    idx1 = np.isin(t1, t2)
    idx2 = np.isin(t2, t1)

    t1 = t1[idx1]
    c2_dce_id = c2_dce_id[idx1]
    c2_dce = c2_dce[idx1]
    c3_dce_id = c3_dce_id[idx1]
    c3_dce = c3_dce[idx1]

    t2 = t2[idx2]
    c2_sgx_id = c2_sgx_id[idx2]
    c2_sgx = c2_sgx[idx2]
    c3_sgx_id = c3_sgx_id[idx2]
    c3_sgx = c3_sgx[idx2]
    c4_sgx = c4_sgx[idx2]

    _c2_sgx = c2_sgx.copy()
    _c3_sgx = c3_sgx.copy()

    ############################
    for i in range(len(t1)):
         if c2_sgx_id[i][3:7] != c2_dce_id[i][1:5]:
             _c2_sgx[i] = c3_sgx[i]

         if c3_sgx_id[i][3:7] != c3_dce_id[i][1:5]:
             _c3_sgx[i] = c4_sgx[i]

    compare_price_in_different_currency(t2, _c2_sgx, 'USD', t1, c2_dce, 'CNH', variety='i c2')
    compare_price_in_different_currency(t2, _c3_sgx, 'USD', t1, c3_dce, 'CNH', variety='i c3')



def plot_sgx_option_data(variety):
    plot_sgx_option_iv(variety)

    plot_sgx_option_strike_volume_oi(variety)


def update_sgx_fut_opt_data():
    update_sgx_future_data()
    update_sgx_fef_3pm_price()
    update_sgx_option_data()
    update_sgx_fut_option_info_detail('FEF')
    update_sgx_fut_option_info_detail('UC')


if __name__=="__main__":

    # update_treasury_yield()
    
    # update_sgx_fut_opt_data()

    # update_sgx_fef_3pm_price()

    # plot_fef_3pm()

    # plot_sgx_option_data('FEF')
    # plot_sgx_option_data('UC')



    pass
