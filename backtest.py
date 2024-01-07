import os
import threading
import time
import math
import numpy as np
import pandas as pd
import datetime
from scipy.stats import linregress
from utils import *
from cn_fut_opt import get_future_inst_id_data, get_option_last_trading_day


def find_middle_price(strike, put_oi, call_oi):
    diff = put_oi - call_oi

    try:
        w1 = np.where(diff >= 0)[0][-1]
        w2 = np.where(diff <= 0)[0][0]
    except:
        return None, None, None, None

    if ((w2 - w1) >= 0) and ((w2 - w1) <= 1):
        return strike[w1-1], strike[w1], strike[w2], strike[w2+1]
    else:
        return None, None, None, None



def backtest_option(exchange, variety):
    path = os.path.join(option_price_dir, exchange, variety+'_info'+'.csv')
    if not(os.path.exists(path)):
        return
    info_df = pd.read_csv(path)
    info_t = pd.DatetimeIndex(pd.to_datetime(info_df['time'], format='%Y-%m-%d'))
    L = len(info_df)
    dom = np.array(info_df['dom1'], dtype=str)

    path2 = os.path.join(future_price_dir, exchange, variety+'.csv')
    fut_df = pd.read_csv(path2, header=[0,1])
    fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))


    start_time = '2022-10-01'
    start_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')
    start_idx = np.where(info_t >= start_time_dt)[0][0]
    start_time_dt = info_t[start_idx]

    opt_dict = {}
    price_diff = []
    cs = ['c9','c8','c7','c6','c5','c4','c3','c2','c1']
    while (start_idx < L):
        inst_id = dom[start_idx]
        end_idx = start_idx
        while ((end_idx+1) < L):
            end_idx = end_idx + 1
            if dom[end_idx] != inst_id:
                end_idx = end_idx - 1
                break

        if (end_idx - start_idx) <= 7:
            start_idx = end_idx + 1
            continue

        print(info_t[start_idx], info_t[end_idx])
        # strike, oi
        if not(inst_id in opt_dict):
            try:
                path3 = os.path.join(option_price_dir, exchange, inst_id+'.csv')
            except:
                exit()
            opt_df = pd.read_csv(path3, header=[0,1,2])
            opt_t = pd.DatetimeIndex(pd.to_datetime(opt_df['time']['time']['time'], format='%Y-%m-%d'))

            col = opt_df.columns.tolist()

            temp = [(col[i][1]) for i in range(len(col)) if col[i][0] == 'P']
            res = []
            for i in temp:
                if i not in res:
                    res.append(i)
            strike_str = np.array(res, dtype=str)
            strike = np.array(res, dtype=float)
            # get ordered strike
            sort = np.argsort(strike)
            strike = strike[sort]
            strike_str = strike_str[sort]
            opt_dict[inst_id] = [opt_df, opt_t, strike, strike_str]
        
        opt_df = opt_dict[inst_id][0]
        opt_t = opt_dict[inst_id][1]
        strike = opt_dict[inst_id][2]
        strike_str = opt_dict[inst_id][3]
        call_oi = []
        put_oi = []

        data_list = []
        for i in range(start_idx+2, end_idx+1): # 跳过前两天
            w = np.where(opt_t == info_t[i])[0]
            if len(w) == 0:
                break
            else:
                w = w[0]
            opt_temp_df = opt_df.loc[w,:]
            
            # price
            try:
                w = np.where(fut_t == info_t[i])[0][0]
            except:
                print('fut_t == info_t[i], ', inst_id, info_t[i])
                exit()
            temp_df = fut_df.loc[w,:]
            fut_price = None
            for c in cs:
                if (temp_df[c]['inst_id'] == inst_id):
                    fut_price = (temp_df[c]['close'])
                    break

            # get oi
            put_oi = []
            call_oi = []
            for k in range(len(strike_str)):
                put_oi.append(opt_temp_df.loc[pd.IndexSlice['P', strike_str[k], 'oi']])
                call_oi.append(opt_temp_df.loc[pd.IndexSlice['C', strike_str[k], 'oi']])

            data_list.append([info_t[i], fut_price, np.array(put_oi, dtype=float), np.array(call_oi, dtype=float)])


        ###############################

        data_L = len(data_list)
        i = 0
        while (i < data_L):
            call2, call1, put1, put2 = find_middle_price(strike, data_list[i][2], data_list[i][3])
            # print(call2, call1, put1, put2)
            if call2 is None:
                i = i + 1
                continue

            dt = 7
            fut_price = data_list[i][1]
            if fut_price < call2:
                try:
                    w = np.where(fut_t == info_t[start_idx+2+i+dt])[0][0]
                except:
                    i = i + 1
                    continue
                temp_df = fut_df.loc[w,:]
                fut_price_later = None
                for c in cs:
                    if (temp_df[c]['inst_id'] == inst_id):
                        fut_price_later = (temp_df[c]['close'])
                        break

                fut_price_diff_long = fut_price_later - fut_price
                price_diff.append([data_list[i][0], 'long', fut_price_diff_long, fut_price, data_list[i][2], data_list[i][3], strike])


            if fut_price > put2:
                try:
                    w = np.where(fut_t == info_t[start_idx+2+i+dt])[0][0]
                except:
                    i = i + 1
                    continue
                temp_df = fut_df.loc[w,:]
                fut_price_later = None
                for c in cs:
                    if (temp_df[c]['inst_id'] == inst_id):
                        fut_price_later = (temp_df[c]['close'])
                        break
                print(info_t[i+dt], inst_id, fut_price_later)
                fut_price_diff_short = fut_price - fut_price_later

                price_diff.append([data_list[i][0], 'short', fut_price_diff_short, fut_price, data_list[i][2], data_list[i][3], strike])


            i = i + 1

        start_idx = end_idx + 1

    # summary
    sum = 0

    for i in range(len(price_diff)):
    # for i in range(20):
        strike = price_diff[i][6]
        gap = float((strike[1]-strike[0]) / 5)
        if gap == 0:
            gap = 1

        print(price_diff[i][0], price_diff[i][1], price_diff[i][2], price_diff[i][3])
        fig1 = figure(frame_width=1300, frame_height=400, title=price_diff[i][0].strftime('%Y-%m-%d') + '  ' + str(price_diff[i][2]))
        fig1.quad(left=strike-gap, right=strike, bottom=0, top=price_diff[i][4], fill_color='darkgreen')
        fig1.quad(left=strike, right=strike+gap, bottom=0, top=price_diff[i][5], fill_color='red')
        fig1.line(x=[price_diff[i][3], price_diff[i][3]], y=[0, np.nanmax(price_diff[i][4])], line_width=1, line_color='black')
        # fig1.legend.location='top_left'


        show(column(fig1))
        time.sleep(0.2)
        sum = sum + price_diff[i][2]

    print(sum)

      
def plot_monthly_option_data(exchange, variety, ym):
    inst_id = variety + ym

    path = os.path.join(option_price_dir, exchange, variety+'_info'+'.csv')
    if not(os.path.exists(path)):
        return
    info_df = pd.read_csv(path)
    info_t = pd.DatetimeIndex(pd.to_datetime(info_df['time'], format='%Y-%m-%d'))
    dom = np.array(info_df['dom1'], dtype=str)

    fut_t, o, h, l, c = get_future_inst_id_data(exchange, inst_id)

    idx = np.where(dom == inst_id)[0]
    data_t = info_t[idx]
    idx = np.where(fut_t >= data_t[0])[0]
    fut_t = fut_t[idx]
    o = o[idx]
    h = h[idx]
    l = l[idx]
    c = c[idx]
    print(l)
    fut_price = c

    try:
        path3 = os.path.join(option_price_dir, exchange, inst_id+'.csv')
        opt_df = pd.read_csv(path3, header=[0,1,2])
        opt_t = pd.DatetimeIndex(pd.to_datetime(opt_df['time']['time']['time'], format='%Y-%m-%d'))
        col = opt_df.columns.tolist()

        temp = [(col[i][1]) for i in range(len(col)) if col[i][0] == 'P']
        res = []
        for i in temp:
            if i not in res:
                res.append(i)
        strike_str = np.array(res, dtype=str)
        strike = np.array(res, dtype=float)
        # get ordered strike
        sort = np.argsort(strike)
        strike = strike[sort]
        strike_str = strike_str[sort]

        gap = float((strike[1]-strike[0]) / 5)
        if gap == 0:
            gap = 1
    except:
        return()

    call_oi_yd = None
    put_oi_yd = None

    for t in data_t:
        call_oi = []
        put_oi = []
        call_volume = []
        put_volume = []
        w = np.where(opt_t == t)[0]
        if len(w) > 0:
            w = w[0]
            opt_temp_df = opt_df.loc[w,:]

            for k in range(len(strike_str)):
                put_oi.append(opt_temp_df.loc[pd.IndexSlice['P', strike_str[k], 'oi']])
                call_oi.append(opt_temp_df.loc[pd.IndexSlice['C', strike_str[k], 'oi']])
                put_volume.append(opt_temp_df.loc[pd.IndexSlice['P', strike_str[k], 'volume']])
                call_volume.append(opt_temp_df.loc[pd.IndexSlice['C', strike_str[k], 'volume']])
            put_oi = np.array(put_oi)
            call_oi = np.array(call_oi)
            put_volume = np.array(put_volume)
            call_volume = np.array(call_volume)
            w = np.where(fut_t == t)[0][0]

            fig1 = figure(frame_width=1400, frame_height=220, title=inst_id + ' ' + t.strftime('%Y-%m-%d'))
            fig1.quad(left=strike-gap, right=strike, bottom=0, top=put_oi, fill_color='darkgreen')
            fig1.quad(left=strike, right=strike+gap, bottom=0, top=call_oi, fill_color='red')
            fig1.line(x=[fut_price[w], fut_price[w]], y=[0, np.nanmax(call_oi)], line_width=1, line_color='black')
            # fig1.legend.location='top_left'
            if call_oi_yd is not None:
                fig11 = figure(frame_width=1400, frame_height=220, x_range=fig1.x_range)
                fig11.quad(left=strike-gap, right=strike, bottom=0, top=put_oi-put_oi_yd, fill_color='darkgreen')
                fig11.quad(left=strike, right=strike+gap, bottom=0, top=call_oi-call_oi_yd, fill_color='red')
                fig11.line(x=[fut_price[w], fut_price[w]], y=[0, np.nanmax(call_oi-call_oi_yd)], line_width=1, line_color='black')

            fig2 = figure(frame_width=1400, frame_height=220)
            fig2.quad(left=strike-gap, right=strike, bottom=0, top=put_volume, fill_color='darkgreen')
            fig2.quad(left=strike, right=strike+gap, bottom=0, top=call_volume, fill_color='red')
            fig2.line(x=[fut_price[w], fut_price[w]], y=[0, np.nanmax(call_volume)], line_width=1, line_color='black')
           

            fig3 = plot_candle(fut_t, o, h, l, c, ret=True)
            fig3.circle(x=fut_t[w], y=fut_price[w], line_width=4, line_color='black')

            if call_oi_yd is not None:
                show(column(fig1,fig11,fig2,fig3))
            else:
                show(column(fig1,fig2,fig3))
            time.sleep(0.2)

            call_oi_yd = call_oi.copy()
            put_oi_yd = put_oi.copy()
            # return



def backtest_option2(exchange, variety):
    path = os.path.join(option_price_dir, exchange, variety+'_info'+'.csv')
    if not(os.path.exists(path)):
        return
    info_df = pd.read_csv(path)
    info_t = pd.DatetimeIndex(pd.to_datetime(info_df['time'], format='%Y-%m-%d'))
    L = len(info_df)
    dom = np.array(info_df['dom1'], dtype=str)

    path2 = os.path.join(future_price_dir, exchange, variety+'.csv')
    fut_df = pd.read_csv(path2, header=[0,1])
    fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))


    start_time = '2022-01-01'
    start_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')
    start_idx = np.where(info_t >= start_time_dt)[0][0]
    start_time_dt = info_t[start_idx]

    opt_dict = {}
    cs = ['c9','c8','c7','c6','c5','c4','c3','c2','c1']

    indicator = []
    while (start_idx < L):
        inst_id = dom[start_idx]
        end_idx = start_idx
        while ((end_idx+1) < L):
            end_idx = end_idx + 1
            if dom[end_idx] != inst_id:
                end_idx = end_idx - 1
                break

        if (end_idx - start_idx) <= 7:
            start_idx = end_idx + 1
            continue

        print(info_t[start_idx], info_t[end_idx])
        # strike, oi
        if not(inst_id in opt_dict):
            try:
                path3 = os.path.join(option_price_dir, exchange, inst_id+'.csv')
            except:
                exit()
            opt_df = pd.read_csv(path3, header=[0,1,2])
            opt_t = pd.DatetimeIndex(pd.to_datetime(opt_df['time']['time']['time'], format='%Y-%m-%d'))

            col = opt_df.columns.tolist()

            temp = [(col[i][1]) for i in range(len(col)) if col[i][0] == 'P']
            res = []
            for i in temp:
                if i not in res:
                    res.append(i)
            strike_str = np.array(res, dtype=str)
            strike = np.array(res, dtype=float)
            # get ordered strike
            sort = np.argsort(strike)
            strike = strike[sort]
            strike_str = strike_str[sort]
            opt_dict[inst_id] = [opt_df, opt_t, strike, strike_str]
        
        opt_df = opt_dict[inst_id][0]
        opt_t = opt_dict[inst_id][1]
        strike = opt_dict[inst_id][2]
        strike_str = opt_dict[inst_id][3]
        call_oi = []
        put_oi = []
        data_list = []

        for i in range(start_idx, end_idx+1): # 跳过前两天
            w = np.where(opt_t == info_t[i])[0]
            if len(w) == 0:
                break
            else:
                w1 = w[0]
            
            # price
            try:
                w = np.where(fut_t == info_t[i])[0][0]
            except:
                print('fut_t == info_t[i], ', inst_id, info_t[i])
                exit()
            temp_df = fut_df.loc[w,:]
            fut_price = None
            for c in cs:
                if (temp_df[c]['inst_id'] == inst_id):
                    fut_price = (temp_df[c]['close'])
                    break

            # get oi
            put_oi = []
            call_oi = []
            put_volume = []
            call_volume = []
            opt_temp_df = opt_df.loc[w1,:]
            for k in range(len(strike_str)):
                put_oi.append(opt_temp_df.loc[pd.IndexSlice['P', strike_str[k], 'oi']])
                call_oi.append(opt_temp_df.loc[pd.IndexSlice['C', strike_str[k], 'oi']])
                put_volume.append(opt_temp_df.loc[pd.IndexSlice['P', strike_str[k], 'volume']])
                call_volume.append(opt_temp_df.loc[pd.IndexSlice['C', strike_str[k], 'volume']])
            data_list.append([info_t[i], fut_price, 
                              np.array(put_oi, dtype=float), np.array(call_oi, dtype=float),
                              np.array(put_volume, dtype=float), np.array(call_volume, dtype=float)])


        ###############################
        data_L = len(data_list)
        thres = 0.15
        for i in range(2, data_L-2):
            put_oi_diff_1d = data_list[i][2] - data_list[i-1][2]
            put_oi_diff_2d = data_list[i][2] - data_list[i-2][2]
            call_oi_diff_1d = data_list[i][3] - data_list[i-1][3]
            call_oi_diff_2d = data_list[i][3] - data_list[i-2][3]

            put_volume_1d = data_list[i][4] + 1
            put_volume_2d = data_list[i][4] + data_list[i-1][4] + 1
            call_volume_1d = data_list[i][5] + 1
            call_volume_2d = data_list[i][5] + data_list[i-1][5] + 1

            price_dff_1d = data_list[i][1] - data_list[i-1][1]
            price_dff_1d_later = data_list[i+1][1] - data_list[i][1]
            price_dff_2d_later = data_list[i+2][1] - data_list[i][1]

            tmp = (put_oi_diff_1d)
            # tmp = (put_volume_1d)
            # adjust = np.log10(put_volume_1d)
            # tmp *= (adjust+1)
            # tmp -= adjust
            put_resistence_1d = np.sum(tmp)

            tmp = (put_oi_diff_2d)
            # tmp = (put_volume_2d)
            # adjust = np.log10(put_volume_2d)
            # tmp *= (adjust+1)
            # tmp -= adjust
            put_resistence_2d = np.sum(tmp)

            tmp = (call_oi_diff_1d)
            # tmp = (call_volume_1d)
            # adjust = np.log10(call_volume_1d)
            # tmp *= (adjust+1)
            # tmp -= adjust
            call_resistence_1d = np.sum(tmp)

            tmp = (call_oi_diff_2d)
            # tmp = (call_volume_2d)
            # adjust = np.log10(call_volume_2d)
            # tmp *= (adjust+1)
            # tmp -= adjust
            call_resistence_2d = np.sum(tmp)

            strength_1d = put_resistence_1d - call_resistence_1d
            strength_2d = put_resistence_2d - call_resistence_2d

            indicator.append([data_list[i][0], price_dff_1d, price_dff_1d_later, price_dff_2d_later,
                              put_resistence_1d, put_resistence_2d, call_resistence_1d, call_resistence_2d,
                              strength_1d, strength_2d])

        start_idx = end_idx + 1

    # summary
    df = pd.DataFrame(columns=['time', 'price_dff_1d', 'price_dff_1d_later', 'price_dff_2d_later',
                              'put_resistence_1d', 'put_resistence_2d', 'call_resistence_1d', 'call_resistence_2d',
                              'strength_1d', 'strength_2d'],
                      data=indicator)

    t = np.array(df['time'], dtype=pd.DatetimeIndex)
    price_dff_1d = np.array(df['price_dff_1d'])
    price_dff_1d_later = np.array(df['price_dff_1d_later'])
    price_dff_2d_later = np.array(df['price_dff_2d_later'])

    put_resistence_1d = np.array(df['put_resistence_1d'])
    put_resistence_2d = np.array(df['put_resistence_2d'])
    call_resistence_1d = np.array(df['call_resistence_1d'])
    call_resistence_2d = np.array(df['call_resistence_2d'])

    strength_1d = np.array(df['strength_1d'])
    strength_2d = np.array(df['strength_2d'])

    datas = [[t, price_dff_1d, 'price_dff_1d', ''], 
             [t, strength_1d, 'strength_1d', '']]
    fig1 = plot_circle(datas, start_time=start_time, ret=True)

    datas = [[t, price_dff_1d_later, 'price_dff_1d_later', ''], 
             [t, strength_1d, 'strength_1d', '']]
    fig2 = plot_circle(datas, start_time=start_time, ret=True)

    datas = [[t, price_dff_1d, 'price_dff_1d', ''], 
             [t, strength_2d, 'strength_2d', '']]
    fig3 = plot_circle(datas, start_time=start_time, ret=True)

    show(row(fig1,fig3))


    # datas = [[t, price_dff_1d_later, 'price_dff_1d_later', ''], 
    #          [t, put_resistence_1d, 'put_resistence_1d', '']]
    # fig1 = plot_circle(datas, start_time=start_time, ret=True)

    # datas = [[t, price_dff_1d_later, 'price_dff_1d_later', ''], 
    #          [t, call_resistence_1d, 'call_resistence_1d', '']]
    # fig2 = plot_circle(datas, start_time=start_time, ret=True)

    # show(row(fig1,fig2))


# 末日期权
def backtest_option3(variety):
    exchange = None
    for ex in exchange_dict:
        if variety in exchange_dict[ex]:
            exchange = ex
            break
    
    if exchange is None:
        return
    
    path = os.path.join(future_price_dir, exchange, variety+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))


    cs = ['c1','c2','c3','c4','c5','c6','c7','c8','c9']
    gains = []
    for _, _, files in os.walk(os.path.join(option_price_dir, exchange)):
        for file in files:
            if (file[0:len(variety)] == variety) and (file[len(variety)].isdigit()) and (not('intraday' in file)):
                path = os.path.join(option_price_dir, exchange, file)
                inst_id = file.split('.')[0]
                opt_df = pd.read_csv(path, header=[0,1,2])
                opt_t = pd.DatetimeIndex(pd.to_datetime(opt_df['time']['time']['time'], format='%Y-%m-%d'))

                col = opt_df.columns.tolist()
                put_strike = [(col[i][1]) for i in range(len(col)) if col[i][0] == 'P']
                # call_strike = [(col[i][1]) for i in range(len(col)) if col[i][0] == 'C']

                strike_str = []
                for i in put_strike:
                    if i not in strike_str:
                        strike_str.append(i)
                strike_str = np.array(strike_str, dtype=str)
                strike = np.array(strike_str, dtype=float)

                sort = np.argsort(strike)
                strike = strike[sort]
                strike_str = strike_str[sort]

                
                last_day = get_option_last_trading_day(inst_id)
                last_day_dt = pd.to_datetime(last_day, format='%Y-%m-%d')

                if not(last_day_dt in fut_t):
                    print(last_day + ' not in ' + inst_id + ' future')
                    continue

                if not(last_day_dt in opt_t):
                    print(last_day + 'not in ' + inst_id + ' option')
                    continue

                fut_idx = np.where(fut_t == last_day_dt)[0][0]
                # fut ex0
                cn = None
                for c in cs:
                    if fut_df.loc[fut_idx, pd.IndexSlice[c,'inst_id']] == inst_id:
                        cn = c
                        break
                if cn is not None:
                    fut_close_ex0 = fut_df.loc[fut_idx, pd.IndexSlice[cn,'close']]
                else:
                    print(inst_id + 'fut ex0 no data')
                    continue

                # fut ex10
                cn = None
                for c in cs:
                    if fut_df.loc[fut_idx-10, pd.IndexSlice[c,'inst_id']] == inst_id:
                        cn = c
                        break
                if cn is not None:
                    fut_close_ex10 = fut_df.loc[fut_idx-10, pd.IndexSlice[cn,'close']]
                else:
                    print(inst_id + 'fut ex10 no data')
                    continue

                # fut ex1
                cn = None
                for c in cs:
                    if fut_df.loc[fut_idx-1, pd.IndexSlice[c,'inst_id']] == inst_id:
                        cn = c
                        break
                if cn is not None:
                    # fut_open_ex1 = fut_df.loc[fut_idx-1, pd.IndexSlice[cn,'open']]
                    # fut_high_ex1 = fut_df.loc[fut_idx-1, pd.IndexSlice[cn,'high']]
                    # fut_low_ex1 = fut_df.loc[fut_idx-1, pd.IndexSlice[cn,'low']]
                    fut_close_ex1 = fut_df.loc[fut_idx-1, pd.IndexSlice[cn,'close']]
                else:
                    print(inst_id + 'fut ex1 no data')
                    continue

                opt_idx = np.where(opt_t == last_day_dt)[0][0]
                # opt
                call_strike_str1 = strike_str[np.where(strike >= fut_close_ex1)[0][0]]
                put_strike_str1 = strike_str[np.where(strike <= fut_close_ex1)[0][-1]]

                call_strike_str2 = call_strike_str1
                put_strike_str2 = strike_str[np.where(strike <= fut_close_ex1)[0][-2]]   

                call_strike_str3 = strike_str[np.where(strike >= fut_close_ex1)[0][1]]
                put_strike_str3 = put_strike_str1


                call_gain1 = opt_df.loc[opt_idx, pd.IndexSlice['C', call_strike_str1, 'high']] / \
                             opt_df.loc[opt_idx-1, pd.IndexSlice['C', call_strike_str1, 'close']] - 1
                put_gain1 = opt_df.loc[opt_idx, pd.IndexSlice['P', put_strike_str1, 'high']] / \
                            opt_df.loc[opt_idx-1, pd.IndexSlice['P', put_strike_str1, 'close']] - 1

                call_gain2 = opt_df.loc[opt_idx, pd.IndexSlice['C', call_strike_str2, 'high']] / \
                             opt_df.loc[opt_idx-1, pd.IndexSlice['C', call_strike_str2, 'close']] - 1
                put_gain2 = opt_df.loc[opt_idx, pd.IndexSlice['P', put_strike_str2, 'high']] / \
                            opt_df.loc[opt_idx-1, pd.IndexSlice['P', put_strike_str2, 'close']] - 1

                call_gain3 = opt_df.loc[opt_idx, pd.IndexSlice['C', call_strike_str3, 'high']] / \
                             opt_df.loc[opt_idx-1, pd.IndexSlice['C', call_strike_str3, 'close']] - 1
                put_gain3 = opt_df.loc[opt_idx, pd.IndexSlice['P', put_strike_str3, 'high']] / \
                            opt_df.loc[opt_idx-1, pd.IndexSlice['P', put_strike_str3, 'close']] - 1
                
                gains.append([inst_id, call_gain1, put_gain1, call_gain2, put_gain2, call_gain3, put_gain3])

                print(inst_id)
                print('fut ex10 close:', fut_close_ex10, 'ex1 close:', fut_close_ex1, 'ex0 close:', fut_close_ex0)
                print('strike1:', put_strike_str1, put_gain1, call_strike_str1, call_gain1)
                print('strike2:', put_strike_str2, put_gain2, call_strike_str2, call_gain2)
                print('strike3:', put_strike_str3, put_gain3, call_strike_str3, call_gain3)
                print('')


if __name__=="__main__":
    # backtest_option2('shfe', 'al')
    backtest_option3('v')

    # plot_monthly_option_data('dce', 'b', '2308')
    # backtest_option('dce', 'pg')

