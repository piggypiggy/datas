import os
import time
import numpy as np
import pandas as pd
import datetime
from bokeh.io import output_file, show
from bokeh.layouts import column, row
from bokeh.plotting import figure
from bokeh.models import LinearAxis, Range1d, VBar, NumeralTickFormatter
from scipy.stats import linregress
from utils import *
from cn_fut_opt import get_option_last_trading_day
from position import *
from intraday import plot_intraday_dominant_option_datas
from spot import *
from sgx_fut_opt import *
from nasdaq import *
from black import *
from sge import *
from msci import *
from vix import *
from lme import plot_lme_option_data
from lbma import *
from us_debt import *

# 25D RR = [IV(25delta) - IV(-25delta)] / IV(50delta) OR IV(ATM)

# SK10 = [Vol(ATM) – Vol(ATM – 10%))/sqrt(DTE)


def near_atm_price_chg(df, fut_close, strike):
    idx_offset = df.index[0]
    strike_sort = np.sort(strike)

    i = np.searchsorted(strike_sort, fut_close)
    r = (strike_sort[i]-fut_close)/(fut_close-strike_sort[i-1])
    # print(strike_sort, fut_close, i)
    strike_list = [] # [put, call]
    if ((strike[i] == fut_close) or (r < 1/3) or (r > 3)):
        strike_list.append([strike_sort[i], strike_sort[i]])
        strike_list.append([strike_sort[i-1], strike_sort[i+1]])
        strike_list.append([strike_sort[i-2], strike_sort[i+2]])
        strike_list.append([strike_sort[i-3], strike_sort[i+3]])
        strike_list.append([strike_sort[i-4], strike_sort[i+4]])
        # strike_list.append([strike_sort[i-5], strike_sort[i+5]])
        # strike_list.append([strike_sort[i-6], strike_sort[i+6]])
    else:
        strike_list.append([strike_sort[i-1], strike_sort[i]])
        strike_list.append([strike_sort[i-2], strike_sort[i+1]])
        strike_list.append([strike_sort[i-3], strike_sort[i+2]])
        strike_list.append([strike_sort[i-4], strike_sort[i+3]])
        strike_list.append([strike_sort[i-5], strike_sort[i+4]])
        # strike_list.append([strike_sort[i-6], strike_sort[i+5]])
        # strike_list.append([strike_sort[i-7], strike_sort[i+6]])

    ret_list = [] # put_close_pre+call_close_pre, put_high+call_low 1day, put_low+call_high 1day, put_high+call_low 2day, put_low+call_high 2day, put_close+call_close 2day
    for i in range(len(strike_list)):
        ret_list.append(df.loc[0+idx_offset, pd.IndexSlice['P', str(int(strike_list[i][0])), 'close']] + df.loc[0+idx_offset, pd.IndexSlice['C', str(int(strike_list[i][1])), 'close']])
        ret_list.append(df.loc[1+idx_offset, pd.IndexSlice['P', str(int(strike_list[i][0])), 'high']] + df.loc[1+idx_offset, pd.IndexSlice['C', str(int(strike_list[i][1])), 'low']])
        ret_list.append(df.loc[1+idx_offset, pd.IndexSlice['P', str(int(strike_list[i][0])), 'low']] + df.loc[1+idx_offset, pd.IndexSlice['C', str(int(strike_list[i][1])), 'high']])
        ret_list.append(df.loc[1+idx_offset, pd.IndexSlice['P', str(int(strike_list[i][0])), 'close']] + df.loc[1+idx_offset, pd.IndexSlice['C', str(int(strike_list[i][1])), 'close']])
        # ret_list.append(df.loc[2+idx_offset, pd.IndexSlice['P', str(int(strike_list[i][0])), 'high']] + df.loc[2+idx_offset, pd.IndexSlice['C', str(int(strike_list[i][1])), 'low']])
        # ret_list.append(df.loc[2+idx_offset, pd.IndexSlice['P', str(int(strike_list[i][0])), 'low']] + df.loc[2+idx_offset, pd.IndexSlice['C', str(int(strike_list[i][1])), 'high']])
        # ret_list.append(df.loc[2+idx_offset, pd.IndexSlice['P', str(int(strike_list[i][0])), 'close']] + df.loc[2+idx_offset, pd.IndexSlice['C', str(int(strike_list[i][1])), 'close']])

    # print(ret_list)
    return ret_list


def get_option_atm_price(exchange, inst_id, fut_price):
    path = os.path.join(option_price_dir, exchange, inst_id+'.csv')
    df = pd.read_csv(path, header=[0,1,2])
    t = pd.DatetimeIndex(pd.to_datetime(df['time']['time']['time'], format='%Y-%m-%d'))

    col = df.columns.tolist()
    strike = [(col[i][1]) for i in range(len(col)) if col[i][0] == 'P']

    res = []
    for i in strike:
        if i not in res:
            res.append(i)
    strikes_str = np.array(res, dtype=str)
    strikes = np.array(strikes_str, dtype=float)

    sort = np.argsort(strikes)
    strikes = strikes[sort]
    strikes_str = strikes_str[sort]

    w_call = np.where(strikes >= fut_price)[0][0]
    w_put = np.where(strikes <= fut_price)[0][-1]

    call_strike_str = strikes_str[w_call]
    put_strike_str = strikes_str[w_put]

    call_atm_price = np.array(df.loc[:, pd.IndexSlice['C', call_strike_str, 'close']], dtype=float)
    put_atm_price = np.array(df.loc[:, pd.IndexSlice['P', put_strike_str, 'close']], dtype=float)

    call_atm_price[call_atm_price == 0] = np.nan
    put_atm_price[put_atm_price == 0] = np.nan

    return t, call_atm_price, put_atm_price


def plot_dominant_option_datas1(exchange, variety):
    path1 = os.path.join(option_price_dir, exchange, variety+'_info'+'.csv')
    if not(os.path.exists(path1)):
        return

    print(variety)

    path2 = os.path.join(future_price_dir, exchange, variety+'.csv')
    fut_df = pd.read_csv(path2, header=[0,1])
    fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    index_price = np.array(fut_df['index']['close'], dtype=float)
    index_volume = np.array(fut_df['index']['vol'], dtype=float)
    index_oi = np.array(fut_df['index']['oi'], dtype=float)
    dom_contract = np.array(fut_df['dom']['inst_id'], dtype=str)
    _dom_price = np.array(fut_df['dom']['close'], dtype=float)

    path3 = os.path.join(option_price_dir, exchange, variety+'_info_detail'+'.csv')
    df = pd.read_csv(path3)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))

    t1 = []
    dom_price = []
    c_40d_put_iv = []
    c_40d_call_iv = []
    c_25d_put_iv = []
    c_25d_call_iv = []
    c_10d_put_iv = []
    c_10d_call_iv = []
    c_5d_put_iv = []
    c_5d_call_iv = []
    c_atm_put_iv = []
    c_atm_call_iv = []

    put_volume_max1 = []
    put_volume_max1_strike = []
    put_volume_max2 = []
    put_volume_max2_strike = []
    put_volume_max3 = []
    put_volume_max3_strike = []
    put_volume_max4 = []
    put_volume_max4_strike = []
    put_volume_max5 = []
    put_volume_max5_strike = []

    call_volume_max1 = []
    call_volume_max1_strike = []
    call_volume_max2 = []
    call_volume_max2_strike = []
    call_volume_max3 = []
    call_volume_max3_strike = []
    call_volume_max4 = []
    call_volume_max4_strike = []
    call_volume_max5 = []
    call_volume_max5_strike = []

    put_oi_max1 = []
    put_oi_max1_strike = []
    put_oi_max2 = []
    put_oi_max2_strike = []
    put_oi_max3 = []
    put_oi_max3_strike = []
    put_oi_max4 = []
    put_oi_max4_strike = []
    put_oi_max5 = []
    put_oi_max5_strike = []

    call_oi_max1 = []
    call_oi_max1_strike = []
    call_oi_max2 = []
    call_oi_max2_strike = []
    call_oi_max3 = []
    call_oi_max3_strike = []
    call_oi_max4 = []
    call_oi_max4_strike = []
    call_oi_max5 = []
    call_oi_max5_strike = []

    for i in range(len(fut_t)):
        inst_id = dom_contract[i]

        try:
            w = np.where(t == fut_t[i])[0][0]
        except:
            continue

        if (df.loc[w, 'dom1'] == inst_id):
            col = 'dom1'
        elif (df.loc[w, 'dom2'] == inst_id):
            col = 'dom2'
        elif (df.loc[w, 'dom3'] == inst_id):
            col = 'dom3'
        else:
            continue

        t1.append(fut_t[i])
        dom_price.append(_dom_price[i])
        c_40d_put_iv.append(df.loc[w, col+'_'+'c_40d_put_iv'])
        c_40d_call_iv.append(df.loc[w, col+'_'+'c_40d_call_iv'])
        c_25d_put_iv.append(df.loc[w, col+'_'+'c_25d_put_iv'])
        c_25d_call_iv.append(df.loc[w, col+'_'+'c_25d_call_iv'])
        c_10d_put_iv.append(df.loc[w, col+'_'+'c_10d_put_iv'])
        c_10d_call_iv.append(df.loc[w, col+'_'+'c_10d_call_iv'])
        c_5d_put_iv.append(df.loc[w, col+'_'+'c_5d_put_iv'])
        c_5d_call_iv.append(df.loc[w, col+'_'+'c_5d_call_iv'])
        c_atm_put_iv.append(df.loc[w, col+'_'+'c_atm_put_iv'])
        c_atm_call_iv.append(df.loc[w, col+'_'+'c_atm_call_iv'])

        put_volume_max1.append(df.loc[w, col+'_'+'put_volume_max1'])
        put_volume_max1_strike.append(df.loc[w, col+'_'+'put_volume_max1_strike'])
        put_volume_max2.append(df.loc[w, col+'_'+'put_volume_max2'])
        put_volume_max2_strike.append(df.loc[w, col+'_'+'put_volume_max2_strike'])
        put_volume_max3.append(df.loc[w, col+'_'+'put_volume_max3'])
        put_volume_max3_strike.append(df.loc[w, col+'_'+'put_volume_max3_strike'])
        put_volume_max4.append(df.loc[w, col+'_'+'put_volume_max4'])
        put_volume_max4_strike.append(df.loc[w, col+'_'+'put_volume_max4_strike'])
        put_volume_max5.append(df.loc[w, col+'_'+'put_volume_max5'])
        put_volume_max5_strike.append(df.loc[w, col+'_'+'put_volume_max5_strike'])

        call_volume_max1.append(df.loc[w, col+'_'+'call_volume_max1'])
        call_volume_max1_strike.append(df.loc[w, col+'_'+'call_volume_max1_strike'])
        call_volume_max2.append(df.loc[w, col+'_'+'call_volume_max2'])
        call_volume_max2_strike.append(df.loc[w, col+'_'+'call_volume_max2_strike'])
        call_volume_max3.append(df.loc[w, col+'_'+'call_volume_max3'])
        call_volume_max3_strike.append(df.loc[w, col+'_'+'call_volume_max3_strike'])
        call_volume_max4.append(df.loc[w, col+'_'+'call_volume_max4'])
        call_volume_max4_strike.append(df.loc[w, col+'_'+'call_volume_max4_strike'])
        call_volume_max5.append(df.loc[w, col+'_'+'call_volume_max5'])
        call_volume_max5_strike.append(df.loc[w, col+'_'+'call_volume_max5_strike'])

        put_oi_max1.append(df.loc[w, col+'_'+'put_oi_max1'])
        put_oi_max1_strike.append(df.loc[w, col+'_'+'put_oi_max1_strike'])
        put_oi_max2.append(df.loc[w, col+'_'+'put_oi_max2'])
        put_oi_max2_strike.append(df.loc[w, col+'_'+'put_oi_max2_strike'])
        put_oi_max3.append(df.loc[w, col+'_'+'put_oi_max3'])
        put_oi_max3_strike.append(df.loc[w, col+'_'+'put_oi_max3_strike'])
        put_oi_max4.append(df.loc[w, col+'_'+'put_oi_max4'])
        put_oi_max4_strike.append(df.loc[w, col+'_'+'put_oi_max4_strike'])
        put_oi_max5.append(df.loc[w, col+'_'+'put_oi_max5'])
        put_oi_max5_strike.append(df.loc[w, col+'_'+'put_oi_max5_strike'])

        call_oi_max1.append(df.loc[w, col+'_'+'call_oi_max1'])
        call_oi_max1_strike.append(df.loc[w, col+'_'+'call_oi_max1_strike'])
        call_oi_max2.append(df.loc[w, col+'_'+'call_oi_max2'])
        call_oi_max2_strike.append(df.loc[w, col+'_'+'call_oi_max2_strike'])
        call_oi_max3.append(df.loc[w, col+'_'+'call_oi_max3'])
        call_oi_max3_strike.append(df.loc[w, col+'_'+'call_oi_max3_strike'])
        call_oi_max4.append(df.loc[w, col+'_'+'call_oi_max4'])
        call_oi_max4_strike.append(df.loc[w, col+'_'+'call_oi_max4_strike'])
        call_oi_max5.append(df.loc[w, col+'_'+'call_oi_max5'])
        call_oi_max5_strike.append(df.loc[w, col+'_'+'call_oi_max5_strike'])

    t1 = np.array(t1)
    dom_price = np.array(dom_price)
    contract1 = inst_id
    # t_dom, dom_call_atm_price, dom_put_atm_price = get_option_atm_price(exchange, contract1, dom_price[-1])

    c_40d_put_iv = np.array(c_40d_put_iv, dtype=float)
    c_40d_call_iv = np.array(c_40d_call_iv, dtype=float)
    c_25d_put_iv = np.array(c_25d_put_iv, dtype=float)
    c_25d_call_iv = np.array(c_25d_call_iv, dtype=float)
    c_10d_put_iv = np.array(c_10d_put_iv, dtype=float)
    c_10d_call_iv = np.array(c_10d_call_iv, dtype=float)
    c_5d_put_iv = np.array(c_5d_put_iv, dtype=float)
    c_5d_call_iv = np.array(c_5d_call_iv, dtype=float)
    c_atm_put_iv = np.array(c_atm_put_iv, dtype=float)
    c_atm_call_iv = np.array(c_atm_call_iv, dtype=float)


    put_volume_max1= np.array(put_volume_max1, dtype=float)
    put_volume_max1_strike= np.array(put_volume_max1_strike, dtype=float)
    put_volume_max2= np.array(put_volume_max2, dtype=float)
    put_volume_max2_strike= np.array(put_volume_max2_strike, dtype=float)
    put_volume_max3= np.array(put_volume_max3, dtype=float)
    put_volume_max3_strike= np.array(put_volume_max3_strike, dtype=float)
    put_volume_max4= np.array(put_volume_max4, dtype=float)
    put_volume_max4_strike= np.array(put_volume_max4_strike, dtype=float)
    put_volume_max5= np.array(put_volume_max5, dtype=float)
    put_volume_max5_strike= np.array(put_volume_max5_strike, dtype=float)

    call_volume_max1= np.array(call_volume_max1, dtype=float)
    call_volume_max1_strike= np.array(call_volume_max1_strike, dtype=float)
    call_volume_max2= np.array(call_volume_max2, dtype=float)
    call_volume_max2_strike= np.array(call_volume_max2_strike, dtype=float)
    call_volume_max3= np.array(call_volume_max3, dtype=float)
    call_volume_max3_strike= np.array(call_volume_max3_strike, dtype=float)
    call_volume_max4= np.array(call_volume_max4, dtype=float)
    call_volume_max4_strike= np.array(call_volume_max4_strike, dtype=float)
    call_volume_max5= np.array(call_volume_max5, dtype=float)
    call_volume_max5_strike= np.array(call_volume_max5_strike, dtype=float)

    put_oi_max1= np.array(put_oi_max1, dtype=float)
    put_oi_max1_strike= np.array(put_oi_max1_strike, dtype=float)
    put_oi_max2= np.array(put_oi_max2, dtype=float)
    put_oi_max2_strike= np.array(put_oi_max2_strike, dtype=float)
    put_oi_max3= np.array(put_oi_max3, dtype=float)
    put_oi_max3_strike= np.array(put_oi_max3_strike, dtype=float)
    put_oi_max4= np.array(put_oi_max4, dtype=float)
    put_oi_max4_strike= np.array(put_oi_max4_strike, dtype=float)
    put_oi_max5= np.array(put_oi_max5, dtype=float)
    put_oi_max5_strike= np.array(put_oi_max5_strike, dtype=float)

    call_oi_max1= np.array(call_oi_max1, dtype=float)
    call_oi_max1_strike= np.array(call_oi_max1_strike, dtype=float)
    call_oi_max2= np.array(call_oi_max2, dtype=float)
    call_oi_max2_strike= np.array(call_oi_max2_strike, dtype=float)
    call_oi_max3= np.array(call_oi_max3, dtype=float)
    call_oi_max3_strike= np.array(call_oi_max3_strike, dtype=float)
    call_oi_max4= np.array(call_oi_max4, dtype=float)
    call_oi_max4_strike= np.array(call_oi_max4_strike, dtype=float)
    call_oi_max5= np.array(call_oi_max5, dtype=float)
    call_oi_max5_strike= np.array(call_oi_max5_strike, dtype=float)

    c_atm_iv = (c_atm_put_iv + c_atm_call_iv)/2
    c_40d_skew_iv = c_40d_put_iv - c_40d_call_iv
    c_25d_skew_iv = c_25d_put_iv - c_25d_call_iv
    c_10d_skew_iv = c_10d_put_iv - c_10d_call_iv
    c_5d_skew_iv = c_5d_put_iv - c_5d_call_iv

    # 期权主力 
    t2 = t
    contracts2 = df['dom1']
    t22 = []
    dom_v_price = []
    cs = ['c1','c2','c3','c4','c5','c6','c7','c8']
    for i in range(len(t2)):
        w = np.where(fut_t == t2[i])[0][0]
        for c in cs:
            if (fut_df.loc[w, pd.IndexSlice[c, 'inst_id']] == contracts2[i]):
                t22.append(t2[i])
                dom_v_price.append(fut_df.loc[w, pd.IndexSlice[c, 'close']])

    t22 = np.array(t22)
    dom_v_price = np.array(dom_v_price)

    maxv_contract = np.array(df['dom1'], dtype=str)
    contract2 = df.loc[len(df)-1, 'dom1']

    # t_maxv, maxv_call_atm_price, maxv_put_atm_price = get_option_atm_price(exchange, contract2, dom_v_price[-1])

    c_40d_put_iv_v = np.array(df['dom1_c_40d_put_iv'], dtype=float)
    c_40d_call_iv_v = np.array(df['dom1_c_40d_call_iv'], dtype=float)
    c_25d_put_iv_v = np.array(df['dom1_c_25d_put_iv'], dtype=float)
    c_25d_call_iv_v = np.array(df['dom1_c_25d_call_iv'], dtype=float)
    c_10d_put_iv_v = np.array(df['dom1_c_10d_put_iv'], dtype=float)
    c_10d_call_iv_v = np.array(df['dom1_c_10d_call_iv'], dtype=float)
    c_5d_put_iv_v = np.array(df['dom1_c_5d_put_iv'], dtype=float)
    c_5d_call_iv_v = np.array(df['dom1_c_5d_call_iv'], dtype=float)
    c_atm_put_iv_v = np.array(df['dom1_c_atm_put_iv'], dtype=float)
    c_atm_call_iv_v = np.array(df['dom1_c_atm_call_iv'], dtype=float)

    put_volume_max1_v = np.array(df['dom1_put_volume_max1'], dtype=float)
    put_volume_max1_strike_v = np.array(df['dom1_put_volume_max1_strike'], dtype=float)
    put_volume_max2_v = np.array(df['dom1_put_volume_max2'], dtype=float)
    put_volume_max2_strike_v = np.array(df['dom1_put_volume_max2_strike'], dtype=float)
    put_volume_max3_v = np.array(df['dom1_put_volume_max3'], dtype=float)
    put_volume_max3_strike_v = np.array(df['dom1_put_volume_max3_strike'], dtype=float)
    put_volume_max4_v = np.array(df['dom1_put_volume_max4'], dtype=float)
    put_volume_max4_strike_v = np.array(df['dom1_put_volume_max4_strike'], dtype=float)
    put_volume_max5_v = np.array(df['dom1_put_volume_max5'], dtype=float)
    put_volume_max5_strike_v = np.array(df['dom1_put_volume_max5_strike'], dtype=float)

    call_volume_max1_v = np.array(df['dom1_call_volume_max1'], dtype=float)
    call_volume_max1_strike_v = np.array(df['dom1_call_volume_max1_strike'], dtype=float)
    call_volume_max2_v = np.array(df['dom1_call_volume_max2'], dtype=float)
    call_volume_max2_strike_v = np.array(df['dom1_call_volume_max2_strike'], dtype=float)
    call_volume_max3_v = np.array(df['dom1_call_volume_max3'], dtype=float)
    call_volume_max3_strike_v = np.array(df['dom1_call_volume_max3_strike'], dtype=float)
    call_volume_max4_v = np.array(df['dom1_call_volume_max4'], dtype=float)
    call_volume_max4_strike_v = np.array(df['dom1_call_volume_max4_strike'], dtype=float)
    call_volume_max5_v = np.array(df['dom1_call_volume_max5'], dtype=float)
    call_volume_max5_strike_v = np.array(df['dom1_call_volume_max5_strike'], dtype=float)

    put_oi_max1_v = np.array(df['dom1_put_oi_max1'], dtype=float)
    put_oi_max1_strike_v = np.array(df['dom1_put_oi_max1_strike'], dtype=float)
    put_oi_max2_v = np.array(df['dom1_put_oi_max2'], dtype=float)
    put_oi_max2_strike_v = np.array(df['dom1_put_oi_max2_strike'], dtype=float)
    put_oi_max3_v = np.array(df['dom1_put_oi_max3'], dtype=float)
    put_oi_max3_strike_v = np.array(df['dom1_put_oi_max3_strike'], dtype=float)
    put_oi_max4_v = np.array(df['dom1_put_oi_max4'], dtype=float)
    put_oi_max4_strike_v = np.array(df['dom1_put_oi_max4_strike'], dtype=float)
    put_oi_max5_v = np.array(df['dom1_put_oi_max5'], dtype=float)
    put_oi_max5_strike_v = np.array(df['dom1_put_oi_max5_strike'], dtype=float)

    call_oi_max1_v = np.array(df['dom1_call_oi_max1'], dtype=float)
    call_oi_max1_strike_v = np.array(df['dom1_call_oi_max1_strike'], dtype=float)
    call_oi_max2_v = np.array(df['dom1_call_oi_max2'], dtype=float)
    call_oi_max2_strike_v = np.array(df['dom1_call_oi_max2_strike'], dtype=float)
    call_oi_max3_v = np.array(df['dom1_call_oi_max3'], dtype=float)
    call_oi_max3_strike_v = np.array(df['dom1_call_oi_max3_strike'], dtype=float)
    call_oi_max4_v = np.array(df['dom1_call_oi_max4'], dtype=float)
    call_oi_max4_strike_v = np.array(df['dom1_call_oi_max4_strike'], dtype=float)
    call_oi_max5_v = np.array(df['dom1_call_oi_max5'], dtype=float)
    call_oi_max5_strike_v = np.array(df['dom1_call_oi_max5_strike'], dtype=float)

    c_atm_iv_v = (c_atm_put_iv_v + c_atm_call_iv_v)/2
    c_40d_skew_iv_v = c_40d_put_iv_v - c_40d_call_iv_v
    c_25d_skew_iv_v = c_25d_put_iv_v - c_25d_call_iv_v
    c_10d_skew_iv_v = c_10d_put_iv_v - c_10d_call_iv_v
    c_5d_skew_iv_v = c_5d_put_iv_v - c_5d_call_iv_v

    w = np.where(fut_t >= t[0])[0]
    fut_t = fut_t[w]
    index_price = index_price[w]
    index_volume = index_volume[w]
    index_oi = index_oi[w]

    # datas = [
    #          [[[t1,c_25d_put_iv,'主力合约 '+contract1+' c_25d_put_iv','color=darkgreen'],
    #            [t1,c_25d_call_iv,'主力合约 '+contract1+' c_25d_call_iv','color=red'],
    #            [t1,c_atm_iv,'主力合约 '+contract1+' c_atm_iv','color=gray'],
    #           ],
    #           [],''],

    #          [[[t1,c_40d_skew_iv,'主力合约 '+contract1+' c_40d_put-call_iv',''],
    #            [t2,c_40d_skew_iv_v,'期权主力 '+contract2+' c_40d_put-call_iv','']],
    #           [],''],

    #          [[[t1,c_25d_skew_iv,'主力合约 '+contract1+' c_25d_put-call_iv',''],
    #            [t2,c_25d_skew_iv_v,'期权主力 '+contract2+' c_25d_put-call_iv','']],
    #           [],''],

    #          [[[fut_t,index_price,variety+' 指数','color=black']],[],''],

    #          [[[t1,c_10d_skew_iv,'主力合约 '+contract1+' c_10d_put-call_iv',''],
    #            [t2,c_10d_skew_iv_v,'期权主力 '+contract2+' c_10d_put-call_iv','']],
    #           [],''],

    #          [[[t1,c_5d_skew_iv,'主力合约 '+contract1+' c_5d_put-call_iv',''],
    #            [t2,c_5d_skew_iv_v,'期权主力 '+contract2+' c_5d_put-call_iv','']],
    #           [],''],

    #          [[[t2,c_25d_put_iv_v,'期权主力 '+contract2+' c_25d_put_iv','color=darkgreen'],
    #            [t2,c_25d_call_iv_v,'期权主力 '+contract2+' c_25d_call_iv','color=red'],
    #            [t2,c_atm_iv_v,'期权主力 '+contract2+' c_atm_iv','color=gray'],
    #           ],
    #           [],''],

    #          ]
    # plot_many_figure(datas, max_height=1200)


    # datas1 = [[t1,c_25d_skew_iv,'主力合约 '+contract1+' c_25d_put-call_iv'],
    #           [t1,c_25d_put_iv - c_atm_put_iv,'主力合约 '+contract1+' c_25d_put-atm_iv'],
    #           [t1,c_25d_call_iv - c_atm_call_iv,'主力合约 '+contract1+' c_25d_call-atm_iv'],]
    # datas2 = [[fut_t,index_price,variety+'指数']]
    # plot_mean_std(datas1, datas2, T=int(250*1.5), max_height=500)


    total_put_volume = np.array(df['total_put_volume'], dtype=float)
    total_call_volume = np.array(df['total_call_volume'], dtype=float)
    total_put_oi = np.array(df['total_put_oi'], dtype=float)
    total_call_oi = np.array(df['total_call_oi'], dtype=float)
    total_put_exercise = np.array(df['total_put_exercise'], dtype=float)
    total_call_exercise = np.array(df['total_call_exercise'], dtype=float)

    t3, volume_ratio = data_div(t2, total_put_volume+total_call_volume, fut_t, index_volume)
    t4, oi_ratio = data_div(t2, total_put_oi+total_call_oi, fut_t, index_oi)

    # datas = [
    #          [[[t2,total_call_volume,'total_call_volume','color=red'],
    #            [t2,total_put_volume,'total_put_volume','color=darkgreen'],
    #           ],
    #           [[t2,total_put_volume-total_call_volume,'total_put_volume - total_call_volume','style=vbar'],],''],

    #          [[[t2,total_call_oi,'total_call_oi','color=red'],
    #            [t2,total_put_oi,'total_put_oi','color=darkgreen'],
    #           ],
    #           [[t2,total_put_oi-total_call_oi,'total_put_oi - total_call_oi','style=vbar'],],''],

    #          [[[fut_t,index_price,variety+' 指数','color=black']],[],''],

    #          [[[t2,total_put_volume/total_call_volume,'total_put_volume / total_call_volume lhs',''],
    #           ],
    #           [[t2,total_put_oi/total_call_oi,'total_put_oi / total_call_oi rhs',''],],''],

    #          [[[t3,volume_ratio,'total_option_volume / total_future_volume lhs',''],
    #           ],
    #           [[t4,oi_ratio,'total_option_oi / total_future_oi rhs',''],],''],

    #          [[[t1,c_25d_skew_iv,'主力合约 '+contract1+' c_25d_put-call_iv',''],
    #            [t2,c_25d_skew_iv_v,'期权主力 '+contract2+' c_25d_put-call_iv','']],
    #           [],''],

    #          [[[t2,total_put_exercise,'total_put_exercise','color=darkgreen'],
    #            [t2,total_call_exercise,'total_call_exercise','color=red']],
    #           [],''],
    #          ]
    # plot_many_figure(datas, max_height=1000)


    # 主力 volume
    put_volume_max = np.vstack((put_volume_max1, put_volume_max2, put_volume_max3, put_volume_max4, put_volume_max5))
    put_volume_max_strike = np.vstack((put_volume_max1_strike, put_volume_max2_strike, put_volume_max3_strike, put_volume_max4_strike, put_volume_max5_strike))
    sort = np.argsort(put_volume_max_strike, axis=0)
    put_volume_max_strike = np.take_along_axis(put_volume_max_strike, sort, axis=0)
    put_volume_max = np.take_along_axis(put_volume_max, sort, axis=0)
    put_volume_avg_strike = np.sum(put_volume_max[2:5, :]*put_volume_max_strike[2:5, :], axis=0) / np.sum(put_volume_max[2:5, :], axis=0)
    
    call_volume_max = np.vstack((call_volume_max1, call_volume_max2, call_volume_max3, call_volume_max4, call_volume_max5))
    call_volume_max_strike = np.vstack((call_volume_max1_strike, call_volume_max2_strike, call_volume_max3_strike, call_volume_max4_strike, call_volume_max5_strike))
    sort = np.argsort(call_volume_max_strike, axis=0)
    call_volume_max_strike = np.take_along_axis(call_volume_max_strike, sort, axis=0)
    call_volume_max = np.take_along_axis(call_volume_max, sort, axis=0)
    call_volume_avg_strike = np.sum(call_volume_max[0:3, :]*call_volume_max_strike[0:3, :], axis=0) / np.sum(call_volume_max[0:3, :], axis=0)


    # 成交量最大 volume
    put_volume_max_v = np.vstack((put_volume_max1_v, put_volume_max2_v, put_volume_max3_v, put_volume_max4_v, put_volume_max5_v))
    put_volume_max_strike_v = np.vstack((put_volume_max1_strike_v, put_volume_max2_strike_v, put_volume_max3_strike_v, put_volume_max4_strike_v, put_volume_max5_strike_v))
    sort = np.argsort(put_volume_max_strike_v, axis=0)
    put_volume_max_strike_v = np.take_along_axis(put_volume_max_strike_v, sort, axis=0)
    put_volume_max_v = np.take_along_axis(put_volume_max_v, sort, axis=0)
    put_volume_avg_strike_v = np.sum(put_volume_max_v[2:5, :]*put_volume_max_strike_v[2:5, :], axis=0) / np.sum(put_volume_max_v[2:5, :], axis=0)
    
    call_volume_max_v = np.vstack((call_volume_max1_v, call_volume_max2_v, call_volume_max3_v, call_volume_max4_v, call_volume_max5_v))
    call_volume_max_strike_v = np.vstack((call_volume_max1_strike_v, call_volume_max2_strike_v, call_volume_max3_strike_v, call_volume_max4_strike_v, call_volume_max5_strike_v))
    sort = np.argsort(call_volume_max_strike_v, axis=0)
    call_volume_max_strike_v = np.take_along_axis(call_volume_max_strike_v, sort, axis=0)
    call_volume_max_v = np.take_along_axis(call_volume_max_v, sort, axis=0)
    call_volume_avg_strike_v = np.sum(call_volume_max_v[0:3, :]*call_volume_max_strike_v[0:3, :], axis=0) / np.sum(call_volume_max_v[0:3, :], axis=0)

    # 主力 oi
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

    # 成交量最大 oi
    put_oi_max_v = np.vstack((put_oi_max1_v, put_oi_max2_v, put_oi_max3_v, put_oi_max4_v, put_oi_max5_v))
    put_oi_max_strike_v = np.vstack((put_oi_max1_strike_v, put_oi_max2_strike_v, put_oi_max3_strike_v, put_oi_max4_strike_v, put_oi_max5_strike_v))
    sort = np.argsort(put_oi_max_strike_v, axis=0)
    put_oi_max_strike_v = np.take_along_axis(put_oi_max_strike_v, sort, axis=0)
    put_oi_max_v = np.take_along_axis(put_oi_max_v, sort, axis=0)
    put_oi_avg_strike_v = np.sum(put_oi_max_v[2:5, :]*put_oi_max_strike_v[2:5, :], axis=0) / np.sum(put_oi_max_v[2:5, :], axis=0)

    call_oi_max_v = np.vstack((call_oi_max1_v, call_oi_max2_v, call_oi_max3_v, call_oi_max4_v, call_oi_max5_v))
    call_oi_max_strike_v = np.vstack((call_oi_max1_strike_v, call_oi_max2_strike_v, call_oi_max3_strike_v, call_oi_max4_strike_v, call_oi_max5_strike_v))
    sort = np.argsort(call_oi_max_strike_v, axis=0)
    call_oi_max_strike_v = np.take_along_axis(call_oi_max_strike_v, sort, axis=0)
    call_oi_max_v = np.take_along_axis(call_oi_max_v, sort, axis=0)
    call_oi_avg_strike_v = np.sum(call_oi_max_v[0:3, :]*call_oi_max_strike_v[0:3, :], axis=0) / np.sum(call_oi_max_v[0:3, :], axis=0)


    # 
    w1 = np.where(put_oi_max_strike[2,:] <= call_oi_max_strike[0,:])[0]
    w2 = np.where(put_oi_max_strike_v[2,:] <= call_oi_max_strike_v[0,:])[0]

    datas = [
             [[[t1,dom_price,variety+' 主力 '+contract1,'color=black, width=4'],
               [t1[w1],call_oi_max_strike[0,w1],variety+' 主力 call 持仓量一','color=red,visible=False'],
               [t1[w1],call_oi_max_strike[1,w1],variety+' 主力 call 持仓量二','color=orange,visible=False'],
               [t1[w1],call_oi_max_strike[2,w1],variety+' 主力 call 持仓量三','color=deeppink'],
               [t1[w1],call_oi_avg_strike[w1],'加权平均','color=darkgray'],
               [t1[w1],put_oi_max_strike[4,w1],variety+' 主力 put 持仓量一','color=darkgreen,visible=False'],
               [t1[w1],put_oi_max_strike[3,w1],variety+' 主力 put 持仓量二','color=blue,visible=False'],
               [t1[w1],put_oi_max_strike[2,w1],variety+' 主力 put 持仓量三','color=purple'],
               [t1[w1],put_oi_avg_strike[w1],'加权平均','color=darkgray'],],[],''],

             [[[t22,dom_v_price,variety+' 期权主力 '+contract2,'color=black, width=4'],
               [t2[w2],call_oi_max_strike_v[0,w2],variety+' 期权主力 call 持仓量一','color=red,visible=False'],
               [t2[w2],call_oi_max_strike_v[1,w2],variety+' 期权主力 call 持仓量二','color=orange,visible=False'],
               [t2[w2],call_oi_max_strike_v[2,w2],variety+' 期权主力 call 持仓量三','color=deeppink'],
               [t2[w2],call_oi_avg_strike_v[w2],'加权平均','color=darkgray'],
               [t2[w2],put_oi_max_strike_v[4,w2],variety+' 期权主力 put 持仓量一','color=darkgreen,visible=False'],
               [t2[w2],put_oi_max_strike_v[3,w2],variety+' 期权主力 put 持仓量二','color=blue,visible=False'],
               [t2[w2],put_oi_max_strike_v[2,w2],variety+' 期权主力 put 持仓量三','color=purple'],
               [t2[w2],put_oi_avg_strike_v[w2],'加权平均','color=darkgray'],],[],''],

             [[[fut_t,index_price,variety+' 指数','color=black'],
              ],
              [[t2,total_put_oi/total_call_oi,'total_put_oi / total_call_oi',''],],''],

             [[[t2,total_call_oi,'total_call_oi','color=red'],
               [t2,total_put_oi,'total_put_oi','color=darkgreen'],
              ],
              [[t2,total_put_oi-total_call_oi,'total_put_oi - total_call_oi','style=vbar'],],''],

             ###################
             [[[t2,total_call_volume,'total_call_volume','color=red'],
               [t2,total_put_volume,'total_put_volume','color=darkgreen'],
              ],
              [[t2,total_put_volume-total_call_volume,'total_put_volume - total_call_volume','style=vbar'],],''],

            #  [[[t_dom,dom_call_atm_price,variety+' 主力 期权 ATM CALL PRICE','color=red'],
            #    [t_dom,dom_put_atm_price,variety+' 主力 期权 ATM PUT PRICE','color=darkgreen'],
            #   ],
            #   [],''],

            #  [[[t_maxv,maxv_call_atm_price,variety+' 期权主力 ATM CALL PRICE','color=red'],
            #    [t_maxv,maxv_put_atm_price,variety+' 期权主力 ATM PUT PRICE','color=darkgreen'],
            #   ],
            #   [],''],

             [[[t1,c_25d_put_iv,'主力合约 '+contract1+' 25d_put_iv','color=darkgreen'],
               [t1,c_25d_call_iv,'主力合约 '+contract1+' 25d_call_iv','color=red'],
               [t1,c_atm_iv,'主力合约 '+contract1+' atm_iv','color=darkgray'],
              ],
              [[t1,c_25d_skew_iv,'主力合约 '+contract1+' 25d_put-call_iv','style=vbar'],],''],

             [[[fut_t,index_price,variety+' 指数','color=black'],
              ],
              [[t2,total_put_volume/total_call_volume,'total_put_volume / total_call_volume',''],],''],

             [[[t1,dom_price,variety+' 主力 '+contract1,'color=black, width=4'],
               [t1[w1],call_volume_max_strike[0,w1],variety+' 主力 call 成交量一','color=red,visible=False'],
               [t1[w1],call_volume_max_strike[1,w1],variety+' 主力 call 成交量二','color=orange,visible=False'],
               [t1[w1],call_volume_max_strike[2,w1],variety+' 主力 call 成交量三','color=deeppink'],
               [t1[w1],call_volume_avg_strike[w1],'加权平均','color=darkgray'],
               [t1[w1],put_volume_max_strike[4,w1],variety+' 主力 put 成交量一','color=darkgreen,visible=False'],
               [t1[w1],put_volume_max_strike[3,w1],variety+' 主力 put 成交量二','color=blue,visible=False'],
               [t1[w1],put_volume_max_strike[2,w1],variety+' 主力 put 成交量三','color=purple'],
               [t1[w1],put_volume_avg_strike[w1],'加权平均','color=darkgray'],],[],''],

             [[[t22,dom_v_price,variety+' 期权主力 '+contract2,'color=black, width=4'],
               [t2[w2],call_volume_max_strike_v[0,w2],variety+' 期权主力 call 成交量一','color=red,visible=False'],
               [t2[w2],call_volume_max_strike_v[1,w2],variety+' 期权主力 call 成交量二','color=orange,visible=False'],
               [t2[w2],call_volume_max_strike_v[2,w2],variety+' 期权主力 call 成交量三','color=deeppink'],
               [t2[w2],call_volume_avg_strike_v[w2],'加权平均','color=darkgray'],
               [t2[w2],put_volume_max_strike_v[4,w2],variety+' 期权主力 put 成交量一','color=darkgreen,visible=False'],
               [t2[w2],put_volume_max_strike_v[3,w2],variety+' 期权主力 put 成交量二','color=blue,visible=False'],
               [t2[w2],put_volume_max_strike_v[2,w2],variety+' 期权主力 put 成交量三','color=purple'],
               [t2[w2],put_volume_avg_strike_v[w2],'加权平均','color=darkgray'],],[],''],
             ]
    plot_many_figure(datas, max_height=1800)


    datas = [[fut_t,index_price,variety+' 指数','color=black'], [t2,total_put_oi/total_call_oi,'total_put_oi / total_call_oi',''],
            [fut_t,index_price,variety+' 指数','color=black'], [t2,total_call_oi/total_put_oi,'total_call_oi / total_put_oi','']]
    compare_two_option_data(datas, start_time='2020-01-01')


def plot_option_strike_volume_oi(exchange, variety):
    path3 = os.path.join(option_price_dir, exchange, variety+'_info_detail'+'.csv')
    if not os.path.exists(path3):
        return
    df = pd.read_csv(path3)
    inst_id_opt = df.loc[len(df)-1, 'dom1']

    path3 = os.path.join(future_price_dir, exchange, variety+'.csv')
    if not os.path.exists(path3):
        return
    df = pd.read_csv(path3, header=[0,1])
    inst_id_fut = df.loc[len(df)-1, pd.IndexSlice['dom', 'inst_id']]

    if inst_id_opt == inst_id_fut:
        inst_ids = [inst_id_opt]
    else:
        inst_ids = [inst_id_opt, inst_id_fut]  

    for inst_id in inst_ids:
        path = os.path.join(option_price_dir, exchange, inst_id+'.csv')
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


        path = os.path.join(future_price_dir, exchange, variety+'.csv')
        fut_df = pd.read_csv(path, header=[0,1])
        fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
        row = np.where(fut_t == t[L-1])[0]
        if len(row) > 0:
            for c in ['c1','c2','c3','c4','c5','c6','c7','c8','c9']:
                if (fut_df.loc[row[0], pd.IndexSlice[c, 'inst_id']] == inst_id):
                    fut_price = fut_df.loc[row[0], pd.IndexSlice[c, 'close']]

        row = np.where(fut_t == t[L-2])[0]
        if len(row) > 0:
            for c in ['c1','c2','c3','c4','c5','c6','c7','c8','c9']:
                if (fut_df.loc[row[0], pd.IndexSlice[c, 'inst_id']] == inst_id):
                    fut_price_1d = fut_df.loc[row[0], pd.IndexSlice[c, 'close']]

        row = np.where(fut_t == t[L-3])[0]
        if len(row) > 0:
            for c in ['c1','c2','c3','c4','c5','c6','c7','c8','c9']:
                if (fut_df.loc[row[0], pd.IndexSlice[c, 'inst_id']] == inst_id):
                    fut_price_2d = fut_df.loc[row[0], pd.IndexSlice[c, 'close']]

        row = np.where(fut_t == t[L-6])[0]
        if len(row) > 0:
            for c in ['c1','c2','c3','c4','c5','c6','c7','c8','c9']:
                if (fut_df.loc[row[0], pd.IndexSlice[c, 'inst_id']] == inst_id):
                    fut_price_5d = fut_df.loc[row[0], pd.IndexSlice[c, 'close']]

        strike_sort = np.sort(strike)
        bar_width = (strike_sort[1]-strike_sort[0]) / 5
        if bar_width < 0.5:
            bar_width = 1

        fig1 = figure(frame_width=1400, frame_height=155)
        fig1.quad(left=strike-bar_width, right=strike, bottom=0, top=put_oi, fill_color='darkgreen')
        fig1.quad(left=strike, right=strike+bar_width, bottom=0, top=call_oi, fill_color='red')
        fig1.line(x=[fut_price, fut_price], y=[0, np.nanmax(call_oi)], line_width=1, line_color='black', legend_label=inst_id + ' oi')
        fig1.legend.location='top_left'

        net_oi = put_oi - call_oi
        put_idx = np.where(net_oi >= 0)[0]
        call_idx = np.where(net_oi < 0)[0]

        fig11 = figure(frame_width=1400, frame_height=155, x_range=fig1.x_range)
        fig11.quad(left=strike[put_idx]-bar_width/2, right=strike[put_idx]+bar_width/2, bottom=0, top=net_oi[put_idx], fill_color='darkgreen')
        fig11.quad(left=strike[call_idx]-bar_width/2, right=strike[call_idx]+bar_width/2, bottom=0, top=-net_oi[call_idx], fill_color='red')
        fig11.line(x=[fut_price, fut_price], y=[0, np.nanmax(call_oi)], line_width=1, line_color='black', legend_label=inst_id + ' net oi')
        fig11.legend.location='top_left'

        fig2 = figure(frame_width=1400, frame_height=155, x_range=fig1.x_range)
        fig2.quad(left=strike-bar_width, right=strike, bottom=0, top=put_vol, fill_color='darkgreen')
        fig2.quad(left=strike, right=strike+bar_width, bottom=0, top=call_vol, fill_color='red')
        fig2.line(x=[fut_price_1d, fut_price_1d], y=[0, np.nanmax(call_vol)], line_width=1, line_color='black', legend_label=inst_id + ' 1d volume')
        fig2.legend.location='top_left'
        fig2.background_fill_color = "lightgray"

        put_oi_ld_change = put_oi - put_oi_1d
        call_oi_ld_change = call_oi - call_oi_1d
        fig21 = figure(frame_width=1400, frame_height=155, x_range=fig1.x_range)
        fig21.quad(left=strike-bar_width, right=strike, bottom=0, top=put_oi_ld_change, fill_color='darkgreen')
        fig21.quad(left=strike, right=strike+bar_width, bottom=0, top=call_oi_ld_change, fill_color='red')
        fig21.line(x=[fut_price_1d, fut_price_1d], y=[np.nanmin(call_oi_ld_change), np.nanmax(call_oi_ld_change)], line_width=1, line_color='black', legend_label=inst_id + ' oi 1d change')
        fig21.legend.location='top_left'

        net_oi_1d_change = put_oi_ld_change - call_oi_ld_change
        put_idx = np.where(net_oi_1d_change >= 0)[0]
        call_idx = np.where(net_oi_1d_change < 0)[0]

        fig22 = figure(frame_width=1400, frame_height=155, x_range=fig1.x_range)
        fig22.quad(left=strike[put_idx]-bar_width/2, right=strike[put_idx]+bar_width/2, bottom=0, top=net_oi_1d_change[put_idx], fill_color='darkgreen')
        fig22.quad(left=strike[call_idx]-bar_width/2, right=strike[call_idx]+bar_width/2, bottom=0, top=-net_oi_1d_change[call_idx], fill_color='red')
        fig22.line(x=[fut_price_1d, fut_price_1d], y=[0, np.nanmax(call_oi_ld_change)], line_width=1, line_color='black', legend_label=inst_id + ' 1d net oi change')
        fig22.legend.location='top_left'

        fig3 = figure(frame_width=1400, frame_height=155, x_range=fig1.x_range)
        fig3.quad(left=strike-bar_width, right=strike, bottom=0, top=put_vol+put_vol_1d, fill_color='darkgreen')
        fig3.quad(left=strike, right=strike+bar_width, bottom=0, top=call_vol+call_vol_1d, fill_color='red')
        fig3.line(x=[fut_price_2d, fut_price_2d], y=[0, np.nanmax(call_vol+call_vol_1d)], line_width=1, line_color='black', legend_label=inst_id + ' 2d volume')
        fig3.legend.location='top_left'
        fig3.background_fill_color = "lightgray"

        fig31 = figure(frame_width=1400, frame_height=155, x_range=fig1.x_range)
        fig31.quad(left=strike-bar_width, right=strike, bottom=0, top=put_oi-put_oi_2d, fill_color='darkgreen')
        fig31.quad(left=strike, right=strike+bar_width, bottom=0, top=call_oi-call_oi_2d, fill_color='red')
        fig31.line(x=[fut_price_2d, fut_price_2d], y=[np.nanmin(call_oi-call_oi_2d), np.nanmax(call_oi-call_oi_2d)], line_width=1, line_color='black', legend_label=inst_id + ' oi 2d change')
        fig31.legend.location='top_left'

        fig4 = figure(frame_width=1400, frame_height=155, x_range=fig1.x_range)
        fig4.quad(left=strike-bar_width, right=strike, bottom=0, top=put_vol+put_vol_1d+put_vol_2d+put_vol_3d+put_vol_4d, fill_color='darkgreen')
        fig4.quad(left=strike, right=strike+bar_width, bottom=0, top=call_vol+call_vol_1d+call_vol_1d+call_vol_3d+call_vol_4d, fill_color='red')
        fig4.line(x=[fut_price_5d, fut_price_5d], y=[0, np.nanmax(call_vol+call_vol_1d+call_vol_1d+call_vol_3d+call_vol_4d)], line_width=1, line_color='black', legend_label=inst_id + ' 5d volume')
        fig4.legend.location='top_left'
        fig4.background_fill_color = "lightgray"

        fig41 = figure(frame_width=1400, frame_height=155, x_range=fig1.x_range)
        fig41.quad(left=strike-bar_width, right=strike, bottom=0, top=put_oi-put_oi_5d, fill_color='darkgreen')
        fig41.quad(left=strike, right=strike+bar_width, bottom=0, top=call_oi-call_oi_5d, fill_color='red')
        fig41.line(x=[fut_price_5d, fut_price_5d], y=[np.nanmin(call_oi-call_oi_5d), np.nanmax(call_oi-call_oi_5d)], line_width=1, line_color='black', legend_label=inst_id + ' oi 5d change')
        fig41.legend.location='top_left'

        show(column(fig1,fig11,fig2,fig21,fig22,fig3,fig31,fig4,fig41))



def plot_sse_etf_option_datas1(exchange, variety):
    path1 = os.path.join(option_price_dir, exchange, variety+'_info'+'.csv')
    if not(os.path.exists(path1)):
        return
    info_df = pd.read_csv(path1)
    info_t = pd.DatetimeIndex(pd.to_datetime(info_df['time'], format='%Y-%m-%d'))


    info_t = pd.DatetimeIndex(pd.to_datetime(info_df['time'], format='%Y-%m-%d'))
    c1_contract = np.array(info_df['dom1'])
    c2_contract = np.array(info_df['dom2'])

    path2 = os.path.join(option_price_dir, exchange, variety+'.csv')
    fut_df = pd.read_csv(path2)
    fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time'], format='%Y-%m-%d'))
    fut_price = np.array(fut_df['close'], dtype=float)
    fut_volume = np.array(fut_df['volume'], dtype=float)

    idx1 = np.isin(info_t, fut_t)

    info_t = info_t[idx1]
    c1_contract = c1_contract[idx1]
    c2_contract = c2_contract[idx1]

    path3 = os.path.join(option_price_dir, 'sse', variety+'_option_volume_oi'+'.csv')
    oi_df = pd.read_csv(path3)
    oi_t = pd.DatetimeIndex(pd.to_datetime(oi_df['time'], format='%Y-%m-%d'))

    path3 = os.path.join(option_price_dir, exchange, variety+'_info_detail'+'.csv')
    df = pd.read_csv(path3)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))

    w = np.where(fut_t >= t[0])[0]
    fut_t = fut_t[w]
    fut_price = fut_price[w]
    fut_volume = fut_volume[w]

    # 期权c1
    t1 = t
    c_40d_put_iv_1 = np.array(df['dom1_c_40d_put_iv'], dtype=float)
    c_40d_call_iv_1 = np.array(df['dom1_c_40d_call_iv'], dtype=float)
    c_25d_put_iv_1 = np.array(df['dom1_c_25d_put_iv'], dtype=float)
    c_25d_call_iv_1 = np.array(df['dom1_c_10d_call_iv'], dtype=float)
    c_10d_put_iv_1 = np.array(df['dom1_c_10d_put_iv'], dtype=float)
    c_10d_call_iv_1 = np.array(df['dom1_c_25d_call_iv'], dtype=float)
    c_atm_put_iv_1 = np.array(df['dom1_c_atm_put_iv'], dtype=float)
    c_atm_call_iv_1 = np.array(df['dom1_c_atm_call_iv'], dtype=float)
    c_atm_iv_1 = (c_atm_put_iv_1 + c_atm_call_iv_1)/2
    c_40d_skew_iv_1 = c_40d_put_iv_1 - c_40d_call_iv_1
    c_25d_skew_iv_1 = c_25d_put_iv_1 - c_25d_call_iv_1
    c_10d_skew_iv_1 = c_10d_put_iv_1 - c_10d_call_iv_1

    # 期权c2
    t2 = t
    c_40d_put_iv_2 = np.array(df['dom2_c_40d_put_iv'], dtype=float)
    c_40d_call_iv_2 = np.array(df['dom2_c_40d_call_iv'], dtype=float)
    c_25d_put_iv_2 = np.array(df['dom2_c_25d_put_iv'], dtype=float)
    c_25d_call_iv_2 = np.array(df['dom2_c_10d_call_iv'], dtype=float)
    c_10d_put_iv_2 = np.array(df['dom2_c_10d_put_iv'], dtype=float)
    c_10d_call_iv_2 = np.array(df['dom2_c_25d_call_iv'], dtype=float)
    c_atm_put_iv_2 = np.array(df['dom2_c_atm_put_iv'], dtype=float)
    c_atm_call_iv_2 = np.array(df['dom2_c_atm_call_iv'], dtype=float)
    c_atm_iv_2 = (c_atm_put_iv_2 + c_atm_call_iv_2)/2
    c_40d_skew_iv_2 = c_40d_put_iv_2 - c_40d_call_iv_2
    c_25d_skew_iv_2 = c_25d_put_iv_2 - c_25d_call_iv_2
    c_10d_skew_iv_2 = c_10d_put_iv_2 - c_10d_call_iv_2

    # 期权c3
    t3 = t
    c_40d_put_iv_3 = np.array(df['dom3_c_40d_put_iv'], dtype=float)
    c_40d_call_iv_3 = np.array(df['dom3_c_40d_call_iv'], dtype=float)
    c_25d_put_iv_3 = np.array(df['dom3_c_25d_put_iv'], dtype=float)
    c_25d_call_iv_3 = np.array(df['dom3_c_10d_call_iv'], dtype=float)
    c_10d_put_iv_3 = np.array(df['dom3_c_10d_put_iv'], dtype=float)
    c_10d_call_iv_3 = np.array(df['dom3_c_25d_call_iv'], dtype=float)
    c_atm_put_iv_3 = np.array(df['dom3_c_atm_put_iv'], dtype=float)
    c_atm_call_iv_3 = np.array(df['dom3_c_atm_call_iv'], dtype=float)
    c_atm_iv_3 = (c_atm_put_iv_3 + c_atm_call_iv_3)/2
    c_40d_skew_iv_3 = c_40d_put_iv_3 - c_40d_call_iv_3
    c_25d_skew_iv_3 = c_25d_put_iv_3 - c_25d_call_iv_3
    c_10d_skew_iv_3 = c_10d_put_iv_3 - c_10d_call_iv_3

    datas = [
             [[[t1,c_25d_put_iv_1,'期权c1 c_25d_put_iv','color=darkgreen'],
               [t1,c_25d_call_iv_1,'期权c1 c_25d_call_iv','color=red'],
               [t1,c_atm_iv_1,'期权c1 c_atm_iv','color=gray'],
              ],
              [],''],

             [[[t1,c_40d_skew_iv_1,'期权c1 c_40d_put-call_iv',''],
               [t2,c_40d_skew_iv_2,'期权c2 c_40d_put-call_iv',''],
               [t3,c_40d_skew_iv_3,'期权c3 c_40d_put-call_iv',''],],
              [],''],

             [[[t1,c_25d_skew_iv_1,'期权c1 c_25d_put-call_iv',''],
               [t2,c_25d_skew_iv_2,'期权c2 c_25d_put-call_iv',''],
               [t3,c_25d_skew_iv_3,'期权c3 c_25d_put-call_iv',''],],
              [],''],

             [[[fut_t,fut_price,variety,'color=black']],[],''],

             [[[t1,c_10d_skew_iv_1,'期权c1 c_10d_put-call_iv',''],
               [t2,c_10d_skew_iv_2,'期权c2 c_10d_put-call_iv',''],
               [t3,c_10d_skew_iv_3,'期权c3 c_10d_put-call_iv','']],
              [],''],

             [[[t2,c_25d_put_iv_2,'期权c2 c_25d_put_iv','color=darkgreen'],
               [t2,c_25d_call_iv_2,'期权c2 c_25d_call_iv','color=red'],
               [t2,c_atm_iv_2,'期权c2 c_atm_iv','color=gray'],
              ],
              [],''],

             ]
    plot_many_figure(datas, max_height=1000)


    datas1 = [[t2,c_25d_skew_iv_2,'期权c2 c_25d_put-call_iv'],
              [t2,c_25d_put_iv_2 - c_atm_put_iv_2,'期权c2 c_25d_put-atm_iv'],
              [t2,c_25d_call_iv_2 - c_atm_call_iv_2,'期权c2 c_25d_call-atm_iv'],]
    datas2 = [[fut_t,fut_price,variety]]
    plot_mean_std(datas1, datas2, T=int(250*1.5), max_height=500)


    total_put_volume = np.array(oi_df['total_put_volume'], dtype=float)
    total_call_volume = np.array(oi_df['total_call_volume'], dtype=float)
    total_put_oi = np.array(oi_df['total_put_oi'], dtype=float)
    total_call_oi = np.array(oi_df['total_call_oi'], dtype=float)

    t3, volume_ratio = data_div(t2, total_put_volume+total_call_volume, fut_t, fut_volume)

    datas = [
             [[[t2,total_call_volume,'total_call_volume','color=red'],
               [t2,total_put_volume,'total_put_volume','color=darkgreen'],
              ],
              [[t2,total_put_volume-total_call_volume,'total_put_volume - total_call_volume','style=vbar'],],''],

             [[[t2,total_call_oi,'total_call_oi','color=red'],
               [t2,total_put_oi,'total_put_oi','color=darkgreen'],
              ],
              [[t2,total_put_oi-total_call_oi,'total_put_oi - total_call_oi','style=vbar'],],''],

             [[[t1,c_25d_skew_iv_1,'期权c1 c_25d_put-call_iv',''],
               [t2,c_25d_skew_iv_2,'期权c2 c_25d_put-call_iv',''],
               [t3,c_25d_skew_iv_3,'期权c3 c_25d_put-call_iv','']],
              [],''],

             [[[fut_t,fut_price,variety+' 指数','color=black']],[],''],

             [[[t2,total_put_volume/total_call_volume,'total_put_volume / total_call_volume lhs',''],
              ],
              [[t2,total_put_oi/total_call_oi,'total_put_oi / total_call_oi rhs',''],],''],

             [[[t3,volume_ratio,'total_option_volume / total_future_volume lhs',''],
              ],
              [],''],
             ]
    plot_many_figure(datas, max_height=1000)

    datas = [[fut_t,fut_price,variety,''], [t2,total_put_oi/total_call_oi,'total_put_oi / total_call_oi',''],
             [fut_t,fut_price,variety,''], [t2,total_call_oi/total_put_oi,'total_call_oi / total_put_oi','']]
    compare_two_option_data(datas, start_time='2020-01-01')


#######
def plot_etf_option_strike_volume_oi(exchange, variety):
    path3 = os.path.join(option_price_dir, exchange, variety+'_info_detail'+'.csv')
    if not os.path.exists(path3):
        return
    df = pd.read_csv(path3)
    inst_ids = [df.loc[len(df)-1, 'dom1'], df.loc[len(df)-1, 'dom2']]

    path3 = os.path.join(option_price_dir, exchange, variety+'.csv')
    if not os.path.exists(path3):
        return
    etf_df = pd.read_csv(path3)
    etf_t = pd.DatetimeIndex(pd.to_datetime(etf_df['time'], format='%Y-%m-%d'))


    for inst_id in inst_ids:
        path = os.path.join(option_price_dir, exchange, inst_id+'.csv')
        df = pd.read_csv(path, header=[0,1,2])
        t = pd.DatetimeIndex(pd.to_datetime(df['time']['time']['time'], format='%Y-%m-%d'))

        L = len(t)
        if (L < 5):
            print('L < 5')
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

        row = np.where(etf_t == t[L-1])[0]
        if len(row) > 0:
            etf_price = etf_df.loc[row[0], 'close']


        row = np.where(etf_t == t[L-2])[0]
        if len(row) > 0:
            etf_price_1d = etf_df.loc[row[0], 'close']

        row = np.where(etf_t == t[L-3])[0]
        if len(row) > 0:
            etf_price_2d = etf_df.loc[row[0], 'close']

        row = np.where(etf_t == t[L-6])[0]
        if len(row) > 0:
            etf_price_5d = etf_df.loc[row[0], 'close']


        bar_width = int((strike[1]-strike[0]) / 5)
        if bar_width == 0:
            bar_width = 1

        fig1 = figure(frame_width=1300, frame_height=170)
        fig1.quad(left=strike-bar_width, right=strike, bottom=0, top=put_oi, fill_color='darkgreen')
        fig1.quad(left=strike, right=strike+bar_width, bottom=0, top=call_oi, fill_color='red')
        fig1.line(x=[etf_price, etf_price], y=[0, np.nanmax(call_oi)], line_width=1, line_color='black', legend_label=inst_id + ' oi')
        fig1.legend.location='top_left'

        fig2 = figure(frame_width=1300, frame_height=170, x_range=fig1.x_range)
        fig2.quad(left=strike-bar_width, right=strike, bottom=0, top=put_vol, fill_color='darkgreen')
        fig2.quad(left=strike, right=strike+bar_width, bottom=0, top=call_vol, fill_color='red')
        fig2.line(x=[etf_price_1d, etf_price_1d], y=[0, np.nanmax(call_vol)], line_width=1, line_color='black', legend_label=inst_id + ' 1d volume')
        fig2.legend.location='top_left'
        fig2.background_fill_color = "lightgray"

        fig21 = figure(frame_width=1300, frame_height=150, x_range=fig1.x_range)
        fig21.quad(left=strike-bar_width, right=strike, bottom=0, top=put_oi-put_oi_1d, fill_color='darkgreen')
        fig21.quad(left=strike, right=strike+bar_width, bottom=0, top=call_oi-call_oi_1d, fill_color='red')
        fig21.line(x=[etf_price_1d, etf_price_1d], y=[np.nanmin(call_oi-call_oi_1d), np.nanmax(call_oi-call_oi_1d)], line_width=1, line_color='black', legend_label=inst_id + ' oi 1d change')
        fig21.legend.location='top_left'

        fig3 = figure(frame_width=1300, frame_height=170, x_range=fig1.x_range)
        fig3.quad(left=strike-bar_width, right=strike, bottom=0, top=put_vol+put_vol_1d, fill_color='darkgreen')
        fig3.quad(left=strike, right=strike+bar_width, bottom=0, top=call_vol+call_vol_1d, fill_color='red')
        fig3.line(x=[etf_price_2d, etf_price_2d], y=[0, np.nanmax(call_vol+call_vol_1d)], line_width=1, line_color='black', legend_label=inst_id + ' 2d volume')
        fig3.legend.location='top_left'
        fig3.background_fill_color = "lightgray"

        fig31 = figure(frame_width=1300, frame_height=150, x_range=fig1.x_range)
        fig31.quad(left=strike-bar_width, right=strike, bottom=0, top=put_oi-put_oi_2d, fill_color='darkgreen')
        fig31.quad(left=strike, right=strike+bar_width, bottom=0, top=call_oi-call_oi_2d, fill_color='red')
        fig31.line(x=[etf_price_2d, etf_price_2d], y=[np.nanmin(call_oi-call_oi_2d), np.nanmax(call_oi-call_oi_2d)], line_width=1, line_color='black', legend_label=inst_id + ' oi 2d change')
        fig31.legend.location='top_left'

        fig4 = figure(frame_width=1300, frame_height=170, x_range=fig1.x_range)
        fig4.quad(left=strike-bar_width, right=strike, bottom=0, top=put_vol+put_vol_1d+put_vol_2d+put_vol_3d+put_vol_4d, fill_color='darkgreen')
        fig4.quad(left=strike, right=strike+bar_width, bottom=0, top=call_vol+call_vol_1d+call_vol_1d+call_vol_3d+call_vol_4d, fill_color='red')
        fig4.line(x=[etf_price_5d, etf_price_5d], y=[0, np.nanmax(call_vol+call_vol_1d+call_vol_1d+call_vol_3d+call_vol_4d)], line_width=1, line_color='black', legend_label=inst_id + ' 5d volume')
        fig4.legend.location='top_left'
        fig4.background_fill_color = "lightgray"

        fig41 = figure(frame_width=1300, frame_height=150, x_range=fig1.x_range)
        fig41.quad(left=strike-bar_width, right=strike, bottom=0, top=put_oi-put_oi_5d, fill_color='darkgreen')
        fig41.quad(left=strike, right=strike+bar_width, bottom=0, top=call_oi-call_oi_5d, fill_color='red')
        fig41.line(x=[etf_price_5d, etf_price_5d], y=[np.nanmin(call_oi-call_oi_5d), np.nanmax(call_oi-call_oi_5d)], line_width=1, line_color='black', legend_label=inst_id + ' oi 5d change')
        fig41.legend.location='top_left'

        show(column(fig1,fig2,fig21,fig3,fig31,fig4,fig41))


###############################################
def plot_etf_option_datas(exchange, variety):
    plot_sse_etf_option_datas1(exchange, variety)

    # 没有各个期权的 成交量 持仓量 数据
    # plot_etf_option_strike_volume_oi(exchange, variety)


def plot_szse_etf_option_datas(exchange, variety):
    path2 = os.path.join(option_price_dir, exchange, variety+'.csv')
    fut_df = pd.read_csv(path2)
    fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time'], format='%Y-%m-%d'))
    fut_price = np.array(fut_df['close'], dtype=float)

    path3 = os.path.join(option_price_dir, exchange, variety+'_option_volume_oi'+'.csv')
    oi_df = pd.read_csv(path3)
    t2 = pd.DatetimeIndex(pd.to_datetime(oi_df['time'], format='%Y-%m-%d'))

    total_put_oi = np.array(oi_df['total_put_oi'], dtype=float)
    total_call_oi = np.array(oi_df['total_call_oi'], dtype=float)

    w = np.where(fut_t == t2[0])[0][0]
    fut_t = fut_t[w:]
    fut_price = fut_price[w:]

    datas = [[fut_t,fut_price,variety,''], [t2,total_put_oi/total_call_oi,'total_put_oi / total_call_oi',''],
             [fut_t,fut_price,variety,''], [t2,total_call_oi/total_put_oi,'total_call_oi / total_put_oi','']]
    compare_two_option_data(datas, start_time='2020-01-01')





def plot_option_position_basis_data(exchange, variety):
    plot_dominant_option_datas1(exchange, variety)
    time.sleep(0.25)

    plot_option_strike_volume_oi(exchange, variety)
    time.sleep(0.25)

    plot_lme_option_data(variety)

    if variety == 'i':
        plot_sgx_option_data('FEF')
        plot_fef_3pm()

    if variety == 'sc':
        plot_nasdaq_option_datas('USO')
        plot_nasdaq_intraday_option_strike_volume_oi('nasdaq', 'USO')
        plot_nasdaq_option_datas('XOP')
        plot_nasdaq_option_datas('XLE')
        plot_commodity_vix(variety)
        plot_saudi_vs_oil()

    if variety == 'au':
        plot_nasdaq_option_datas('GLD')
        plot_nasdaq_intraday_option_strike_volume_oi('nasdaq', 'GLD')
        plot_nasdaq_option_datas('TLT')
        plot_gold_vs_tlt()
        plot_onrrp_data()
        plot_commodity_vix(variety)
        plot_sge_au9999_data()
        plot_sge_forward('Au99.99')
        plot_sge_vs_sgei()
        plot_lbma_vault_data('gold')

    if variety == 'ag':
        plot_nasdaq_option_datas('SLV')
        plot_nasdaq_intraday_option_strike_volume_oi('nasdaq', 'SLV')
        plot_sge_td_data('Ag(T+D)')
        plot_sge_forward('Ag99.99')
        plot_lbma_vault_data('silver')

    # plot_intraday_dominant_option_datas(exchange, variety)
    # time.sleep(0.25)

    plot_term_structure(exchange, variety)
    time.sleep(0.25)

    plot_many_position([[exchange, variety, variety]])
    time.sleep(0.25)

    plot_institution_position(variety)

    if variety == 'lh':
        plot_basis(exchange, variety, adjust=1000)
    elif variety == 'PK':
        plot_pk_basis()
        plot_pk_production_profit()
    elif variety == 'CF':
        plot_cf_spot_price()
        plot_basis(exchange, variety)
    elif variety == 'b':
        plot_soybean_production_profit()
    elif variety == 'i':
        plot_steel_profit()
        plot_i_port_stock()
    else:
        plot_basis(exchange, variety)
    time.sleep(0.25)

    # if variety == 'au' or variety == 'ag':
    plot_exchange_stock(exchange, variety)


def plot_some_option_position_basis_data(varieties):
    varieties = varieties.replace(' ','')
    s = varieties.split(',')

    for variety in s:
        for exchange in exchange_dict:
            if variety in exchange_dict[exchange]:
                plot_option_position_basis_data(exchange, variety)


def plot_option_pcr(exchange, variety):
    print(variety)

    path2 = os.path.join(future_price_dir, exchange, variety+'.csv')
    fut_df = pd.read_csv(path2, header=[0,1])
    fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    fut_price = np.array(fut_df['index']['close'], dtype=float)

    path3 = os.path.join(option_price_dir, exchange, variety+'_info_detail'+'.csv')
    df = pd.read_csv(path3)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    total_put_oi = np.array(df['total_put_oi'], dtype=float)
    total_call_oi = np.array(df['total_call_oi'], dtype=float)

    pce = total_put_oi/total_call_oi

    start_time = '2021-04-01'
    start_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')
    current_time_dt = datetime.datetime.now()

    data_dict = {}
    while(start_time_dt < current_time_dt):
        if start_time_dt.month == 12:
            start_time_dt = datetime.datetime(year=start_time_dt.year+1, month=1, day=1)
        else:
            start_time_dt = datetime.datetime(year=start_time_dt.year, month=start_time_dt.month+1, day=1)

        month1 = start_time_dt.strftime(format='%Y%m%d')[2:6]
        inst_id1 = variety + month1
        last_trading_day1 = get_option_last_trading_day(inst_id1)

        month2 = (start_time_dt + pd.Timedelta(days=40)).strftime(format='%Y%m%d')[2:6]
        inst_id2 = variety + month2
        last_trading_day2 = get_option_last_trading_day(inst_id2)

        w1 = np.where((last_trading_day1 < fut_t) & (fut_t < last_trading_day2))[0]
        w2 = np.where((last_trading_day1 < t) & (t < last_trading_day2))[0]
        if (len(w1) == 0) or (len(w2) == 0):
            continue

        data_dict[inst_id2] = [t[w2], fut_price[w1], pce[w2]]

    fig = figure(frame_width=800, frame_height=800, title=variety + ' PCR')
    i = 0
    for key in data_dict:
        t = data_dict[key][0]
        price = data_dict[key][1]
        oi = data_dict[key][2]
        if (len(price) == len(oi)):
            a = fig.circle(price, oi, color=many_colors[i], legend_label=key)
            a.visible = False
            i = (i + 1)%(len(many_colors))

    fig.legend.click_policy="hide"
    show(fig)

    pass


def xxx(exchange, variety):
    path3 = os.path.join(option_price_dir, exchange, variety+'_info_detail'+'.csv')
    if not os.path.exists(path3):
        return
    df = pd.read_csv(path3)
    inst_id = np.array(df['dom1'], dtype=str)



    path = os.path.join(option_price_dir, exchange, inst_id+'.csv')
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


    path = os.path.join(future_price_dir, exchange, variety+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    row = np.where(fut_t == t[L-1])[0]
    if len(row) > 0:
        for c in ['c1','c2','c3','c4','c5','c6','c7','c8','c9']:
            if (fut_df.loc[row[0], pd.IndexSlice[c, 'inst_id']] == inst_id):
                fut_price = fut_df.loc[row[0], pd.IndexSlice[c, 'close']]

    row = np.where(fut_t == t[L-2])[0]
    if len(row) > 0:
        for c in ['c1','c2','c3','c4','c5','c6','c7','c8','c9']:
            if (fut_df.loc[row[0], pd.IndexSlice[c, 'inst_id']] == inst_id):
                fut_price_1d = fut_df.loc[row[0], pd.IndexSlice[c, 'close']]

    row = np.where(fut_t == t[L-3])[0]
    if len(row) > 0:
        for c in ['c1','c2','c3','c4','c5','c6','c7','c8','c9']:
            if (fut_df.loc[row[0], pd.IndexSlice[c, 'inst_id']] == inst_id):
                fut_price_2d = fut_df.loc[row[0], pd.IndexSlice[c, 'close']]

    row = np.where(fut_t == t[L-6])[0]
    if len(row) > 0:
        for c in ['c1','c2','c3','c4','c5','c6','c7','c8','c9']:
            if (fut_df.loc[row[0], pd.IndexSlice[c, 'inst_id']] == inst_id):
                fut_price_5d = fut_df.loc[row[0], pd.IndexSlice[c, 'close']]


if __name__=="__main__":
    # plot_some_option_position_basis_data('au, ag, cu, al, zn, sc, rb, i')
    # plot_some_option_position_basis_data('c, RM, a, b, m, TA, SR, CF, PK, si, lc')
    # plot_some_option_position_basis_data('a, b, m, RM')
    # plot_some_option_position_basis_data('IF, IH, IM')
    # plot_some_option_position_basis_data('al, zn, rb, i, CF, p, y, OI')

    # plot_some_option_position_basis_data('c, al, i, CF, l, ru, m, RM, y, MA, pg, PK, si')


    # plot_some_option_position_basis_data('CF, SA')
    # plot_some_option_position_basis_data('UR')

    # plot_option_pcr('shfe', 'sc')

    plot_option_position_basis_data('shfe', 'sc')
    plot_option_position_basis_data('shfe', 'au')
    plot_option_position_basis_data('shfe', 'ag')

    # plot_option_position_basis_data('shfe', 'cu')
    # plot_option_position_basis_data('shfe', 'bc')
    # plot_option_position_basis_data('shfe', 'al')
    # plot_option_position_basis_data('shfe', 'ao')
    # plot_option_position_basis_data('shfe', 'pb')
    # plot_option_position_basis_data('shfe', 'zn')
    # plot_option_position_basis_data('shfe', 'ni')
    # plot_option_position_basis_data('shfe', 'sn')
    # plot_option_position_basis_data('shfe', 'ss')
    # plot_option_position_basis_data('shfe', 'ru')
    # plot_option_position_basis_data('shfe', 'br')
    # plot_option_position_basis_data('shfe', 'nr')
    # plot_option_position_basis_data('shfe', 'sp')
    # plot_option_position_basis_data('shfe', 'fu')
    # plot_option_position_basis_data('shfe', 'bu')
    # plot_option_position_basis_data('shfe', 'lu')

    # plot_option_position_basis_data('shfe', 'ec')

    # plot_option_position_basis_data('shfe', 'rb')
    # plot_option_position_basis_data('shfe', 'hc')
    plot_option_position_basis_data('dce', 'i')
    # plot_option_position_basis_data('dce', 'j')
    # plot_option_position_basis_data('dce', 'jm')
    # plot_option_position_basis_data('czce', 'MA')
    # plot_option_position_basis_data('czce', 'UR')

    # plot_option_position_basis_data('dce', 'c')
    # plot_option_position_basis_data('dce', 'cs')

    # plot_option_position_basis_data('dce', 'eg')
    # plot_option_position_basis_data('dce', 'eb')


    # plot_option_position_basis_data('dce', 'p')
    # plot_option_position_basis_data('dce', 'y')
    # plot_option_position_basis_data('czce', 'OI')

    # plot_option_position_basis_data('dce', 'a')
    # plot_option_position_basis_data('dce', 'b')
    # plot_option_position_basis_data('dce', 'm')
    # plot_option_position_basis_data('czce', 'RM')

    # plot_option_position_basis_data('dce', 'pg')
    # plot_option_position_basis_data('dce', 'pp')

    # plot_option_position_basis_data('czce', 'SH')
    # plot_option_position_basis_data('dce', 'v')
    # plot_option_position_basis_data('dce', 'l')

    # plot_option_position_basis_data('czce', 'PX')
    # plot_option_position_basis_data('czce', 'TA')
    # plot_option_position_basis_data('czce', 'PF')

    # plot_option_position_basis_data('czce', 'SR')

    # plot_option_position_basis_data('czce', 'CF')
    # plot_option_position_basis_data('czce', 'CY')

    # plot_option_position_basis_data('dce', 'lh')
    # plot_option_position_basis_data('dce', 'jd')

    # plot_option_position_basis_data('czce', 'SA')
    # plot_option_position_basis_data('czce', 'FG')

    # plot_option_position_basis_data('czce', 'SM')
    # plot_option_position_basis_data('czce', 'SF')

    # plot_option_position_basis_data('czce', 'CJ')
    # plot_option_position_basis_data('czce', 'AP')
    # plot_option_position_basis_data('czce', 'PK')

    # plot_option_position_basis_data('gfex', 'si')
    # plot_option_position_basis_data('gfex', 'lc')

    # plot_option_position_basis_data('cffex', 'IF')
    # plot_option_position_basis_data('cffex', 'IH')
    # plot_option_position_basis_data('cffex', 'IM')

    # plot_option_position_basis_data('cffex', 'T')
    # plot_option_position_basis_data('cffex', 'TF')
    # plot_option_position_basis_data('cffex', 'TL')
    # plot_option_position_basis_data('cffex', 'TS')

    # plot_etf_option_datas('sse', '50ETF')
    # plot_etf_option_datas('sse', '300ETF')
    # plot_etf_option_datas('sse', '500ETF')
    # plot_etf_option_datas('sse', '科创50')
    # plot_etf_option_datas('sse', '科创板50')
    # plot_szse_etf_option_datas('szse', '创业板ETF')
    # plot_szse_etf_option_datas('szse', '沪深300ETF')
    # plot_szse_etf_option_datas('szse', '中证500ETF')
    # plot_szse_etf_option_datas('szse', '深证100ETF')

    pass




