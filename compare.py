import os
import pandas as pd
import datetime
import numpy as np
from utils import *
import requests
from cfd import *



def compare_two_future_data(var1, var2, intraday=True, start_time='2017-01-01', end_time='2100-01-01'):
    for exchange in exchange_dict:
        if var1 in exchange_dict[exchange]:
            exchange1 = exchange
        if var2 in exchange_dict[exchange]:
            exchange2 = exchange

    if intraday == False:
        path = os.path.join(future_price_dir, exchange1, var1+'.csv')
        fut_df1 = pd.read_csv(path, header=[0,1])
        t1 = pd.DatetimeIndex(pd.to_datetime(fut_df1['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
        data1 = np.array(fut_df1['index']['close'], dtype=float)

        path = os.path.join(future_price_dir, exchange2, var2+'.csv')
        fut_df2 = pd.read_csv(path, header=[0,1])
        t2 = pd.DatetimeIndex(pd.to_datetime(fut_df2['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
        data2 = np.array(fut_df2['index']['close'], dtype=float)
    else:
        path = os.path.join(future_price_dir, exchange1, var1+'.csv')
        fut_df1 = pd.read_csv(path, header=[0,1])
        t1 = pd.DatetimeIndex(pd.to_datetime(fut_df1['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
        data1 = np.array(fut_df1['index']['close'], dtype=float)

        path = os.path.join(future_price_dir, exchange2, var2+'.csv')
        fut_df2 = pd.read_csv(path, header=[0,1])
        t2 = pd.DatetimeIndex(pd.to_datetime(fut_df2['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
        data2 = np.array(fut_df2['index']['close'], dtype=float)

        ##############
        path = os.path.join(future_price_dir, exchange1, var1+'_intraday'+'.csv')
        fut_df11 = pd.read_csv(path, header=[0,1])
        t11 = pd.DatetimeIndex(pd.to_datetime(fut_df11['time']['time'], format='%Y-%m-%d %H:%M:%S'))
        data11 = np.array(fut_df11['index']['close'], dtype=float)

        path = os.path.join(future_price_dir, exchange2, var2+'_intraday'+'.csv')
        fut_df22 = pd.read_csv(path, header=[0,1])
        t22 = pd.DatetimeIndex(pd.to_datetime(fut_df22['time']['time'], format='%Y-%m-%d %H:%M:%S'))
        data22 = np.array(fut_df22['index']['close'], dtype=float)

        t11_start_time_dt = t1[-1] + pd.Timedelta(hours=18)
        w = np.where(t11 > t11_start_time_dt)[0]
        if len(w) > 0:
            w = w[0]
            t1 = t1.append(t11[w:])
            data1 = np.append(data1, data11[w:])

        t22_start_time_dt = t2[-1] + pd.Timedelta(hours=18)
        w = np.where(t22 > t22_start_time_dt)[0]
        if len(w) > 0:
            w = w[0]
            t2 = t2.append(t22[w:])
            data2 = np.append(data2, data22[w:])

        # sync
        if t2[-1] >= t1[-1]:
            t1 = t1.drop([t1[-1]]).append(pd.Index([t2[-1]]))
        else:
            t2 = t2.drop([t2[-1]]).append(pd.Index([t1[-1]]))

    t3, sub = data_sub(t1, data1, t2, data2)
    t4, div = data_div(t1, data1, t2, data2)

    # datas1 = [[t3,sub,var1 + ' - ' + var2 + ' 指数'],
    #           [t4,div,var1 + ' / ' + var2 + ' 指数'],
    #           ]
    # datas2 = [[t1,data1,var1 + ' 指数'],
    #           [t2,data2,var2 + ' 指数']]
    datas1 = [
              [t4,div,var1 + ' / ' + var2 + ' 指数'],
              ]
    datas2 = [[t1,data1,var1 + ' 指数'],
              [t2,data2,var2 + ' 指数']]
    plot_mean_std(datas1, datas2, T=int(250*3), max_height=220, start_time=start_time, end_time=end_time)
    
    # # 散点图
    # fig1 = plot_circle(datas2, width=600, height=600, ret=True)

    # datas2 = [[t1[-250:],data1[-250:],var1 + ' 指数 (最近一年)'],
    #           [t2[-250:],data2[-250:],var2 + ' 指数 (最近一年)']]
    # fig2 = plot_circle(datas2, width=600, height=600, ret=True)
    # show(row(fig1,fig2))

    # t1, data1 = get_period_data(t1,data1, start_time, end_time, remove_nan=True)
    # t2, data2 = get_period_data(t2,data2, start_time, end_time, remove_nan=True)
    # idx1 = np.isin(t1, t2)
    # idx2 = np.isin(t2, t1)
    # x = data1[idx1]
    # y = data2[idx2]
    # _, intercept, _, _, _ = linregress(x, y)
    # data2 -= intercept
    # t4, div = data_div(t1, data1, t2, data2)

    # datas1 = [
    #           [t4,div,var1 + ' / (' + var2 + ' - intercept)' + ' 指数, ' + 'intercept = ' + str(round(intercept,2))],
    #           ]
    # datas2 = [[t1,data1,var1 + ' 指数'],
    #           [t2,data2,var2 + ' - intercept)' + ' 指数']]
    # plot_mean_std(datas1, datas2, T=int(250*2), max_height=220, start_time=start_time, end_time=end_time)


    if intraday == False:
        t1 = pd.DatetimeIndex(pd.to_datetime(fut_df1['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
        t2 = pd.DatetimeIndex(pd.to_datetime(fut_df2['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    else:
        t1 = pd.DatetimeIndex(pd.to_datetime(fut_df1['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
        t2 = pd.DatetimeIndex(pd.to_datetime(fut_df2['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))

    data1 = np.array(fut_df1['dom']['close'], dtype=float)
    data2 = np.array(fut_df2['dom']['close'], dtype=float)
    inst_id1 = np.array(fut_df1['dom']['inst_id'])[-1]
    inst_id2 = np.array(fut_df2['dom']['inst_id'])[-1]

    t3, sub = data_sub(t1, data1, t2, data2)
    t4, div = data_div(t1, data1, t2, data2)

    # datas1 = [[t3,sub,var1 + ' - ' + var2 + ' 主力'],
    #           [t4,div,var1 + ' / ' + var2 + ' 主力'],
    #           ]
    # datas2 = [[t1,data1,var1 + ' 主力'],
    #           [t2,data2,var2 + ' 主力']]
    datas1 = [
              [t4,div,var1 + ' / ' + var2 + ' 主力 '],
              ]
    datas2 = [[t1,data1,var1 + ' 主力 ' + inst_id1],
              [t2,data2,var2 + ' 主力 ' + inst_id2]]
    plot_mean_std(datas1, datas2, T=int(250*2), max_height=220, start_time=start_time, end_time=end_time)



def compare_price_in_different_currency(t0, price0, currency0, t1, price1, currency1, adjust=1.0, variety=''):
    name = currency0 + currency1  # example: 'USD' + 'CNY' = USDCNY
    path = os.path.join(fx_dir, name+'.csv')
    if not(os.path.exists(path)):
        print('ERROR:', name)
        exit()
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    fx = np.array(df['close'], dtype=float)

    price0 *= adjust
    t2, price2 = data_mul(t0, price0, t, fx)
    t3, diff = data_sub(t1, price1, t2, price2)

    datas = [
             [[[t1,price1,variety+' in '+currency1,'color=orange'],
               [t2,price2,variety+' from '+currency0+' to '+currency1,'color=blue'],
              ],
              [[t3,diff,currency1+' 溢价','style=vbar'],],''],

             [[[t1,price1,variety+' in '+currency1,'color=orange'],],
              [[t0,price0,variety+' in '+currency0,'color=blue'],],''],
    ]
    plot_many_figure(datas)

    # 散点图
    plot_circle(datas[0][0], width=600, height=600)



def plot_future_month_diff(variety, month1, month2):
    earlist_year = 2018
    now = datetime.datetime.now()

    for exchange in exchange_dict:
        if variety in exchange_dict[exchange]:
            break

    fig = figure(frame_width=1400, frame_height=600, tools=TOOLS, title=variety + ' ' + str(month1) + '-' + str(month2), x_axis_type = "datetime")
    n = -1
    
    start_year = now.year + 1
    while (start_year >= earlist_year):
        if exchange == 'czce':
            y_str = str(start_year)[3]
        else:
            y_str = str(start_year)[2:]
        start_year -= 1

        m_str1 = str(month1)
        if len(m_str1) == 1:
            m_str1 = '0' + m_str1
        m_str2 = str(month2)
        if len(m_str2) == 1:
            m_str2 = '0' + m_str2

        inst_id1 = variety + y_str + m_str1
        inst_id2 = variety + y_str + m_str2

        try:
            t1, _, _, _, c1 = get_future_inst_id_data(exchange, inst_id1)
            t2, _, _, _, c2 = get_future_inst_id_data(exchange, inst_id2)
            idx1 = np.where(c1 > 1)[0]
            idx2 = np.where(c2 > 1)[0]
            t1 = t1[idx1]
            c1 = c1[idx1]
            t2 = t2[idx2]
            c2 = c2[idx2]

            tmp, diff = data_sub(t1, c1, t2, c2)
            t3 = []
            for i in range(len(tmp)):
                t3.append(datetime.datetime(year=tmp[i].year-(tmp[0].year-2000), month=tmp[i].month, day=tmp[i].day))
            t3 = np.array(t3)

            if n == -1:
                fig.line(t3, diff, line_width=4, line_color='black', legend_label=str(start_year+1))
            else:
                fig.line(t3, diff, line_width=2, line_color=many_colors[n], legend_label=str(start_year+1))
            n += 1

            fig.xaxis[0].ticker.desired_num_ticks = 20
            fig.legend.click_policy="hide"
            fig.legend.location='top_left'

        except:
            pass

    show(fig)


sina_usd_symbol_dict = {
    'sc': 'WTI',
    'au': 'GOLD',
    'ag': 'SILVER',
    'cu': 'COPPER',
    'al': 'ALUMINUM',
    'zn': 'ZINC',
}

sina_cny_symbol_dict = {
    'sc': 'SC0',
    'au': 'AU0',
    'ag': 'AG0',
    'cu': 'CU0',
    'zn': 'ZN0',
    'al': 'AL0',
}


def compare_cfd_data(variety):
    path = os.path.join(cfd_dir, sina_cny_symbol_dict[variety]+'_intraday'+'.csv')
    cny_df = pd.read_csv(path)
    cny_t = pd.DatetimeIndex(pd.to_datetime(cny_df['time'], format='%Y-%m-%d %H:%M:%S'))
    cny_close = np.array(cny_df['close'], dtype=float)

    path = os.path.join(cfd_dir, sina_usd_symbol_dict[variety]+'_CFD_intraday'+'.csv')
    usd_df = pd.read_csv(path)
    usd_t = pd.DatetimeIndex(pd.to_datetime(usd_df['time'], format='%Y-%m-%d %H:%M:%S'))
    usd_close = np.array(usd_df['close'], dtype=float)

    # usdcny
    path = os.path.join(cfd_dir, 'USDCNY_intraday'+'.csv')
    usdcny_df = pd.read_csv(path)
    usdcny_t = pd.DatetimeIndex(pd.to_datetime(usdcny_df['time'], format='%Y-%m-%d %H:%M:%S'))
    usdcny_close = np.array(usdcny_df['close'], dtype=float)

    if (variety == 'au'):
        adjust = 31.103481
    elif (variety == 'ag'):
        adjust = 31.103481 / 1000
    else:
        adjust = 1/1.13

    usd_close = usd_close / adjust
    t1, usd_close_to_cny = data_mul(usd_t, usd_close, usdcny_t, usdcny_close)
    t2, diff = data_sub(cny_t, cny_close, t1, usd_close_to_cny)

    datas = [
             [[[cny_t,cny_close,variety+' CNY',''],
               [t1,usd_close_to_cny,variety+' USD TO CNY',''],
               ],
               [],''],

             [[[t2,diff,variety+' CNY溢价','style=vbar'],
               ],
               [],''],
    ]
    plot_many_figure(datas, start_time='2020-11-01')


def compare_future_month_diff():
    # 价差
    # plot_future_month_diff('y', 1, 5)
    # plot_future_month_diff('i', 5, 9)
    # plot_future_month_diff('cs', 1, 5)
    # plot_future_month_diff('UR', 1, 3)


    # for variety in exchange_dict['dce']:
    #     plot_future_month_diff(variety, 1, 3)
    #     plot_future_month_diff(variety, 1, 5)
    #     plot_future_month_diff(variety, 5, 7)
    #     plot_future_month_diff(variety, 5, 9)

    pass


if __name__=="__main__":
    update_commodity_cfd_intraday_data()
    update_cn_commodity_cfd_intraday_data()
    update_usdcny_intraday()
    compare_cfd_data('sc')
    compare_cfd_data('au')
    compare_cfd_data('ag')
    compare_cfd_data('cu')
    compare_cfd_data('al')
    compare_cfd_data('zn')

    # compare_two_future_data('ao', 'SH')
    # compare_future_month_diff()
    compare_two_future_data('j', 'SM')
    compare_two_future_data('j', 'SF')
    compare_two_future_data('jm', 'SM')
    compare_two_future_data('jm', 'SF')
    # compare_two_future_data('SM', 'SF')

    compare_two_future_data('au', 'ag')
    # compare_two_future_data('cu', 'al')
    # compare_two_future_data('hc', 'rb')
    # compare_two_future_data('sc', 'bu')
    # compare_two_future_data('sc', 'TA')
    compare_two_future_data('i', 'rb')
    compare_two_future_data('i', 'j')


    compare_two_future_data('a', 'b')
    # compare_two_future_data('y', 'b')
    # compare_two_future_data('y', 'a')
    compare_two_future_data('m', 'RM')
    compare_two_future_data('a', 'RM')
    compare_two_future_data('b', 'RM')

    # # compare_two_future_data('pg', 'eb')
    # # compare_two_future_data('pp', 'eb')

    # compare_two_future_data('TA', 'PF')
    # # compare_two_future_data('TA', 'sc')
    # compare_two_future_data('TA', 'PX')
    compare_two_future_data('nr', 'l')

    # compare_two_future_data('c', 'cs')
    compare_two_future_data('CF', 'PK')

    compare_two_future_data('pp', 'MA')
    compare_two_future_data('eg', 'MA')
    compare_two_future_data('pp', 'pg')
    compare_two_future_data('MA', 'pg')

    compare_two_future_data('UR', 'pg')
    compare_two_future_data('UR', 'MA')
    compare_two_future_data('UR', 'j')
    compare_two_future_data('UR', 'a')
    compare_two_future_data('UR', 'b')
    compare_two_future_data('UR', 'c')
    compare_two_future_data('UR', 'l')
    compare_two_future_data('UR', 'p')
    compare_two_future_data('UR', 'y')
    compare_two_future_data('UR', 'OI')
    compare_two_future_data('UR', 'SF')

    compare_two_future_data('MA', 'l')

    compare_two_future_data('y', 'OI')
    # compare_two_future_data('y', 'p')
    compare_two_future_data('p', 'OI')


    pass
