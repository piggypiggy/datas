import os
import time
import numpy as np
import pandas as pd
import datetime
from bokeh.io import output_file, show
from bokeh.layouts import column
from bokeh.plotting import figure
from bokeh.models import LinearAxis, Range1d, VBar, NumeralTickFormatter
from scipy.stats import linregress
from utils import *

# ,Unnamed: [0-9]+_level_[0-9]
INSITUTION_POSITION_WATCH = {

    'i': ['摩根大通', '乾坤期货', '中信期货', '方正中期'],
    'p': ['摩根大通', '乾坤期货', '一德期货', '华泰期货'],
    'TA': ['摩根大通', '乾坤期货', '中信期货', '国贸期货'],
    'a': ['摩根大通', '乾坤期货', '一德期货', '中粮期货'],
    'b': ['摩根大通', '乾坤期货', '国泰君安', '海证期货'],
    'm': ['摩根大通', '乾坤期货', '华泰期货', '国投安信'],
    'y': ['摩根大通', '国投安信', '中粮期货', '浙商期货'],

    'RM': ['摩根大通', '乾坤期货', '中粮期货', '华泰期货', '广发期货'],
    'OI': ['摩根大通', '乾坤期货', '中粮期货', '瑞银期货'],

    'MA': ['瑞银期货', '乾坤期货', '国泰君安', '国贸期货'],
    'c': ['摩根大通', '中粮期货', '国投安信', '新湖期货'],
    'SR': ['浙商期货', '中粮期货', '华泰期货'],
    'PK': ['摩根大通', '乾坤期货', '东吴期货', '国富期货'],
    'CF': ['恒力期货', '中粮期货', '银河期货', '金瑞期货'],
    'SA': ['中信期货', '国泰君安', '先锋期货', '宝城期货'],

    'au': ['中财期货', '乾坤期货', '海通期货', '齐盛期货', '平安期货'],
    'ag': ['中财期货', '乾坤期货', '国泰君安', '华泰期货', '中泰期货'],
    'cu': ['铜冠金源', '瑞银期货', '国泰君安', '五矿期货'],
    'bc': ['摩根大通', '乾坤期货', '五矿期货'],   
    'al': ['乾坤期货', '中信期货', '方正中期', '海通期货'],
    'zn': ['乾坤期货', '中信期货', '铜冠金源', '五矿期货', '国信期货'],

    'lh': ['中粮期货', '中信期货', '方正中期'],
    'v': ['中财期货', '浙商期货', '中信期货', '银河期货'],
    'l': ['乾坤期货', '永安期货', '东证期货', '光大期货'],
    'PF': ['乾坤期货', '华泰期货', '光大期货'],

}


def moving_correlation(t_price, price, price_days, t_position, position, position_days):
    idx1 = np.isin(t_price, t_position)
    idx2 = np.isin(t_position, t_price)
    t1 = t_price[idx1]
    data1 = price[idx1]
    t2 = t_position[idx2]
    data2 = position[idx2]

    data1_chg_pct = data1.copy()
    data1_chg_pct[price_days:] = data1[price_days:]/data1[:-price_days] - 1
    data1_chg_pct[:price_days] = 0
    data2_chg_abs = data2.copy()
    data2_chg_abs[position_days:] = data2[position_days:]-data2[:-position_days]
    data2_chg_abs[:position_days] = 0

    data1_chg_bin = data1.copy()
    data1_chg_bin[price_days:] = data1[price_days:]>data1[:-price_days]
    data1_chg_bin[:price_days] = False
    data1_chg_bin = data1_chg_bin.astype(float) * 2 - 1
    data2_chg_bin = data2.copy()
    data2_chg_bin[position_days:] = data2[position_days:]>data2[:-position_days]
    data2_chg_bin[:position_days] = False
    data2_chg_bin = data2_chg_bin.astype(float) * 2 - 1

    # exclude friday
    # idx = []
    # for i in range(len(t1)):
    #     if (t1[i].weekday() == 4):
    #         idx.append(False)
    #     else:
    #         idx.append(True)
    # t1 = t1[idx]
    # t2 = t2[idx]
    # data1_chg_pct = data1_chg_pct[idx]
    # data2_chg_abs = data2_chg_abs[idx]
    # data1_chg_bin = data1_chg_bin[idx]
    # data2_chg_bin = data2_chg_bin[idx]

    delay = 0
    pct_corr_1y = np.corrcoef(data1_chg_pct[-52:], data2_chg_abs[-52:])[0,1]
    bin_corr_1y = np.corrcoef(data1_chg_bin[-52:], data2_chg_bin[-52:])[0,1]
    pct_corr_2y = np.corrcoef(data1_chg_pct[-104:], data2_chg_abs[-104:])[0,1]
    bin_corr_2y = np.corrcoef(data1_chg_bin[-104:], data2_chg_bin[-104:])[0,1]
    pct_corr_3y = np.corrcoef(data1_chg_pct[-156:], data2_chg_abs[-156:])[0,1]
    bin_corr_3y = np.corrcoef(data1_chg_bin[-156:], data2_chg_bin[-156:])[0,1]
    print('delay=',delay,round(pct_corr_1y,3),round(bin_corr_1y,3),round(pct_corr_2y,3),round(bin_corr_2y,3),round(pct_corr_3y,3),round(bin_corr_3y,3))

    delay = 1
    pct_corr_1y = np.corrcoef(data1_chg_pct[-52:], data2_chg_abs[-52-delay:-delay])[0,1]
    bin_corr_1y = np.corrcoef(data1_chg_bin[-52:], data2_chg_bin[-52-delay:-delay])[0,1]
    pct_corr_2y = np.corrcoef(data1_chg_pct[-104:], data2_chg_abs[-104-delay:-delay])[0,1]
    bin_corr_2y = np.corrcoef(data1_chg_bin[-104:], data2_chg_bin[-104-delay:-delay])[0,1]
    pct_corr_3y = np.corrcoef(data1_chg_pct[-156:], data2_chg_abs[-156-delay:-delay])[0,1]
    bin_corr_3y = np.corrcoef(data1_chg_bin[-156:], data2_chg_bin[-156-delay:-delay])[0,1]
    print('delay=',delay,round(pct_corr_1y,3),round(bin_corr_1y,3),round(pct_corr_2y,3),round(bin_corr_2y,3),round(pct_corr_3y,3),round(bin_corr_3y,3))

    delay = 2
    pct_corr_1y = np.corrcoef(data1_chg_pct[-52:], data2_chg_abs[-52-delay:-delay])[0,1]
    bin_corr_1y = np.corrcoef(data1_chg_bin[-52:], data2_chg_bin[-52-delay:-delay])[0,1]
    pct_corr_2y = np.corrcoef(data1_chg_pct[-104:], data2_chg_abs[-104-delay:-delay])[0,1]
    bin_corr_2y = np.corrcoef(data1_chg_bin[-104:], data2_chg_bin[-104-delay:-delay])[0,1]
    pct_corr_3y = np.corrcoef(data1_chg_pct[-156:], data2_chg_abs[-156-delay:-delay])[0,1]
    bin_corr_3y = np.corrcoef(data1_chg_bin[-156:], data2_chg_bin[-156-delay:-delay])[0,1]
    print('delay=',delay,round(pct_corr_1y,3),round(bin_corr_1y,3),round(pct_corr_2y,3),round(bin_corr_2y,3),round(pct_corr_3y,3),round(bin_corr_3y,3))

    delay = 3
    pct_corr_1y = np.corrcoef(data1_chg_pct[-52:], data2_chg_abs[-52-delay:-delay])[0,1]
    bin_corr_1y = np.corrcoef(data1_chg_bin[-52:], data2_chg_bin[-52-delay:-delay])[0,1]
    pct_corr_2y = np.corrcoef(data1_chg_pct[-104:], data2_chg_abs[-104-delay:-delay])[0,1]
    bin_corr_2y = np.corrcoef(data1_chg_bin[-104:], data2_chg_bin[-104-delay:-delay])[0,1]
    pct_corr_3y = np.corrcoef(data1_chg_pct[-156:], data2_chg_abs[-156-delay:-delay])[0,1]
    bin_corr_3y = np.corrcoef(data1_chg_bin[-156:], data2_chg_bin[-156-delay:-delay])[0,1]
    print('delay=',delay,round(pct_corr_1y,3),round(bin_corr_1y,3),round(pct_corr_2y,3),round(bin_corr_2y,3),round(pct_corr_3y,3),round(bin_corr_3y,3))



def calculate_many_correlation(fut):
    for i in range(len(fut)):
        path = os.path.join(future_position_dir, fut[i][0], fut[i][1]+'.csv')
        df = pd.read_csv(path, header=[0,1,2]).fillna('0')
        t1 = pd.DatetimeIndex(pd.to_datetime(df['time']['Unnamed: 0_level_1']['Unnamed: 0_level_2'], format='%Y-%m-%d'))

        top5_L = np.array(df.loc[:, pd.IndexSlice[['1','2','3','4'], 'top5', 'long_open_interest']], dtype=float)
        top5_S = np.array(df.loc[:, pd.IndexSlice[['1','2','3','4'], 'top5', 'short_open_interest']], dtype=float)
        top10_L = np.array(df.loc[:, pd.IndexSlice[['1','2','3','4'], 'top10', 'long_open_interest']], dtype=float)
        top10_S = np.array(df.loc[:, pd.IndexSlice[['1','2','3','4'], 'top10', 'short_open_interest']], dtype=float)
        top15_L = np.array(df.loc[:, pd.IndexSlice[['1','2','3','4'], 'top15', 'long_open_interest']], dtype=float)
        top15_S = np.array(df.loc[:, pd.IndexSlice[['1','2','3','4'], 'top15', 'short_open_interest']], dtype=float)
        top20_L = np.array(df.loc[:, pd.IndexSlice[['1','2','3','4'], 'top20', 'long_open_interest']], dtype=float)
        top20_S = np.array(df.loc[:, pd.IndexSlice[['1','2','3','4'], 'top20', 'short_open_interest']], dtype=float)

        top5_L_sum = np.sum(top5_L, axis=1)
        top5_S_sum = np.sum(top5_S, axis=1)
        top10_L_sum = np.sum(top10_L, axis=1)
        top10_S_sum = np.sum(top10_S, axis=1)
        top15_L_sum = np.sum(top15_L, axis=1)
        top15_S_sum = np.sum(top15_S, axis=1)
        top20_L_sum = np.sum(top20_L, axis=1)
        top20_S_sum = np.sum(top20_S, axis=1)

        path2 = os.path.join(future_price_dir, fut[i][0], fut[i][1]+'.csv')
        fut_df = pd.read_csv(path2, header=[0,1])
        t2 = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
        price_cny = np.array(fut_df['c3']['close'], dtype=float)

        top_net = top20_L_sum - top20_S_sum
        # top_net_acc = top_net.copy()
        # acc = 2
        # for k in range(1,acc):
        #     top_net_acc[acc-1:] += top_net[acc-1-k:-k]

        print(fut[i][1])
        moving_correlation(t2, price_cny, 1, t1, top_net, 5)




def test1():
    fut = [
           ['shfe', 'au', '黄金'],
           ['shfe', 'ag', '白银'],
           ['shfe', 'cu', '铜'],
           ['shfe', 'al', '铝'],
           ['shfe', 'zn', '锌'],
        #    ['shfe', 'ni', '镍'],
        #    ['shfe', 'sn', '锡'],
           ['shfe', 'fu', '燃料油'],
           ['shfe', 'bu', '沥青'],
           ['shfe', 'rb', '螺纹钢'],
           ['shfe', 'hc', '热轧卷板'],
           ['shfe', 'ss', '不锈钢'],
        ]
    
    # fut = [
    #        ['dce', 'i', '铁矿石'],
    #        ['dce', 'j', '焦炭'],
    #        ['dce', 'jm', '焦煤'],
        #    ['dce', 'l', '塑料'],
        #    ['dce', 'a', '豆一'],
        #    ['dce', 'b', '豆二'],
        #    ['dce', 'c', '玉米'],
        #    ['dce', 'cs', '淀粉'],
        #    ['dce', 'jd', '鸡蛋'],
        #    ['dce', 'm', '豆粕'],
        #    ['dce', 'p', '棕榈'],
        #    ['dce', 'pp', 'PP'],
        #    ['dce', 'v', 'PVC'],
        #    ['dce', 'y', '豆油'],
        #    ['dce', 'eg', '甘醇'],
        #    ['dce', 'eb', '苯乙烯'],
        #    ['dce', 'pg', 'LPG'],
        #    ['dce', 'lh', '生猪'],
        # ]
    
    # fut = [
    #        ['czce', 'SR', '白糖'],
    #     ]
    
    # fut = [['cffex', 'IF', '沪深300股指'],
    #        ['cffex', 'IH', '上证50股指'],
    #        ['cffex', 'IC', '中证500股指'],
    #     ]
    
    plot_many_position(fut)


def test2():
    fut = [['shfe', 'au', '黄金'],
           ['shfe', 'ag', '白银'],
           ['shfe', 'cu', '铜'],
           ['shfe', 'al', '铝'],
           ['shfe', 'zn', '锌'],
           ['shfe', 'ni', '镍'],
           ['shfe', 'sn', '锡'],
           ['shfe', 'fu', '燃料油'],
           ['shfe', 'bu', '沥青'],
           ['shfe', 'rb', '螺纹钢'],
           ['shfe', 'hc', '热轧卷板'],
           ['shfe', 'ss', '不锈钢'],
        ]
    
    # fut = [['cffex', 'IF', '沪深300股指'],
    #        ['cffex', 'IH', '上证50股指'],
    #        ['cffex', 'IC', '中证500股指'],
    #     ]
    
    calculate_many_correlation(fut)



def calculate_intersect_position_chg(fut):
    for i in range(len(fut)):
        path = os.path.join(future_position_dir, fut[i][0], fut[i][1]+'.csv')
        df = pd.read_csv(path, header=[0,1,2]).fillna('0')
        t1 = pd.DatetimeIndex(pd.to_datetime(df['time']['time']['time'], format='%Y-%m-%d'))

        z = ['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20']
        zn = np.array(z)

        long_sum_list = []
        short_sum_list = []
        long_chg_list = []
        short_chg_list = []
        for k in range(len(t1)-1):
            long2_sum = 0
            long2_chg = 0
            short2_sum = 0
            short2_chg = 0
            for j in range(3):
                long_party_name1 = np.array(df.loc[k, pd.IndexSlice[z[j], z, 'long_party_name']], dtype=str)
                short_party_name1 = np.array(df.loc[k, pd.IndexSlice[z[j], z, 'short_party_name']], dtype=str)

                long_party_name2 = np.array(df.loc[k+1, pd.IndexSlice[z[j], z, 'long_party_name']], dtype=str)
                short_party_name2 = np.array(df.loc[k+1, pd.IndexSlice[z[j], z, 'short_party_name']], dtype=str)

                idx1 = np.isin(long_party_name1, short_party_name1)
                common1 = long_party_name1[idx1]
                idx2 = np.isin(long_party_name2, short_party_name2)
                common2 = long_party_name2[idx2]
                idx = np.isin(common1, common2)
                common = common1[idx]

                idx_long2 = np.isin(long_party_name2, common)
                # long2_sum += float(df.loc[k+1, pd.IndexSlice[z[j], zn[idx_long2], 'long_open_interest']].sum())
                long2_chg += float(df.loc[k+1, pd.IndexSlice[z[j], zn[idx_long2], 'long_open_interest_chg']].sum())
                idx_short2 = np.isin(short_party_name2, common)
                # short2_sum += float(df.loc[k+1, pd.IndexSlice[z[j], zn[idx_short2], 'short_open_interest']].sum())
                short2_chg += float(df.loc[k+1, pd.IndexSlice[z[j], zn[idx_short2], 'short_open_interest_chg']].sum())

            # long_sum_list.append(long2_sum)
            long_chg_list.append(long2_chg)
            # short_sum_list.append(short2_sum)
            short_chg_list.append(short2_chg)

        t1 = t1[1:]
        # long_sum = np.array(long_sum_list, dtype=float)
        long_chg = np.array(long_chg_list, dtype=float)
        # short_sum = np.array(short_sum_list, dtype=float)
        short_chg = np.array(short_chg_list, dtype=float)
        
        path2 = os.path.join(future_price_dir, fut[i][0], fut[i][1]+'.csv')
        fut_df = pd.read_csv(path2, header=[0,1])
        t2 = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
        price_cny = np.array(fut_df['c3']['close'], dtype=float)

        # top_net = long_sum - short_sum
        # top_net_acc = top_net.copy()
        # acc = 2
        # for k in range(1,acc):
        #     top_net_acc[acc-1:] += top_net[acc-1-k:-k]

        # ts = [t1]
        # longs = [long_sum]
        # shorts = [short_sum]
        # names = ['top']
        # plot_position(t2, price_cny, '期货收盘价(c3):'+fut[i][2], ts=ts, longs=longs, shorts=shorts, names=names, period=250)

        fig1 = figure(frame_width=1400, frame_height=300, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right", toolbar_location='right')
        fig1.line(t2, price_cny, color='black', line_width=2, legend_label='期货收盘价(c3):'+fut[i][2])
        fig1.xaxis[0].ticker.desired_num_ticks = 20

        fig2 = figure(frame_width=1400, frame_height=300, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right", toolbar_location='right')
        fig2.varea(x=t1, y1=0, y2=long_chg-short_chg, fill_color='gray', legend_label='long_chg - short_chg')
        fig2.line(t1, long_chg, line_width=2, color='darkgreen', legend_label='多头变化量')
        fig2.line(t1, short_chg, line_width=2, color='red', legend_label='空头变化量')        
        fig2.xaxis[0].ticker.desired_num_ticks = 20
        fig2.legend.click_policy="hide"

        show(column(fig1,fig2))


def plot_institution_position(variety):
    for exchange in exchange_dict:
        if variety in exchange_dict[exchange]:
            break

    if variety in INSITUTION_POSITION_WATCH:
        institutions = INSITUTION_POSITION_WATCH[variety]
    else:
        return

    path = os.path.join(future_price_dir, exchange, variety+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    t1 = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    index_close = np.array(fut_df['index']['close'], dtype=float)

    datas_total = []
    datas1 = []
    datas2 = []

    for institution in institutions:
        time.sleep(0.1)
        path = os.path.join(future_position_dir, exchange, variety+'_'+institution+'.csv')
        df = pd.read_csv(path)
        t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
        long = np.array(df['total_long_oi'], dtype=float)
        short = np.array(df['total_short_oi'], dtype=float)

        long1 = np.array(df['c1_long_oi'], dtype=float)
        short1 = np.array(df['c1_short_oi'], dtype=float)

        long2 = np.array(df['c2_long_oi'], dtype=float)
        short2 = np.array(df['c2_short_oi'], dtype=float)

        c1_inst_id = df.loc[len(t)-1, 'c1_inst_id']
        c2_inst_id = df.loc[len(t)-1, 'c2_inst_id']

        datas_total.append(
                [[[t,long,institution + ' total long','color=red'],
                [t,short,institution + ' total short','color=darkgreen'],
                [t,long-short,institution + ' total net long','style=vbar'],
                ],
                [[t1,index_close,variety+' 指数','color=black,width=3'],],''])

        datas1.append(
                [[[t,long1,institution + ' c1 long','color=red'],
                [t,short1,institution + ' c1 short','color=darkgreen'],
                [t,long1-short1,institution + ' c1 net long, ' + c1_inst_id,'style=vbar'],
                ],
                [[t1,index_close,variety+' 指数','color=black,width=3'],],''])

        if type(c2_inst_id) == str:
            datas2.append(
                    [[[t,long2,institution + ' c2 long','color=red'],
                    [t,short2,institution + ' c2 short','color=darkgreen'],
                    [t,long2-short2,institution + ' c2 net long, ' + c2_inst_id,'style=vbar'],
                    ],
                    [[t1,index_close,variety+' 指数','color=black,width=3'],],''])

    plot_many_figure(datas_total)
    plot_many_figure(datas1)
    if type(c2_inst_id) == str:
        plot_many_figure(datas2)


def plot_some_institution_position(varieties):
    varieties = varieties.replace(' ','')
    s = varieties.split(',')

    for variety in s:
        plot_institution_position(variety)


def plot_all_institution_position():
    for variety in INSITUTION_POSITION_WATCH:
        plot_institution_position(variety)


def update_institution_position(variety):
    for exchange in exchange_dict:
        if variety in exchange_dict[exchange]:
            break

    if exchange == 'czce':
        czce = 1
    else:
        czce = 0

    if variety in INSITUTION_POSITION_WATCH:
        institutions = INSITUTION_POSITION_WATCH[variety]
    else:
        return

    path = os.path.join(future_position_dir, exchange, variety+'.csv')
    p_df = pd.read_csv(path, header=[0,1,2]).fillna('0')
    p_t = pd.DatetimeIndex(pd.to_datetime(p_df['time']['time']['time'], format='%Y-%m-%d'))

    earlist_time = '2017-01-01'
    for institution in institutions:
        path = os.path.join(future_position_dir, exchange, variety+'_'+institution+'.csv')
        if os.path.exists(path):
            old_df = pd.read_csv(path)
            t = pd.DatetimeIndex(pd.to_datetime(old_df['time'], format='%Y-%m-%d'))
            start_time_dt = t[-1] + pd.Timedelta(days=1)
        else:
            start_time_dt = pd.to_datetime(earlist_time, format='%Y-%m-%d')

        start_idx = np.where(p_t >= start_time_dt)[0]
        if len(start_idx) > 0:
            start_idx = start_idx[0]
        else:
            continue

        datas = []
        for i in range(start_idx, len(p_t)):
            print(variety, institution, p_t[i])
            total_volume = 0
            total_long = 0
            total_short = 0

            data = [p_t[i].strftime('%Y-%m-%d'), total_volume, total_long, total_short]

            for j in range(1+czce,5+czce):
                inst_id = ''
                volume = 0
                long = 0
                short = 0

                # print(p_df.loc[i,:])
                inst_id = p_df.loc[i, pd.IndexSlice[str(j), 'inst_id', 'inst_id']]
                if inst_id == '0':
                    inst_id = ''
                for k in range(1,21):
                    vol_party_name = p_df.loc[i, pd.IndexSlice[str(j), str(k), 'vol_party_name']]
                    if vol_party_name == institution:
                        volume = p_df.loc[i, pd.IndexSlice[str(j), str(k), 'vol']]
                        break

                for k in range(1,21):
                    long_party_name = p_df.loc[i, pd.IndexSlice[str(j), str(k), 'long_party_name']]
                    if long_party_name == institution:
                        long = p_df.loc[i, pd.IndexSlice[str(j), str(k), 'long_open_interest']]
                        break

                for k in range(1,21):
                    short_party_name = p_df.loc[i, pd.IndexSlice[str(j), str(k), 'short_party_name']]
                    if short_party_name == institution:
                        short = p_df.loc[i, pd.IndexSlice[str(j), str(k), 'short_open_interest']]
                        break

                data[1] += volume
                data[2] += long
                data[3] += short
                data += [inst_id, volume, long, short]
            datas.append(data)

        df = pd.DataFrame(columns=['time','total_volume','total_long_oi','total_short_oi',
                                   'c1_inst_id', 'c1_volume', 'c1_long_oi', 'c1_short_oi',
                                   'c2_inst_id', 'c2_volume', 'c2_long_oi', 'c2_short_oi',
                                   'c3_inst_id', 'c3_volume', 'c3_long_oi', 'c3_short_oi',
                                   'c4_inst_id', 'c4_volume', 'c4_long_oi', 'c4_short_oi',],
                         data=datas)

        if os.path.exists(path):
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            old_df.sort_values(by = 'time', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            df.sort_values(by = 'time', inplace=True)
            df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            df.to_csv(path, encoding='utf-8', index=False)


def update_all_institution_position():
    for variety in INSITUTION_POSITION_WATCH:
        update_institution_position(variety)


def plot_many_option_position(fut):
    for i in range(len(fut)):
        if (fut[i][1] == 'sc'):
            continue

        try:
            path = os.path.join(option_position_dir, fut[i][0], fut[i][1]+'.csv')
            df = pd.read_csv(path, header=[0,1,2,3]).fillna('0')
            df.drop_duplicates(inplace=True)
            t1 = pd.DatetimeIndex(pd.to_datetime(df['time']['time']['time']['time'], format='%Y-%m-%d'))
        except:
            return

        if (fut[i][0] == 'czce'):
            s = ['1']
        else:
            s = ['1','2','3','4','5']
        C_top5_L = np.array(df.loc[:, pd.IndexSlice['C', s, 'top5', 'long_open_interest']], dtype=float)
        C_top5_S = np.array(df.loc[:, pd.IndexSlice['C', s, 'top5', 'short_open_interest']], dtype=float)
        C_top10_L = np.array(df.loc[:, pd.IndexSlice['C', s, 'top10', 'long_open_interest']], dtype=float)
        C_top10_S = np.array(df.loc[:, pd.IndexSlice['C', s, 'top10', 'short_open_interest']], dtype=float)
        C_top15_L = np.array(df.loc[:, pd.IndexSlice['C', s, 'top15', 'long_open_interest']], dtype=float)
        C_top15_S = np.array(df.loc[:, pd.IndexSlice['C', s, 'top15', 'short_open_interest']], dtype=float)
        C_top20_L = np.array(df.loc[:, pd.IndexSlice['C', s, 'top20', 'long_open_interest']], dtype=float)
        C_top20_S = np.array(df.loc[:, pd.IndexSlice['C', s, 'top20', 'short_open_interest']], dtype=float)

        P_top5_L = np.array(df.loc[:, pd.IndexSlice['P', s, 'top5', 'long_open_interest']], dtype=float)
        P_top5_S = np.array(df.loc[:, pd.IndexSlice['P', s, 'top5', 'short_open_interest']], dtype=float)
        P_top10_L = np.array(df.loc[:, pd.IndexSlice['P', s, 'top10', 'long_open_interest']], dtype=float)
        P_top10_S = np.array(df.loc[:, pd.IndexSlice['P', s, 'top10', 'short_open_interest']], dtype=float)
        P_top15_L = np.array(df.loc[:, pd.IndexSlice['P', s, 'top15', 'long_open_interest']], dtype=float)
        P_top15_S = np.array(df.loc[:, pd.IndexSlice['P', s, 'top15', 'short_open_interest']], dtype=float)
        P_top20_L = np.array(df.loc[:, pd.IndexSlice['P', s, 'top20', 'long_open_interest']], dtype=float)
        P_top20_S = np.array(df.loc[:, pd.IndexSlice['P', s, 'top20', 'short_open_interest']], dtype=float)

        C_top5_L_sum = np.sum(C_top5_L, axis=1)
        C_top5_S_sum = np.sum(C_top5_S, axis=1)
        C_top10_L_sum = np.sum(C_top10_L, axis=1)
        C_top10_S_sum = np.sum(C_top10_S, axis=1)
        C_top15_L_sum = np.sum(C_top15_L, axis=1)
        C_top15_S_sum = np.sum(C_top15_S, axis=1)
        C_top20_L_sum = np.sum(C_top20_L, axis=1)
        C_top20_S_sum = np.sum(C_top20_S, axis=1)

        P_top5_L_sum = np.sum(P_top5_L, axis=1)
        P_top5_S_sum = np.sum(P_top5_S, axis=1)
        P_top10_L_sum = np.sum(P_top10_L, axis=1)
        P_top10_S_sum = np.sum(P_top10_S, axis=1)
        P_top15_L_sum = np.sum(P_top15_L, axis=1)
        P_top15_S_sum = np.sum(P_top15_S, axis=1)
        P_top20_L_sum = np.sum(P_top20_L, axis=1)
        P_top20_S_sum = np.sum(P_top20_S, axis=1)

        path2 = os.path.join(future_price_dir, fut[i][0], fut[i][1]+'.csv')
        fut_df = pd.read_csv(path2, header=[0,1])
        t2 = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
        price_cny = np.array(fut_df['c3']['close'], dtype=float)
        volume = np.array(fut_df['index']['vol'], dtype=float)
        oi = np.array(fut_df['index']['oi'], dtype=float)
        zeros = np.zeros_like(oi)

        time.sleep(0.25)
        ts = [t1, t1, t1, t1]
        longs = [C_top20_L_sum, C_top5_L_sum, C_top15_L_sum, C_top10_L_sum]
        shorts = [C_top20_S_sum, C_top5_S_sum, C_top15_S_sum, C_top10_S_sum]
        names = ['CALL top20', 'CALL top5', 'CALL top15', 'CALL top10']
        plot_position(t2, price_cny, '期货收盘价(c3):'+fut[i][2], ts=ts, longs=longs, shorts=shorts, names=names, period=250)

        ts = [t1, t1, t1, t1]
        longs = [P_top20_L_sum, P_top5_L_sum, P_top15_L_sum, P_top10_L_sum]
        shorts = [P_top20_S_sum, P_top5_S_sum, P_top15_S_sum, P_top10_S_sum]
        names = ['PUT top20', 'PUT top5', 'PUT top15', 'PUT top10']
        plot_position(t2, price_cny, '期货收盘价(c3):'+fut[i][2], ts=ts, longs=longs, shorts=shorts, names=names, period=250)

        ts = [t1, t1, t1, t1]
        longs =  [C_top20_L_sum+P_top20_S_sum, C_top5_L_sum+P_top5_S_sum, C_top15_L_sum+P_top15_S_sum, C_top10_L_sum+P_top10_S_sum]
        shorts = [C_top20_S_sum+P_top20_L_sum, C_top5_S_sum+P_top5_L_sum, C_top15_S_sum+P_top15_L_sum, C_top10_S_sum+P_top10_L_sum]
        names = ['top20', 'top5', 'top15', 'top10']
        plot_position(t2, price_cny, '期货收盘价(c3):'+fut[i][2], ts=ts, longs=longs, shorts=shorts, names=names, period=250)


        # time.sleep(0.25)
        # datas1 = [[t1, top20_L_sum-top20_S_sum, 'top20 net long'],
        #           [t1, top10_L_sum-top10_S_sum, 'top10 net long'],
        #           [t1, top5_L_sum-top5_S_sum, 'top5 net long']]
        # datas2 = [[t2, price_cny, '期货收盘价(c3):'+fut[i][2]],
        #           [t2, oi, '总持仓:'+fut[i][2]],
        #           [t2, volume, '总成交量:'+fut[i][2]],]
        # plot_mean_std(datas1, datas2, T=int(250*2))

        # _, top5_L_pct = data_div(t1, top5_L_sum, t2, oi)
        # _, top5_S_pct = data_div(t1, top5_S_sum, t2, oi)
        # _, top20_L_pct = data_div(t1, top20_L_sum, t2, oi)
        # t3, top20_S_pct = data_div(t1, top20_S_sum, t2, oi)
    
        # time.sleep(0.25)
        # datas1 = [[t3, top20_L_pct, 'top20 long / total oi'],
        #           [t3, top20_S_pct, 'top20 short / total oi'],
        #           [t3, top5_L_pct, 'top5 long / total oi'],
        #           [t3, top5_S_pct, 'top5 short / total oi']]
        # datas2 = [[t2, price_cny, '期货收盘价(c3):'+fut[i][2]]]
        # plot_mean_std(datas1, datas2, T=int(250*2))



def plot_term_structure(exchange, variety):
    path = os.path.join(future_price_dir, exchange, variety+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))

    z = [['today', 0],
         ['1d before', -1],
         ['2d before', -2],
         ['3d before', -3],
         ['1w before', -7],
         ['10d before', -10],
         ['2w before', -14],
        #  ['3w before', -21],
         ['1m before', -30],
         ['6w before', -42],
        #  ['2m before', -60],
         ]

    L = len(fut_t)
    datas = []
    cs = ['c1','c2','c3','c4','c5','c6','c7','c8','c9']
    cs_array = np.array(cs)
    for i in range(len(z)):
        offset = z[i][1]
        t = fut_t[L-1] + pd.Timedelta(days=offset)
        w = np.where(fut_t >= t)[0][0]
        if (w == 0):
            break

        temp = np.array(fut_df.loc[w, pd.IndexSlice[cs, 'close']])
        inst_ids = np.array(fut_df.loc[w, pd.IndexSlice[cs, 'inst_id']])
        datas.append([t, inst_ids, temp])

    temp = []
    for i in range(len(datas)-1, -1, -1):
        temp += list(datas[i][1])
    temp = [a for a in temp if type(a) == str] 
    inst_ids_set = list(set(temp))
    inst_ids_set.sort(key=temp.index)

    fig1 = figure(x_range = inst_ids_set, frame_width=1400, frame_height=300, tools=TOOLS, title=variety)
    for i in range(len(datas)):
        mask1 = np.logical_not(np.isnan(datas[i][2]))
        mask2 = np.abs(datas[i][2]) > 100
        mask = np.logical_and(mask1, mask2)
        
        if i == 0:
            fig1.line(x=datas[i][1][mask], y=datas[i][2][mask], line_width=4, line_color='black', legend_label=z[i][0])
        else:
            fig1.line(x=datas[i][1][mask], y=datas[i][2][mask], line_width=2, line_color=many_colors[i], legend_label=z[i][0])

    fig1.legend.click_policy="hide"
    fig1.legend.location='top_left'

    fig2 = figure(x_range = cs, frame_width=1400, frame_height=300, tools=TOOLS, title=variety)
    for i in range(len(datas)):
        mask1 = np.logical_not(np.isnan(datas[i][2]))
        mask2 = np.abs(datas[i][2]) > 100
        mask = np.logical_and(mask1, mask2)
        datas[i][2] = datas[i][2] - datas[i][2][0]
        if i == 0:
            fig2.line(x=cs_array[mask], y=datas[i][2][mask], line_width=4, line_color='black', legend_label=z[i][0])
        else:
            fig2.line(x=cs_array[mask], y=datas[i][2][mask], line_width=2, line_color=many_colors[i], legend_label=z[i][0])

    fig2.legend.click_policy="hide"
    fig2.legend.location='top_left'


    c1 = np.array(fut_df['c1']['close'], dtype=float)
    mask1 = np.abs(c1) > 100
    c2 = np.array(fut_df['c2']['close'], dtype=float)
    mask2 = np.abs(c2) > 100
    c3 = np.array(fut_df['c3']['close'], dtype=float)
    mask3 = np.abs(c3) > 100
    c4 = np.array(fut_df['c4']['close'], dtype=float)
    mask4 = np.abs(c4) > 100
    index_close = np.array(fut_df['index']['close'], dtype=float)

    t1, diff1 = data_sub(fut_t[mask1], c1[mask1], fut_t[mask3], c3[mask3])
    # t2, diff2 = data_sub(fut_t[mask1], c1[mask1], fut_t[mask4], c4[mask4])
    t3, diff3 = data_sub(fut_t[mask2], c2[mask2], fut_t[mask4], c4[mask4])
    datas = [[[[t1,diff1,'c1 - c3',''],
               [t3,diff3,'c2 - c4','']],
              [[fut_t,index_close,variety+ '指数','color=black'],],''],]
    fig3 = plot_many_figure(datas, max_height=300, ret=True)
    show(column(fig3+[fig1,fig2]))


if __name__=="__main__":
    # fut = [['dce', 'a','a'],
    #        ['dce', 'b','b'],
    #        ['dce', 'm','m'],
    #        ['dce', 'i','i'],
    #        ['dce', 'p','p'],
    #        ['dce', 'pp','pp'],
    #        ['dce', 'pg','pg'],
    #        ['dce', 'y','y'],
    #        ['dce', 'eg','eg'],
    #        ['dce', 'eb','eb'],
    #        ['dce', 'v','v'],
    #        ['dce', 'l','l'],]
    # plot_many_option_position(fut)

    update_all_institution_position()
    # plot_all_institution_position()
    # plot_some_institution_position('a,b,m,y,RM')
    # plot_some_institution_position('au,ag')
    # plot_some_institution_position('cu,bc,al,zn')
    # plot_some_institution_position('PK,CF')
    # plot_some_institution_position('TA')
    # fut = [['czce', 'MA', 'MA']]
    # calculate_intersect_position_chg(fut)

    pass

# test2()



