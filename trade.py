import os
import numpy as np
import pandas as pd
import datetime
from utils import *


# 出口
def test1():
    path = os.path.join(data_dir, '进出口'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))
    
    start_time = '2005-01-01'
    end_time = '2055-01-01'
    imbalance = np.array(df['贸易顺差:当月值'], dtype=float)
    datas = [[t, imbalance, '贸易顺差:当月值']]
    plot_one_figure(datas, '', start_time, end_time)


    export_all = np.array(df['出口金额:当月值'], dtype=float)
    export_us = np.array(df['美国:出口金额:当月值'], dtype=float)
    export_eu = np.array(df['欧盟:出口金额:当月值'], dtype=float)
    export_de = np.array(df['德国:出口金额:当月值'], dtype=float)
    export_fr = np.array(df['法国:出口金额:当月值'], dtype=float)
    export_uk = np.array(df['英国:出口金额:当月值'], dtype=float)
    export_jp = np.array(df['日本:出口金额:当月值'], dtype=float)
    export_kr = np.array(df['韩国:出口金额:当月值'], dtype=float)
    export_vn = np.array(df['越南:出口金额:当月值'], dtype=float)
    export_asean = np.array(df['东南亚国家联盟:出口金额:当月值'], dtype=float)

    plot_seasonality(t, export_all, start_year=2012, title='export_all')
    plot_seasonality(t, export_us, start_year=2012, title='export_us')
    plot_seasonality(t, export_eu, start_year=2012, title='export_eu')
    plot_seasonality(t, export_de, start_year=2012, title='export_de')
    plot_seasonality(t, export_fr, start_year=2012, title='export_fr')
    plot_seasonality(t, export_uk, start_year=2012, title='export_uk')
    plot_seasonality(t, export_jp, start_year=2012, title='export_jp')
    plot_seasonality(t, export_kr, start_year=2012, title='export_kr')
    plot_seasonality(t, export_vn, start_year=2012, title='export_vn')
    plot_seasonality(t, export_asean, start_year=2012, title='export_asean')


    # datas = [[t, export_us, '美国:出口金额:当月值'],
    #          [t, export_eu, '欧盟:出口金额:当月值'],
    #          [t, export_asean, '东南亚国家联盟:出口金额:当月值']]
    # plot_one_figure(datas, '', start_time, end_time)

    # datas = [[t, export_de, '德国:出口金额:当月值'],
    #          [t, export_fr, '法国:出口金额:当月值'],
    #          [t, export_uk, '英国:出口金额:当月值'],
    #          [t, export_jp, '日本:出口金额:当月值'],
    #          [t, export_kr, '韩国:出口金额:当月值'],
    #          [t, export_vn, '越南:出口金额:当月值'],]
    # plot_one_figure(datas, '', start_time, end_time)

    # export_all_acc = np.array(df['出口金额:累计值'], dtype=float)
    # export_us_acc = np.array(df['美国:出口金额:累计值'], dtype=float)
    # export_eu_acc = np.array(df['欧盟:出口金额:累计值'], dtype=float)
    # export_de_acc = np.array(df['德国:出口金额:累计值'], dtype=float)
    # export_fr_acc = np.array(df['法国:出口金额:累计值'], dtype=float)
    # export_uk_acc = np.array(df['英国:出口金额:累计值'], dtype=float)
    # export_jp_acc = np.array(df['日本:出口金额:累计值'], dtype=float)
    # export_kr_acc = np.array(df['韩国:出口金额:累计值'], dtype=float)
    # export_vn_acc = np.array(df['越南:出口金额:累计值'], dtype=float)
    # export_asean_acc = np.array(df['东南亚国家联盟:出口金额:累计值'], dtype=float)

    # export_all_acc_yoy = np.array(df['出口金额:累计同比'], dtype=float)
    # export_us_acc_yoy = np.array(df['美国:出口金额:累计同比'], dtype=float)
    # export_eu_acc_yoy = np.array(df['欧盟:出口金额:累计同比'], dtype=float)
    # export_de_acc_yoy = np.array(df['德国:出口金额:累计同比'], dtype=float)
    # export_fr_acc_yoy = np.array(df['法国:出口金额:累计同比'], dtype=float)
    # export_uk_acc_yoy = np.array(df['英国:出口金额:累计同比'], dtype=float)
    # export_jp_acc_yoy = np.array(df['日本:出口金额:累计同比'], dtype=float)
    # export_kr_acc_yoy = np.array(df['韩国:出口金额:累计同比'], dtype=float)
    # export_vn_acc_yoy = np.array(df['越南:出口金额:累计同比'], dtype=float)
    # export_asean_acc_yoy = np.array(df['东南亚国家联盟:出口金额:累计同比'], dtype=float)

    # datas = [[t, export_all_acc_yoy, '出口金额:累计同比'],
    #          [t, export_us_acc_yoy, '美国:出口金额:累计同比'],
    #          [t, export_eu_acc_yoy, '欧盟:出口金额:累计同比'],
    #          [t, export_de_acc_yoy, '德国:出口金额:累计同比'],
    #          [t, export_fr_acc_yoy, '法国:出口金额:累计同比'],
    #          [t, export_uk_acc_yoy, '英国:出口金额:累计同比'],
    #          [t, export_jp_acc_yoy, '日本:出口金额:累计同比'],
    #          [t, export_kr_acc_yoy, '韩国:出口金额:累计同比'],
    #          [t, export_vn_acc_yoy, '越南:出口金额:累计同比'],
    #          [t, export_asean_acc_yoy, '东南亚国家联盟:出口金额:累计同比']]

    # start_time = '2005-01-01'
    # end_time = '2055-01-01'
    # plot_one_figure(datas, '', start_time, end_time)

    # t1, export_useu_acc = data_add(t, export_us_acc, t, export_eu_acc)
    # t1, export_useu_acc_yoy = yoy_for_monthly_data(t1, export_useu_acc)
    # t2, export_yoy_diff = data_sub(t, export_asean_acc_yoy, t1, export_useu_acc_yoy)
    # datas = [[t2, export_yoy_diff, '东南亚-美欧:出口金额:累计同比差值']]
    # plot_one_figure(datas, '', start_time, end_time)

    # plot_seasonality(t, export_all_acc_yoy, start_year=2012, title='出口金额:累计同比')
    # plot_seasonality(t, export_us_acc_yoy, start_year=2012, title='美国:出口金额:累计同比')
    # plot_seasonality(t, export_eu_acc_yoy, start_year=2012, title='欧盟:出口金额:累计同比')
    # plot_seasonality(t, export_de_acc_yoy, start_year=2012, title='德国:出口金额:累计同比')
    # plot_seasonality(t, export_jp_acc_yoy, start_year=2012, title='日本:出口金额:累计同比')
    # plot_seasonality(t, export_kr_acc_yoy, start_year=2012, title='韩国:出口金额:累计同比')
    # plot_seasonality(t, export_vn_acc_yoy, start_year=2012, title='越南:出口金额:累计同比')
    # plot_seasonality(t, export_asean_acc_yoy, start_year=2012, title='东南亚国家联盟:出口金额:累计同比')


    # export_all_yoy = np.array(df['出口金额:当月同比'], dtype=float)
    # export_us_yoy = np.array(df['美国:出口金额:当月同比'], dtype=float)
    # export_eu_yoy = np.array(df['欧盟:出口金额:当月同比'], dtype=float)
    # export_de_yoy = np.array(df['德国:出口金额:当月同比'], dtype=float)
    # export_fr_yoy = np.array(df['法国:出口金额:当月同比'], dtype=float)
    # export_uk_yoy = np.array(df['英国:出口金额:当月同比'], dtype=float)
    # export_jp_yoy = np.array(df['日本:出口金额:当月同比'], dtype=float)
    # export_kr_yoy = np.array(df['韩国:出口金额:当月同比'], dtype=float)
    # export_vn_yoy = np.array(df['越南:出口金额:当月同比'], dtype=float)
    # export_asean_yoy = np.array(df['东南亚国家联盟:出口金额:当月同比'], dtype=float)

    # datas = [[t, export_all_yoy, '出口金额:当月同比'],
    #          [t, export_us_yoy, '美国:出口金额:当月同比'],
    #          [t, export_eu_yoy, '欧盟:出口金额:当月同比'],
    #          [t, export_de_yoy, '德国:出口金额:当月同比'],
    #          [t, export_fr_yoy, '法国:出口金额:当月同比'],
    #          [t, export_uk_yoy, '英国:出口金额:当月同比'],
    #          [t, export_jp_yoy, '日本:出口金额:当月同比'],
    #          [t, export_kr_yoy, '韩国:出口金额:当月同比'],
    #          [t, export_vn_yoy, '越南:出口金额:当月同比'],
    #          [t, export_asean_yoy, '东南亚国家联盟:出口金额:当月同比']]

    # start_time = '2005-01-01'
    # end_time = '2055-01-01'
    # plot_one_figure(datas, '', start_time, end_time)

    # plot_seasonality(t, export_all_yoy, start_year=2012, title='出口金额:当月同比')
    # plot_seasonality(t, export_us_yoy, start_year=2012, title='美国:出口金额:当月同比')
    # plot_seasonality(t, export_eu_yoy, start_year=2012, title='欧盟:出口金额:当月同比')
    # plot_seasonality(t, export_de_yoy, start_year=2012, title='德国:出口金额:当月同比')
    # plot_seasonality(t, export_jp_yoy, start_year=2012, title='日本:出口金额:当月同比')
    # plot_seasonality(t, export_kr_yoy, start_year=2012, title='韩国:出口金额:当月同比')
    # plot_seasonality(t, export_vn_yoy, start_year=2012, title='越南:出口金额:当月同比')
    # plot_seasonality(t, export_asean_yoy, start_year=2012, title='东南亚国家联盟:出口金额:当月同比')


# 原材料进口
def test2():
    path = os.path.join(data_dir, '进出口'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))
    
    start_time = '2005-01-01'
    end_time = '2055-01-01'

    import_i = np.array(df['进口数量:铁矿砂及其精矿:当月值'], dtype=float)
    import_oil = np.array(df['进口数量:原油:当月值'], dtype=float)
    # import_lng = np.array(df['进口数量:液化天然气:当月值'], dtype=float)
    import_cu1 = np.array(df['进口数量:铜矿砂及其精矿:当月值'], dtype=float)
    import_cu2 = np.array(df['进口数量:未锻造的铜及铜材:当月值'], dtype=float)
    t1, import_i_yoy = yoy_for_monthly_data(t, import_i)
    t2, import_oil_yoy = yoy_for_monthly_data(t, import_oil)
    # t3, import_lng_yoy = yoy_for_monthly_data(t, import_lng)
    t4, import_cu1_yoy = yoy_for_monthly_data(t, import_cu1)
    t5, import_cu2_yoy = yoy_for_monthly_data(t, import_cu2)

    datas = [[[[t,import_i,'进口数量:铁矿砂及其精矿:当月值','']],[[t1,import_i_yoy,'当月同比','']],'']]
    plot_many_figure(datas, start_time=start_time, end_time=end_time)
    datas = [[[[t,import_oil,'进口数量:原油:当月值','']],[[t2,import_oil_yoy,'当月同比','']],'']]
    plot_many_figure(datas, start_time=start_time, end_time=end_time)
    datas = [[[[t,import_cu1,'进口数量:铜矿砂及其精矿:当月值','']],[[t4,import_cu1_yoy,'当月同比','']],'']]
    plot_many_figure(datas, start_time=start_time, end_time=end_time)
    datas = [[[[t,import_cu2,'进口数量:未锻造的铜及铜材:当月值','']],[[t5,import_cu2_yoy,'当月同比','']],'']]
    plot_many_figure(datas, start_time=start_time, end_time=end_time)


    import_i_acc = np.array(df['进口数量:铁矿砂及其精矿:累计值'], dtype=float)
    import_oil_acc = np.array(df['进口数量:原油:累计值'], dtype=float)
    import_lng_acc = np.array(df['进口数量:液化天然气:累计值'], dtype=float)
    import_cu1_acc = np.array(df['进口数量:铜矿砂及其精矿:累计值'], dtype=float)
    import_cu2_acc = np.array(df['进口数量:未锻造的铜及铜材:累计值'], dtype=float)
    t11, import_i_acc_yoy = yoy_for_monthly_data(t, import_i_acc)
    t12, import_oil_acc_yoy = yoy_for_monthly_data(t, import_oil_acc)
    t13, import_lng_acc_yoy = yoy_for_monthly_data(t, import_lng_acc)
    t14, import_cu1_acc_yoy = yoy_for_monthly_data(t, import_cu1_acc)
    t15, import_cu2_acc_yoy = yoy_for_monthly_data(t, import_cu2_acc)

    datas = [[[[t,import_i_acc,'进口数量:铁矿砂及其精矿:累计值','']],[[t11,import_i_acc_yoy,'累计同比','']],'']]
    plot_many_figure(datas, start_time=start_time, end_time=end_time)
    datas = [[[[t,import_oil_acc,'进口数量:原油:累计值','']],[[t12,import_oil_acc_yoy,'累计同比','']],'']]
    plot_many_figure(datas, start_time=start_time, end_time=end_time)
    datas = [[[[t,import_lng_acc,'进口数量:液化天然气:累计值','']],[[t13,import_lng_acc_yoy,'累计同比','']],'']]
    plot_many_figure(datas, start_time=start_time, end_time=end_time)
    datas = [[[[t,import_cu1_acc,'进口数量:铜矿砂及其精矿:累计值','']],[[t14,import_cu1_acc_yoy,'累计同比','']],'']]
    plot_many_figure(datas, start_time=start_time, end_time=end_time)
    datas = [[[[t,import_cu2_acc,'进口数量:未锻造的铜及铜材:累计值','']],[[t15,import_cu2_acc_yoy,'累计同比','']],'']]
    plot_many_figure(datas, start_time=start_time, end_time=end_time)


if __name__=="__main__":
    # 出口
    test1()


    # 原材料进口
    # test2()



