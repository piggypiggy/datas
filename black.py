import os
import numpy as np
import pandas as pd
import datetime
import requests
import json
from utils import *

headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Host": "www.steelx2.com",
        "Proxy-Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
        "Sec-Fetch-Site": "same-origin"
    }

black_name_code = {
    '钢铁原料成本价格': [[78, '废钢价格'], [178, '钢材成本指数'], [1006, '澳大利亚粉矿价格(56.5%，日照港)'],
                        [106, '澳大利亚粉矿价格(61.5%青岛港，元/吨)'], [107, '巴西粉矿价格(65% 日照港，元/吨)'],
                        [95, '西澳-北仑铁矿海运价'], [94, '巴西图巴朗-北仑铁矿海运价'], 
                        [77, '波罗的海干散货指数(BDI)']],

    '钢铁商品指数': [[1100, '中国(上海)自贸区进口铁矿石价格指数'], [65, '钢材指数'], [61, '铁矿指数'],
                    [64, '焦炭指数'], [1002, '煤炭指数'], [1003, '水泥指数']],

    '钢铁行业PMI': [[118, '钢铁行业PMI'], [119, '钢铁行业PMI:生产'], [120, '钢铁行业PMI:新订单'],
                   [121, '钢铁行业PMI:新出口订单'], [122, '钢铁行业PMI:产成品库存'], [123, '钢铁行业PMI:原材料库存']],

    '钢铁产量': [[124, '重点企业钢材库存量(旬)'], [161, '唐山地区钢坯库存量'], [43, '铁矿石港口存量'],
                [67, '螺纹钢社会库存量'], [68, '线材社会库存量'], [69, '国内主要城市热轧卷板库存量'], [70, '国内主要城市冷轧卷板库存量'], 
                [117, '主要钢材品种库存总量'], 
                [177, '月度生铁产量'], [116, '月度中厚板产量'],
                [35, '月度粗钢产量'], [36, '月度铁矿石原矿产量'], [38, '月度钢材出口量'], [39, '月度钢材进口量']],

}


def update_black_data():
    BLACK_URL = 'https://www.steelx2.com/Handler2022/GetCommodityChartData.ashx?indicesids={}&datetype={}'

    session = requests.session()
    earlist_time = '2005-01-01'
    current_time_dt = datetime.datetime.now()

    # get cookies
    url = BLACK_URL.format('78', 'Half')
    r = session.get(url, verify=False, headers=headers)
    cookies = r.cookies

    for name in black_name_code:
        path = os.path.join(data_dir, name+'.csv')
        df = pd.DataFrame()

        last_line_time = get_last_line_time(path, name, earlist_time, 10, '%Y-%m-%d')
        start_time_dt = pd.to_datetime(last_line_time, format='%Y-%m-%d')
        if (current_time_dt == start_time_dt):
            continue

        for codes_detail in black_name_code[name]:
            if ((current_time_dt - start_time_dt) <= pd.Timedelta(days=360)):
                url = BLACK_URL.format(str(codes_detail[0]), 'Year1')
            elif ((current_time_dt - start_time_dt) <= pd.Timedelta(days=720)):
                url = BLACK_URL.format(str(codes_detail[0]), 'Year2')
            else:
                url = BLACK_URL.format(str(codes_detail[0]), 'Full')

            r = session.get(url, verify=False, headers=headers, cookies=cookies)
            print(codes_detail)

            if (r.status_code == 200):
                s = r.content.decode('utf-8')
                z = json.loads(s)
                # print(z)
                temp_df = pd.DataFrame(z["Data"])
                if (len(temp_df) > 0):
                    temp_df.rename(columns={"PriceDate":"time", 'Price':codes_detail[1]}, inplace=True)
                    temp_df.drop(['IndicesId'], axis=1, inplace=True)
                    temp_df['time'] = temp_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
                    temp_df.sort_values(by = 'time', inplace=True)
                    temp_df['time'] = temp_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))

                    if (df.empty):
                        df = temp_df.copy()
                    else:
                        df = pd.merge(df, temp_df, on='time', how='outer')

        # print(df)
        if (len(df) > 0):
            path = os.path.join(data_dir, name+'.csv')
            if os.path.exists(path):
                old_df = pd.read_csv(path)
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


def update_black_profit():
    url = 'https://www.steelx2.com/Handler2022/GetHomePageChartData.ashx?type=gclr'
    session = requests.session()

    r = session.get(url, verify=False, headers=headers)
    data = r.json()['Data']
    data_json = json.loads(data)
    df = pd.DataFrame(data_json)
    
    df.rename(columns={'Date':'time', 'ElectricFurnaceProfit':'电炉利润', 'BlastFurnaceProfit':'高炉利润'}, inplace=True)
    df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%dT%H:%M:%S')
    df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
    path = os.path.join(data_dir, '钢厂利润'+'.csv')
    if os.path.exists(path):
        old_df = pd.read_csv(path)
        old_df = pd.concat([old_df, df], axis=0)
        old_df.drop_duplicates(subset=['time'], keep='last', inplace=True) # last
        old_df['time'] = pd.to_datetime(old_df['time'], format='%Y-%m-%d')
        old_df.sort_values(by='time', axis=0, ascending=True, inplace=True)
        old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
        old_df.to_csv(path, encoding='utf-8', index=False)
    else:
        df.to_csv(path, encoding='utf-8', index=False)


def plot_steel_profit():
    # 螺纹钢和线材的成本=（1.03吨粗钢+轧制费）*1.13，其中轧制费为150-300元/吨
    # 粗钢成本=（0.96*生铁价格+0.15*废钢价格）/0.82
    # 生铁、铁水制造成本=（1.6*铁矿石+0.5*焦炭）/0.9


    path = os.path.join(data_dir, '钢铁原料成本价格'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    i1 = np.array(df['澳大利亚粉矿价格(56.5%，日照港)'], dtype=float)
    i2 = np.array(df['澳大利亚粉矿价格(61.5%青岛港，元/吨)'], dtype=float)
    i3 = np.array(df['巴西粉矿价格(65% 日照港，元/吨)'], dtype=float)
    # cost = np.array(df['钢材成本指数'], dtype=float)

    # fee1 = np.array(df['西澳-北仑铁矿海运价'], dtype=float)
    # fee2 = np.array(df['巴西图巴朗-北仑铁矿海运价'], dtype=float)

    path = os.path.join(data_dir, '钢厂利润'+'.csv') 
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    p1 = np.array(df['电炉利润'], dtype=float)
    p2 = np.array(df['高炉利润'], dtype=float)

    datas = [[[[t1, p1, '电炉利润', 'color=orange'],
               [t1, p2, '高炉利润', 'color=blue']],
              [],''],

             [[[t, i1, '铁矿 56.5%', ''],
               [t, i2, '铁矿 61.5%', ''],
               [t, i3, '铁矿 65%', ''],],
              [],''],


             [[[t, i2-i1, '铁矿 61.5% - 56.5%', ''], [t, i3-i2, '铁矿 65% - 61.5%', '']],
              [],'']]
    plot_many_figure(datas)

    pass


def plot_i_port_stock():
    path = os.path.join(data_dir, '钢铁产量'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    # 库存
    data3 = np.array(df['铁矿石港口存量'], dtype=float)
    plot_seasonality(t, data3, start_year=2016, title='铁矿石港口存量')


def plot_steel_stock():
    # 螺纹钢和线材的成本=（1.03吨粗钢+轧制费）*1.13，其中轧制费为150-300元/吨
    # 粗钢成本=（0.96*生铁价格+0.15*废钢价格）/0.82
    # 生铁、铁水制造成本=（1.6*铁矿石+0.5*焦炭）/0.9


    path = os.path.join(data_dir, '钢铁产量'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    # 库存
    data1 = np.array(df['重点企业钢材库存量(旬)'], dtype=float)
    data2 = np.array(df['唐山地区钢坯库存量'], dtype=float)
    data3 = np.array(df['铁矿石港口存量'], dtype=float)
    data4 = np.array(df['螺纹钢社会库存量'], dtype=float)
    data5 = np.array(df['线材社会库存量'], dtype=float)
    data6 = np.array(df['国内主要城市热轧卷板库存量'], dtype=float)
    data7 = np.array(df['国内主要城市冷轧卷板库存量'], dtype=float)
    data8 = np.array(df['主要钢材品种库存总量'], dtype=float)
    
    plot_seasonality(t, data1, start_year=2016, title='重点企业钢材库存量(旬)')
    plot_seasonality(t, data2, start_year=2016, title='唐山地区钢坯库存量')
    plot_seasonality(t, data3, start_year=2016, title='铁矿石港口存量')
    plot_seasonality(t, data4, start_year=2016, title='螺纹钢社会库存量')
    plot_seasonality(t, data5, start_year=2016, title='线材社会库存量')
    plot_seasonality(t, data6, start_year=2016, title='国内主要城市热轧卷板库存量')
    plot_seasonality(t, data7, start_year=2016, title='国内主要城市冷轧卷板库存量')
    plot_seasonality(t, data8, start_year=2016, title='主要钢材品种库存总量')
    
    
    # 产量
    data1 = np.array(df['月度生铁产量'], dtype=float)
    data2 = np.array(df['月度粗钢产量'], dtype=float)
    data3 = np.array(df['月度中厚板产量'], dtype=float)
    data4 = np.array(df['月度铁矿石原矿产量'], dtype=float)

    plot_seasonality(t, data1, start_year=2016, title='月度生铁产量')
    plot_seasonality(t, data2, start_year=2016, title='月度粗钢产量')
    plot_seasonality(t, data3, start_year=2016, title='月度中厚板产量')
    plot_seasonality(t, data4, start_year=2016, title='月度铁矿石原矿产量')


    # 进出口
    data1 = np.array(df['月度钢材出口量'], dtype=float)
    data2 = np.array(df['月度钢材进口量'], dtype=float)
    t1, data3 = data_sub(t, data1, t, data2)

    plot_seasonality(t, data1, start_year=2012, title='月度钢材出口量')
    plot_seasonality(t, data2, start_year=2012, title='月度钢材进口量')
    plot_seasonality(t1, data3, start_year=2012, title='月度钢材净出口量')


def plot_corporate_profit():
    path = os.path.join(nbs_dir, '按行业分工业企业主要经济指标'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))
    data1 = np.array(df['黑色金属冶炼和压延加工业利润总额_累计值'], dtype=float)
    data2 = np.array(df['黑色金属矿采选业利润总额_累计值'], dtype=float)
    data3 = np.array(df['工业企业利润总额_累计值'], dtype=float)

    plot_seasonality(t, data1, start_year=2012, title='黑色金属冶炼和压延加工业利润总额 累计值(亿元)')
    plot_seasonality(t, data2, start_year=2012, title='黑色金属矿采选业利润总额 累计值(亿元)')
    plot_seasonality(t, data3, start_year=2012, title='工业企业利润总额 累计值(亿元)')


def plot_steel_pmi():
    path = os.path.join(data_dir, '钢铁行业PMI'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    pmi = np.array(df['钢铁行业PMI'], dtype=float)
    pmi1 = np.array(df['钢铁行业PMI:生产'], dtype=float)
    pmi2 = np.array(df['钢铁行业PMI:新订单'], dtype=float)
    pmi3 = np.array(df['钢铁行业PMI:新出口订单'], dtype=float)
    pmi4 = np.array(df['钢铁行业PMI:产成品库存'], dtype=float)
    pmi5 = np.array(df['钢铁行业PMI:原材料库存'], dtype=float)

    datas = [[[[t, pmi, '钢铁行业PMI', 'color=black'],],
              [],''],

             [[[t, pmi1, '钢铁行业PMI:生产', ''],
               [t, pmi2, '钢铁行业PMI:新订单', ''],
               [t, pmi3, '钢铁行业PMI:新出口订单', ''],],
              [],''],

             [[[t, pmi4, '钢铁行业PMI:产成品库存', ''],
               [t, pmi5, '钢铁行业PMI:原材料库存', ''],],
              [],''],
            ]
    plot_many_figure(datas)


# 盈利 产能利用率
def test8():
    path = os.path.join(data_dir, '钢铁利润'+'.csv') 
    df = pd.read_csv(path)

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    z1 = np.array(df['全国盈利钢厂比例'], dtype=float)
    plot_seasonality(t, z1, start_year=2012, title='全国盈利钢厂比例')

    z1 = np.array(df['唐山钢厂:产能利用率'], dtype=float)
    plot_seasonality(t, z1, start_year=2012, title='唐山钢厂:产能利用率')

    z1 = np.array(df['高炉炼铁产能利用率:样本钢厂'], dtype=float)
    plot_seasonality(t, z1, start_year=2012, title='高炉炼铁产能利用率:样本钢厂')

    z1 = np.array(df['高炉开工率:样本钢厂'], dtype=float)
    plot_seasonality(t, z1, start_year=2012, title='高炉开工率:样本钢厂')



if __name__=="__main__":
    update_black_profit()
    update_black_data()

    plot_steel_profit()
    plot_steel_pmi()
    plot_steel_stock()
    plot_corporate_profit()


    # 全国盈利钢厂比例, 产能利用率
    test8()


    pass
