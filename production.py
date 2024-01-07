import os
import numpy as np
import pandas as pd
import datetime
from utils import *


def plot_electricity_production():
    path = os.path.join(nbs_dir, '能源主要产品产量'+'.csv') 
    df = pd.read_csv(path)

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))
    name_list = ['发电量_当期值',
                 '火力发电量_当期值',
                 '水力发电量_当期值',
                 '核能发电量_当期值',
                 '风力发电量_当期值',
                 '太阳能发电量_当期值',
                 ]

    data = {}
    for name in name_list:
        data[name] = np.array(df[name], dtype=float)
        plot_seasonality(t, data[name], start_year=2015, title=name)
    
    # 占比
    datas = [
             [[[t,data['火力发电量_当期值']/data['发电量_当期值'],'火力发电量 占比','color=black'],
               [t,data['水力发电量_当期值']/data['发电量_当期值'],'水力发电量 占比','color=blue'],
               [t,data['核能发电量_当期值']/data['发电量_当期值'],'核能发电量 占比','color=darkgreen'],
               [t,data['风力发电量_当期值']/data['发电量_当期值'],'风力发电量 占比','color=gray'],
               [t,data['太阳能发电量_当期值']/data['发电量_当期值'],'太阳能发电量 占比','color=red'],
               ],[],''],
    ]
    plot_many_figure(datas)


def plot_oil_coal_gas_production():
    path = os.path.join(nbs_dir, '能源主要产品产量'+'.csv') 
    df = pd.read_csv(path)

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))
    name_list = ['原煤产量_当期值',
                 '原油产量_当期值',
                 '天然气产量_当期值',
                 '煤层气产量_当期值',
                 '液化天然气产量_当期值',
                 '原油加工量产量_当期值',
                 '汽油产量_当期值',
                 '煤油产量_当期值',
                 '柴油产量_当期值',
                 '燃料油产量_当期值',
                 '石脑油产量_当期值',
                 '液化石油气产量_当期值',
                 '石油焦产量_当期值',
                 '石油沥青产量_当期值',
                 '焦炭产量_当期值',
                 '煤气产量_当期值',
                 ]

    data = {}
    for name in name_list:
        data[name] = np.array(df[name], dtype=float)
        plot_seasonality(t, data[name], start_year=2015, title=name)



def plot_industry_production():
    path = os.path.join(nbs_dir, '主要工业产品产量'+'.csv') 
    df = pd.read_csv(path)

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))
    name_list = [
                #  '铁矿石原矿产量_当期值',
                #  '原盐产量_当期值',
                #  '成品糖产量_当期值',
                #  '白酒（折65度，商品量）产量_当期值',
                #  '卷烟产量_当期值',
                #  '烧碱（折100％）产量_当期值',
                #  '纯碱（碳酸钠）产量_当期值',
                #  '乙烯产量_当期值',
                #  '合成橡胶产量_当期值',
                #  '中成药产量_当期值',
                #  '橡胶轮胎外胎产量_当期值',
                #  '水泥产量_当期值',
                #  '生铁产量_当期值',
                #  '粗钢产量_当期值',
                #  '线材（盘条）产量_当期值',
                #  '冷轧薄板产量_当期值',
                #  '中厚宽钢带产量_当期值',
                #  '焊接钢管产量_当期值',
                #  '铁合金产量_当期值',
                #  '氧化铝产量_当期值',
                #  '十种有色金属产量_当期值',
                #  '精炼铜（电解铜）产量_当期值',
                #  '铅产量_当期值',
                #  '锌产量_当期值',
                #  '原铝（电解铝）产量_当期值',
                #  '金属集装箱产量_当期值',
                #  '工业锅炉产量_当期值',
                #  '发动机产量_当期值',
                #  '金属切削机床产量_当期值',
                #  '金属成形机床产量_当期值',
                #  '电梯、自动扶梯及升降机产量_当期值',
                #  '电动手提式工具产量_当期值',
                #  '包装专用设备产量_当期值',
                #  '复印和胶版印制设备产量_当期值',
                #  '挖掘铲土运输机械产量_当期值',
                #  '挖掘机产量_当期值',
                #  '大气污染防治设备产量_当期值',
                #  '工业机器人产量_当期值',
                #  '服务机器人产量_当期值',
 
                 '汽车产量_当期值',
                 '基本型乘用车（轿车）产量_当期值',
                 '运动型多用途乘用车（SUV）产量_当期值',
                 '载货汽车产量_当期值',
                 '新能源汽车产量_当期值',
                 '铁路机车产量_当期值',
                 '动车组产量_当期值',
                 '民用钢质船舶产量_当期值',
                 '发电机组（发电设备）产量_当期值',
                 '交流电动机产量_当期值',
                 '光缆产量_当期值',
                 '锂离子电池产量_当期值',
                 '太阳能电池（光伏电池）产量_当期值',
                 '家用电冰箱（家用冷冻冷藏箱）产量_当期值',
                 '家用冷柜（家用冷冻箱）产量_当期值',
                 '房间空气调节器产量_当期值',
                 '家用洗衣机产量_当期值',
                 '电子计算机整机产量_当期值',
                 '微型计算机设备产量_当期值',
                 '程控交换机产量_当期值',
                 '移动通信基站设备产量_当期值',
                 '移动通信手持机（手机）产量_当期值',
                 '智能手机产量_当期值',
                 '彩色电视机产量_当期值',
                 '集成电路产量_当期值',
                 '光电子器件产量_当期值',
                 '智能手表产量_当期值',
                 '电工仪器仪表产量_当期值',


                 ]

    data = {}
    for name in name_list:
        data[name] = np.array(df[name], dtype=float)
        plot_seasonality(t, data[name], start_year=2015, title=name)


if __name__=="__main__":
    # 发电量
    # plot_electricity_production()

    # 煤 石油 天然气 等
    # plot_oil_coal_gas_production()

    # 工业产品
    # plot_industry_production()

    pass
