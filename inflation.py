import os
import numpy as np
import pandas as pd
import datetime
from utils import *


def plot_cs_fs_data(df, name, type, thres_list, title):
    t = pd.DatetimeIndex(pd.to_datetime(df['time']['time'], format='%Y-%m'))
    datas = []
    name1 = name.split('(')[0]
    for i in range(len(thres_list)):
        hi_count, eq_count, lo_count = get_cs_price_change_count(df, name, thres=thres_list[i])
        if (i==0):
            data = [[[t, hi_count, name1+' '+type+' '+'大于'+("%.2f" % (thres_list[i]-100))+'% 个数', 'color=red'],
                     [t, eq_count, name1+' '+type+' '+'等于'+("%.2f" % (thres_list[i]-100))+'% 个数', 'color=gray'],
                     [t, lo_count, name1+' '+type+' '+'小于'+("%.2f" % (thres_list[i]-100))+'% 个数', 'color=darkgreen']],
                    [],title]
        else:
            data = [[[t, hi_count, name1+' '+type+' '+'大于'+("%.2f" % (thres_list[i]-100))+'% 个数', 'color=red'],
                     [t, eq_count, name1+' '+type+' '+'等于'+("%.2f" % (thres_list[i]-100))+'% 个数', 'color=gray'],
                     [t, lo_count, name1+' '+type+' '+'小于'+("%.2f" % (thres_list[i]-100))+'% 个数', 'color=darkgreen']],
                    [],'']
        datas.append(data)
    plot_many_figure(datas, max_height=300*(len(thres_list)))
    
    
# 
def test2():
    path = os.path.join(nbs_dir, '36个城市居民消费价格分类指数'+'.csv') 
    df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(df['time']['time'], format='%Y-%m'))

    # thres_list = [100.0, 100.1, 100.15, 100.2, 100.3]
    # plot_cs_fs_data(df, '城市居民消费价格指数(上月=100)', '环比', thres_list, '36个城市')

    thres_list = [99, 99.5, 99.75, 100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 105.0]
    plot_cs_fs_data(df, '城市居民消费价格指数(上年同月=100)', '同比', thres_list, '36个城市')

    # thres_list = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 105.0]
    # plot_cs_fs_data(df, '粮食类城市居民消费价格指数(上年同月=100)', '同比', thres_list, '36个城市')

    # thres_list = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 105.0]
    # plot_cs_fs_data(df, '衣着类城市居民消费价格指数(上年同月=100)', '同比', thres_list, '36个城市')

    # thres_list = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 105.0]
    # plot_cs_fs_data(df, '居住类城市居民消费价格指数(上年同月=100)', '同比', thres_list, '36个城市')

    # thres_list = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 105.0]
    # plot_cs_fs_data(df, '生活用品及服务类城市居民消费价格指数(上年同月=100)', '同比', thres_list, '36个城市')

    # thres_list = [99, 99.5, 99.75, 100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 105.0]
    # plot_cs_fs_data(df, '鲜菜类城市居民消费价格指数(上年同月=100)', '同比', thres_list, '36个城市')

    # thres_list = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 105.0]
    # plot_cs_fs_data(df, '鲜果类城市居民消费价格指数(上年同月=100)', '同比', thres_list, '36个城市')

    # thres_list = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 105.0]
    # plot_cs_fs_data(df, '畜肉类城市居民消费价格指数(上年同月=100)', '同比', thres_list, '36个城市')

def test22():
    path = os.path.join(nbs_dir, '31个省居民消费价格分类指数'+'.csv') 
    df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(df['time']['time'], format='%Y-%m'))

    # thres_list = [100.0, 100.1, 100.15, 100.2, 100.3]
    # plot_cs_fs_data(df, '居民消费价格指数(上月=100)', '环比', thres_list, '31个省')

    thres_list = [99, 99.5, 99.75, 100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 105.0]
    plot_cs_fs_data(df, '居民消费价格指数(上年同月=100)', '同比', thres_list, '31个省')

    # thres_list = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 105.0]
    # plot_cs_fs_data(df, '粮食类居民消费价格指数(上年同月=100)', '同比', thres_list, '31个省')

    # thres_list = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 105.0]
    # plot_cs_fs_data(df, '衣着类居民消费价格指数(上年同月=100)', '同比', thres_list, '31个省')

    # thres_list = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 105.0]
    # plot_cs_fs_data(df, '居住类居民消费价格指数(上年同月=100)', '同比', thres_list, '31个省')

    # thres_list = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 105.0]
    # plot_cs_fs_data(df, '生活用品及服务类居民消费价格指数(上年同月=100)', '同比', thres_list, '31个省')

    # thres_list = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 105.0]
    # plot_cs_fs_data(df, '鲜菜类居民消费价格指数(上年同月=100)', '同比', thres_list, '31个省')

    # thres_list = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 105.0]
    # plot_cs_fs_data(df, '鲜果类居民消费价格指数(上年同月=100)', '同比', thres_list, '31个省')

    # thres_list = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 105.0]
    # plot_cs_fs_data(df, '畜肉类居民消费价格指数(上年同月=100)', '同比', thres_list, '31个省')


start_time = '2005-1-1'
end_time = '2028-12-31'

# cpi
def test3():
    path = os.path.join(nbs_dir, 'CPI'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))

    cpi_yoy = np.array(df['居民消费价格指数(上年同月=100)'], dtype=float) - 100  
    cpi1_yoy = np.array(df['食品烟酒类居民消费价格指数(上年同月=100)'], dtype=float) - 100
    cpi2_yoy = np.array(df['衣着类居民消费价格指数(上年同月=100)'], dtype=float) - 100
    cpi3_yoy = np.array(df['居住类居民消费价格指数(上年同月=100)'], dtype=float) - 100
    cpi4_yoy = np.array(df['生活用品及服务类居民消费价格指数(上年同月=100)'], dtype=float) - 100
    cpi5_yoy = np.array(df['交通和通信类居民消费价格指数(上年同月=100)'], dtype=float) - 100
    cpi6_yoy = np.array(df['教育文化和娱乐类居民消费价格指数(上年同月=100)'], dtype=float) - 100
    cpi7_yoy = np.array(df['医疗保健类居民消费价格指数(上年同月=100)'], dtype=float) - 100
    cpi8_yoy = np.array(df['其他用品和服务类居民消费价格指数(上年同月=100)'], dtype=float) - 100

    cpi_mom = np.array(df['居民消费价格指数(上月=100)'], dtype=float) - 100  
    cpi1_mom = np.array(df['食品烟酒类居民消费价格指数(上月=100)'], dtype=float) - 100
    cpi2_mom = np.array(df['衣着类居民消费价格指数(上月=100)'], dtype=float) - 100
    cpi3_mom = np.array(df['居住类居民消费价格指数(上月=100)'], dtype=float) - 100
    cpi4_mom = np.array(df['生活用品及服务类居民消费价格指数(上月=100)'], dtype=float) - 100
    cpi5_mom = np.array(df['交通和通信类居民消费价格指数(上月=100)'], dtype=float) - 100
    cpi6_mom = np.array(df['教育文化和娱乐类居民消费价格指数(上月=100)'], dtype=float) - 100
    cpi7_mom = np.array(df['医疗保健类居民消费价格指数(上月=100)'], dtype=float) - 100
    cpi8_mom = np.array(df['其他用品和服务类居民消费价格指数(上月=100)'], dtype=float) - 100

    datas = [[[[t,cpi_yoy,'CPI:当月同比','color=black'],
               [t,cpi1_yoy,'CPI:食品烟酒:当月同比',''],
               [t,cpi2_yoy,'CPI:衣着:当月同比',''],
               [t,cpi3_yoy,'CPI:居住:当月同比',''],
               [t,cpi4_yoy,'CPI:生活用品及服务:当月同比',''],
               [t,cpi5_yoy,'CPI:交通和通信:当月同比',''],
               [t,cpi6_yoy,'CPI:教育文化和娱乐:当月同比',''],
               [t,cpi7_yoy,'CPI:医疗保健:当月同比',''],
               [t,cpi8_yoy,'CPI:其他用品和服务:当月同比',''],
               ],[],''],

               [[[t,cpi_mom,'CPI:环比','color=black'],
               [t,cpi1_mom,'CPI:食品烟酒:环比',''],
               [t,cpi2_mom,'CPI:衣着:环比',''],
               [t,cpi3_mom,'CPI:居住:环比',''],
               [t,cpi4_mom,'CPI:生活用品及服务:环比',''],
               [t,cpi5_mom,'CPI:交通和通信:环比',''],
               [t,cpi6_mom,'CPI:教育文化和娱乐:环比',''],
               [t,cpi7_mom,'CPI:医疗保健:环比',''],
               [t,cpi8_mom,'CPI:其他用品和服务:环比',''],],[],''],]

    plot_many_figure(datas, start_time=start_time, end_time=end_time)


    cpi_yoy = np.array(df['居民消费价格指数(上年同月=100)'], dtype=float) - 100  
    cpi1_yoy = np.array(df['食品类居民消费价格指数(上年同月=100)'], dtype=float) - 100
    cpi2_yoy = np.array(df['粮食类居民消费价格指数(上年同月=100)'], dtype=float) - 100
    cpi3_yoy = np.array(df['畜肉类居民消费价格指数(上年同月=100)'], dtype=float) - 100
    cpi4_yoy = np.array(df['蛋类居民消费价格指数(上年同月=100)'], dtype=float) - 100
    cpi5_yoy = np.array(df['水产品类居民消费价格指数(上年同月=100)'], dtype=float) - 100
    cpi6_yoy = np.array(df['鲜菜类居民消费价格指数(上年同月=100)'], dtype=float) - 100
    cpi7_yoy = np.array(df['鲜果类居民消费价格指数(上年同月=100)'], dtype=float) - 100

    datas = [[[[t,cpi_yoy,'CPI:当月同比','color=black'],
               [t,cpi1_yoy,'CPI:食品:当月同比',''],
               [t,cpi2_yoy,'CPI:粮食:当月同比',''],
               [t,cpi3_yoy,'CPI:畜肉:当月同比',''],
               [t,cpi4_yoy,'CPI:蛋:当月同比',''],
               [t,cpi5_yoy,'CPI:水产品:当月同比',''],
               [t,cpi6_yoy,'CPI:鲜菜:当月同比',''],
               [t,cpi7_yoy,'CPI:鲜果:当月同比',''],
               ],[],''],]
    
    plot_many_figure(datas, start_time=start_time, end_time=end_time)


# ppi
def test4():
    path = os.path.join(nbs_dir, 'PPI'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))

    # PPI 同比
    ppi_yoy = np.array(df['工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100
    # PPI 生产资料 同比
    ppi_production_yoy = np.array(df['生产资料工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100
    ppi_production1_yoy = np.array(df['采掘业生产资料工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100
    ppi_production2_yoy = np.array(df['原料业生产资料工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100
    ppi_production3_yoy = np.array(df['加工业生产资料工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100
    # PPI 生活资料 同比
    ppi_subsistence_yoy = np.array(df['生活资料工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100
    ppi_subsistence1_yoy = np.array(df['食品类工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100
    ppi_subsistence2_yoy = np.array(df['衣着类工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100
    ppi_subsistence3_yoy = np.array(df['一般日用品类工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100
    ppi_subsistence4_yoy = np.array(df['耐用消费品类工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100

    # PPI 环比
    ppi_mom = np.array(df['工业生产者出厂价格指数(上月=100)'], dtype=float) - 100
    # PPI 生产资料 环比
    ppi_production_mom = np.array(df['生产资料工业生产者出厂价格指数(上月=100)'], dtype=float) - 100
    ppi_production1_mom = np.array(df['采掘业生产资料工业生产者出厂价格指数(上月=100)'], dtype=float) - 100
    ppi_production2_mom = np.array(df['原料业生产资料工业生产者出厂价格指数(上月=100)'], dtype=float) - 100
    ppi_production3_mom = np.array(df['加工业生产资料工业生产者出厂价格指数(上月=100)'], dtype=float) - 100
    # PPI 生活资料 环比
    ppi_subsistence_mom = np.array(df['生活资料工业生产者出厂价格指数(上月=100)'], dtype=float) - 100
    ppi_subsistence1_mom = np.array(df['食品类工业生产者出厂价格指数(上月=100)'], dtype=float) - 100
    ppi_subsistence2_mom = np.array(df['衣着类工业生产者出厂价格指数(上月=100)'], dtype=float) - 100
    ppi_subsistence3_mom = np.array(df['一般日用品类工业生产者出厂价格指数(上月=100)'], dtype=float) - 100
    ppi_subsistence4_mom = np.array(df['耐用消费品类工业生产者出厂价格指数(上月=100)'], dtype=float) - 100

    datas = [[[[t,ppi_yoy,'PPI:当月同比','color=black'],
               [t,ppi_production_yoy,'PPI:生产资料:当月同比',''],
               [t,ppi_production1_yoy,'PPI:生产资料:采掘业:当月同比',''],
               [t,ppi_production2_yoy,'PPI:生产资料:原料业:当月同比',''],
               [t,ppi_production3_yoy,'PPI:生产资料:加工业:当月同比',''],
               ],[],''],
               [[[t,ppi_mom,'PPI:环比','color=black'],
               [t,ppi_production_mom,'PPI:生产资料:环比',''],
               [t,ppi_production1_mom,'PPI:生产资料:采掘业:环比',''],
               [t,ppi_production2_mom,'PPI:生产资料:原料业:环比',''],
               [t,ppi_production3_mom,'PPI:生产资料:加工业:环比',''],
               ],[],''],
                ]

    plot_many_figure(datas, start_time=start_time, end_time=end_time)

    datas = [[[
               [t,ppi_yoy,'PPI:当月同比','color=black'],
               [t,ppi_subsistence_yoy,'PPI:生活资料:当月同比',''],
               [t,ppi_subsistence1_yoy,'PPI:生活资料:食品类:当月同比',''],
               [t,ppi_subsistence2_yoy,'PPI:生活资料:衣着类:当月同比',''],
               [t,ppi_subsistence3_yoy,'PPI:生活资料:一般日用品类:当月同比',''],
               [t,ppi_subsistence4_yoy,'PPI:生活资料:耐用消费品类:当月同比',''],
               ],[],''],

               [[
               [t,ppi_mom,'PPI:环比','color=black'],
               [t,ppi_subsistence_mom,'PPI:生活资料:环比',''],
               [t,ppi_subsistence1_mom,'PPI:生活资料:食品类:环比',''],
               [t,ppi_subsistence2_mom,'PPI:生活资料:衣着类:环比',''],
               [t,ppi_subsistence3_mom,'PPI:生活资料:一般日用品类:环比',''],
               [t,ppi_subsistence4_mom,'PPI:生活资料:耐用消费品类:环比',''],
               ],[],''],]

    plot_many_figure(datas, start_time=start_time, end_time=end_time)

    ppi1_yoy = np.array(df['煤炭开采和洗选业工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100  
    ppi2_yoy = np.array(df['石油和天然气开采业工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100  
    ppi3_yoy = np.array(df['黑色金属矿采选业工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100
    ppi4_yoy = np.array(df['黑色金属冶炼和压延加工业工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100
    ppi5_yoy = np.array(df['有色金属矿采选业工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100
    ppi6_yoy = np.array(df['有色金属冶炼和压延加工业工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100
    ppi7_yoy = np.array(df['纺织业工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100
    ppi8_yoy = np.array(df['化学原料和化学制品制造业工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100
    ppi9_yoy = np.array(df['橡胶和塑料制品业工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100
    ppi10_yoy = np.array(df['汽车制造业工业生产者出厂价格指数(上年同月=100)'], dtype=float) - 100

    ppi1_mom = np.array(df['煤炭开采和洗选业工业生产者出厂价格指数(上月=100)'], dtype=float) - 100  
    ppi2_mom = np.array(df['石油和天然气开采业工业生产者出厂价格指数(上月=100)'], dtype=float) - 100  
    ppi3_mom = np.array(df['黑色金属矿采选业工业生产者出厂价格指数(上月=100)'], dtype=float) - 100
    ppi4_mom = np.array(df['黑色金属冶炼和压延加工业工业生产者出厂价格指数(上月=100)'], dtype=float) - 100
    ppi5_mom = np.array(df['有色金属矿采选业工业生产者出厂价格指数(上月=100)'], dtype=float) - 100
    ppi6_mom = np.array(df['有色金属冶炼和压延加工业工业生产者出厂价格指数(上月=100)'], dtype=float) - 100
    ppi7_mom = np.array(df['纺织业工业生产者出厂价格指数(上月=100)'], dtype=float) - 100
    ppi8_mom = np.array(df['化学原料和化学制品制造业工业生产者出厂价格指数(上月=100)'], dtype=float) - 100
    ppi9_mom = np.array(df['橡胶和塑料制品业工业生产者出厂价格指数(上月=100)'], dtype=float) - 100
    ppi10_mom = np.array(df['汽车制造业工业生产者出厂价格指数(上月=100)'], dtype=float) - 100

    datas = [[[[t,ppi_yoy,'PPI:当月同比','color=black'],
               [t,ppi1_yoy,'PPI:煤炭开采和洗选业:当月同比',''],
               [t,ppi2_yoy,'PPI:石油和天然气开采业:当月同比',''],
               [t,ppi3_yoy,'PPI:黑色金属矿采选业:当月同比',''],
               [t,ppi4_yoy,'PPI:黑色金属冶炼和压延加工业:当月同比',''],
               [t,ppi5_yoy,'PPI:有色金属矿采选业:当月同比',''],
               [t,ppi6_yoy,'PPI:有色金属冶炼和压延加工业:当月同比',''],
               [t,ppi7_yoy,'PPI:纺织业:当月同比',''],
               [t,ppi8_yoy,'PPI:化学原料和化学制品制造业:当月同比',''],
               [t,ppi9_yoy,'PPI:橡胶和塑料制品业工业:当月同比',''],
               [t,ppi10_yoy,'PPI:汽车制造业:当月同比',''],
               ],[],''],
               
               [[[t,ppi_mom,'PPI:环比','color=black'],
               [t,ppi1_mom,'PPI:煤炭开采和洗选业:环比',''],
               [t,ppi2_mom,'PPI:石油和天然气开采业:环比',''],
               [t,ppi3_mom,'PPI:黑色金属矿采选业:环比',''],
               [t,ppi4_mom,'PPI:黑色金属冶炼和压延加工业:环比',''],
               [t,ppi5_mom,'PPI:有色金属矿采选业:环比',''],
               [t,ppi6_mom,'PPI:有色金属冶炼和压延加工业:环比',''],
               [t,ppi7_mom,'PPI:纺织业:环比',''],
               [t,ppi8_mom,'PPI:化学原料和化学制品制造业:环比',''],
               [t,ppi9_mom,'PPI:橡胶和塑料制品业工业:环比',''],
               [t,ppi10_mom,'PPI:汽车制造业:环比',''],
               ],[],''],
               
               ]

    plot_many_figure(datas, start_time=start_time, end_time=end_time)


# ppirm
def test5():
    path = os.path.join(nbs_dir, 'PPIRM'+'.csv')
    df = pd.read_csv(path) 
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))

    ppirm_yoy = np.array(df['工业生产者购进价格指数(上年同月=100)'], dtype=float) - 100
    ppirm1_yoy = np.array(df['燃料、动力类购进价格指数(上年同月=100)'], dtype=float) - 100  
    ppirm2_yoy = np.array(df['黑色金属材料类购进价格指数(上年同月=100)'], dtype=float) - 100  
    ppirm3_yoy = np.array(df['有色金属材料和电线类购进价格指数(上年同月=100)'], dtype=float) - 100
    ppirm4_yoy = np.array(df['化工原料类购进价格指数(上年同月=100)'], dtype=float) - 100
    ppirm5_yoy = np.array(df['木材及纸浆类购进价格指数(上年同月=100)'], dtype=float) - 100
    ppirm6_yoy = np.array(df['建筑材料及非金属矿类购进价格指数(上年同月=100)'], dtype=float) - 100
    ppirm7_yoy = np.array(df['其它工业原材料及半成品类购进价格指数(上年同月=100)'], dtype=float) - 100
    ppirm8_yoy = np.array(df['农副产品类购进价格指数(上年同月=100)'], dtype=float) - 100
    ppirm9_yoy = np.array(df['纺织原料类购进价格指数(上年同月=100)'], dtype=float) - 100

    ppirm_mom = np.array(df['工业生产者购进价格指数(上月=100)'], dtype=float) - 100
    ppirm1_mom = np.array(df['燃料、动力类购进价格指数(上月=100)'], dtype=float) - 100  
    ppirm2_mom = np.array(df['黑色金属材料类购进价格指数(上月=100)'], dtype=float) - 100  
    ppirm3_mom = np.array(df['有色金属材料和电线类购进价格指数(上月=100)'], dtype=float) - 100
    ppirm4_mom = np.array(df['化工原料类购进价格指数(上月=100)'], dtype=float) - 100
    ppirm5_mom = np.array(df['木材及纸浆类购进价格指数(上月=100)'], dtype=float) - 100
    ppirm6_mom = np.array(df['建筑材料及非金属矿类购进价格指数(上月=100)'], dtype=float) - 100
    ppirm7_mom = np.array(df['其它工业原材料及半成品类购进价格指数(上月=100)'], dtype=float) - 100
    ppirm8_mom = np.array(df['农副产品类购进价格指数(上月=100)'], dtype=float) - 100
    ppirm9_mom = np.array(df['纺织原料类购进价格指数(上月=100)'], dtype=float) - 100

    datas = [[[[t,ppirm_yoy,'PPIRM:当月同比','color=black'],
               [t,ppirm1_yoy,'PPIRM:燃料、动力类:当月同比',''],
               [t,ppirm2_yoy,'PPIRM:黑色金属材料类:当月同比',''],
               [t,ppirm3_yoy,'PPIRM:有色金属材料和电线类:当月同比',''],
               [t,ppirm4_yoy,'PPIRM:化工原料类:当月同比',''],
               [t,ppirm5_yoy,'PPIRM:木材及纸浆类:当月同比',''],
               [t,ppirm6_yoy,'PPIRM:建筑材料及非金属矿类:当月同比',''],
               [t,ppirm7_yoy,'PPIRM:其它工业原材料及半成品类:当月同比',''],
               [t,ppirm8_yoy,'PPIRM:农副产品类:当月同比',''],
               [t,ppirm9_yoy,'PPIRM:纺织原料类:当月同比',''],
               ],[],''],
               [[[t,ppirm_mom,'PPIRM:环比','color=black'],
               [t,ppirm1_mom,'PPIRM:燃料、动力类:环比',''],
               [t,ppirm2_mom,'PPIRM:黑色金属材料类:环比',''],
               [t,ppirm3_mom,'PPIRM:有色金属材料和电线类:环比',''],
               [t,ppirm4_mom,'PPIRM:化工原料类:环比',''],
               [t,ppirm5_mom,'PPIRM:木材及纸浆类:环比',''],
               [t,ppirm6_mom,'PPIRM:建筑材料及非金属矿类:环比',''],
               [t,ppirm7_mom,'PPIRM:其它工业原材料及半成品类:环比',''],
               [t,ppirm8_mom,'PPIRM:农副产品类:环比',''],
               [t,ppirm9_mom,'PPIRM:纺织原料类:环比',''],
               ],[],''],]

    plot_many_figure(datas, start_time=start_time, end_time=end_time)


# Underlying Inflation Gauge
# https://fred.stlouisfed.org/series/UIGFULL
def update_uig_data():
    # Underlying Inflation Gauge
    code = [ 
          ['UIGFULL', 'Underlying Inflation Gauge: Full Data Set Measure'],
          ['UIGPRICES', 'Underlying Inflation Gauge: Prices-Only Measure'],
        ]

    name_code = {'Underlying Inflation Gauge': code}
    update_fred_data(name_code, data_dir)


if __name__=="__main__":
    # cpi
    # 36个城市居民消费价格分类指数
    test2()
    # 31个省居民消费价格分类指数
    test22()

    # # cpi
    # test3()

    # # ppi
    # test4()

    # # ppirm
    # test5()

    pass
