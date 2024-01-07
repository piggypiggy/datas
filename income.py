import os
import time
import numpy as np
import pandas as pd
import datetime
from utils import *


# 人均收入、支出
def test1():
    path = os.path.join(nbs_dir, '人民生活'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))

    i = np.array(df['居民人均可支配收入_累计值'], dtype=float)
    i1 = np.array(df['居民人均可支配工资性收入_累计值'], dtype=float)
    i2 = np.array(df['居民人均可支配经营净收入_累计值'], dtype=float)
    i3 = np.array(df['居民人均可支配财产净收入_累计值'], dtype=float)
    i4 = np.array(df['居民人均可支配转移净收入_累计值'], dtype=float)

    ir = np.array(df['居民人均可支配收入_累计增长'], dtype=float)
    ir1 = np.array(df['居民人均可支配工资性收入_累计增长'], dtype=float)
    ir2 = np.array(df['居民人均可支配经营净收入_累计增长'], dtype=float)
    ir3 = np.array(df['居民人均可支配财产净收入_累计增长'], dtype=float)
    ir4 = np.array(df['居民人均可支配转移净收入_累计增长'], dtype=float)

    datas = [[[[t, i, '居民人均可支配收入_累计值',''],
               [t, i1, '居民人均可支配工资性收入_累计值',''],
               [t, i2, '居民人均可支配经营净收入_累计值',''],
               [t, i3, '居民人均可支配财产净收入_累计值',''],
               [t, i4, '居民人均可支配转移净收入_累计值',''],],[],''],

             [[[t, ir, '居民人均可支配收入_累计增长',''],
               [t, ir1, '居民人均可支配工资性收入_累计增长',''],
               [t, ir2, '居民人均可支配经营净收入_累计增长',''],
               [t, ir3, '居民人均可支配财产净收入_累计增长',''],
               [t, ir4, '居民人均可支配转移净收入_累计增长',''],],[],'']]
    plot_many_figure(datas)

    datas = [[t, i1/i, '居民人均可支配工资性收入_累计值 占比'],
             [t, i2/i, '居民人均可支配经营净收入_累计值 占比'],
             [t, i3/i, '居民人均可支配财产净收入_累计值 占比'],
             [t, i4/i, '居民人均可支配转移净收入_累计值 占比'],]
    plot_one_figure(datas)


    ### 城镇 ###
    ci = np.array(df['城镇居民人均可支配收入_累计值'], dtype=float)
    ci1 = np.array(df['城镇居民人均可支配工资性收入_累计值'], dtype=float)
    ci2 = np.array(df['城镇居民人均可支配经营净收入_累计值'], dtype=float)
    ci3 = np.array(df['城镇居民人均可支配财产净收入_累计值'], dtype=float)
    ci4 = np.array(df['城镇居民人均可支配转移净收入_累计值'], dtype=float)

    cir = np.array(df['城镇居民人均可支配收入_累计增长'], dtype=float)
    cir1 = np.array(df['城镇居民人均可支配工资性收入_累计增长'], dtype=float)
    cir2 = np.array(df['城镇居民人均可支配经营净收入_累计增长'], dtype=float)
    cir3 = np.array(df['城镇居民人均可支配财产净收入_累计增长'], dtype=float)
    cir4 = np.array(df['城镇居民人均可支配转移净收入_累计增长'], dtype=float)

    datas = [[[[t, ci, '城镇居民人均可支配收入_累计值',''],
               [t, ci1, '城镇居民人均可支配工资性收入_累计值',''],
               [t, ci2, '城镇居民人均可支配经营净收入_累计值',''],
               [t, ci3, '城镇居民人均可支配财产净收入_累计值',''],
               [t, ci4, '城镇居民人均可支配转移净收入_累计值',''],],[],''],

             [[[t, cir, '城镇居民人均可支配收入_累计增长',''],
               [t, cir1, '城镇居民人均可支配工资性收入_累计增长',''],
               [t, cir2, '城镇居民人均可支配经营净收入_累计增长',''],
               [t, cir3, '城镇居民人均可支配财产净收入_累计增长',''],
               [t, cir4, '城镇居民人均可支配转移净收入_累计增长',''],],[],'']]
    plot_many_figure(datas)

    datas = [[t, ci1/ci, '城镇居民人均可支配工资性收入_累计值 占比'],
             [t, ci2/ci, '城镇居民人均可支配经营净收入_累计值 占比'],
             [t, ci3/ci, '城镇居民人均可支配财产净收入_累计值 占比'],
             [t, ci4/ci, '城镇居民人均可支配转移净收入_累计值 占比'],]
    plot_one_figure(datas)



    ### 农村 ###
    ri = np.array(df['农村居民人均可支配收入_累计值'], dtype=float)
    ri1 = np.array(df['农村居民人均可支配工资性收入_累计值'], dtype=float)
    ri2 = np.array(df['农村居民人均可支配经营净收入_累计值'], dtype=float)
    ri3 = np.array(df['农村居民人均可支配财产净收入_累计值'], dtype=float)
    ri4 = np.array(df['农村居民人均可支配转移净收入_累计值'], dtype=float)

    rir = np.array(df['农村居民人均可支配收入_累计增长'], dtype=float)
    rir1 = np.array(df['农村居民人均可支配工资性收入_累计增长'], dtype=float)
    rir2 = np.array(df['农村居民人均可支配经营净收入_累计增长'], dtype=float)
    rir3 = np.array(df['农村居民人均可支配财产净收入_累计增长'], dtype=float)
    rir4 = np.array(df['农村居民人均可支配转移净收入_累计增长'], dtype=float)

    datas = [[[[t, ri, '农村居民人均可支配收入_累计值',''],
               [t, ri1, '农村居民人均可支配工资性收入_累计值',''],
               [t, ri2, '农村居民人均可支配经营净收入_累计值',''],
               [t, ri3, '农村居民人均可支配财产净收入_累计值',''],
               [t, ri4, '农村居民人均可支配转移净收入_累计值',''],],[],''],

             [[[t, rir, '农村居民人均可支配收入_累计增长',''],
               [t, rir1, '农村居民人均可支配工资性收入_累计增长',''],
               [t, rir2, '农村居民人均可支配经营净收入_累计增长',''],
               [t, rir3, '农村居民人均可支配财产净收入_累计增长',''],
               [t, rir4, '农村居民人均可支配转移净收入_累计增长',''],],[],'']]
    plot_many_figure(datas)

    datas = [[t, ri1/ri, '农村居民人均可支配工资性收入_累计值 占比'],
             [t, ri2/ri, '农村居民人均可支配经营净收入_累计值 占比'],
             [t, ri3/ri, '农村居民人均可支配财产净收入_累计值 占比'],
             [t, ri4/ri, '农村居民人均可支配转移净收入_累计值 占比'],]
    plot_one_figure(datas)



    c = np.array(df['居民人均消费支出_累计值'], dtype=float)
    cr = np.array(df['居民人均消费支出_累计增长'], dtype=float)

    cc = np.array(df['城镇居民人均消费支出_累计值'], dtype=float)
    ccr = np.array(df['城镇居民人均消费支出_累计增长'], dtype=float)

    cr1 = np.array(df['农村居民人均消费支出_累计值'], dtype=float)
    crr = np.array(df['农村居民人均消费支出_累计增长'], dtype=float)

    datas = [[t, c, '农村居民人均可支配工资性收入_累计值'],
             [t, cc, '城镇居民人均消费支出_累计值 占比'],
             [t, cr1, '农村居民人均消费支出_累计值 占比'],
             ]
    plot_one_figure(datas)



    datas = [[[[t, ir-cr, '居民人均可支配 收入-支出 累计增长',''],],[],''],
             [[[t, cir-ccr, '城镇居民人均可支配 收入-支出 累计增长',''],],[],''],
             [[[t, rir-crr, '农村居民人均可支配 收入-支出 累计增长',''],],[],''],]
    plot_many_figure(datas)


if __name__=="__main__":
    # 人均收入、支出
    test1()
