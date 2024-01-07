import os
import numpy as np
import pandas as pd
import datetime
from utils import *


def test1():
    path = os.path.join(nbs_dir, '固定资产投资'+'.csv') 
    df = pd.read_csv(path)

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))
    name_list = ['固定资产投资额_累计增长',
                 '民间固定资产投资_累计增长',
                 '国有及国有控股固定资产投资额_累计增长',
                 '新开工项目计划总投资_累计增长',
                 '施工项目计划总投资_累计增长',
                 '第一产业固定资产投资额_累计增长',
                 '第二产业固定资产投资额_累计增长',
                 '第三产业固定资产投资额_累计增长',
                 ]

    for name in name_list:
        data = np.array(df[name], dtype=float)
        plot_seasonality(t, data, start_year=2012, title=name)
    


if __name__=="__main__":
    # 投资累计1增长 季节性
    test1()


    pass
