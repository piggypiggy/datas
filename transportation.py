import os
import numpy as np
import pandas as pd
import datetime
from utils import *


def test1():
    path = os.path.join(nbs_dir, '交通运输'+'.csv') 
    df = pd.read_csv(path)

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))
    name_list = ['货运量_当期值',
                 '民航货运量_当期值',
                 '公路货运量_当期值',
                 '铁路货运量_当期值',
                 '水运货运量_当期值',

                 '客运量_当期值',
                 '民航客运量_当期值',
                 '公路客运量_当期值',
                 '铁路客运量_当期值',
                 '水运客运量_当期值',
                 ]

    for name in name_list:
        data = np.array(df[name], dtype=float)
        plot_seasonality(t, data, start_year=2012, title=name)
    


if __name__=="__main__":
    # 货运量 客运量 季节性
    test1()


    pass
