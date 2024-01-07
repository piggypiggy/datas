import os
import time
import numpy as np
import pandas as pd
import datetime
from utils import *


GOODS = ['粮油、食品', '饮料', '烟酒', '服装鞋帽、针、纺织品', '化妆品', '金银珠宝', '日用品',
         '体育、娱乐用品', '书报杂志', '家用电器和音像器材', '中西药品', '文化办公用品', '家具',
         '通讯器材', '石油及制品', '建筑及装潢材料', '汽车', '其他']


def plot_retail_sales_by_goods():
    path = os.path.join(nbs_dir, '限上单位商品零售类值'+'.csv')
    df = pd.read_csv(path)
    temp_df = df.loc[len(df)-1, :]

    GOODS_YTD_COLS = [x + '类商品零售类值_累计值' if x != '其他' else x + '商品零售类值_累计值' for x in GOODS]
    GOODS_YTD_YOY_COLS = [x + '类商品零售类值_累计增长' if x != '其他' else x + '商品零售类值_累计增长' for x in GOODS]
    GOODS_MONTH_COLS = [x + '类商品零售类值_当期值' if x != '其他' else x + '商品零售类值_当期值' for x in GOODS]
    GOODS_MONTH_YOY_COLS = [x + '类商品零售类值_同比增长' if x != '其他' else x + '商品零售类值_同比增长' for x in GOODS]

    sales_ytd = np.array(temp_df[GOODS_YTD_COLS], dtype=float)
    sales_ytd_yoy = np.array(temp_df[GOODS_YTD_YOY_COLS], dtype=float)
    sales_month = np.array(temp_df[GOODS_MONTH_COLS], dtype=float)
    sales_month_yoy = np.array(temp_df[GOODS_MONTH_YOY_COLS], dtype=float)

    sort = np.argsort(sales_ytd)[::-1]
    goods = np.array(GOODS)[sort]
    sales_ytd = sales_ytd[sort]
    sales_ytd_yoy = sales_ytd_yoy[sort]
    sales_month = sales_month[sort]
    sales_month_yoy = sales_month_yoy[sort]

    fig1 = figure(x_range=goods, width=1400, height=400, title="限上单位商品零售类值 累计值 (亿元)")
    fig1.vbar(x=goods, top=sales_ytd, width=0.3, fill_color='orange')
    fig1.xaxis.major_label_orientation = 1

    fig2 = figure(x_range=goods, width=1400, height=350, title="累计同比")
    fig2.vbar(x=goods, top=sales_ytd_yoy, width=0.3, fill_color='blue')
    fig2.xaxis.major_label_orientation = 1

    show(column(fig1,fig2))
    time.sleep(0.3)


    fig1 = figure(x_range=goods, width=1400, height=400, title="限上单位商品零售类值 当月值 (亿元)")
    fig1.vbar(x=goods, top=sales_month, width=0.3, fill_color='orange')
    fig1.xaxis.major_label_orientation = 1

    fig2 = figure(x_range=goods, width=1400, height=350, title="当月同比")
    fig2.vbar(x=goods, top=sales_month_yoy, width=0.3, fill_color='blue')
    fig2.xaxis.major_label_orientation = 1

    show(column(fig1,fig2))



if __name__=="__main__":
    plot_retail_sales_by_goods()





    pass
