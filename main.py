import os
import numpy as np
import akshare as ak
import pandas as pd
import datetime
from utils import *
from rate import *


# 指数 市净率 市盈率 大盘拥挤度
def get_stock_index_data():
    # 市净率
    stock_index_pb_df1 = ak.stock_index_pb_lg(symbol="上证50")
    stock_index_pb_df1.rename(columns={'日期':'time', '指数':'上证50', '市净率':'上证50市净率', '等权市净率':'上证50等权市净率', '市净率中位数':'上证50市净率中位数'}, inplace=True)
    
    stock_index_pb_df2 = ak.stock_index_pb_lg(symbol="沪深300")
    stock_index_pb_df2.rename(columns={'日期':'time', '指数':'沪深300', '市净率':'沪深300市净率', '等权市净率':'沪深300等权市净率', '市净率中位数':'沪深300市净率中位数'}, inplace=True)

    stock_index_pb_df3 = ak.stock_index_pb_lg(symbol="中证500")
    stock_index_pb_df3.rename(columns={'日期':'time', '指数':'中证500', '市净率':'中证500市净率', '等权市净率':'中证500等权市净率', '市净率中位数':'中证500市净率中位数'}, inplace=True)

    # 市盈率
    stock_index_pe_df1 = ak.stock_index_pe_lg(symbol="上证50")
    stock_index_pe_df1.rename(columns={'日期':'time', '指数':'上证50', '等权静态市盈率':'上证50等权静态市盈率', '静态市盈率':'上证50静态市盈率', '静态市盈率中位数':'上证50静态市盈率中位数', '等权滚动市盈率':'上证50等权滚动市盈率', '滚动市盈率':'上证50滚动市盈率', '滚动市盈率中位数':'上证50滚动市盈率中位数'}, inplace=True)

    stock_index_pe_df2 = ak.stock_index_pe_lg(symbol="沪深300")
    stock_index_pe_df2.rename(columns={'日期':'time', '指数':'沪深300', '等权静态市盈率':'沪深300等权静态市盈率', '静态市盈率':'沪深300静态市盈率', '静态市盈率中位数':'沪深300静态市盈率中位数', '等权滚动市盈率':'沪深300等权滚动市盈率', '滚动市盈率':'沪深300滚动市盈率', '滚动市盈率中位数':'沪深300滚动市盈率中位数'}, inplace=True)

    stock_index_pe_df3 = ak.stock_index_pe_lg(symbol="中证500")
    stock_index_pe_df3.rename(columns={'日期':'time', '指数':'中证500', '等权静态市盈率':'中证500等权静态市盈率', '静态市盈率':'中证500静态市盈率', '静态市盈率中位数':'中证500静态市盈率中位数', '等权滚动市盈率':'中证500等权滚动市盈率', '滚动市盈率':'中证500滚动市盈率', '滚动市盈率中位数':'中证500滚动市盈率中位数'}, inplace=True)

    # # 大盘拥挤度
    stock_a_congestion_lg_df = ak.stock_a_congestion_lg()
    stock_a_congestion_lg_df.rename(columns={'date':'time'}, inplace=True)
    # stock_a_congestion_lg_df['time'] = stock_a_congestion_lg_df['time'].astype('str')
    time_array1 = pd.DatetimeIndex(pd.to_datetime(stock_a_congestion_lg_df['time'], format='%Y-%m-%d'))

    path = os.path.join(data_dir, '股指'+'.csv')
    df2 = pd.read_csv(path)
    df3 = df2[['time', 'close', 'congestion']]
    time_array2 = pd.DatetimeIndex(pd.to_datetime(df3['time'], format='%Y-%m-%d'))
    df3["time"] = pd.to_datetime(df3["time"], format='%Y-%m-%d').dt.date
    where = np.where(time_array1 > time_array2[-1])[0]
    if (len(where) > 0):
        df3 = df3._append(stock_a_congestion_lg_df.iloc[where[0]:], ignore_index=True)
        print(df3)

    df = pd.merge(stock_index_pb_df1, stock_index_pb_df2, on='time', how='outer')
    df = pd.merge(df, stock_index_pb_df3, on='time', how='outer')
    df = pd.merge(df, stock_index_pe_df1, on='time', how='outer')
    df = pd.merge(df, stock_index_pe_df2, on='time', how='outer')
    df = pd.merge(df, stock_index_pe_df3, on='time', how='outer')
    df = pd.merge(df, df3, on='time', how='outer')

    path = os.path.join(data_dir, '股指'+'.csv')
    df.to_csv(path, encoding='utf-8', index=False)


def get_libor_data():
    df = ak.rate_interbank(market="伦敦银行同业拆借市场", symbol="Libor美元", indicator="3月")
    df.rename(columns={"报告日":"time", '利率':'3M'}, inplace=True)
    path = os.path.join(data_dir, 'libor.csv')
    df.to_csv(path, encoding='utf-8', index=False)


if __name__=="__main__":
    
    # # # Underlying Inflation Gauge
    # # get_UIG_data()

    # # # # 美国信贷条件
    # # get_loan_standard()

    # # 股指
    get_stock_index_data()


    # 中国房地产美元债 ETF(香港)
    stock_hk_hist_df = ak.stock_hk_hist(symbol="03001", start_date="19700101", end_date="22220101", adjust="hfq")
    stock_hk_hist_df.rename(columns={'日期':'time', '开盘':'open', '收盘':'close', '最高':'high', '最低':'low', '成交量':'vol'}, inplace=True)
    path = os.path.join(data_dir, '中国房地产美元债 ETF'+'.csv') 
    stock_hk_hist_df.to_csv(path, encoding='utf-8', index=False)

    get_libor_data()
    pass

