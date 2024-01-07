import os
import numpy as np
import akshare as ak
import pandas as pd

cur_dir = os.path.dirname(__file__)

# 获取所有股票代码、名称
def get_stock_list():
    # 所有股票代码、名称存储在 stock_list.csv
    list_path = os.path.join(cur_dir, "stock_list.csv")

    # 获取实时行情数据，从中提取股票代码、名称
    stock_zh_a_spot_df = ak.stock_zh_a_spot()

    stock_zh_a_spot_df[["代码", "名称"]].to_csv(list_path, index=None, encoding='utf_8') # 中文名称  

get_stock_list()
