import os
import time
import numpy as np
import pandas as pd
import datetime
import requests
from utils import *

##### 中国地方政府债券信息公开平台 #####
# https://www.celma.org.cn/ydsj/index.jhtml


DATA_CODE_NAME = [
    [['03', '债券发行额'], 
           [['0301', '债券发行额小计'],
            ['030101', '一般债券发行额'],
            ['030102', '专项债券发行额'],
            ['0302', '新增债券发行额小计'],
            ['030201', '新增一般债券发行额'],
            ['030202', '新增专项债券发行额'],
            ['0304', '再融资债券发行额小计'],
            ['030401', '再融资一般债券发行额'],
            ['030402', '再融资专项债券发行额'],]],
    [['04', '债券还本'], 
           [['0401', '债券还本额小计'],
            ['040101', '一般债券还本额'],
            ['040102', '专项债券还本额'],]],
    [['05', '债券付息'], 
           [['0501', '债券付息额小计'],
            ['050101', '一般债券付息额'],
            ['050102', '专项债券付息额'],]],
    [['06', '债务余额'], 
           [['0601', '债务余额'],
            ['060101', '一般债务余额'],
            ['060102', '专项债务余额'],]],
    [['07', '债券余额'], 
           [['0601', '债券余额'],
            ['060101', '一般债券余额'],
            ['060102', '专项债券余额'],]],
]


def update_lg_bond_monthly_data():
    se = requests.session()
    headers = {
        "Accept": "application/json, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "www.governbond.org.cn:4443",
        'origin': 'https://www.celma.org.cn',
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    }

    # 'https://www.governbond.org.cn:4443/api/loadBondData.action?dataType=FDQYDZB&zb=0301&monthSpan=36'
    URL = 'https://www.governbond.org.cn:4443/api/loadBondData.action?dataType=FDQYDZB&zb={}&monthSpan={}'
    max_month = 108
    now = datetime.datetime.now()


    path = os.path.join(lg_bond_dir, '地方政府债'+'.csv')
    if os.path.exists(path):
        old_df = pd.read_csv(path)
        old_t = pd.DatetimeIndex(pd.to_datetime(old_df['time'], format='%Y-%m'))
        month = (now - old_t[-1]).days // 30 + 3
    else:
        month = max_month

    df = pd.DataFrame()
    for root_code_name in DATA_CODE_NAME:
        # code = root_code_name[0][0]
        # name = root_code_name[0][1]

        for code_name in root_code_name[1]:
            url = URL.format(code_name[0], str(month))

            while (1):
                try:
                    print(code_name[1])
                    r = se.get(url, headers=headers)
                    data_json = r.json()
                    temp_df = pd.DataFrame(data_json["data"])
                    break
                except Exception as e:
                    print(e)
                    time.sleep(15)

            temp_df.drop_duplicates(subset=['SET_MONTH', 'AD_NAME'], keep='last', inplace=True)
            temp_df.reset_index(inplace=True, drop=True)
            temp_df = temp_df[['SET_MONTH', 'AD_NAME', 'AMOUNT']]
            temp_df['SET_MONTH'] = temp_df['SET_MONTH'].apply(lambda x:x[:4]+'-'+x[4:])
            temp_df['AD_NAME'] = temp_df['AD_NAME'].apply(lambda x:x+'_'+code_name[1])
            t = np.array(temp_df['SET_MONTH'], dtype=str)
            cols = np.array(temp_df['AD_NAME'], dtype=str)

            ms = set(t)
            _df = pd.DataFrame()
            for m in ms:
                w = np.where(t == m)[0]
                col = ['time'] + cols[w].tolist()
                amount = [m] + temp_df.loc[w, 'AMOUNT'].values.tolist()
                col += ['全国'+'_'+code_name[1]]
                amount += [temp_df.loc[w, 'AMOUNT'].sum()]

                __df = pd.DataFrame(columns=col, data=[amount])
                _df = pd.concat([_df, __df], axis=0)

            if (df.empty):
                df = _df.copy()
            else:
                df = pd.merge(df, _df, on='time', how='outer')

    if os.path.exists(path):
        old_df = pd.read_csv(path)
        old_df = pd.concat([old_df, df], axis=0)
        old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
        old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m'))
        old_df.sort_values(by = 'time', inplace=True)
        old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m'))
        old_df.to_csv(path, encoding='utf-8', index=False)
    else:
        df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m'))
        df.sort_values(by = 'time', inplace=True)
        df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m'))
        df.to_csv(path, encoding='utf-8', index=False)  



if __name__=="__main__":
    update_lg_bond_monthly_data()


