import os
import requests
import csv
import pandas as pd
import zipfile
import datetime
import numpy as np
import akshare as ak
import bs4
from utils import *
from akshare.futures import cons, receipt
from io import StringIO, BytesIO
import dateutil.relativedelta
from chinamoney import *
from lme import *
from pork import *
from spot import *
from intraday import update_sse_intraday_option_data
from sgx_fut_opt import update_sgx_fut_opt_data
from nasdaq import update_all_nasdaq_etf_option_data, update_all_nasdaq_etf_data
from hkma import update_hkma_data
from hkex_fut_opt import update_hkex_fut_opt_data
from us_rate import update_all_us_rate
from us_debt import update_treasury_auction_data
from fx import update_fx_data
from moa import *
from black import *
from position import update_all_institution_position
from sge import update_all_sge_data
from cfd import *
from vix import *
from fed import *
from jp_rate import *
from lbma import *


import warnings
from akshare.option.cons import (
    get_calendar,
    convert_date,
    DCE_DAILY_OPTION_URL,
    SHFE_OPTION_URL,
    CZCE_DAILY_OPTION_URL_3,
    SHFE_HEADERS,
)


# future_dict = {}


##########################################################################################
######################################## POSITION ########################################
##########################################################################################


# shfe
# symbol rank vol_party_name vol vol_chg long_party_name long_open_interest long_open_interest_chg short_party_name short_open_interest short_open_interest_chg variety

# dce
# rank vol_party_name vol vol_chg long_party_name long_open_interest long_open_interest_chg short_party_name short_open_interest short_open_interest_chg symbol var date

# czce
# rank vol_party_name vol vol_chg long_party_name long_open_interest long_open_interest_chg short_party_name short_open_interest short_open_interest_chg symbol variety

# cffex
# long_open_interest long_open_interest_chg long_party_name rank short_open_interest short_open_interest_chg short_party_name symbol vol vol_chg vol_party_name variety

# 统一
# inst_id
# vol_party_name vol vol_chg long_party_name long_open_interest long_open_interest_chg short_party_name short_open_interest short_open_interest_chg


def create_future_position_file(path):
    if not os.path.exists(path):
        c1 = ['time']
        c2 = ['']
        c2_add = ['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','top5','top10','top15','top20']
        c3 = ['']
        keys = ['vol_party_name', 'vol', 'vol_chg', 'long_party_name', 'long_open_interest', 'long_open_interest_chg', 'short_party_name', 'short_open_interest', 'short_open_interest_chg']
        for i in range(6): # 6个合约
            c1.append(str(i+1))
            c2.append('inst_id')
            c3.append('')
            for j in range(len(c2_add)): # 持仓排名 1~20
                for _ in range(len(keys)): # keys
                    c1.append(str(i+1))
                    c2.append(c2_add[j])
                c3 += keys

        df = pd.DataFrame(columns=[c1,c2,c3])
        df.to_csv(path, encoding='utf-8', index=False)
        print('FUTURE POSITION CREATE ' + path)


def get_future_position(exchange, date):
    if exchange == 'shfe':
        d = ak.get_shfe_rank_table(date=date) # , vars_list=['HC']
    if exchange == 'dce':
        d = ak.futures_dce_position_rank(date=date) # , vars_list=['I']
    if exchange == 'czce':
        d = ak.get_czce_rank_table(date=date)
    if exchange == 'cffex':
        d = ak.get_cffex_rank_table(date=date)

    keys = ['vol_party_name', 'vol', 'vol_chg', 'long_party_name', 'long_open_interest', 'long_open_interest_chg', 'short_party_name', 'short_open_interest', 'short_open_interest_chg']
    # 每个品种归类
    keys_list = list(d.keys())
    dd = dict()
    for s in keys_list:
        if (s[1].isdigit()): # 
            if not (s[0] in dd):
                z = list()
                for ss in keys_list:
                    if ss[0] == s[0] and ss[1].isdigit():
                        z.append(ss)
                dd[s[0]] = z
        else:
            if not (s[0:2] in dd):
                z = list()
                for ss in keys_list:
                    if ss[0:2] == s[0:2]:
                        z.append(ss)
 
                dd[s[0:2]] = z

    ret_dict = {}
    null_data = []
    # 每个品种
    keys_list = list(dd.keys())
    for s in keys_list:
        tmps = list()
        inst_id_list = dd[s]
        L = len(inst_id_list)
        actual_L = 0
        vol20 = list()
        for i in range(L):
            # czce, dce的数字带逗号
            if (exchange == 'czce' or exchange == 'dce'):
                d[inst_id_list[i]] = d[inst_id_list[i]].replace(',', '', regex=True)
            # 有空值，忽略所有数据
            if (d[inst_id_list[i]].iloc[0:20].isnull().any().any() or len(d[inst_id_list[i]].iloc[0:20]) < 20):
                # print(inst_id_list[i])
                # print('NULLLLLLLLLLLLLLLLLLL')
                continue

            tmp = d[inst_id_list[i]].iloc[0:20]
            tmp_list = tmp[keys].values.flatten().tolist()
            tmp_list = [inst_id_list[i]] + tmp_list  # inst_id + keys

            # czce, dce的数字带逗号
            try:
                vol = np.array(tmp['vol'], dtype=float)
                vol_chg = np.array(tmp['vol_chg'], dtype=float)
                long_open_interest = np.array(tmp['long_open_interest'], dtype=float)
                long_open_interest_chg = np.array(tmp['long_open_interest_chg'], dtype=float)
                short_open_interest = np.array(tmp['short_open_interest'], dtype=float)
                short_open_interest_chg = np.array(tmp['short_open_interest_chg'], dtype=float)
            except:
                continue
            # vol
            actual_L += 1
            vol20.append(np.sum(vol[:20]))

            # top5
            top5 = ['', np.sum(vol[:5]), np.sum(vol_chg[:5]), '', np.sum(long_open_interest[:5]), np.sum(long_open_interest_chg[:5]), '', np.sum(short_open_interest[:5]), np.sum(short_open_interest_chg[:5])]
            tmp_list += top5
            # top10
            top10 = ['', np.sum(vol[:10]), np.sum(vol_chg[:10]), '', np.sum(long_open_interest[:10]), np.sum(long_open_interest_chg[:10]), '', np.sum(short_open_interest[:10]), np.sum(short_open_interest_chg[:10])]
            tmp_list += top10
            # top15
            top15 = ['', np.sum(vol[:15]), np.sum(vol_chg[:15]), '', np.sum(long_open_interest[:15]), np.sum(long_open_interest_chg[:15]), '', np.sum(short_open_interest[:15]), np.sum(short_open_interest_chg[:15])]
            tmp_list += top15
            # top20
            top20 = ['', np.sum(vol[:20]), np.sum(vol_chg[:20]), '', np.sum(long_open_interest[:20]), np.sum(long_open_interest_chg[:20]), '', np.sum(short_open_interest[:20]), np.sum(short_open_interest_chg[:20])]
            tmp_list += top20

            tmps.append(tmp_list.copy())

        vol20 = np.array(vol20, dtype=float)
        order = np.argsort(vol20)[::-1]
        date_str = date[0:4]
        date_str += '-'
        date_str += date[4:6]
        date_str += '-'
        date_str += date[6:8]

        row = [date_str]
        if (actual_L > 0):
            for i in range(min(actual_L,6)):
                row += tmps[order[i]]

            if (len(null_data) < 1):
                for i in range(len(tmps[0])):
                    null_data += [None]

            # 补数据
            for i in range(6 - actual_L):
                row += null_data

        ret_dict[s] = row

    return ret_dict


def get_all_future_position(exchange, start_time):
    calendar = cons.get_calendar()

    data_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')
    current_time_dt = datetime.datetime.now()
    writers = {}
    while data_time_dt <= current_time_dt:
        print(data_time_dt)
        # 获取的数据的时间
        data_time_str = data_time_dt.strftime('%Y%m%d')
        date = cons.convert_date(data_time_str)
        if date.strftime("%Y%m%d") not in calendar:
            data_time_dt += pd.Timedelta(days=1)
            continue

        ret_dict = get_future_position(exchange, data_time_str)
        for key in ret_dict:
            if not(key in writers):
                path = os.path.join(future_position_dir, exchange, key+'.csv')
                if os.path.exists(path):
                    print('FUTURE POSITION APPEND ' + path)
                else:
                    create_future_position_file(path)

                f = open(path, 'a', newline='', encoding='utf-8')
                writer = csv.writer(f)
                writers[key] = writer

            if len(ret_dict[key]) > 1:
                writers[key].writerow(ret_dict[key])

        data_time_dt += pd.Timedelta(days=1)
        time.sleep(0.5)
        # return

def update_all_future_position(exchange):
    print('UPDATE FUTURE POSITION: ', exchange)
    if exchange == 'shfe':
        path = os.path.join(future_position_dir, exchange, 'au'+'.csv')
    if exchange == 'dce':
        path = os.path.join(future_position_dir, exchange, 'i'+'.csv')
    if exchange == 'czce':
        path = os.path.join(future_position_dir, exchange, 'SR'+'.csv')
    if exchange == 'cffex':
        path = os.path.join(future_position_dir, exchange, 'IC'+'.csv')

    # 最后一行的时间
    with open(path, 'rb') as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell() - 1  # 不算最后一个字符'\n'
        while pos > 0:
            pos -= 1
            f.seek(pos, os.SEEK_SET)
            if f.read(1) == b'\n':
                break
        last_line = f.readline().decode().strip()
    
    print('FUTURE POSITION LAST TIME: ', last_line[:10])
    data_time_dt = pd.to_datetime(last_line[:10], format='%Y-%m-%d')
    data_time_dt += pd.Timedelta(days=1)
    data_time_str = data_time_dt.strftime('%Y-%m-%d')

    get_all_future_position(exchange, data_time_str)

######## OPTION POSITION ########
def create_option_position_file(path):
    if not os.path.exists(path):
        c0 = ['time']
        c1 = ['time']
        c2 = ['time']
        c2_add = ['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','top5','top10','top15','top20']
        c3 = ['time']
        keys = ['vol_party_name', 'vol', 'vol_chg', 'long_party_name', 'long_open_interest', 'long_open_interest_chg', 'short_party_name', 'short_open_interest', 'short_open_interest_chg']
        for i in range(6): # 6个合约
            c0.append('C')
            c1.append(str(i+1))
            c2.append('inst_id')
            c3.append('')
            for j in range(len(c2_add)): # 持仓排名 1~20
                for _ in range(len(keys)): # keys
                    c0.append('C')
                    c1.append(str(i+1))
                    c2.append(c2_add[j])
                c3 += keys

            c0.append('P')
            c1.append(str(i+1))
            c2.append('inst_id')
            c3.append('')
            for j in range(len(c2_add)): # 持仓排名 1~20
                for _ in range(len(keys)): # keys
                    c0.append('P')
                    c1.append(str(i+1))
                    c2.append(c2_add[j])
                c3 += keys

        df = pd.DataFrame(columns=[c0,c1,c2,c3])
        df.to_csv(path, encoding='utf-8', index=False)
        print('OPTION POSITION CREATE ' + path)

def options_dce_position_rank(date: str = "20160919") -> dict:
    """
    大连商品交易所-每日持仓排名-具体合约
    http://www.dce.com.cn/dalianshangpin/xqsj/tjsj26/rtj/rcjccpm/index.html
    :param date: 指定交易日; e.g., "20200511"
    :type date: str
    :return: 指定日期的持仓排名数据
    :rtype: pandas.DataFrame
    """
    calendar = cons.get_calendar()
    date = (
        cons.convert_date(date) if date is not None else datetime.date.today()
    )
    if date.strftime("%Y%m%d") not in calendar:
        warnings.warn("%s非交易日" % date.strftime("%Y%m%d"))
        return {}
    url = "http://www.dce.com.cn/publicweb/quotesdata/exportMemberDealPosiQuotesBatchData.html"
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "160",
        "Content-Type": "application/x-www-form-urlencoded",
        "Host": "www.dce.com.cn",
        "Origin": "http://www.dce.com.cn",
        "Pragma": "no-cache",
        "Referer": "http://www.dce.com.cn/publicweb/quotesdata/memberDealPosiQuotes.html",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
    }
    payload = {
        "memberDealPosiQuotes.variety": "a",
        "memberDealPosiQuotes.trade_type": "1",
        "contract.contract_id": "a2009",
        "contract.variety_id": "a",
        "year": date.year,
        "month": date.month - 1,
        "day": date.day,
        "batchExportFlag": "batch",
    }
    r = requests.post(url, payload, headers=headers)
    big_dict = dict()
    with zipfile.ZipFile(BytesIO(r.content), "r") as z:
        # print(z.namelist())
        for i in z.namelist():
            file_name = i.encode("cp437").decode("GBK")
            if not file_name.startswith(date.strftime("%Y%m%d")):
                continue
            try:
                data = pd.read_table(z.open(i), header=None, sep="\t")
                if len(data) < 16:  # 处理没有活跃合约的情况
                    big_dict[file_name.split("_")[1]] = [pd.DataFrame(), pd.DataFrame()]
                    continue
                temp_filter = data[
                    data.iloc[:, 0].str.find("名次") == 0
                ].index.tolist()
                if (
                    temp_filter[1] - temp_filter[0] < 5
                ):  # 过滤有无成交量但是有买卖持仓的数据, 如 20201105_c2011_成交量_买持仓_卖持仓排名.txt
                    big_dict[file_name.split("_")[1]] = [pd.DataFrame(), pd.DataFrame()]
                    continue
                start_list = data[
                    data.iloc[:, 0].str.find("名次") == 0
                ].index.tolist()

                data_c = data.iloc[
                    start_list[0] : start_list[3], # 看涨期权
                    data.columns[data.iloc[start_list[0], :].notnull()],
                ].copy()
                data_c.reset_index(inplace=True, drop=True)
                start_list = data_c[
                    data_c.iloc[:, 0].str.find("名次") == 0
                ].index.tolist()
                end_list = data_c[
                    data_c.iloc[:, 0].str.find("总计") == 0
                ].index.tolist()
                part_one = data_c[start_list[0] : end_list[0]].iloc[1:, :]
                part_two = data_c[start_list[1] : end_list[1]].iloc[1:, :]
                part_three = data_c[start_list[2] : end_list[2]].iloc[1:, :]
                temp_df_c = pd.concat(
                    [
                        part_one.reset_index(drop=True),
                        part_two.reset_index(drop=True),
                        part_three.reset_index(drop=True),
                    ],
                    axis=1,
                    ignore_index=True,
                )
                temp_df_c.columns = [
                    "名次",
                    "会员简称",
                    "成交量",
                    "增减",
                    "名次",
                    "会员简称",
                    "持买单量",
                    "增减",
                    "名次",
                    "会员简称",
                    "持卖单量",
                    "增减",
                ]
                temp_df_c["rank"] = range(1, len(temp_df_c) + 1)
                del temp_df_c["名次"]
                temp_df_c.columns = [
                    "vol_party_name",
                    "vol",
                    "vol_chg",
                    "long_party_name",
                    "long_open_interest",
                    "long_open_interest_chg",
                    "short_party_name",
                    "short_open_interest",
                    "short_open_interest_chg",
                    "rank",
                ]
                temp_df_c["symbol"] = file_name.split("_")[1]
                temp_df_c["variety"] = file_name.split("_")[1][:-4].upper()
                temp_df_c = temp_df_c[
                    [
                        "long_open_interest",
                        "long_open_interest_chg",
                        "long_party_name",
                        "rank",
                        "short_open_interest",
                        "short_open_interest_chg",
                        "short_party_name",
                        "vol",
                        "vol_chg",
                        "vol_party_name",
                        "symbol",
                        "variety",
                    ]
                ]

                start_list = data[
                    data.iloc[:, 0].str.find("名次") == 0
                ].index.tolist()
                data_p = data.iloc[
                    start_list[3] : , # 看跌期权
                    data.columns[data.iloc[start_list[3], :].notnull()],
                ].copy()
                data_p.reset_index(inplace=True, drop=True)
                start_list = data_p[
                    data_p.iloc[:, 0].str.find("名次") == 0
                ].index.tolist()
                end_list = data_p[
                    data_p.iloc[:, 0].str.find("总计") == 0
                ].index.tolist()
                part_one = data_p[start_list[0] : end_list[0]].iloc[1:, :]
                part_two = data_p[start_list[1] : end_list[1]].iloc[1:, :]
                part_three = data_p[start_list[2] : end_list[2]].iloc[1:, :]
                temp_df_p = pd.concat(
                    [
                        part_one.reset_index(drop=True),
                        part_two.reset_index(drop=True),
                        part_three.reset_index(drop=True),
                    ],
                    axis=1,
                    ignore_index=True,
                )
                temp_df_p.columns = [
                    "名次",
                    "会员简称",
                    "成交量",
                    "增减",
                    "名次",
                    "会员简称",
                    "持买单量",
                    "增减",
                    "名次",
                    "会员简称",
                    "持卖单量",
                    "增减",
                ]
                temp_df_p["rank"] = range(1, len(temp_df_p) + 1)
                del temp_df_p["名次"]
                temp_df_p.columns = [
                    "vol_party_name",
                    "vol",
                    "vol_chg",
                    "long_party_name",
                    "long_open_interest",
                    "long_open_interest_chg",
                    "short_party_name",
                    "short_open_interest",
                    "short_open_interest_chg",
                    "rank",
                ]
                temp_df_p["symbol"] = file_name.split("_")[1]
                temp_df_p["variety"] = file_name.split("_")[1][:-4].upper()
                temp_df_p = temp_df_p[
                    [
                        "long_open_interest",
                        "long_open_interest_chg",
                        "long_party_name",
                        "rank",
                        "short_open_interest",
                        "short_open_interest_chg",
                        "short_party_name",
                        "vol",
                        "vol_chg",
                        "vol_party_name",
                        "symbol",
                        "variety",
                    ]
                ]
                # print(temp_df_c)
                # print(temp_df_p)
                # return
                big_dict[file_name.split("_")[1]] = [temp_df_c, temp_df_p]

            except UnicodeDecodeError as e:
                print('UnicodeDecodeError: ', e)
                exit()
                # try:
                #     data = pd.read_table(
                #         z.open(i),
                #         header=None,
                #         sep="\\s+",
                #         encoding="gb2312",
                #         skiprows=3,
                #     )
                # except:
                #     data = pd.read_table(
                #         z.open(i),
                #         header=None,
                #         sep="\\s+",
                #         encoding="gb2312",
                #         skiprows=4,
                #     )
                # start_list = data[
                #     data.iloc[:, 0].str.find("名次") == 0
                # ].index.tolist()
                # end_list = data[
                #     data.iloc[:, 0].str.find("总计") == 0
                # ].index.tolist()
                # part_one = data[start_list[0] : end_list[0]].iloc[1:, :]
                # part_two = data[start_list[1] : end_list[1]].iloc[1:, :]
                # part_three = data[start_list[2] : end_list[2]].iloc[1:, :]
                # temp_df = pd.concat(
                #     [
                #         part_one.reset_index(drop=True),
                #         part_two.reset_index(drop=True),
                #         part_three.reset_index(drop=True),
                #     ],
                #     axis=1,
                #     ignore_index=True,
                # )
                # temp_df.columns = [
                #     "名次",
                #     "会员简称",
                #     "成交量",
                #     "增减",
                #     "名次",
                #     "会员简称",
                #     "持买单量",
                #     "增减",
                #     "名次",
                #     "会员简称",
                #     "持卖单量",
                #     "增减",
                # ]
                # temp_df["rank"] = range(1, len(temp_df) + 1)
                # del temp_df["名次"]
                # temp_df.columns = [
                #     "vol_party_name",
                #     "vol",
                #     "vol_chg",
                #     "long_party_name",
                #     "long_open_interest",
                #     "long_open_interest_chg",
                #     "short_party_name",
                #     "short_open_interest",
                #     "short_open_interest_chg",
                #     "rank",
                # ]
                # temp_df["symbol"] = file_name.split("_")[1]
                # temp_df["variety"] = file_name.split("_")[1][:-4].upper()
                # temp_df = temp_df[
                #     [
                #         "long_open_interest",
                #         "long_open_interest_chg",
                #         "long_party_name",
                #         "rank",
                #         "short_open_interest",
                #         "short_open_interest_chg",
                #         "short_party_name",
                #         "vol",
                #         "vol_chg",
                #         "vol_party_name",
                #         "symbol",
                #         "variety",
                #     ]
                # ]
                # big_dict[file_name.split("_")[1]] = temp_df

    return big_dict

def get_option_position(exchange, date):
    if exchange == 'dce':
        d = options_dce_position_rank(date=date)
    if exchange == 'czce':
        d = ak.get_czce_rank_table(date=date)

    keys = ['vol_party_name', 'vol', 'vol_chg', 'long_party_name', 'long_open_interest', 'long_open_interest_chg', 'short_party_name', 'short_open_interest', 'short_open_interest_chg']
    # 每个品种归类
    keys_list = list(d.keys())
    dd = dict()
    for s in keys_list:
        if (s[1].isdigit()): # 
            if not (s[0] in dd):
                z = list()
                for ss in keys_list:
                    if ss[0] == s[0] and ss[1].isdigit():
                        z.append(ss)
                dd[s[0]] = z
        else:
            if not (s[0:2] in dd):
                z = list()
                for ss in keys_list:
                    if ss[0:2] == s[0:2]:
                        z.append(ss)
                
                dd[s[0:2]] = z

    ret_dict = {}
    null_data = []
    # 每个品种
    keys_list = list(dd.keys())
    for s in keys_list:
        tmps = list()
        inst_id_list = dd[s]
        L = len(inst_id_list)
        actual_L = 0
        vol20 = list()
        for i in range(L):
            # czce, dce的数字带逗号
            if (exchange == 'czce' or exchange == 'dce'):
                d[inst_id_list[i]][0] = d[inst_id_list[i]][0].replace(',', '', regex=True)
                d[inst_id_list[i]][1] = d[inst_id_list[i]][1].replace(',', '', regex=True)
            # 有空值，忽略所有数据
            if (d[inst_id_list[i]][0].iloc[0:20].isnull().any().any() or len(d[inst_id_list[i]][0].iloc[0:20]) < 20 or
                d[inst_id_list[i]][1].iloc[0:20].isnull().any().any() or len(d[inst_id_list[i]][1].iloc[0:20]) < 20):
                # print(inst_id_list[i])
                # print('NULLLLLLLLLLLLLLLLLLL')
                continue

            actual_L += 1
            for k in range(2):  # 0:CALL, 1:PUT
                tmp = d[inst_id_list[i]][k].iloc[0:20]
                tmp_list = tmp[keys].values.flatten().tolist()
                tmp_list = [inst_id_list[i]] + tmp_list  # inst_id + keys

                vol = np.array(tmp['vol'], dtype=float)
                vol_chg = np.array(tmp['vol_chg'], dtype=float)
                long_open_interest = np.array(tmp['long_open_interest'], dtype=float)
                long_open_interest_chg = np.array(tmp['long_open_interest_chg'], dtype=float)
                short_open_interest = np.array(tmp['short_open_interest'], dtype=float)
                short_open_interest_chg = np.array(tmp['short_open_interest_chg'], dtype=float)

                # vol
                if k==0:
                    vol20.append(np.sum(vol[:20]))
                else:
                    vol20[len(vol20)-1] += np.sum(vol[:20])

                # top5
                top5 = ['', np.sum(vol[:5]), np.sum(vol_chg[:5]), '', np.sum(long_open_interest[:5]), np.sum(long_open_interest_chg[:5]), '', np.sum(short_open_interest[:5]), np.sum(short_open_interest_chg[:5])]
                tmp_list += top5
                # top10
                top10 = ['', np.sum(vol[:10]), np.sum(vol_chg[:10]), '', np.sum(long_open_interest[:10]), np.sum(long_open_interest_chg[:10]), '', np.sum(short_open_interest[:10]), np.sum(short_open_interest_chg[:10])]
                tmp_list += top10
                # top15
                top15 = ['', np.sum(vol[:15]), np.sum(vol_chg[:15]), '', np.sum(long_open_interest[:15]), np.sum(long_open_interest_chg[:15]), '', np.sum(short_open_interest[:15]), np.sum(short_open_interest_chg[:15])]
                tmp_list += top15
                # top20
                top20 = ['', np.sum(vol[:20]), np.sum(vol_chg[:20]), '', np.sum(long_open_interest[:20]), np.sum(long_open_interest_chg[:20]), '', np.sum(short_open_interest[:20]), np.sum(short_open_interest_chg[:20])]
                tmp_list += top20

                tmps.append(tmp_list.copy())

        date_str = date[0:4]
        date_str += '-'
        date_str += date[4:6]
        date_str += '-'
        date_str += date[6:8]
        row = [date_str]
        vol20 = np.array(vol20, dtype=float)
        order = np.argsort(vol20)[::-1]

        if (actual_L > 0):
            for i in range(min(actual_L,6)):
                row += tmps[order[i]*2]
                row += tmps[order[i]*2+1]

            if (len(null_data) < 1):
                for i in range(len(tmps[0])):
                    null_data += [None]
                    null_data += [None]

            # 补数据
            for i in range(6 - actual_L):
                row += null_data

        ret_dict[s] = row

    return ret_dict



def get_all_option_position(exchange, start_time):
    calendar = cons.get_calendar()

    data_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')
    current_time_dt = datetime.datetime.now()
    writers = {}
    while data_time_dt <= current_time_dt:
        print(data_time_dt)
        # 获取的数据的时间
        data_time_str = data_time_dt.strftime('%Y%m%d')
        date = cons.convert_date(data_time_str)
        if date.strftime("%Y%m%d") not in calendar:
            data_time_dt += pd.Timedelta(days=1)
            continue

        ret_dict = get_option_position(exchange, data_time_str)
        for key in ret_dict:
            if not(key in writers):
                path = os.path.join(option_position_dir, exchange, key+'.csv')
                if os.path.exists(path):
                    print('OPTION POSITION APPEND ' + path)
                else:
                    create_option_position_file(path)

                f = open(path, 'a', newline='', encoding='utf-8')
                writer = csv.writer(f)
                writers[key] = writer

            if len(ret_dict[key]) > 1:
                writers[key].writerow(ret_dict[key])

        data_time_dt += pd.Timedelta(days=1)
        time.sleep(0.5)
        # return

def update_all_option_position(exchange):
    print('UPDATE POSITION: ', exchange)
    if exchange == 'dce':
        path = os.path.join(option_position_dir, exchange, 'i'+'.csv')
    if exchange == 'czce':
        path = os.path.join(option_position_dir, exchange, 'SR'+'.csv')

    # 最后一行的时间
    with open(path, 'rb') as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell() - 1  # 不算最后一个字符'\n'
        while pos > 0:
            pos -= 1
            f.seek(pos, os.SEEK_SET)
            if f.read(1) == b'\n':
                break
        last_line = f.readline().decode().strip()
    
    print('OPTION POSITION LAST TIME: ', last_line[:10])
    data_time_dt = pd.to_datetime(last_line[:10], format='%Y-%m-%d')
    data_time_dt += pd.Timedelta(days=1)
    data_time_str = data_time_dt.strftime('%Y-%m-%d')

    get_all_option_position(exchange, data_time_str)


##########################################################################################
###################################### FUTURE PRICE ######################################
##########################################################################################


def create_future_price_file(path):
    if not os.path.exists(path):
        c1 = ['time','index','index','index']
        c1_add = ['c1','c2','c3','c4','c5','c6','c7','c8','c9','dom']
        c2 = ['','close','vol','oi']
        c2_add = ['inst_id','open','high','low','close','vol','oi','settle']
        for i in range(len(c1_add)): # 连续合约 + 主力合约 + 指数合约
            for j in range(len(c2_add)): # 
                    c1.append(c1_add[i])
                    c2.append(c2_add[j])

        df = pd.DataFrame(columns=[c1,c2])
        df.to_csv(path, encoding='utf-8', index=False)
        print('FUTURE PRICE CREATE ' + path)


def get_future_price(exchange, date):
    if exchange == 'shfe':
        df = ak.get_futures_daily(start_date=date, end_date=date, market="SHFE")
        df['symbol'] = df['symbol'].str.lower()
        df['variety'] = df['variety'].str.lower()
    if exchange == 'dce':
        df = ak.get_futures_daily(start_date=date, end_date=date, market="DCE")
        df['symbol'] = df['symbol'].str.lower()
        df['variety'] = df['variety'].str.lower()
    if exchange == 'czce':
        df = ak.get_futures_daily(start_date=date, end_date=date, market="CZCE")
    if exchange == 'cffex':
        df = ak.get_futures_daily(start_date=date, end_date=date, market="CFFEX")
    if exchange == 'gfex':
        df = ak.get_futures_daily(start_date=date, end_date=date, market="GFEX")
        df['symbol'] = df['symbol'].str.lower()
        df['variety'] = df['variety'].str.lower()

    df.replace('', '0', inplace=True) 
    # 
    variety = np.array(df['variety'], dtype=str)
    variety_dict = {}
    for i in range(len(variety)):
        if (not(variety[i] in variety_dict)):
            variety_dict[variety[i]] = [i, i]
        else:
            variety_dict[variety[i]][1] = variety_dict[variety[i]][1] + 1

    ret_dict = {}
    null_data = [None,None,None,None,None,None,None,None]
    date_str = date[0:4]
    date_str += '-'
    date_str += date[4:6]
    date_str += '-'
    date_str += date[6:8]
    for v in variety_dict:
        row = [date_str]
        n = variety_dict[v][1] + 1 - variety_dict[v][0]
        if (n > 0):
            # 指数合约数据
            tmp = df.loc[variety_dict[v][0]:variety_dict[v][1], ['symbol','open','high','low','close','volume','open_interest','settle']]
            close = np.array(tmp['close'], dtype=float)
            volumn = np.array(tmp['volume'], dtype=float)
            oi = np.array(tmp['open_interest'], dtype=float)
            # 指数合约代码
            index_oi = np.sum(oi)
            if (index_oi > 0):
                index_close = np.sum(close*oi)/np.sum(oi)
                index_volumn = np.sum(volumn)
                row += [index_close, index_volumn, index_oi]
            else:
                row += [0, 0, 0]

            if (n >= 9):
                # 有至少9个合约
                row += (tmp.loc[variety_dict[v][0]:variety_dict[v][0]+8]).values.flatten().tolist()
            elif (n > 0):
                # 不足9个合约
                row += (tmp.loc[variety_dict[v][0]:variety_dict[v][0]+n-1]).values.flatten().tolist()
                for _ in range(9-n):
                    row += null_data

            # 主力
            idx = np.nanargmax(volumn)
            row += (tmp.loc[variety_dict[v][0]+idx]).values.flatten().tolist()

            ret_dict[v] = row

    return ret_dict


def get_all_future_price(exchange, start_time):
    calendar = cons.get_calendar()

    data_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')
    current_time_dt = datetime.datetime.now()
    writers = {}
    while data_time_dt <= current_time_dt:
        print(data_time_dt)
        # 获取的数据的时间
        data_time_str = data_time_dt.strftime('%Y%m%d')
        date = cons.convert_date(data_time_str)
        if date.strftime("%Y%m%d") not in calendar:
            data_time_dt += pd.Timedelta(days=1)
            continue

        ret_dict = get_future_price(exchange, data_time_str)
        for key in ret_dict:
            if not(key in writers):
                path = os.path.join(future_price_dir, exchange, key+'.csv')
                if os.path.exists(path):
                    print('FUTURE PRICE APPEND ' + path)
                else:
                    create_future_price_file(path)

                f = open(path, 'a', newline='', encoding='utf-8')
                writer = csv.writer(f)
                writers[key] = writer

            if len(ret_dict[key]) > 10:
                writers[key].writerow(ret_dict[key])

        data_time_dt += pd.Timedelta(days=1)
        time.sleep(0.5)
        # return

def update_all_future_price(exchange):
    print('UPDATE FUTURE PRICE: ', exchange)
    if exchange == 'shfe':
        path = os.path.join(future_price_dir, exchange, 'au'+'.csv')
    if exchange == 'dce':
        path = os.path.join(future_price_dir, exchange, 'i'+'.csv')
    if exchange == 'czce':
        path = os.path.join(future_price_dir, exchange, 'SR'+'.csv')
    if exchange == 'cffex':
        path = os.path.join(future_price_dir, exchange, 'IC'+'.csv')
    if exchange == 'gfex':
        path = os.path.join(future_price_dir, exchange, 'si'+'.csv')

    # 最后一行的时间
    with open(path, 'rb') as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell() - 1  # 不算最后一个字符'\n'
        while pos > 0:
            pos -= 1
            f.seek(pos, os.SEEK_SET)
            if f.read(1) == b'\n':
                break
        last_line = f.readline().decode().strip()
    
    print('FUTURE PRICE LAST TIME: ', last_line[:10])
    data_time_dt = pd.to_datetime(last_line[:10], format='%Y-%m-%d')
    data_time_dt += pd.Timedelta(days=1)
    data_time_str = data_time_dt.strftime('%Y-%m-%d')

    get_all_future_price(exchange, data_time_str)


##########################################################################################
####################################### SPOT PRICE #######################################
##########################################################################################


def create_spot_price_file(path):
    if not os.path.exists(path):
        c1 = ['time', 'spot_price', 'near_contract', 'near_contract_price',
       'dominant_contract', 'dominant_contract_price', 'near_basis',
       'dom_basis', 'near_basis_rate', 'dom_basis_rate']
        df = pd.DataFrame(columns=c1)
        df.to_csv(path, encoding='utf-8', index=False)
        print('SPOT PRICE CREATE ' + path)


def get_spot_price(date):
    df = ak.futures_spot_price(date=date)
    symbol = np.array(df['symbol'], dtype=str)
    df = df.loc[:, ['spot_price', 'near_contract', 'near_contract_price',
       'dominant_contract', 'dominant_contract_price', 'near_basis',
       'dom_basis', 'near_basis_rate', 'dom_basis_rate']]

    ret_dict = {}
    date_str = date[0:4]
    date_str += '-'
    date_str += date[4:6]
    date_str += '-'
    date_str += date[6:8]
    for i in range(len(symbol)):
        row = [date_str]
        row += (df.loc[i]).values.flatten().tolist()

        s = symbol[i].lower()
        if (s in exchange_dict['shfe']):
            inst_id = s
            exchange = 'shfe'
        elif (symbol[i] in exchange_dict['cffex']):
            inst_id = symbol[i]
            exchange = 'cffex'
        elif (s in exchange_dict['dce']):
            inst_id = s
            exchange = 'dce'
        elif (symbol[i] in exchange_dict['czce']):
            inst_id = symbol[i]
            exchange = 'czce'
        elif (symbol[i] in exchange_dict['gfex']):
            inst_id = s
            exchange = 'gfex'
        else:
            continue

        if (s == 'si'):
            row[2] = row[2].lower()
            row[4] = row[4].lower()

        ret_dict[inst_id] = [exchange, row]

    return ret_dict


def get_all_future_spot_price(start_time):
    calendar = cons.get_calendar()

    data_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')
    current_time_dt = datetime.datetime.now()
    writers = {}
    while data_time_dt <= current_time_dt:
        print(data_time_dt)
        # 获取的数据的时间
        data_time_str = data_time_dt.strftime('%Y%m%d')
        date = cons.convert_date(data_time_str)
        if date.strftime("%Y%m%d") not in calendar:
            data_time_dt += pd.Timedelta(days=1)
            continue

        ret_dict = get_spot_price(data_time_str)
        for key in ret_dict:
            if not(key in writers):
                path = os.path.join(future_price_dir, ret_dict[key][0], key+'_spot'+'.csv')
                if os.path.exists(path):
                    print('SPOT PRICE APPEND ' + path)
                else:
                    create_spot_price_file(path)

                f = open(path, 'a', newline='', encoding='utf-8')
                writer = csv.writer(f)
                writers[key] = writer

            # if len(ret_dict[key]) > 10:
            writers[key].writerow(ret_dict[key][1])

        data_time_dt += pd.Timedelta(days=1)
        time.sleep(0.5)
        # return

def update_all_future_spot_price():
    print('UPDATE SPOT PRICE')
    path = os.path.join(future_price_dir, 'dce', 'i_spot'+'.csv')

    # 最后一行的时间
    with open(path, 'rb') as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell() - 1  # 不算最后一个字符'\n'
        while pos > 0:
            pos -= 1
            f.seek(pos, os.SEEK_SET)
            if f.read(1) == b'\n':
                break
        last_line = f.readline().decode().strip()
    
    print('SPOT PRICE LAST TIME: ', last_line[:10])
    data_time_dt = pd.to_datetime(last_line[:10], format='%Y-%m-%d')
    data_time_dt += pd.Timedelta(days=1)
    data_time_str = data_time_dt.strftime('%Y-%m-%d')
    if (data_time_str == '2018-09-12'):
        # 2018-09-12 生意社源数据缺失
        data_time_dt += pd.Timedelta(days=1)
        data_time_str = data_time_dt.strftime('%Y-%m-%d')

    get_all_future_spot_price(data_time_str)


##########################################################################################
###################################### OPTION PRICE ######################################
##########################################################################################

def put_call_delta_volatility(df, delta, price, strike):
    # # CALL HIGH
    # tmp = df.loc[pd.IndexSlice['C', :, 'delta_h']]
    # idx1, idx2, delta1, delta2 = column_index_delta(tmp, delta)
    # price = df.loc[pd.IndexSlice['C', :, 'high']]
    # iv = df.loc[pd.IndexSlice['C', :, 'imp_vol_h']]

    # if delta1 == delta:
    #     h_25d_call_price = price[idx1]
    #     h_25d_call_iv = iv[idx1]
    # else:
    #     w1 = (delta2 - delta)/(delta2 - delta1)
    #     w2 = (delta - delta1)/(delta2 - delta1)
    #     h_25d_call_price = w1*price[idx1] + w2*price[idx2]
    #     h_25d_call_iv = w1*iv[idx1] + w2*iv[idx2]

    # # CALL LOW
    # tmp = df.loc[pd.IndexSlice['C', :, 'delta_l']]
    # idx1, idx2, delta1, delta2 = column_index_delta(tmp, delta)
    # price = df.loc[pd.IndexSlice['C', :, 'low']]
    # iv = df.loc[pd.IndexSlice['C', :, 'imp_vol_l']]

    # if delta1 == delta:
    #     l_25d_call_price = price[idx1]
    #     l_25d_call_iv = iv[idx1]
    # else:
    #     w1 = (delta2 - delta)/(delta2 - delta1)
    #     w2 = (delta - delta1)/(delta2 - delta1)
    #     l_25d_call_price = w1*price[idx1] + w2*price[idx2]
    #     l_25d_call_iv = w1*iv[idx1] + w2*iv[idx2]

    # CALL CLOSE
    tmp = df.loc[pd.IndexSlice['C', :, 'delta_c']]
    idx1, idx2, delta1, delta2 = column_index_delta(tmp, delta)
    o_price = df.loc[pd.IndexSlice['C', :, 'close']]
    iv = df.loc[pd.IndexSlice['C', :, 'imp_vol_c']]

    if delta1 == delta:
        c_25d_call_price = o_price[idx1]
        c_25d_call_iv = iv[idx1]
    else:
        w1 = (delta2 - delta)/(delta2 - delta1)
        w2 = (delta - delta1)/(delta2 - delta1)
        c_25d_call_price = w1*o_price[idx1] + w2*o_price[idx2]
        c_25d_call_iv = w1*iv[idx1] + w2*iv[idx2]


    if (price > 1):
        idx1, idx2, price1, price2 = column_index_price(strike, price)
        if price1 == price:
            c_atm_call_iv = iv[idx1]
        else:
            w1 = (price2 - price)/(price2 - price1)
            w2 = (price - price1)/(price2 - price1)
            c_atm_call_iv = w1*iv[idx1] + w2*iv[idx2]

    else:
        c_atm_call_iv = np.nan


    ############################################################################
    delta = -delta
    # # PUT HIGH
    # tmp = df.loc[pd.IndexSlice['P', :, 'delta_h']]
    # idx1, idx2, delta1, delta2 = column_index_delta(tmp, delta)
    # o_price = df.loc[pd.IndexSlice['P', :, 'high']]
    # iv = df.loc[pd.IndexSlice['P', :, 'imp_vol_h']]

    # if delta1 == delta:
    #     h_25d_put_price = o_price[idx1]
    #     h_25d_put_iv = iv[idx1]
    # else:
    #     w1 = (delta2 - delta)/(delta2 - delta1)
    #     w2 = (delta - delta1)/(delta2 - delta1)
    #     h_25d_put_price = w1*o_price[idx1] + w2*o_price[idx2]
    #     h_25d_put_iv = w1*iv[idx1] + w2*iv[idx2]

    # # PUT LOW
    # tmp = df.loc[pd.IndexSlice['P', :, 'delta_l']]
    # idx1, idx2, delta1, delta2 = column_index_delta(tmp, delta)
    # o_price = df.loc[pd.IndexSlice['P', :, 'low']]
    # iv = df.loc[pd.IndexSlice['P', :, 'imp_vol_l']]

    # if delta1 == delta:
    #     l_25d_put_price = o_price[idx1]
    #     l_25d_put_iv = iv[idx1]
    # else:
    #     w1 = (delta2 - delta)/(delta2 - delta1)
    #     w2 = (delta - delta1)/(delta2 - delta1)
    #     l_25d_put_price = w1*o_price[idx1] + w2*o_price[idx2]
    #     l_25d_put_iv = w1*iv[idx1] + w2*iv[idx2]

    # PUT CLOSE
    tmp = df.loc[pd.IndexSlice['P', :, 'delta_c']]
    idx1, idx2, delta1, delta2 = column_index_delta(tmp, delta)
    o_price = df.loc[pd.IndexSlice['P', :, 'close']]
    iv = df.loc[pd.IndexSlice['P', :, 'imp_vol_c']]

    if delta1 == delta:
        c_25d_put_price = o_price[idx1]
        c_25d_put_iv = iv[idx1]
    else:
        w1 = (delta2 - delta)/(delta2 - delta1)
        w2 = (delta - delta1)/(delta2 - delta1)
        c_25d_put_price = w1*o_price[idx1] + w2*o_price[idx2]
        c_25d_put_iv = w1*iv[idx1] + w2*iv[idx2]


    if (price > 1):
        idx1, idx2, price1, price2 = column_index_price(strike, price)
        if price1 == price:
            c_atm_put_iv = iv[idx1]
        else:
            w1 = (price2 - price)/(price2 - price1)
            w2 = (price - price1)/(price2 - price1)
            c_atm_put_iv = w1*iv[idx1] + w2*iv[idx2]
    else:
        c_atm_put_iv = np.nan


    return [c_25d_put_price, c_25d_put_iv,\
            c_25d_call_price, c_25d_call_iv,\
            c_atm_put_iv,\
            c_atm_call_iv
            ]

    return [h_25d_put_price, h_25d_put_iv, l_25d_put_price, l_25d_put_iv, c_25d_put_price, c_25d_put_iv,\
           h_25d_call_price, h_25d_call_iv, l_25d_call_price, l_25d_call_iv, c_25d_call_price, c_25d_call_iv]



def get_strike_price(df):
    col = df.columns.tolist()
    call_strike = [float(col[i][1]) for i in range(len(col)) if col[i][0] == 'C']
    call_strike = np.array(call_strike)
    call_strike = np.unique(call_strike)

    # call_strike = put_strike
    return call_strike, call_strike


# 标的期货合约交割月前第一月的倒数第 几 个交易日
option_expiry_date_dict = \
{
    'au':5,
    'ag':5,
    'cu':5,
    'al':5,
    'zn':5,
    'ru':5,
    'br':5,
    'rb':5,
    'sc':13,

    'c':-5,
    'i':-5,
    'm':-5,
    'y':-5,
    'pg':-5,
    'eg':-5,
    'p':-5,
    'a':-5,
    'b':-5,
    'l':-5,
    'eb':-5,
    'v':-5,
    'pp':-5,

    'SR':-3,
    'OI':-3,
    'CF':-3,
    'RM':-3,
    'MA':-3,
    'TA':-3,
    'PK':-3,
    'PX':-3,
    'SH':-3,
    'SA':-3,
    'SM':-3,
    'SF':-3,
    'PF':-3,
    'AP':-3,
    'UR':-3,

    'si':-5,
    'lc':-5,
}


def get_future_inst_id_data(exchange, inst_id):
    if (inst_id[1].isdigit()):
        variety = inst_id[0]
    else:
        variety = inst_id[0:2]

    path1 = os.path.join(future_price_dir, exchange, variety+'.csv')
    fut_df = pd.read_csv(path1, header=[0,1])
    t1 = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))

    cs = ['c8','c7','c6','c5','c4','c3','c2','c1']
    k = 0
    start = 0
    end = 0
    for i in range(len(cs)):
        c = cs[i]
        tmp = (fut_df[c]['inst_id'] == inst_id)
        ii = tmp[tmp == True].index
        if (len(ii) > 0):
            if (k == 0):
                start = ii[0]
                end = ii[len(ii)-1]
                open = np.array(fut_df.loc[ii, pd.IndexSlice[c, 'open']], dtype=float)
                high = np.array(fut_df.loc[ii, pd.IndexSlice[c, 'high']], dtype=float)
                low = np.array(fut_df.loc[ii, pd.IndexSlice[c, 'low']], dtype=float)
                close = np.array(fut_df.loc[ii, pd.IndexSlice[c, 'close']], dtype=float)
            else:
                open = np.concatenate((open, np.array(fut_df.loc[ii, pd.IndexSlice[c, 'open']], dtype=float)), axis=0)
                high = np.concatenate((high, np.array(fut_df.loc[ii, pd.IndexSlice[c, 'high']], dtype=float)), axis=0)
                low = np.concatenate((low, np.array(fut_df.loc[ii, pd.IndexSlice[c, 'low']], dtype=float)), axis=0)
                close = np.concatenate((close, np.array(fut_df.loc[ii, pd.IndexSlice[c, 'close']], dtype=float)), axis=0)
                end = ii[len(ii)-1]
        else:
            continue

        k = k + 1

    if (start == 0 and end == 0):
        return None, None, None, None, None

    t = t1[start:end+1]
    return t, open, high, low, close

def get_option_last_trading_day(inst_id):
    if (inst_id[1].isdigit()):
        variety = inst_id[0]
        if (len(inst_id[1:]) == 3):
            start_time_dt = pd.to_datetime('202'+inst_id[1:], format='%Y%m')
        else:
            start_time_dt = pd.to_datetime('20'+inst_id[1:], format='%Y%m')
    else:
        variety = inst_id[0:2]
        if (len(inst_id[2:]) == 3):
            start_time_dt = pd.to_datetime('202'+inst_id[2:], format='%Y%m')
        else:
            start_time_dt = pd.to_datetime('20'+inst_id[2:], format='%Y%m')
  
    z = pd.to_datetime('202501', format='%Y%m')
    k = 0
    calendar = cons.get_calendar()

    if (variety == 'IF' or variety == 'IH' or variety == 'IM'):
        # 第三个星期五
        while (1):
            if (start_time_dt.weekday() == 4):
                k = k + 1
            if (k == 3):
                return start_time_dt.strftime('%Y-%m-%d')
            start_time_dt = start_time_dt + pd.Timedelta(days=1)

    days = option_expiry_date_dict[variety]
    # print(inst_id, days, start_time_dt)
    if (days == -3):
        print('czce')
        # czce
        if (start_time_dt >= pd.to_datetime('2024-01-01', format='%Y-%m-%d')):
            start_time_dt = start_time_dt - dateutil.relativedelta.relativedelta(months=1) + pd.Timedelta(days=16)
            while (k < -days):
                start_time_dt = start_time_dt - pd.Timedelta(days=1)
                if (start_time_dt.strftime("%Y%m%d") in calendar) or (start_time_dt > z):
                    k = k + 1
            # print(start_time_dt)
            return start_time_dt.strftime('%Y-%m-%d')
        else:
            start_time_dt = start_time_dt - dateutil.relativedelta.relativedelta(months=1) - pd.Timedelta(days=1)
            while (k < -days):
                start_time_dt = start_time_dt + pd.Timedelta(days=1)
                if (start_time_dt.strftime("%Y%m%d") in calendar) or (start_time_dt > z):
                    k = k + 1
            # print(start_time_dt)
            return start_time_dt.strftime('%Y-%m-%d')

    if (days > 0):
        while (k < days):
            start_time_dt = start_time_dt - pd.Timedelta(days=1)
            if (start_time_dt.strftime("%Y%m%d") in calendar) or (start_time_dt > z):
                k = k + 1
    else:
        start_time_dt = start_time_dt - dateutil.relativedelta.relativedelta(months=1) - pd.Timedelta(days=1)
        while (k < -days):
            start_time_dt = start_time_dt + pd.Timedelta(days=1)
            if (start_time_dt.strftime("%Y%m%d") in calendar) or (start_time_dt > z):
                k = k + 1

    return start_time_dt.strftime('%Y-%m-%d')

def get_option_days_to_expiry(inst_id, today):
    last_day = get_option_last_trading_day(inst_id)
    last_day_dt = pd.to_datetime(last_day, format='%Y-%m-%d')
    today_dt = pd.to_datetime(today, format='%Y-%m-%d')

    dt = last_day_dt - today_dt
    return float(dt.days)
    # if (dt.days >= 1):
    #     return float(dt.days)
    # else:
    #     return 1.0


def get_option_price_dict(exchange, df, future_df_dict, r, date_str, count=3):
    # print(df)
    idx_offset = df.index[0]
    if (exchange == 'cffex'):
        inst_ids = np.array(df['合约代码'])
    elif (exchange == 'shfe'):
        inst_ids = np.array(df['合约代码'])
    elif (exchange == 'dce'):
        inst_ids = np.array(df['合约名称'])
    elif (exchange == 'czce'):
        inst_ids = np.array(df['合约代码'])
    elif (exchange == 'gfex'):
        inst_ids = np.array(df['合约名称'])

    inst_id_dict = {}
    cs = np.array(['c1','c2','c3','c4','c5','c6','c7','c8','c9'])

    if exchange == 'cffex':
        n1 = 0
        n2 = 6
        n3 = 7
        n4 = 9
        variety = inst_ids[0][0:2]
    elif exchange == 'shfe':
        n1 = 0
        n2 = 6
        n3 = 6
        n4 = 7
        variety = inst_ids[0][0:2]
    elif exchange == 'dce':
        if (inst_ids[0][1].isdigit()):
            n1 = 0
            n2 = 5
            n3 = 6
            n4 = 8
            variety = inst_ids[0][0:1]
        else:
            n1 = 0
            n2 = 6
            n3 = 7
            n4 = 9
            variety = inst_ids[0][0:2]
    elif exchange == 'czce':
        n1 = 0
        n2 = 5
        n3 = 5
        n4 = 6
        variety = inst_ids[0][0:2]
    elif exchange == 'gfex':
        n1 = 0
        n2 = 6
        n3 = 7
        n4 = 9
        variety = inst_ids[0][0:2]


    for i in range(0, len(df.index)):
        s = inst_ids[i][n1:n2]
        if not(s in inst_id_dict):
            # 从 idx_offset 开始
            inst_id_dict[s] = [i+idx_offset, i+idx_offset]
        else:
            inst_id_dict[s][1] = inst_id_dict[s][1] + 1

    print(inst_id_dict)
    open = np.array(df['开盘价'], dtype=float)
    high = np.array(df['最高价'], dtype=float)
    low = np.array(df['最低价'], dtype=float)
    close = np.array(df['收盘价'], dtype=float)
    settle = np.array(df['结算价'], dtype=float)
    volume = np.array(df['成交量'], dtype=float)
    oi = np.array(df['持仓量'], dtype=float)
    try:
        exercise = np.array(df['行权量'], dtype=float)
    except:
        exercise = np.zeros((len(oi)), dtype=float)
    volume_sum = np.zeros((len(inst_id_dict)))
    inst_id_list = []
    k = 0
    for s in inst_id_dict:
        # print(s, inst_id_dict[s])
        volume_sum[k] = np.sum(volume[inst_id_dict[s][0]-idx_offset: inst_id_dict[s][1]-idx_offset+1])
        inst_id_list.append(s)
        k = k + 1

    order = np.argsort(volume_sum)[::-1]
    ret_dict = {}
    if (len(order) >= count):
        L = count
    else:
        L = len(order)

    for i in range(L):
        ret_df = pd.DataFrame(columns=[['time'],['time'],['time']])
        ret_df.loc[0, pd.IndexSlice['time', 'time', 'time']] = date_str
        future_df = future_df_dict[variety]
        print(inst_id_list[order[i]])
        tmp = future_df[future_df['time']['Unnamed: 0_level_1'] == date_str]
        if (len(tmp) == 0):
            continue
        idx = (tmp.loc[:, pd.IndexSlice[cs, 'inst_id']] == inst_id_list[order[i]]).values[0]
        if (len(cs[idx]) == 0):
            print(inst_id_list[order[i]])
            continue

        c = cs[idx][0]
        high_fut = tmp.loc[tmp.index[0], pd.IndexSlice[c, 'high']]
        low_fut = tmp.loc[tmp.index[0], pd.IndexSlice[c, 'low']]
        close_fut = tmp.loc[tmp.index[0], pd.IndexSlice[c, 'close']]

        # 期权还有几年到期
        T = get_option_days_to_expiry(inst_id_list[order[i]], date_str) / 365
        print(inst_id_list[order[i]], date_str, T)

        # index 下标范围
        rg = inst_id_dict[inst_id_list[order[i]]]
        for k in range(rg[0]-idx_offset, rg[1]-idx_offset+1):
            otype = inst_ids[k][n3]
            strike = inst_ids[k][n4:]
            columns = [[otype,otype,otype,otype,otype,otype,otype,otype,otype,otype,otype,otype,otype,otype],
                    [strike,strike,strike,strike,strike,strike,strike,strike,strike,strike,strike,strike,strike,strike],
                    ['open','high','low','close','volume','oi','settle','exercise','imp_vol_h','imp_vol_l','imp_vol_c','delta_h','delta_l','delta_c']]
            # print(otype, strike)
            df_tmp = pd.DataFrame(columns=columns)
            df_tmp.loc[0, pd.IndexSlice[otype, strike, ['open','high','low','close','volume','oi','settle','exercise']]] = \
            [open[k], high[k], low[k], close[k], volume[k], oi[k], settle[k], exercise[k]]

            if (otype == 'C'):
                df_tmp.loc[0, pd.IndexSlice[otype, strike, ['imp_vol_h', 'delta_h']]] = calculate_greeks(high_fut, float(strike), T, r, high[k], otype)
                df_tmp.loc[0, pd.IndexSlice[otype, strike, ['imp_vol_l', 'delta_l']]] = calculate_greeks(low_fut, float(strike), T, r, low[k], otype)
                df_tmp.loc[0, pd.IndexSlice[otype, strike, ['imp_vol_c', 'delta_c']]] = calculate_greeks(close_fut, float(strike), T, r, close[k], otype)
            else:
                df_tmp.loc[0, pd.IndexSlice[otype, strike, ['imp_vol_h', 'delta_h']]] = calculate_greeks(high_fut, float(strike), T, r, low[k], otype)
                df_tmp.loc[0, pd.IndexSlice[otype, strike, ['imp_vol_l', 'delta_l']]] = calculate_greeks(low_fut, float(strike), T, r, high[k], otype)
                df_tmp.loc[0, pd.IndexSlice[otype, strike, ['imp_vol_c', 'delta_c']]] = calculate_greeks(close_fut, float(strike), T, r, close[k], otype)

            ret_df = pd.concat([ret_df, df_tmp], axis=1)

        # ret_df.loc[0, pd.IndexSlice['time', 'time', 'time']] = date
        ret_dict[inst_id_list[order[i]]] = ret_df

    return ret_dict

def get_option_price(exchange, contract_df, future_df_dict, rate, date_str):
    if (len(contract_df) > 0):
        count = 3
        if (exchange == 'dce'):
            inst_id = np.array(contract_df['商品名称'])[0]
            if (inst_id == '铁矿石'):
                # 铁矿保留4个期权合约数据
                count = 4

        ret_dict = get_option_price_dict(exchange, contract_df, future_df_dict, rate, date_str, count)
        info = [date_str, '', '', '']
        if (count == 4):
            info = [date_str, '', '', '', '']

        k = 1
        # s = 'au2308'
        for s in ret_dict:
            info[k] = s
            k = k + 1
            df = ret_dict[s]
            path = os.path.join(option_price_dir, exchange, s+'.csv')

            if not(os.path.exists(path)):
                print('OPTION PRICE CREATE ' + path)
                df.to_csv(path, encoding='utf-8', index=False)
            else:
                old_df = pd.read_csv(path, header=[0,1,2])
                old_df = pd.concat([old_df, df], axis=0)
                old_df.drop_duplicates(subset=[('time','time','time')], keep='last', inplace=True) # last
                old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
                old_df.sort_values(by = ('time','time','time'), inplace=True)
                old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
                old_df.to_csv(path, encoding='utf-8', index=False)

            if (s[1].isdigit()):
                variety = s[0]
            else:
                variety = s[0:2]

        try:
            path = os.path.join(option_price_dir, exchange, variety+'_info'+'.csv')
            c1 = ['time','dom1','dom2','dom3']
            if (count == 4):
                c1 = ['time','dom1','dom2','dom3','dom4']

            info_df = pd.DataFrame(columns=c1)
            info_df.loc[0] = info
            if os.path.exists(path):
                old_info_df = pd.read_csv(path)
                old_info_df = pd.concat([old_info_df, info_df], axis=0)
                old_info_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
                old_info_df['time'] = old_info_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
                old_info_df.sort_values(by = 'time', inplace=True)
                old_info_df['time'] = old_info_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
                old_info_df.to_csv(path, encoding='utf-8', index=False)
            else:
                info_df.to_csv(path, encoding='utf-8', index=False)
        except Exception as e:
            print(e)
            pass


def get_all_option_price(exchange, start_time):
    calendar = cons.get_calendar()
    data_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')
    current_time_dt = datetime.datetime.now()

    if exchange == 'cffex':
        CFFEX_URL = 'http://www.cffex.com.cn/sj/hqsj/rtj/{}/{}/{}.csv'
        names = ['IF', 'IH', 'IM']
    elif exchange == 'shfe':
        symbols = ['黄金期权', '白银期权', '原油期权', '铜期权', '铝期权', '锌期权', '螺纹钢期权', '天胶期权', '丁二烯橡胶期权']
        names = ['au', 'ag', 'sc', 'cu', 'al', 'zn', 'rb', 'ru', 'br']
    elif exchange == 'dce':
        symbols = ['铁矿石', '玉米', '豆粕', '豆油', '棕榈油', '乙二醇', '液化石油气', '聚乙烯', '豆一', '豆二', '苯乙烯', '聚氯乙烯', '聚丙烯']
        names = ['i', 'c', 'm', 'y', 'p', 'eg', 'pg', 'l', 'a', 'b', 'eb', 'v', 'pp']
    elif exchange == 'czce':
        symbols = ['白糖期权', 'PTA期权', '甲醇期权', '菜籽粕期权', '菜籽油期权', '棉花期权', '花生期权', '对二甲苯期权', '烧碱期权', '苹果期权', '纯碱期权', '短纤期权', '锰硅期权', '硅铁期权', '尿素期权']
        names = ['SR', 'TA', 'MA', 'RM', 'OI', 'CF', 'PK', 'PX', 'SH', 'AP', 'SA', 'PF', 'SM', 'SF', 'UR']
    elif exchange == 'gfex':
        symbols = ['工业硅', '碳酸锂']
        names = ['si', 'lc']

    future_df_dict = {}
    for name in names:
        path = os.path.join(future_price_dir, exchange, name+'.csv')
        future_df_dict[name] = pd.read_csv(path, header=[0,1])

    # shibor 3M
    path = os.path.join(interest_rate_dir, 'shibor'+'.csv') 
    rate_df = pd.read_csv(path)
    shibor_3M = np.array(rate_df['3M'], dtype=float)


    while data_time_dt <= current_time_dt:
        print(data_time_dt)

        # 获取的数据的时间
        data_time_str = data_time_dt.strftime('%Y-%m-%d')
        date = convert_date(data_time_str)

        if date.strftime("%Y%m%d") not in calendar:
            data_time_dt += pd.Timedelta(days=1)
            continue

        idx = rate_df[rate_df['time'] == data_time_str].index[0]
        rate = shibor_3M[idx] / 100

        if exchange == 'cffex':
            date_str = date.strftime("%Y%m%d")
            url = CFFEX_URL.format(date_str[0:6], date_str[6:8], date_str+'_1')
            # url = 'http://www.cffex.com.cn/sj/hqsj/rtj/202307/07/20230707_1.csv'
            headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
                "Host": "www.cffex.com.cn",
            }
            r = requests.get(url, headers=headers)
            table_df = pd.read_csv(BytesIO(r.content), encoding='gb2312')
            table_df['合约代码'] = table_df['合约代码'].str.strip()
            table_df.rename(columns={'今开盘':'开盘价', '今收盘':'收盘价', '今结算':'结算价'}, inplace=True)
            table_df = table_df[table_df['合约代码'].str.contains('-')]
            table_df['合约代码'] = table_df['合约代码'].str.replace('IO','IF')
            table_df['合约代码'] = table_df['合约代码'].str.replace('HO','IH')
            table_df['合约代码'] = table_df['合约代码'].str.replace('MO','IM')
            table_df.reset_index(drop=True, inplace=True)
            for name in names:
                contract_df = table_df[table_df["合约代码"].str.contains(name)]
                get_option_price(exchange, contract_df, future_df_dict, rate, date.strftime("%Y-%m-%d"))

        if exchange == 'shfe':
            # shfe
            if date > datetime.date(2010, 8, 24):
                url = SHFE_OPTION_URL.format(date.strftime("%Y%m%d"))
                # try:
                r = requests.get(url, headers=SHFE_HEADERS)
                json_data = r.json()
                table_df = pd.DataFrame(
                    [
                        row
                        for row in json_data["o_curinstrument"]
                        if row["INSTRUMENTID"] not in ["小计", "合计"]
                        and row["INSTRUMENTID"] != ""
                    ]
                )

                for symbol in symbols:
                    contract_df = table_df[table_df["PRODUCTNAME"].str.strip() == symbol]
                    contract_df.columns = ["_","_","_","合约代码","前结算价","开盘价","最高价","最低价","收盘价","结算价",
                                        "涨跌1","涨跌2","成交量","持仓量","持仓量变化","_","行权量","成交额","德尔塔",
                                        "_","_","_","_"]
                    contract_df = contract_df[
                        ["合约代码","开盘价","最高价","最低价","收盘价","前结算价","结算价","涨跌1","涨跌2",
                        "成交量","持仓量","持仓量变化","成交额","德尔塔","行权量"]
                    ]
                    contract_df['合约代码'] = contract_df['合约代码'].str.strip()
                    contract_df.replace(r'^\s*$', np.nan, regex=True, inplace=True)

                    get_option_price(exchange, contract_df, future_df_dict, rate, date.strftime("%Y-%m-%d"))

        elif exchange == 'dce':
            url = DCE_DAILY_OPTION_URL
            payload = {
                "dayQuotes.variety": "all",
                "dayQuotes.trade_type": "1",
                "year": str(date.year),
                "month": str(date.month - 1),
                "day": str(date.day),
                "exportFlag": "excel",
            }
            res = requests.post(url, data=payload)
            table_df = pd.read_excel(BytesIO(res.content), skiprows=[0])
            table_df = table_df.replace(',', '', regex=True)
            # print(table_df)
            for symbol in symbols:
                # print(symbol)
                contract_df = table_df[table_df["商品名称"] == symbol]
                contract_df.reset_index(inplace=True, drop=True) 
                get_option_price(exchange, contract_df, future_df_dict, rate, date.strftime("%Y-%m-%d"))

        elif (exchange == 'czce'):
            url = CZCE_DAILY_OPTION_URL_3.format(date.strftime("%Y"), date.strftime("%Y%m%d"))
            try:
                r = requests.get(url)
                f = StringIO(r.text)
                table_df = pd.read_table(f, encoding="utf-8", skiprows=1, sep="|") 
                table_df = table_df.replace(',', '', regex=True)           
                for name in names:
                    temp_df = table_df[table_df.iloc[:, 0].str.contains(name)]
                    temp_df.reset_index(inplace=True, drop=True)
                    contract_df = temp_df.iloc[:-1, :]
                    contract_df.columns = ['合约代码', '昨结算', '开盘价', '最高价', '最低价', '收盘价',
       '结算价', '涨跌1', '涨跌2', '成交量', '持仓量', '增减量','成交额(万元)', 'DELTA', '隐含波动率', '行权量']
                    get_option_price(exchange, contract_df, future_df_dict, rate, date.strftime("%Y-%m-%d"))
            except Exception as e:
                print('exception:', e)
                exit()

        elif (exchange == 'gfex'):
            symbol_map = {"工业硅": 1}
            url = "http://www.gfex.com.cn/u/interfacesWebTiDayQuotes/loadList"
            payload = {"trade_date": date.strftime("%Y%m%d"), "trade_type": symbol_map["工业硅"]}
            headers = {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Content-Length": "32",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Host": "www.gfex.com.cn",
                "Origin": "http://www.gfex.com.cn",
                "Pragma": "no-cache",
                "Proxy-Connection": "keep-alive",
                "Referer": "http://www.gfex.com.cn/gfex/rihq/hqsj_tjsj.shtml",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
                "X-Requested-With": "XMLHttpRequest",
                "content-type": "application/x-www-form-urlencoded",
            }
            r = requests.post(url, data=payload, headers=headers)
            data_json = r.json()
            temp_df = pd.DataFrame(data_json["data"])
            temp_df.rename(
                columns={
                    "variety": "商品名称",
                    "diffI": "持仓量变化",
                    "high": "最高价",
                    "turnover": "成交额",
                    "impliedVolatility": "隐含波动率",
                    "diff": "涨跌",
                    "delta": "Delta",
                    "close": "收盘价",
                    "diff1": "涨跌1",
                    "lastClear": "前结算价",
                    "open": "开盘价",
                    "matchQtySum": "行权量",
                    "delivMonth": "合约名称",
                    "low": "最低价",
                    "clearPrice": "结算价",
                    "varietyOrder": "品种代码",
                    "openInterest": "持仓量",
                    "volumn": "成交量",
                },
                inplace=True,
            )
            for symbol in symbols:
                contract_df = temp_df[temp_df["商品名称"].str.strip() == symbol]
                contract_df = contract_df[
                    ["商品名称","合约名称","开盘价","最高价","最低价","收盘价","前结算价","结算价","涨跌",
                    "涨跌1","Delta","成交量","持仓量","持仓量变化","成交额","行权量","隐含波动率",
                    ]
                ]
                if (len(contract_df) > 1):
                    get_option_price(exchange, contract_df, future_df_dict, rate, date.strftime("%Y-%m-%d"))
                    

        data_time_dt += pd.Timedelta(days=1)
        time.sleep(0.5)


def update_all_option_price(exchange):
    print('UPDATE OPTION PRICE')
    if exchange == 'cffex':
        path = os.path.join(option_price_dir, exchange, 'IF_info'+'.csv')
    if exchange == 'shfe':
        path = os.path.join(option_price_dir, exchange, 'au_info'+'.csv')
    if exchange == 'dce':
        path = os.path.join(option_price_dir, exchange, 'i_info'+'.csv')
    if exchange == 'czce':
        path = os.path.join(option_price_dir, exchange, 'SR_info'+'.csv')
    if exchange == 'gfex':
        path = os.path.join(option_price_dir, exchange, 'SI_info'+'.csv')

    # 最后一行的时间
    with open(path, 'rb') as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell() - 1  # 不算最后一个字符'\n'
        while pos > 0:
            pos -= 1
            f.seek(pos, os.SEEK_SET)
            if f.read(1) == b'\n':
                break
        last_line = f.readline().decode().strip()
    
    print('OPTION PRICE LAST TIME: ', last_line[:10])
    data_time_dt = pd.to_datetime(last_line[:10], format='%Y-%m-%d')
    data_time_dt += pd.Timedelta(days=1)
    data_time_str = data_time_dt.strftime('%Y-%m-%d')

    get_all_option_price(exchange, data_time_str)



# 重新计算greeks
def recalculate_greeks(exchange, inst_id):
    # shibor 3M
    path = os.path.join(data_dir, 'shibor'+'.csv') 
    rate_df = pd.read_csv(path)
    shibor_3M = np.array(rate_df['3M'], dtype=float)

    directory = os.path.join(option_price_dir, exchange)
    for _, _, files in os.walk(directory):
        for file in files:
            if ((inst_id in file) and (not('info' in file))):
                # print(file)
                # return

                t1, o, h, l, c = get_future_inst_id_data(exchange, inst_id)
                if (t1 is None):
                    print(inst_id, 't1 is None')
                    continue

                path = os.path.join(option_price_dir, exchange, inst_id+'.csv')
                opt_df = pd.read_csv(path, header=[0,1,2])
                t2 = pd.DatetimeIndex(pd.to_datetime(opt_df['time']['time']['time'], format='%Y-%m-%d'))

                # print(opt_df.columns.to_list())
                cols = opt_df.columns.to_list()

                for i in range(len(t2)-1,len(t2)):
                    if (i < 0):
                        return
                    print(i)
                    tmp = opt_df.loc[i, :]
                    date_dt = t2[i]
                    try:
                        fut_idx = np.where(t1 == date_dt)[0][0]
                    except:
                        continue
                    fut_high = h[fut_idx]
                    fut_low = l[fut_idx]
                    fut_close = c[fut_idx]

                    T = get_option_days_to_expiry(inst_id, date_dt.strftime('%Y-%m-%d')) / 365
                    print(T)

                    idx = rate_df[rate_df['time'] == date_dt.strftime('%Y-%m-%d')].index[0]
                    r = shibor_3M[idx] / 100

                    level1 = cols[0][0]
                    level2 = cols[0][1]
                    for k in range(1, len(cols)):
                        if (cols[k][0] != level1 or cols[k][1] != level2):
                            level1 = cols[k][0]
                            level2 = cols[k][1]
                            opt_high = tmp[level1, level2, 'high']
                            opt_low = tmp[level1, level2, 'low']
                            opt_close = tmp[level1, level2, 'close']
                            if (level1 == 'C'):
                                opt_df.loc[i, pd.IndexSlice[level1, level2, ['imp_vol_h', 'delta_h']]] = calculate_greeks(fut_high, float(level2), T, r, opt_high, level1)
                                opt_df.loc[i, pd.IndexSlice[level1, level2, ['imp_vol_l', 'delta_l']]] = calculate_greeks(fut_low, float(level2), T, r, opt_low, level1)
                                opt_df.loc[i, pd.IndexSlice[level1, level2, ['imp_vol_c', 'delta_c']]] = calculate_greeks(fut_close, float(level2), T, r, opt_close, level1)
                            else:
                                opt_df.loc[i, pd.IndexSlice[level1, level2, ['imp_vol_h', 'delta_h']]] = calculate_greeks(fut_high, float(level2), T, r, opt_low, level1)
                                opt_df.loc[i, pd.IndexSlice[level1, level2, ['imp_vol_l', 'delta_l']]] = calculate_greeks(fut_low, float(level2), T, r, opt_high, level1)
                                opt_df.loc[i, pd.IndexSlice[level1, level2, ['imp_vol_c', 'delta_c']]] = calculate_greeks(fut_close, float(level2), T, r, opt_close, level1)

                opt_df.to_csv(path, encoding='utf-8', index=False)




def recalculate_all_greeks(exchange):
    # shibor 3M
    path = os.path.join(data_dir, 'shibor'+'.csv') 
    rate_df = pd.read_csv(path)

    directory = os.path.join(option_price_dir, exchange)
    for _, _, files in os.walk(directory):
        for file in files:
            if (not('info' in file)):
                # print(file)
                # path = os.path.join(option_price_dir, exchange, file)
                inst_id = file.split(sep='.', maxsplit=-1)[0]
                print(inst_id)
                recalculate_greeks(exchange, inst_id)


##############################################################################################
##################################### OPTION INFO DETAIL #####################################
##############################################################################################


def update_option_info_detail(exchange, variety):
    print(variety)
    path1 = os.path.join(option_price_dir, exchange, variety+'_info'+'.csv')
    if not(os.path.exists(path1)):
        return
    
    info_df = pd.read_csv(path1)
    info_t = pd.DatetimeIndex(pd.to_datetime(info_df['time'], format='%Y-%m-%d'))
    info_last_line_time_dt = info_t[len(info_t)-1]
    info_cols = info_df.columns.tolist()
    info_cols.remove('time')

    path = os.path.join(option_price_dir, exchange, variety+'_info_detail'+'.csv')
    detail_last_line_time = get_last_line_time(path, '', None, 10, '%Y-%m-%d')
    if (detail_last_line_time is not None):
        detail_last_line_time_dt = pd.to_datetime(detail_last_line_time, format='%Y-%m-%d')

        if (detail_last_line_time_dt < info_last_line_time_dt):
            start_idx = np.where(info_t == detail_last_line_time_dt)[0][0] + 1
        else:
            return
    else:
        start_idx = 0

    path2 = os.path.join(future_price_dir, exchange, variety+'.csv')
    fut_df = pd.read_csv(path2, header=[0,1])
    fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))

    #
    # start_idx = np.where(info_t == detail_last_line_time_dt)[0][0]
    #
    opt_dict = {}
    cs = ['c9','c8','c7','c6','c5','c4','c3','c2','c1']
    for i in range(start_idx, len(info_t)):
        df = pd.DataFrame()
        df['time'] = ['']
        df['total_put_volume'] = [0]
        df['total_call_volume'] = [0]
        df['total_put_oi'] = [0]
        df['total_call_oi'] = [0]
        df['total_put_exercise'] = [0]
        df['total_call_exercise'] = [0]
        for col in info_cols:
            df[col] = [np.nan]
            df[col+'_'+'c_40d_put_iv'] = [np.nan]
            df[col+'_'+'c_40d_call_iv'] = [np.nan]
            df[col+'_'+'c_atm_put_iv'] = [np.nan]
            df[col+'_'+'c_atm_call_iv'] = [np.nan]
            df[col+'_'+'c_25d_put_iv'] = [np.nan]
            df[col+'_'+'c_25d_call_iv'] = [np.nan]
            df[col+'_'+'c_10d_put_iv'] = [np.nan]
            df[col+'_'+'c_10d_call_iv'] = [np.nan]
            df[col+'_'+'c_5d_put_iv'] = [np.nan]
            df[col+'_'+'c_5d_call_iv'] = [np.nan]
            df[col+'_'+'put_volume'] = [np.nan]
            df[col+'_'+'call_volume'] = [np.nan]
            df[col+'_'+'put_oi'] = [np.nan]
            df[col+'_'+'call_oi'] = [np.nan]
            df[col+'_'+'put_exercise'] = [np.nan]
            df[col+'_'+'call_exercise'] = [np.nan]

            df[col+'_'+'put_volume_max1'] = [np.nan]
            df[col+'_'+'put_volume_max1_strike'] = [np.nan]
            df[col+'_'+'put_volume_max2'] = [np.nan]
            df[col+'_'+'put_volume_max2_strike'] = [np.nan]
            df[col+'_'+'put_volume_max3'] = [np.nan]
            df[col+'_'+'put_volume_max3_strike'] = [np.nan]
            df[col+'_'+'put_volume_max4'] = [np.nan]
            df[col+'_'+'put_volume_max4_strike'] = [np.nan]
            df[col+'_'+'put_volume_max5'] = [np.nan]
            df[col+'_'+'put_volume_max5_strike'] = [np.nan]

            df[col+'_'+'call_volume_max1'] = [np.nan]
            df[col+'_'+'call_volume_max1_strike'] = [np.nan]
            df[col+'_'+'call_volume_max2'] = [np.nan]
            df[col+'_'+'call_volume_max2_strike'] = [np.nan]
            df[col+'_'+'call_volume_max3'] = [np.nan]
            df[col+'_'+'call_volume_max3_strike'] = [np.nan]
            df[col+'_'+'call_volume_max4'] = [np.nan]
            df[col+'_'+'call_volume_max4_strike'] = [np.nan]
            df[col+'_'+'call_volume_max5'] = [np.nan]
            df[col+'_'+'call_volume_max5_strike'] = [np.nan]

            df[col+'_'+'put_oi_max1'] = [np.nan]
            df[col+'_'+'put_oi_max1_strike'] = [np.nan]
            df[col+'_'+'put_oi_max2'] = [np.nan]
            df[col+'_'+'put_oi_max2_strike'] = [np.nan]
            df[col+'_'+'put_oi_max3'] = [np.nan]
            df[col+'_'+'put_oi_max3_strike'] = [np.nan]
            df[col+'_'+'put_oi_max4'] = [np.nan]
            df[col+'_'+'put_oi_max4_strike'] = [np.nan]
            df[col+'_'+'put_oi_max5'] = [np.nan]
            df[col+'_'+'put_oi_max5_strike'] = [np.nan]

            df[col+'_'+'call_oi_max1'] = [np.nan]
            df[col+'_'+'call_oi_max1_strike'] = [np.nan]
            df[col+'_'+'call_oi_max2'] = [np.nan]
            df[col+'_'+'call_oi_max2_strike'] = [np.nan]
            df[col+'_'+'call_oi_max3'] = [np.nan]
            df[col+'_'+'call_oi_max3_strike'] = [np.nan]
            df[col+'_'+'call_oi_max4'] = [np.nan]
            df[col+'_'+'call_oi_max4_strike'] = [np.nan]
            df[col+'_'+'call_oi_max5'] = [np.nan]
            df[col+'_'+'call_oi_max5_strike'] = [np.nan]


            inst_id = info_df.loc[i, col]
            if (inst_id == ''):
                continue
            if not(inst_id in opt_dict):
                try:
                    path3 = os.path.join(option_price_dir, exchange, inst_id+'.csv')
                except:
                    continue
                opt_df = pd.read_csv(path3, header=[0,1,2])
                opt_t = pd.DatetimeIndex(pd.to_datetime(opt_df['time']['time']['time'], format='%Y-%m-%d'))
                strike = get_full_strike_price(opt_df)
                opt_dict[inst_id] = [opt_df, opt_t, strike]

            opt_df = opt_dict[inst_id][0]
            opt_t = opt_dict[inst_id][1]
            strike = opt_dict[inst_id][2]

            fut_price = 0
            try:
                w = np.where(fut_t == info_t[i])[0][0]
            except:
                print('fut_t == info_t[i], ', inst_id, info_t[i])
                continue
            temp_df = fut_df.loc[w,:]
            for c in cs:
                if (temp_df[c]['inst_id'] == inst_id):
                    fut_price = temp_df[c]['close']
                    break

            try:
                w = np.where(opt_t == info_t[i])[0][0]
            except:
                print('opt_t == info_t[i], ', inst_id, info_t[i])
                continue
            temp_df = opt_df.loc[w,:]

            df['time'] = [info_t[i].strftime('%Y-%m-%d')]
            df[col] = [inst_id]

            # ret = c_25d_put_price, c_25d_put_iv, c_25d_call_price, c_25d_call_iv, c_atm_put_iv, c_atm_call_iv
            ret = put_call_delta_volatility(temp_df, 0.4, fut_price, strike)
            df[col+'_'+'c_40d_put_iv'] = [ret[1]]
            df[col+'_'+'c_40d_call_iv'] = [ret[3]]
            df[col+'_'+'c_atm_put_iv'] = [ret[4]]
            df[col+'_'+'c_atm_call_iv'] = [ret[5]]

            ret = put_call_delta_volatility(temp_df, 0.25, fut_price, strike)
            df[col+'_'+'c_25d_put_iv'] = [ret[1]]
            df[col+'_'+'c_25d_call_iv'] = [ret[3]]

            ret = put_call_delta_volatility(temp_df, 0.1, fut_price, strike)
            df[col+'_'+'c_10d_put_iv'] = [ret[1]]
            df[col+'_'+'c_10d_call_iv'] = [ret[3]]

            ret = put_call_delta_volatility(temp_df, 0.05, fut_price, strike)
            df[col+'_'+'c_5d_put_iv'] = [ret[1]]
            df[col+'_'+'c_5d_call_iv'] = [ret[3]]

            # volume 
            put_volume = temp_df.loc[pd.IndexSlice['P', :, 'volume']].sum()
            call_volume = temp_df.loc[pd.IndexSlice['C', :, 'volume']].sum()
            df[col+'_'+'put_volume'] = [put_volume]
            df[col+'_'+'call_volume'] = [call_volume]
            df['total_put_volume'] += put_volume
            df['total_call_volume'] += call_volume
            ## put volume sort
            tmp = temp_df.loc[pd.IndexSlice['P', :, 'volume']]
            P_volume = tmp.replace(np.nan, -1.0)
            idx = P_volume.index
            P_volume = np.array(P_volume)
            sort = np.argsort(P_volume)
            P_volume = P_volume[sort]
            idx = idx[sort]
            df[col+'_'+'put_volume_max1'] = [P_volume[-1]]
            df[col+'_'+'put_volume_max1_strike'] = [idx[-1]]
            df[col+'_'+'put_volume_max2'] = [P_volume[-2]]
            df[col+'_'+'put_volume_max2_strike'] = [idx[-2]]
            df[col+'_'+'put_volume_max3'] = [P_volume[-3]]
            df[col+'_'+'put_volume_max3_strike'] = [idx[-3]]
            df[col+'_'+'put_volume_max4'] = [P_volume[-4]]
            df[col+'_'+'put_volume_max4_strike'] = [idx[-4]]
            df[col+'_'+'put_volume_max5'] = [P_volume[-5]]
            df[col+'_'+'put_volume_max5_strike'] = [idx[-5]]
            ## call volume sort
            tmp = temp_df.loc[pd.IndexSlice['C', :, 'volume']]
            C_volume = tmp.replace(np.nan, -1.0)
            idx = C_volume.index
            C_volume = np.array(C_volume)
            sort = np.argsort(C_volume)
            C_volume = C_volume[sort]
            idx = idx[sort]
            df[col+'_'+'call_volume_max1'] = [C_volume[-1]]
            df[col+'_'+'call_volume_max1_strike'] = [idx[-1]]
            df[col+'_'+'call_volume_max2'] = [C_volume[-2]]
            df[col+'_'+'call_volume_max2_strike'] = [idx[-2]]
            df[col+'_'+'call_volume_max3'] = [C_volume[-3]]
            df[col+'_'+'call_volume_max3_strike'] = [idx[-3]]
            df[col+'_'+'call_volume_max4'] = [C_volume[-4]]
            df[col+'_'+'call_volume_max4_strike'] = [idx[-4]]
            df[col+'_'+'call_volume_max5'] = [C_volume[-5]]
            df[col+'_'+'call_volume_max5_strike'] = [idx[-5]]

            # oi
            put_oi = temp_df.loc[pd.IndexSlice['P', :, 'oi']].sum()
            call_oi = temp_df.loc[pd.IndexSlice['C', :, 'oi']].sum()
            df[col+'_'+'put_oi'] = [put_oi]
            df[col+'_'+'call_oi'] = [call_oi]
            df['total_put_oi'] += put_oi
            df['total_call_oi'] += call_oi
            ## put oi sort
            tmp = temp_df.loc[pd.IndexSlice['P', :, 'oi']]
            P_oi = tmp.replace(np.nan, -1.0)
            idx = P_oi.index
            P_oi = np.array(P_oi)
            sort = np.argsort(P_oi)
            P_oi = P_oi[sort]
            idx = idx[sort]
            df[col+'_'+'put_oi_max1'] = [P_oi[-1]]
            df[col+'_'+'put_oi_max1_strike'] = [idx[-1]]
            df[col+'_'+'put_oi_max2'] = [P_oi[-2]]
            df[col+'_'+'put_oi_max2_strike'] = [idx[-2]]
            df[col+'_'+'put_oi_max3'] = [P_oi[-3]]
            df[col+'_'+'put_oi_max3_strike'] = [idx[-3]]
            df[col+'_'+'put_oi_max4'] = [P_oi[-4]]
            df[col+'_'+'put_oi_max4_strike'] = [idx[-4]]
            df[col+'_'+'put_oi_max5'] = [P_oi[-5]]
            df[col+'_'+'put_oi_max5_strike'] = [idx[-5]]
            ## call oi sort
            tmp = temp_df.loc[pd.IndexSlice['C', :, 'oi']]
            C_oi = tmp.replace(np.nan, -1.0)
            idx = C_oi.index
            C_oi = np.array(C_oi)
            sort = np.argsort(C_oi)
            C_oi = C_oi[sort]
            idx = idx[sort]
            df[col+'_'+'call_oi_max1'] = [C_oi[-1]]
            df[col+'_'+'call_oi_max1_strike'] = [idx[-1]]
            df[col+'_'+'call_oi_max2'] = [C_oi[-2]]
            df[col+'_'+'call_oi_max2_strike'] = [idx[-2]]
            df[col+'_'+'call_oi_max3'] = [C_oi[-3]]
            df[col+'_'+'call_oi_max3_strike'] = [idx[-3]]
            df[col+'_'+'call_oi_max4'] = [C_oi[-4]]
            df[col+'_'+'call_oi_max4_strike'] = [idx[-4]]
            df[col+'_'+'call_oi_max5'] = [C_oi[-5]]
            df[col+'_'+'call_oi_max5_strike'] = [idx[-5]]


            # exercise
            put_exercise = temp_df.loc[pd.IndexSlice['P', :, 'exercise']].sum()
            call_exercise = temp_df.loc[pd.IndexSlice['C', :, 'exercise']].sum()
            df[col+'_'+'put_exercise'] = [put_exercise]
            df[col+'_'+'call_exercise'] = [call_exercise]
            df['total_put_exercise'] += put_exercise
            df['total_call_exercise'] += call_exercise

        path = os.path.join(option_price_dir, exchange, variety+'_info_detail'+'.csv')
        # print(df)
        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df.to_csv(path, encoding='utf-8', index=False)  


def update_all_option_info_detail(exchange):
    directory = os.path.join(option_price_dir, exchange)
    for _, _, files in os.walk(directory):
        for file in files:
            if (('info' in file) and (not('detail' in file)) and (not('intraday' in file))):
                w = file.find('_info')
                variety = file[:w]
                update_option_info_detail(exchange, variety)


##########################################################################################
########################################## STOCK #########################################
##########################################################################################


sss = {'铜':'cu', '铝':'al', '铅':'pb', '锌':'zn', '镍':'ni', '锡':'sn', '天然橡胶':'ru', 
        '纸浆(仓库)':'sp', '纸浆(厂库)':'sp', '纸浆仓库':'sp', '纸浆厂库':'sp', 
        '石油沥青(厂库)':'bu', '石油沥青(仓库)':'bu', '石油沥青厂库':'bu', '石油沥青仓库':'bu', 
        '燃料油':'fu', '螺纹钢(厂库)':'rb', '螺纹钢(仓库)':'rb', '螺纹钢厂库':'rb', '螺纹钢仓库':'rb',
        '白银':'ag', '黄金':'au', '热轧卷板':'hc', '不锈钢(仓库)':'ss', '不锈钢仓库':'ss', 
        '中质含硫原油':'sc', '20号胶':'nr', '铜(BC)':'bc'}

def get_shfe_future_stock_dict(temp_df, date, freq):  
    ret_dict = {}
    temp_df["VARNAME"] = (
        temp_df["VARNAME"].str.split(r"$", expand=True).iloc[:, 0]
    )

    for item in set(temp_df["VARNAME"]):
        if item in sss:
            variety = sss[item]
            zz = temp_df[temp_df["VARNAME"] == item].tail(n=1)
            if (freq == 'weekly'):
                d1 = zz['SPOTWGHTS'].iloc[0]
                if (d1 != ''):
                    data1 = float(d1)
                else:
                    data1 = 0

            d2 = zz['WRTWGHTS'].iloc[0]
            if (d2 != ''):
                data2 = float(d2)
            else:
                data2 = 0
            
            if not(variety in ret_dict):
                df = pd.DataFrame(columns=['time','小计','期货'])
                date_str = date[0:4]
                date_str += '-'
                date_str += date[4:6]
                date_str += '-'
                date_str += date[6:8]
                df.loc[0, 'time'] = date_str
                if (freq == 'weekly'):
                    df.loc[0, '小计'] = data1
                df.loc[0, '期货'] = data2
                ret_dict[variety] = df
            else:
                if (freq == 'weekly'):
                    ret_dict[variety]['小计'] = ret_dict[variety]['小计'] + data1
                ret_dict[variety]['期货'] = ret_dict[variety]['期货'] + data2

    return ret_dict


def get_future_stock(exchange, date):
    ret_dict = {}
    if exchange == 'shfe':
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"
        }
        weekly_url = f"http://www.shfe.com.cn/data/dailydata/{date}weeklystock.dat"
        daily_url = f"http://www.shfe.com.cn/data/dailydata/{date}dailystock.dat"

        r = requests.get(weekly_url, headers=headers)

        if (r.status_code == 200):
            # weekly
            data_json = r.json()
            temp_df = pd.DataFrame(data_json["o_cursor"])
            ret_dict = get_shfe_future_stock_dict(temp_df, date, 'weekly')

            try:
                temp_df = pd.DataFrame(data_json["o_cursorine"])
                ret_dict1 = get_shfe_future_stock_dict(temp_df, date, 'weekly')    
            except:
                ret_dict1 = {}
            ret_dict.update(ret_dict1)

            try:
                temp_df = pd.DataFrame(data_json["o_cursorfu"])
                ret_dict1 = get_shfe_future_stock_dict(temp_df, date, 'weekly')    
            except:
                ret_dict1 = {}
            ret_dict.update(ret_dict1)

        else:
            # daily
            r = requests.get(daily_url, headers=headers)
            if (r.status_code == 200):
                data_json = r.json()
                temp_df = pd.DataFrame(data_json["o_cursor"])
                ret_dict = get_shfe_future_stock_dict(temp_df, date, 'daily')

    elif exchange == 'dce':
        url = "http://www.dce.com.cn/publicweb/quotesdata/wbillWeeklyQuotes.html"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"
        }
        params = {
            "wbillWeeklyQuotes.variety": "all",
            "year": date[:4],
            "month": str(int(date[4:6]) - 1),
            "day": date[6:],
        }
        r = requests.get(url, params=params, headers=headers)
        temp_df = pd.read_html(r.text)[0]
        index_list = temp_df[
            temp_df.iloc[:, 0].str.contains("小计") == 1
        ].index.to_list()

        for inner_index in range(len(index_list)):
            df = pd.DataFrame(columns=['time','小计','期货'])
            date_str = date[0:4]
            date_str += '-'
            date_str += date[4:6]
            date_str += '-'
            date_str += date[6:8]
            df.loc[0, 'time'] = date_str
            df.loc[0, '期货'] = 0
            df.loc[0, '小计'] = temp_df.loc[index_list[inner_index], '今日仓单量']
            s = temp_df.loc[index_list[inner_index], '品种']
            s = s.strip('小计')
            variety = chinese_to_english(s)
            ret_dict[variety] = df

    elif exchange == 'czce':
        temp_df = receipt.get_czce_receipt_3(date)
        # print(temp_df)
        for i in range(len(temp_df)):
            df = pd.DataFrame(columns=['time','小计','期货'])
            date_str = date[0:4]
            date_str += '-'
            date_str += date[4:6]
            date_str += '-'
            date_str += date[6:8]
            df.loc[0, 'time'] = date_str
            variety = temp_df.loc[i, 'var']
            df.loc[0, '小计'] = temp_df.loc[i, 'receipt']
            df.loc[0, '期货'] = 0
            ret_dict[variety] = df       

    elif exchange == 'gfex':
        url = f"http://www.gfex.com.cn/u/interfacesWebTdWbillWeeklyQuotes/loadList"
        payload = {
            'gen_date': date,
        }
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Content-Length": "32",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Host": "www.gfex.com.cn",
            "Origin": "http://www.gfex.com.cn",
            "Pragma": "no-cache",
            "Referer": "http://www.gfex.com.cn/gfex/cdrb/hqsj_tjsj.shtml",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "content-type": "application/x-www-form-urlencoded"
        }
        r = requests.post(url, data=payload, headers=headers)   
        try:
            data_json = r.json()
            temp_df = pd.DataFrame(data_json['data'])
            temp_df = temp_df[temp_df['variety'].str.contains("小计")]
            temp_df = temp_df.reset_index(drop=True)
            for i in range(len(temp_df)):
                df = pd.DataFrame(columns=['time','小计','期货'])
                date_str = date[0:4]
                date_str += '-'
                date_str += date[4:6]
                date_str += '-'
                date_str += date[6:8]
                df.loc[0, 'time'] = date_str
                variety = temp_df.loc[i, 'varietyOrder']
                df.loc[0, '小计'] = temp_df.loc[i, 'wbillQty']
                df.loc[0, '期货'] = 0
                ret_dict[variety] = df  
        except Exception as e:
            print(e)

    return ret_dict

def get_all_future_stock(exchange, start_time):
    calendar = cons.get_calendar()

    data_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')
    current_time_dt = datetime.datetime.now()
    while data_time_dt <= current_time_dt:
        print(data_time_dt)
        # 获取的数据的时间
        data_time_str = data_time_dt.strftime('%Y%m%d')
        date = cons.convert_date(data_time_str)
        if date.strftime("%Y%m%d") not in calendar:
            data_time_dt += pd.Timedelta(days=1)
            continue

        ret_dict = get_future_stock(exchange, data_time_str)
        for variety in ret_dict:
            path = os.path.join(future_price_dir, exchange, variety+'_stock'+'.csv')

            if os.path.exists(path):
                ret_dict[variety].to_csv(path, mode='a', encoding='utf-8', index=False, header=None)
            else:
                ret_dict[variety].to_csv(path, encoding='utf-8', index=False)

        data_time_dt += pd.Timedelta(days=1)
        time.sleep(0.25)


def update_all_future_stock(exchange):
    print('UPDATE FUTURE STOCK ', exchange)
    if exchange == 'shfe':
        path = os.path.join(future_price_dir, exchange, 'cu_stock'+'.csv')
    if exchange == 'dce':
        path = os.path.join(future_price_dir, exchange, 'a_stock'+'.csv')
    if exchange == 'czce':
        path = os.path.join(future_price_dir, exchange, 'SR_stock'+'.csv')
    if exchange == 'gfex':
        path = os.path.join(future_price_dir, exchange, 'si_stock'+'.csv')

    # 最后一行的时间
    with open(path, 'rb') as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell() - 1  # 不算最后一个字符'\n'
        while pos > 0:
            pos -= 1
            f.seek(pos, os.SEEK_SET)
            if f.read(1) == b'\n':
                break
        last_line = f.readline().decode().strip()
    
    print('FUTURE STOCK LAST TIME: ', last_line[:10])
    data_time_dt = pd.to_datetime(last_line[:10], format='%Y-%m-%d')
    data_time_dt += pd.Timedelta(days=1)
    data_time_str = data_time_dt.strftime('%Y-%m-%d')

    get_all_future_stock(exchange, data_time_str)


##########################################################################################
##################################### SSE SZSE ETF OPTION #####################################
##########################################################################################


def sse_etf_put_call_delta_volatility(df, delta, price, strike):
    # CALL CLOSE
    tmp = df.loc[pd.IndexSlice['C', :, 'delta']]
    idx1, idx2, delta1, delta2 = column_index_delta(tmp, delta)
    iv = df.loc[pd.IndexSlice['C', :, 'imp_vol']]

    if delta1 == delta:
        c_25d_call_iv = iv[idx1]
    else:
        w1 = (delta2 - delta)/(delta2 - delta1)
        w2 = (delta - delta1)/(delta2 - delta1)
        c_25d_call_iv = w1*iv[idx1] + w2*iv[idx2]


    if (price > 1):
        idx1, idx2, price1, price2 = column_index_price(strike, price)
        if price1 == price:
            c_atm_call_iv = iv[idx1]
        else:
            w1 = (price2 - price)/(price2 - price1)
            w2 = (price - price1)/(price2 - price1)
            c_atm_call_iv = w1*iv[idx1] + w2*iv[idx2]

    else:
        c_atm_call_iv = np.nan


    ############################################################################
    delta = -delta
    # PUT CLOSE
    tmp = df.loc[pd.IndexSlice['P', :, 'delta']]
    idx1, idx2, delta1, delta2 = column_index_delta(tmp, delta)
    iv = df.loc[pd.IndexSlice['P', :, 'imp_vol']]

    if delta1 == delta:
        c_25d_put_iv = iv[idx1]
    else:
        w1 = (delta2 - delta)/(delta2 - delta1)
        w2 = (delta - delta1)/(delta2 - delta1)
        c_25d_put_iv = w1*iv[idx1] + w2*iv[idx2]


    if (price > 1):
        idx1, idx2, price1, price2 = column_index_price(strike, price)
        if price1 == price:
            c_atm_put_iv = iv[idx1]
        else:
            w1 = (price2 - price)/(price2 - price1)
            w2 = (price - price1)/(price2 - price1)
            c_atm_put_iv = w1*iv[idx1] + w2*iv[idx2]
    else:
        c_atm_put_iv = np.nan


    return [c_25d_put_iv,\
            c_25d_call_iv,\
            c_atm_put_iv,\
            c_atm_call_iv
            ]

def get_sse_etf_option_volume_oi(se, date):
    date_str = date[0:4]
    date_str += '-'
    date_str += date[4:6]
    date_str += '-'
    date_str += date[6:8]

    url = "http://query.sse.com.cn/commonQuery.do"
    params = {
        "isPagination": "false",
        "tradeDate": date,
        "sqlId": "COMMON_SSE_ZQPZ_YSP_QQ_SJTJ_MRTJ_CX",
        "contractSymbol": "",
        "_": str(int(time.time()*1000)),
    }
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Host": "query.sse.com.cn",
        "Pragma": "no-cache",
        "Referer": "http://www.sse.com.cn/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36",
    }
    try:
        r = se.get(url, params=params, headers=headers)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"])
        temp_df = temp_df.replace(',', '', regex=True)
    except:
        print('ERROR: get_sse_etf_option_volume_oi: ' + date)
        return

    name_dict = {'510050':'50ETF', '510300':'300ETF', '510500':'500ETF', '588000':'科创50', '588080':'科创板50'}
    c1 = ['time','total_put_volume','total_call_volume','total_put_oi','total_call_oi']
    for i in range(len(temp_df)):
        df = pd.DataFrame(columns=c1)
        df.loc[0,:] = [date_str, temp_df.loc[i, 'PUT_VOLUME'], temp_df.loc[i, 'CALL_VOLUME'], temp_df.loc[i, 'LEAVES_PUT_QTY'], temp_df.loc[i, 'LEAVES_CALL_QTY']]
        security_code = temp_df.loc[i, 'SECURITY_CODE']
        name = name_dict[security_code]

        path = os.path.join(option_price_dir, 'sse', name+'_option_volume_oi'+'.csv')

        if os.path.exists(path):
            df.to_csv(path, mode='a', encoding='utf-8', index=False, header=None)
        else:
            df.to_csv(path, encoding='utf-8', index=False)

def update_sse_etf_option_volume_oi():
    calendar = cons.get_calendar()
    earlist_time = '2017-01-01'
    path = os.path.join(option_price_dir, 'sse', '50ETF_option_volume_oi'+'.csv')
    data_time_str = get_last_line_time(path, 'SSE ETF OPTION VOLUME OI', earlist_time, 10, '%Y-%m-%d')
    data_time_dt = pd.to_datetime(data_time_str, format='%Y-%m-%d')
    start_time_dt = data_time_dt + pd.Timedelta(days=1)

    current_time_dt = datetime.datetime.now()
    se = requests.session()
    while start_time_dt <= current_time_dt:
        time.sleep(0.25)
        print(start_time_dt)
        # 获取的数据的时间
        start_time_str = start_time_dt.strftime('%Y-%m-%d')
        date = convert_date(start_time_str)

        if date.strftime("%Y%m%d") not in calendar:
            start_time_dt += pd.Timedelta(days=1)
            continue

        get_sse_etf_option_volume_oi(se, date.strftime("%Y%m%d"))
        start_time_dt += pd.Timedelta(days=1)


def get_sse_etf_option_greeks(date):
    # date = "20230714"
    date_str = date[0:4]
    date_str += '-'
    date_str += date[4:6]
    date_str += '-'
    date_str += date[6:8]
    # 希腊值
    try:
        df1 = ak.option_risk_indicator_sse(date=date)
    except:
        print('ERROR: get_sse_etf_option_greeks: ' + date)
        return
    # 成交量 持仓量
    # df2 = etf_option_volume_oi(date=date)

    ret_dict = {}
    variety_dict = {}
    full_name_dict = {}
    name_dict = {'510050':'50ETF', '510300':'300ETF', '510500':'500ETF', '588000':'科创50', '588080':'科创板50'}
    # columns1 = [['time','total_put_volume','total_call_volume','total_put_oi','total_call_oi'],
    #             ['time','total_put_volume','total_call_volume','total_put_oi','total_call_oi'],
    #             ['time','total_put_volume','total_call_volume','total_put_oi','total_call_oi']]
    columns1 = [['time'],
                ['time'],
                ['time']]
    for i in range(len(df1)):
        contract_id = df1.loc[i, 'CONTRACT_ID']
        contract_symbol = df1.loc[i, 'CONTRACT_SYMBOL']
        inst_id = contract_id[:6]
        name = name_dict[inst_id]

        opt_type = contract_id[6]
        if opt_type == '购':
            opt_type = 'C'
        if opt_type == '沽':
            opt_type = 'P'

        month = contract_id[7:11]
        inst_name = name + month
        w1 = contract_symbol.find('月')
        w2 = contract_symbol.find('A')
        if (w2 >= 0):
            strike = str(int(contract_symbol[w1+1:w2]))
        else:
            strike = str(int(contract_symbol[w1+1:]))
        full_name = inst_name + opt_type + strike
        if not(full_name in full_name_dict):
            full_name_dict[full_name] = 1
        else:
            continue

        if not(name in variety_dict):
            variety_dict[name] = [inst_name]
        else:
            if not(inst_name in variety_dict[name]):
                variety_dict[name] = variety_dict[name] + [inst_name]

        columns = [[opt_type,opt_type,opt_type,opt_type,opt_type,opt_type],
                [strike,strike,strike,strike,strike,strike],
                ['delta','theta','gamma','vega','rho','imp_vol']]

        temp_df = pd.DataFrame(columns=columns, data=[df1.loc[i, ['DELTA_VALUE','THETA_VALUE','GAMMA_VALUE','VEGA_VALUE','RHO_VALUE','IMPLC_VOLATLTY']].tolist()])
        # print(temp_df)
        if not(inst_name in ret_dict):
            df3 = pd.DataFrame(columns=columns1)
            df3.loc[0, ['time','time','time']] = date_str
            # k = np.where(inst_id_array == inst_id)[0][0]
            # df3['total_put_volume']['total_put_volume']['total_put_volume'] = [df2.loc[k, 'PUT_VOLUME']]
            # df3['total_call_volume']['total_call_volume']['total_call_volume'] = [df2.loc[k, 'CALL_VOLUME']]
            # df3['total_put_oi']['total_put_oi']['total_put_oi'] = [df2.loc[k, 'LEAVES_PUT_QTY']]
            # df3['total_call_oi']['total_call_oi']['total_call_oi'] = [df2.loc[k, 'LEAVES_CALL_QTY']]
            ret_dict[inst_name] = df3

        ret_dict[inst_name] = pd.concat([ret_dict[inst_name], temp_df], axis=1)

    # print(variety_dict)

    # 期权greeks
    for inst_name in ret_dict:
        path = os.path.join(option_price_dir, 'sse', inst_name+'.csv')

        if not(os.path.exists(path)):
            print('SSE ETF OPTION GREEKS CREATE ' + path)
            ret_dict[inst_name].to_csv(path, encoding='utf-8', index=False)
        else:
            df_old = pd.read_csv(path, header=[0,1,2])
            # df = pd.merge(df_old, df, how='outer')
            df_old = pd.concat([df_old, ret_dict[inst_name]], axis=0)
            df_old.to_csv(path, encoding='utf-8', index=False)

    # info
    for name in variety_dict:
        path = os.path.join(option_price_dir, 'sse', name+'_info'+'.csv')
        c1 = ['time','dom1','dom2','dom3','dom4']

        info_df = pd.DataFrame(columns=c1)
        print(name, variety_dict[name])
        if (len(variety_dict[name]) < 4):
            while(len(variety_dict[name]) < 4):
                variety_dict[name] = variety_dict[name] + ['']

        info_df.loc[0] = [date_str] + variety_dict[name]
        if os.path.exists(path):
            info_df.to_csv(path, mode='a', encoding='utf-8', index=False, header=None)
        else:
            info_df.to_csv(path, encoding='utf-8', index=False)


def get_sse_etf_price():
    # 上证50ETF
    df = ak.stock_zh_index_daily(symbol="sh510050")
    df.rename(columns={'date':'time'}, inplace=True)
    path = os.path.join(option_price_dir, 'sse', '50ETF'+'.csv')
    df.to_csv(path, encoding='utf-8', index=False)

    # 沪深300ETF
    df = ak.stock_zh_index_daily(symbol="sh510300")
    df.rename(columns={'date':'time'}, inplace=True)
    path = os.path.join(option_price_dir, 'sse', '300ETF'+'.csv')
    df.to_csv(path, encoding='utf-8', index=False)

    # 中证500ETF
    df = ak.stock_zh_index_daily(symbol="sh510500")
    df.rename(columns={'date':'time'}, inplace=True)
    path = os.path.join(option_price_dir, 'sse', '500ETF'+'.csv')
    df.to_csv(path, encoding='utf-8', index=False)

    # 科创50ETF
    df = ak.stock_zh_index_daily(symbol="sh588000")
    df.rename(columns={'date':'time'}, inplace=True)
    path = os.path.join(option_price_dir, 'sse', '科创50'+'.csv')
    df.to_csv(path, encoding='utf-8', index=False)

    # 科创版50ETF
    df = ak.stock_zh_index_daily(symbol="sh588080")
    df.rename(columns={'date':'time'}, inplace=True)
    path = os.path.join(option_price_dir, 'sse', '科创板50'+'.csv')
    df.to_csv(path, encoding='utf-8', index=False)

    # 创业板ETF
    df = ak.stock_zh_index_daily(symbol="sz159915")
    df.rename(columns={'date':'time'}, inplace=True)
    path = os.path.join(option_price_dir, 'szse', '创业板ETF'+'.csv')
    df.to_csv(path, encoding='utf-8', index=False)

    # 深证100ETF
    df = ak.stock_zh_index_daily(symbol="sz159901")
    df.rename(columns={'date':'time'}, inplace=True)
    path = os.path.join(option_price_dir, 'szse', '深证100ETF'+'.csv')
    df.to_csv(path, encoding='utf-8', index=False)

    # 沪深300ETF
    df = ak.stock_zh_index_daily(symbol="sz159919")
    df.rename(columns={'date':'time'}, inplace=True)
    path = os.path.join(option_price_dir, 'szse', '沪深300ETF'+'.csv')
    df.to_csv(path, encoding='utf-8', index=False)

    # 中证500ETF
    df = ak.stock_zh_index_daily(symbol="sz159922")
    df.rename(columns={'date':'time'}, inplace=True)
    path = os.path.join(option_price_dir, 'szse', '中证500ETF'+'.csv')
    df.to_csv(path, encoding='utf-8', index=False)

def update_sse_etf_option_greeks():
    calendar = cons.get_calendar()
    earlist_time = '2017-01-01'
    path = os.path.join(option_price_dir, 'sse', '50ETF_info'+'.csv')
    data_time_str = get_last_line_time(path, 'SSE ETF OPTION', earlist_time, 10, '%Y-%m-%d')
    data_time_dt = pd.to_datetime(data_time_str, format='%Y-%m-%d')
    start_time_dt = data_time_dt + pd.Timedelta(days=1)

    current_time_dt = datetime.datetime.now()
    while start_time_dt <= current_time_dt:
        print(start_time_dt)
        # 获取的数据的时间
        start_time_str = start_time_dt.strftime('%Y-%m-%d')
        date = convert_date(start_time_str)

        if date.strftime("%Y%m%d") not in calendar:
            start_time_dt += pd.Timedelta(days=1)
            continue

        get_sse_etf_option_greeks(date.strftime("%Y%m%d"))
        start_time_dt += pd.Timedelta(days=1)


def update_sse_etf_option_info_detail():
    directory = os.path.join(option_price_dir, 'sse')
    for _, _, files in os.walk(directory):
        for file in files:
            if (('info' in file) and (not('detail' in file)) and (not('intraday' in file)) and (not('volume' in file))):
                w = file.find('_info')
                variety = file[:w]

                path1 = os.path.join(option_price_dir, 'sse', variety+'_info'+'.csv')
                if not(os.path.exists(path1)):
                    return
                
                info_df = pd.read_csv(path1)
                info_t = pd.DatetimeIndex(pd.to_datetime(info_df['time'], format='%Y-%m-%d'))
                info_last_line_time_dt = info_t[len(info_t)-1]
                info_cols = info_df.columns.tolist()
                info_cols.remove('time')

                path = os.path.join(option_price_dir, 'sse', variety+'_info_detail'+'.csv')
                print(path)
                detail_last_line_time = get_last_line_time(path, '', None, 10, '%Y-%m-%d')
                if (detail_last_line_time is not None):
                    detail_last_line_time_dt = pd.to_datetime(detail_last_line_time, format='%Y-%m-%d')

                    if (detail_last_line_time_dt < info_last_line_time_dt):
                        start_time_dt = detail_last_line_time_dt + pd.Timedelta(days=1)
                        start_idx = np.where(info_t == detail_last_line_time_dt)[0][0] + 1
                    else:
                        continue
                else:
                    start_idx = 0

                path2 = os.path.join(option_price_dir, 'sse', variety+'.csv')
                fut_df = pd.read_csv(path2)
                fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time'], format='%Y-%m-%d'))

                path3 = os.path.join(option_price_dir, 'sse', variety+'_option_volume_oi'+'.csv')
                oi_df = pd.read_csv(path3)
                oi_t = pd.DatetimeIndex(pd.to_datetime(oi_df['time'], format='%Y-%m-%d'))

                opt_dict = {}
                for i in range(start_idx, len(info_t)):
                    df = pd.DataFrame()
                    df['time'] = ['']
                    try:
                        w = np.where(oi_t == info_t[i])[0][0]
                        df['total_put_volume'] = [oi_df.loc[w, 'total_put_volume']]
                        df['total_call_volume'] = [oi_df.loc[w, 'total_call_volume']]
                        df['total_put_oi'] = [oi_df.loc[w, 'total_put_oi']]
                        df['total_call_oi'] = [oi_df.loc[w, 'total_call_oi']]
                    except:
                        break

                    df['total_put_exercise'] = [0]
                    df['total_call_exercise'] = [0]
                    for col in info_cols:
                        inst_id = info_df.loc[i, col]
                        if (inst_id == ''):
                            df[col+'_'+'c_40d_put_iv'] = None
                            df[col+'_'+'c_40d_call_iv'] = None
                            df[col+'_'+'c_atm_put_iv'] = None
                            df[col+'_'+'c_atm_call_iv'] = None
                            df[col+'_'+'c_25d_put_iv'] = None
                            df[col+'_'+'c_25d_call_iv'] = None
                            df[col+'_'+'c_10d_put_iv'] = None
                            df[col+'_'+'c_10d_call_iv'] = None
                            df[col+'_'+'c_10d_put_iv'] = None
                            df[col+'_'+'c_10d_call_iv'] = None
                            continue
                        if not(inst_id in opt_dict):
                            try:
                                path3 = os.path.join(option_price_dir, 'sse', inst_id+'.csv')
                            except:
                                df[col+'_'+'c_40d_put_iv'] = None
                                df[col+'_'+'c_40d_call_iv'] = None
                                df[col+'_'+'c_atm_put_iv'] = None
                                df[col+'_'+'c_atm_call_iv'] = None
                                df[col+'_'+'c_25d_put_iv'] = None
                                df[col+'_'+'c_25d_call_iv'] = None
                                df[col+'_'+'c_10d_put_iv'] = None
                                df[col+'_'+'c_10d_call_iv'] = None
                                df[col+'_'+'c_10d_put_iv'] = None
                                df[col+'_'+'c_10d_call_iv'] = None
                                continue
                            opt_df = pd.read_csv(path3, header=[0,1,2])
                            opt_t = pd.DatetimeIndex(pd.to_datetime(opt_df['time']['time']['time'], format='%Y-%m-%d'))
                            srtike = get_full_strike_price(opt_df)
                            opt_dict[inst_id] = [opt_df, opt_t, srtike]

                        opt_df = opt_dict[inst_id][0]
                        opt_t = opt_dict[inst_id][1]
                        strike = opt_dict[inst_id][2]

                        fut_price = 0
                        try:
                            w = np.where(fut_t == info_t[i])[0][0]
                        except:
                            print('fut_t == info_t[i], ', inst_id, info_t[i])
                            df[col+'_'+'c_40d_put_iv'] = None
                            df[col+'_'+'c_40d_call_iv'] = None
                            df[col+'_'+'c_atm_put_iv'] = None
                            df[col+'_'+'c_atm_call_iv'] = None
                            df[col+'_'+'c_25d_put_iv'] = None
                            df[col+'_'+'c_25d_call_iv'] = None
                            df[col+'_'+'c_10d_put_iv'] = None
                            df[col+'_'+'c_10d_call_iv'] = None
                            df[col+'_'+'c_10d_put_iv'] = None
                            df[col+'_'+'c_10d_call_iv'] = None
                            continue
                        fut_price = fut_df.loc[w,'close']*1000

                        try:
                            w = np.where(opt_t == info_t[i])[0][0]
                        except:
                            print('opt_t == info_t[i], ', inst_id, info_t[i])
                            df[col+'_'+'c_40d_put_iv'] = None
                            df[col+'_'+'c_40d_call_iv'] = None
                            df[col+'_'+'c_atm_put_iv'] = None
                            df[col+'_'+'c_atm_call_iv'] = None
                            df[col+'_'+'c_25d_put_iv'] = None
                            df[col+'_'+'c_25d_call_iv'] = None
                            df[col+'_'+'c_10d_put_iv'] = None
                            df[col+'_'+'c_10d_call_iv'] = None
                            df[col+'_'+'c_10d_put_iv'] = None
                            df[col+'_'+'c_10d_call_iv'] = None
                            continue
                        temp_df = opt_df.loc[w,:]
                        
                        df['time'] = [info_t[i].strftime('%Y-%m-%d')]
                        df[col] = [inst_id]

                        # ret = c_25d_put_iv, c_25d_call_iv, c_atm_put_iv, c_atm_call_iv, 
                        ret = sse_etf_put_call_delta_volatility(temp_df, 0.4, fut_price, strike)
                        df[col+'_'+'c_40d_put_iv'] = [ret[0]]
                        df[col+'_'+'c_40d_call_iv'] = [ret[1]]
                        df[col+'_'+'c_atm_put_iv'] = [ret[2]]
                        df[col+'_'+'c_atm_call_iv'] = [ret[3]]

                        ret = sse_etf_put_call_delta_volatility(temp_df, 0.25, fut_price, strike)
                        df[col+'_'+'c_25d_put_iv'] = [ret[0]]
                        df[col+'_'+'c_25d_call_iv'] = [ret[1]]

                        ret = sse_etf_put_call_delta_volatility(temp_df, 0.1, fut_price, strike)
                        df[col+'_'+'c_10d_put_iv'] = [ret[0]]
                        df[col+'_'+'c_10d_call_iv'] = [ret[1]]

                    path = os.path.join(option_price_dir, 'sse', variety+'_info_detail'+'.csv')
                    # print(df)
                    if os.path.exists(path):
                        df.to_csv(path, mode='a', encoding='utf-8', index=False, header=None)
                    else:
                        df.to_csv(path, encoding='utf-8', index=False)  



def get_szse_etf_option_volume_oi(se, date):
    date_str = date[0:4]
    date_str += '-'
    date_str += date[4:6]
    date_str += '-'
    date_str += date[6:8]

    url_ = 'https://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=ysprdzb&TABKEY=tab1&txtQueryDate={}'

    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Host": "www.szse.cn",
        "Pragma": "no-cache",
        "Referer": "https://www.szse.cn/market/option/day/index.html",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36",
    }
    while (1):
        try:
            url = url_.format(date_str)
            r = se.get(url, verify=False, headers=headers, timeout=(10,10))
            data_json = r.json()
            temp_df = pd.DataFrame(data_json[0]["data"])
            temp_df = temp_df.replace(',', '', regex=True)
        except:
            print('ERROR: get_szse_etf_option_volume_oi: ' + date)
            time.sleep(5)

    name_dict = {'159901':'深证100ETF', '159915':'创业板ETF', '159919':'沪深300ETF', '159922':'中证500ETF'}
    c1 = ['time','total_put_volume','total_call_volume','total_put_oi','total_call_oi']
    for i in range(len(temp_df)):
        df = pd.DataFrame(columns=c1)
        df.loc[0,:] = [date_str, temp_df.loc[i, 'rpcjl'], temp_df.loc[i, 'rccjl'], temp_df.loc[i, 'wpcrphys'], temp_df.loc[i, 'wpcrchys']]
        security_code = temp_df.loc[i, 'bddm']
        name = name_dict[security_code]

        path = os.path.join(option_price_dir, 'szse', name+'_option_volume_oi'+'.csv')

        if os.path.exists(path):
            df.to_csv(path, mode='a', encoding='utf-8', index=False, header=None)
        else:
            df.to_csv(path, encoding='utf-8', index=False)

def update_szse_etf_option_volume_oi():
    calendar = cons.get_calendar()
    earlist_time = '2020-01-01'
    path = os.path.join(option_price_dir, 'szse', '沪深300ETF_option_volume_oi'+'.csv')
    data_time_str = get_last_line_time(path, 'SZSE ETF OPTION VOLUME OI', earlist_time, 10, '%Y-%m-%d')
    data_time_dt = pd.to_datetime(data_time_str, format='%Y-%m-%d')
    start_time_dt = data_time_dt + pd.Timedelta(days=1)

    current_time_dt = datetime.datetime.now()
    se = requests.session()
    while start_time_dt <= current_time_dt:
        time.sleep(0.25)
        print(start_time_dt)
        # 获取的数据的时间
        start_time_str = start_time_dt.strftime('%Y-%m-%d')
        date = convert_date(start_time_str)

        if date.strftime("%Y%m%d") not in calendar:
            start_time_dt += pd.Timedelta(days=1)
            continue

        get_szse_etf_option_volume_oi(se, date.strftime("%Y%m%d"))
        start_time_dt += pd.Timedelta(days=1)


if __name__=="__main__":
    # recalculate_all_greeks('shfe')
    # recalculate_all_greeks('cffex')
    # recalculate_all_greeks('dce')
    # recalculate_all_greeks('czce')
    # recalculate_all_greeks('gfex')

    # 库存
    update_all_future_stock('shfe')
    update_all_future_stock('dce')
    update_all_future_stock('czce')
    update_all_future_stock('gfex')


    # # # top20持仓增减量的总和 和 top20持仓量总和的变化不一样，因为每天top20的会员不一定一样
    # # # df['1']['top20']['long_open_interest_chg'] 计算的是 top20持仓增减量的总和
    update_all_future_position('shfe')
    update_all_future_position('cffex')
    update_all_future_position('czce')
    update_all_future_position('dce')
    # 个别机构的期货持仓
    update_all_institution_position()

    # 期权持仓
    update_all_option_position('dce')


    update_all_future_price('shfe')
    update_all_future_price('cffex')
    update_all_future_price('dce')
    update_all_future_price('czce')
    update_all_future_price('gfex')

    # 上海黄金交易所
    update_all_sge_data()


    # ###################### SPOT ######################
    # # # # get_all_future_spot_price('2017-01-01')
    update_all_future_spot_price()
    # 能源 化工 有色 钢铁 橡塑 纺织 建材 农副
    update_all_spot_price()
    # 花生 苹果
    update_other_spot_price('PK')
    update_other_spot_price('AP')
    # # 棉花
    # update_cotton_spot_price()
    # # 棉花加工品价格
    # update_cotton_process_production_price()
    # 开工率 毛利润
    update_moa_profit_production_data()
    # 生猪加工
    update_moa_pork_data()
    update_pork_price()
    # 黑色
    update_black_profit()
    update_black_data()
    ##################################################


    # # # get_all_option_price('cffex', '2020-01-01')
    # # # # get_all_option_price('shfe', '2020-01-21')
    # # # get_all_option_price('dce', '2020-10-01')
    # # # # get_all_option_price('gfex', '2022-12-22')
    # ## get_all_option_price('czce', '2022-08-26')

    # # 国内利率
    update_all_china_rate()

    update_all_option_price('cffex')
    update_all_option_price('shfe')
    update_all_option_price('dce')
    update_all_option_price('gfex')
    update_all_option_price('czce')

    update_all_option_info_detail('cffex')
    update_all_option_info_detail('shfe')
    update_all_option_info_detail('dce')
    update_all_option_info_detail('gfex')
    update_all_option_info_detail('czce')

    # # treasury
    update_all_us_rate()
    update_treasury_auction_data()

    # fed
    update_onrrp_data()

    # # japan government bond
    # update_jgb_rate()

    # lme 
    # update_all_lme_price()
    update_lme_option_data()
    update_lme_option_indicator()
    get_lme_future_stock()

    # daily ETF OPTION
    # update_sse_intraday_option_data()

    get_sse_etf_price()
    update_sse_etf_option_greeks()
    update_sse_etf_option_volume_oi()
    update_sse_etf_option_info_detail()
    ## update_szse_etf_option_volume_oi()


    # fx
    update_fx_data()

    # vix
    update_vix()

    # cfd
    update_commodity_cfd_daily_data()
    update_commodity_cfd_intraday_data()
    update_cn_commodity_cfd_intraday_data()
    update_usdcny_intraday()

    # LBMA
    update_lbma_price()
    update_lbma_vault_data()
    update_lbma_clearing_data()

    # HKMA
    update_hkma_data()

    # HKEX
    update_hkex_fut_opt_data()

    # SGX
    update_sgx_fut_opt_data()

    # NASDAQ
    update_all_nasdaq_etf_data()
    update_all_nasdaq_etf_option_data()


    pass
