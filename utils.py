from asyncio.windows_events import NULL
import os
import time
import math
import math as m
import requests
import scipy.stats as ss
import numpy as np
import pandas as pd
import datetime
import calendar
from bokeh.io import output_file, show
from bokeh.layouts import column, row, gridplot, layout
from bokeh.plotting import figure
from bokeh.models import LinearAxis, Range1d, VBar, DatetimeTickFormatter
from scipy.stats import linregress


cur_dir = os.path.dirname(__file__)
data_dir = os.path.join(cur_dir, 'data')
cme_data_dir = os.path.join(data_dir, 'cme_daily_bulletin')
black_dir = os.path.join(data_dir, 'black')
future_position_dir = os.path.join(data_dir, 'future_position')
option_position_dir = os.path.join(data_dir, 'option_position')
future_price_dir = os.path.join(data_dir, 'future_price')
option_price_dir = os.path.join(data_dir, 'option_price')
lme_price_dir = os.path.join(future_price_dir, 'lme')
msci_dir = os.path.join(data_dir, 'msci')
fx_dir = os.path.join(data_dir, 'fx')
cfd_dir = os.path.join(data_dir, 'cfd')
spot_dir = os.path.join(data_dir, 'spot')
hkma_dir = os.path.join(data_dir, 'hkma')
interest_rate_dir = os.path.join(data_dir, 'interest_rate')
pboc_dir = os.path.join(data_dir, 'pboc')
btc_dir = os.path.join(data_dir, 'btc')
nbs_dir = os.path.join(data_dir, 'nbs')
sge_dir = os.path.join(data_dir, 'sge')
treasury_auction_dir = os.path.join(data_dir, 'treasury_auction')
fed_dir = os.path.join(data_dir, 'fed')
safe_dir = os.path.join(data_dir, 'safe')
lg_bond_dir = os.path.join(data_dir, 'lg_bond')
lbma_dir = os.path.join(data_dir, 'lbma')


exchange_dict = {'cffex':["IH", "IF", "IC", "IM", "TS", "TF", "T", "TL"],
               'shfe':["cu", "au", "ag", "zn", "al", "ao", "pb", "ru", "br", "rb", "fu", "hc", "bu", "ni", "sn", "sp", "ss", "sc", "nr", "lu", "bc"],
               'dce':["a", "b", "c", "cs", "i", "j", "jd", "jm", "l", "m", "p", "pp", "v", "y", "eg", "eb", "pg", "lh"],
               'czce':['PX','SH', "CF", "CY", "SR", "TA", "OI", "MA", "FG", "RM", "SF", "SM", "AP", "CJ", "UR", "SA", "PF", "PK"],
               'gfex':["si", "SI", "lc", "LC"],
}

exchange_option_dict = {'cffex':["IH", "IF", "IM"],
                        'shfe':["cu", "au", "ag", "zn", "al", "ru", "br", "rb", "sc"],
                        'dce':["a", "b", "c", "i", "l", "m", "p", "pp", "v", "y", "eg", "eb", "pg"],
                        'czce':['PX', 'SH', "CF", "SR", "TA", "OI", "MA", "RM", "PK", 'AP', 'PF', 'SA', 'SM', 'SF', 'UR'],
                        'gfex':["si", "lc"],
}


# 20种颜色
many_colors = ['orange','blue','purple','crimson','darkgreen','khaki','deeppink',
                'cyan','darkgray','tomato','turquoise','yellow','yellowgreen','midnightblue','black',
                'teal','cornsilk','red','gold']

month_dict = {'JAN':'01','FEB':'02','MAR':'03','APR':'04','MAY':'05','JUN':'06',
              'JUL':'07','AUG':'08','SEP':'09','OCT':'10','NOV':'11','DEC':'12'}

character_month_dict = {'F':'01', 'G':'02', 'H':'03', 'J':'04', 'K':'05', 'M':'06',
                        'N':'07', 'Q':'08', 'U':'09', 'V':'10', 'X':'11', 'Z':'12'}

TOOLS="crosshair,pan,reset,wheel_zoom,box_zoom,save"
# TOOLS="hover,crosshair,pan,reset,wheel_zoom,box_zoom,save"

def chinese_to_english(chinese_var: str):
    """
    映射期货品种中文名称和英文缩写
    :param chinese_var: 期货品种中文名称
    :return: 对应的英文缩写
    """
    chinese_list = [
        '对二甲苯',
        '烧碱',
        "橡胶",
        "天然橡胶",
        "石油沥青",
        "沥青",
        "沥青仓库",
        "沥青(仓库)",
        "沥青厂库",
        "沥青(厂库)",
        "热轧卷板",
        "热轧板卷",
        "燃料油",
        "白银",
        "线材",
        "螺纹钢",
        "铅",
        "铜",
        "铝",
        "锌",
        "黄金",
        "钯金",
        "锡",
        "镍",
        "纸浆",
        "豆一",
        "大豆",
        "豆二",
        "胶合板",
        "玉米",
        "玉米淀粉",
        "聚乙烯",
        "LLDPE",
        "LDPE",
        "豆粕",
        "豆油",
        "大豆油",
        "棕榈油",
        "纤维板",
        "鸡蛋",
        "聚氯乙烯",
        "PVC",
        "聚丙烯",
        "PP",
        "焦炭",
        "焦煤",
        "铁矿石",
        "乙二醇",
        "强麦",
        "强筋小麦",
        " 强筋小麦",
        "硬冬白麦",
        "普麦",
        "硬白小麦",
        "硬白小麦（）",
        "皮棉",
        "棉花",
        "一号棉",
        "白糖",
        "PTA",
        "菜籽油",
        "菜油",
        "早籼稻",
        "早籼",
        "甲醇",
        "柴油",
        "玻璃",
        "油菜籽",
        "菜籽",
        "菜籽粕",
        "菜粕",
        "动力煤",
        "粳稻",
        "晚籼稻",
        "晚籼",
        "硅铁",
        "锰硅",
        "硬麦",
        "棉纱",
        "苹果",
        "原油",
        "中质含硫原油",
        "尿素",
        "20号胶",
        "苯乙烯",
        "不锈钢",
        "粳米",
        "20号胶20",
        "红枣",
        "不锈钢仓库",
        "纯碱",
        "液化石油气",
        "低硫燃料油",
        "纸浆仓库",
        "石油沥青厂库",
        "石油沥青仓库",
        "螺纹钢仓库",
        "螺纹钢厂库",
        "纸浆厂库",
        "低硫燃料油仓库",
        "低硫燃料油厂库",
        "短纤",
        '涤纶短纤',
        '生猪',
        '花生',
        '工业硅',
        '碳酸锂',
    ]
    english_list = [
        'PX',
        'SH',
        "ru",
        "ru",
        "bu",
        "bu",
        "bu",
        "bu",
        "bu",
        "bu",
        "hc",
        "hc",
        "fu",
        "ag",
        "wr",
        "rb",
        "pb",
        "cu",
        "al",
        "zn",
        "au",
        "au",
        "sn",
        "ni",
        "sp",
        "a",
        "a",
        "b",
        "bb",
        "c",
        "cs",
        "l",
        "l",
        "l",
        "m",
        "y",
        "y",
        "p",
        "fb",
        "jd",
        "v",
        "v",
        "pp",
        "pp",
        "j",
        "jm",
        "i",
        "eg",
        "WH",
        "WH",
        "WH",
        "PM",
        "PM",
        "PM",
        "PM",
        "CF",
        "CF",
        "CF",
        "SR",
        "TA",
        "OI",
        "OI",
        "RI",
        "ER",
        "MA",
        "MA",
        "FG",
        "RS",
        "RS",
        "RM",
        "RM",
        "ZC",
        "JR",
        "LR",
        "LR",
        "SF",
        "SM",
        "WT",
        "CY",
        "AP",
        "sc",
        "sc",
        "UR",
        "NR",
        "eb",
        "ss",
        "RR",
        "NR",
        "CJ",
        "ss",
        "SA",
        "pg",
        "lu",
        "sp",
        "bu",
        "bu",
        "rb",
        "rb",
        "sp",
        "lu",
        "lu",
        "PF",
        "PF",
        "lh",
        "PK",
        "si",
        "lc",
    ]
    pos = chinese_list.index(chinese_var)
    return english_list[pos]


def read_csv_data(path, cols):
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    d = {}
    for col in cols:
        d[col] = np.array(df[col], dtype=float)
    return t, d


########### FRED ##########
def update_fred_data(name_code, directory, earlist_time=None, time_format='%Y-%m-%d'):
    # 
    api_key = None
    if api_key is None:
        return
    se = requests.session()
    URL = 'https://api.stlouisfed.org/fred/series/observations?series_id={}&observation_start={}&api_key={}&file_type=json'

    if earlist_time is None:
        earlist_time = '1990-01-01'

    for name in name_code:
        print(name)
        path = os.path.join(directory, name+'.csv')
        if os.path.exists(path):
            old_df = pd.read_csv(path)
            start_time = old_df.loc[len(old_df)-1, 'time']
            cols = old_df.columns.tolist()
        else:
            old_df = pd.DataFrame()
            start_time = earlist_time
            cols = []

        start_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d') - pd.Timedelta(days=14)
        start_time = start_time_dt.strftime('%Y-%m-%d')

        codes = name_code[name]
        df = pd.DataFrame()
        new_df = pd.DataFrame()
        for i in range(len(codes)):
            code = codes[i][0]
            name = codes[i][1]

            while (1):
                try:
                    if name in cols:
                        url = URL.format(code, start_time, api_key)
                        print(name, start_time + '-')
                    else:
                        url = URL.format(code, earlist_time, api_key)
                        print(name, earlist_time + '-')

                    r = se.get(url, timeout=10)
                    data_json = r.json()
                    break
                except Exception as e:
                    print(e)
                    time.sleep(10)

            temp_df = pd.DataFrame(data_json['observations'])
            temp_df.rename(columns={'date':'time', 'value':name}, inplace=True)
            temp_df = temp_df[['time', name]]
            temp_df.replace('.', np.nan, inplace=True)
            if name in cols:
                if (df.empty):
                    df = temp_df.copy()
                else:
                    df = pd.merge(df, temp_df, on='time', how='outer')
            else:
                if (new_df.empty):
                    new_df = temp_df.copy()
                else:
                    new_df = pd.merge(new_df, temp_df, on='time', how='outer')

        #####
        if (len(df) > 0) and (len(new_df) == 0):
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format=time_format))
            old_df.sort_values(by = 'time', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,time_format))
            old_df.to_csv(path, encoding='utf-8', index=False)

        if (len(df) > 0) and (len(new_df) > 0):
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df = pd.merge(old_df, new_df, on='time', how='outer')
            old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format=time_format))
            old_df.sort_values(by = 'time', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,time_format))
            old_df.to_csv(path, encoding='utf-8', index=False)

        if (len(df) == 0) and (len(new_df) > 0):
            new_df.to_csv(path, encoding='utf-8', index=False)

########### FRED ##########

########### OPTION ##########
def column_index_price(data, price):
    L = len(data)
    # data[idx1] < 0.25
    # data[idx2] > 0.25
    _max = 999999
    _min = -1
    idx1 = -1
    idx2 = -1
    for i in range(L):
        if np.isnan(data[i]):
            continue
        if (data[i] <= 0.0):
            continue

        if (data[i] <= price and data[i] > _min):
            idx1 = i
            _min = data[i]

        if (data[i] >= price and data[i] < _max):
            idx2 = i
            _max = data[i]

    return idx1, idx2, _min, _max


def column_index_delta(data, delta):
    L = len(data)
    # data[idx1] < 0.25
    # data[idx2] > 0.25
    _max = 1
    _min = -1
    idx1 = -1
    idx2 = -1
    for i in range(L):
        if np.isnan(data[i]):
            continue
        if (data[i] == 0.0):
            continue

        if (data[i] <= delta and data[i] > _min):
            idx1 = i
            _min = data[i]

        if (data[i] >= delta and data[i] < _max):
            idx2 = i
            _max = data[i]

    return idx1, idx2, _min, _max


def call_bsm(S0, K, r, T, sqrt_T, Otype, sig):
    d1 = ((m.log(S0/K)) + (r+ (sig*sig)/2)*T)/(sig*sqrt_T)
    d2 = d1 - sig*sqrt_T
    if (Otype == "C"):
        price = S0*(ss.norm.cdf(d1)) \
        - K*(m.exp(-r*T))*(ss.norm.cdf(d2))
        return (price)
    elif (Otype == "P"):
        price  = -S0*(ss.norm.cdf(-d1))\
        + K*(m.exp(-r*T))*(ss.norm.cdf(-d2))
        return price

def vega(S0, K, r, T, sqrt_T, sig):
    d1 = ((m.log(S0/K)) + (r+ (sig*sig)/2)*T)/(sig*sqrt_T)
    vega = S0*(ss.norm.pdf(d1))*sqrt_T
    return vega
    
def implied_volatility(S0, K, T, r, price, Otype):
    e = 1e-3
    x0 = 1
    sqrt_T = m.sqrt(T)

    def newtons_method(S0, K, T, sqrt_T, r, price, Otype, x0, e):
        k=0
        delta = call_bsm(S0, K, r, T, sqrt_T, Otype, x0) - price
        while delta > e:
            k=k+1
            if (k > 30):
                return np.nan
            _vega = vega(S0, K, r, T, sqrt_T, x0)
            if (_vega == 0.0):
                return np.nan
            x0 = (x0 - (call_bsm(S0, K, r, T, sqrt_T, Otype, x0) - price)/_vega)
            delta = abs(call_bsm(S0, K, r, T, sqrt_T, Otype, x0) - price)
        return x0
    iv = newtons_method(S0, K, T, sqrt_T, r, price, Otype, x0, e)   
    return iv


def calculate_greeks(S0, K, T, r, price, Otype):
    if (np.isnan(S0) or np.isnan(K) or np.isnan(price) or S0==0.0 or price==0.0):
        return [np.nan, np.nan]
    
    if (Otype == 'C' and S0/K > 1.25):
        return [0, 0]

    if (Otype == 'P' and K/S0 > 1.25):
        return [0, 0]
    
    if (T < 0.25/365):
        return [np.nan, np.nan]

    sqrt_T = m.sqrt(T)

    # print(S0, K, T, r, price, Otype)

    # imp_vol
    iv = implied_volatility(S0, K, T, r, price, Otype)
    d1 = ((m.log(S0/K)) + (r + (iv*iv)/2)*T)/(iv*sqrt_T)

    # delta
    if Otype == 'C':
        delta = ss.norm.cdf(d1)
    else:
        delta = ss.norm.cdf(d1) - 1

    return [round(iv,5), round(delta,4)]

###############################

def get_last_line_time(path, data_name, earlist_time, time_length, time_format):
    if os.path.exists(path):
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

            try:
                last_line_dt = pd.to_datetime(last_line[:time_length], format=time_format)
                start_time = last_line_dt.strftime(time_format)
            except:
                start_time = earlist_time
            print(data_name + ' UPDATE ' + path + ' ' + start_time)
    else:
        print(data_name + ' CREATE ' + path)
        start_time = earlist_time

    return start_time

# 统计局城市月度数据
def get_cs_price_change_count(df, name, thres=100.0):
    hi_count = []
    eq_count = []
    lo_count = []
    for i in range(len(df)):
        # 环比上涨、持平、下跌城市个数
        change = np.array(df.loc[i, pd.IndexSlice[:, name]])
        hi = len(np.where(change > thres)[0])
        eq = len(np.where(change == thres)[0])
        lo = len(np.where(change < thres)[0])
        hi_count.append(hi)
        eq_count.append(eq)
        lo_count.append(lo)

    hi_count = np.array(hi_count)
    eq_count = np.array(eq_count)
    lo_count = np.array(lo_count)
    return hi_count, eq_count, lo_count


# 同比
#TODO
def yoy(time, data):
    idx = 0
    L = len(time)
    t2 = time[-1]
    t1 = t2 - pd.Timedelta(days=365)
    for i in range(L):
        if time[i] >= t1:
            idx = i
            break

    # print(idx)
    if ((t2 - time[idx]) >= pd.Timedelta(days=370)) or ((t2 - time[idx]) <= pd.Timedelta(days=360)):
        return NULL, NULL

    new_time = time[L-idx-1:]
    new_data = data[L-idx-1:] / data[0:idx+1] - 1
    return new_time, new_data

# 同比
def yoy_for_monthly_data(time, data):
    idx = 0
    L = len(time)
    t2 = time[-1]
    t1 = datetime.datetime(t2.year-1, t2.month, t2.day)
    for i in range(L):
        if time[i] == t1:
            idx = i
            break

    if (idx == 0):
        print('算不了同比')
        return NULL, NULL

    new_time = time[L-idx-1:]
    new_data = data[L-idx-1:] / data[0:idx+1] - 1
    return new_time, new_data

# 线性插值
def interpolate_nan(time, data):
    t = time[-1] - time[0]
    L = t.days
    new_time = pd.date_range(start=time[0], end=time[-1])

    x = np.linspace(0, L, L+1)
    idx = np.where(np.isnan(data)==False)[0]
    idx2 = np.zeros((len(idx)))
    for i in range(len(idx)):
        idx2[i] = np.where(new_time == time[idx[i]])[0]
    
    new_data = np.interp(x, idx2, data[idx])
    
    return new_time, new_data
    


def interpolate_season_to_month(time, data):
    new_time = np.empty((3*len(time)-2), dtype=type(time))
    new_data = np.empty((3*len(time)-2), dtype=type(data))

    for i in range(len(time)-1):
        # 时间
        new_time[i*3+0] = time[i]

        t = time[i] + pd.Timedelta(days=15)
        days_num = calendar.monthrange(t.year, t.month)[1]  # 获取当前月有多少天
        new_time[i*3+1] = new_time[i*3+0] + pd.Timedelta(days=days_num)

        t = time[i] + pd.Timedelta(days=45)
        days_num = calendar.monthrange(t.year, t.month)[1]  # 获取当前月有多少天
        new_time[i*3+2] = new_time[i*3+1] + pd.Timedelta(days=days_num)   

        # 数据
        new_data[i*3+0] = data[i]
        new_data[i*3+1] = 2/3 * data[i] + 1/3 * data[i+1]
        new_data[i*3+2] = 1/3 * data[i] + 2/3 * data[i+1]

    new_time[-1] = time[-1]
    new_data[-1] = data[-1]

    return pd.DatetimeIndex(new_time), new_data

def yyyymm_to_yyyymmdd(time):
    new_time = np.empty((len(time)), dtype=type(time))
    for i in range(len(time)):
        days_num = calendar.monthrange(time[i].year, time[i].month)[1]  # 获取当前月有多少天
        new_time[i] = time[i] + pd.Timedelta(days=(days_num-1))

    return new_time


def get_last_friday(year, month):
    last_day = calendar.monthrange(year, month)[-1]
    dt = datetime.datetime(year=year, month=month, day=last_day)
    while (1):
        weekday = dt.weekday()
        if weekday == 4: # friday
            break
        dt = dt - pd.Timedelta(days=1)
    return datetime.datetime(year=dt.year, month=dt.month, day=dt.day)


def get_month_last_day(year, month):
    last_day = calendar.monthrange(year, month)[-1]
    month_lasy_day_dt = datetime.datetime(year=year, month=month, day=last_day)
    return month_lasy_day_dt


def get_pre_month_last_day(year, month):
    if month == 1:
        last_day = calendar.monthrange(year-1, 12)[-1]
        pre_month_lasy_day_dt = datetime.datetime(year=year-1, month=12, day=last_day)
    else:
        last_day = calendar.monthrange(year, month-1)[-1]
        pre_month_lasy_day_dt = datetime.datetime(year=year, month=month-1, day=last_day)
    return pre_month_lasy_day_dt


# 截取时间从 start 到 end 的数据
def get_period_data(time, data, start, end='2099-01-01', remove_nan=False, format='%Y-%m-%d'):
    start_time = pd.to_datetime(start, format=format)
    end_time = pd.to_datetime(end, format=format)

    if len(time) == 0:
        return np.zeros(1), np.zeros(1)

    if remove_nan == False:
        idx = np.where((start_time <= time) & (time <= end_time))[0]
        return time[idx], data[idx]
    else:
        idx = np.where((start_time <= time) & (time <= end_time))[0]
        t = time[idx]
        d = data[idx]
        idx = np.logical_not(np.isnan(d))
        return t[idx], d[idx]

def moving_average(time, data, T):
    weights = np.ones(T)/T
    new_data = np.convolve(data, weights)[T-1:-T+1]
    new_time = time[T-1:]
    return new_time, new_data

def moving_std(time, data, T):
    L = len(time)
    new_data = np.empty((L), dtype=float)
    for i in range(T-1, L):
        new_data[i] = np.std(data[i-T+1:i])
    new_time = time[T-1:]
    new_data = new_data[T-1:]
    return new_time, new_data


def compare_two_data(datas, start_time='2010-01-01', end_time='2100-01-01'):
    datas_ = [[[datas[0]], [datas[1]], ''],]

    fig_list = plot_many_figure(datas_, max_height=400, start_time=start_time, end_time=end_time, ret=True)
    # 散点图
    fig2 = plot_circle(datas, start_time=start_time, end_time=end_time, ret=True)

    figs = fig_list + [fig2]
    show(column(figs))

def compare_two_option_data(datas, start_time, end_time='2100-01-01'):
    datas_ = [[[datas[0]], [datas[1]], ''],
              [[datas[2]], [datas[3]], ''],]

    fig_list = plot_many_figure(datas_, max_height=250, start_time=start_time, end_time=end_time, ret=True)
    # 散点图
    fig2 = plot_circle([datas[0], datas[1]], start_time=start_time, end_time=end_time, ret=True)
    fig3 = plot_circle([datas[2], datas[3]], start_time=start_time, end_time=end_time, ret=True)

    l = layout([[fig_list[0]], [fig_list[1]], [fig2,fig3]])
    show(l)


def get_full_strike_price(df):
    col = df.columns.tolist()

    put_strike = [(col[i][1]) for i in range(len(col)) if col[i][0] == 'P']
    # call_strike = [(col[i][1]) for i in range(len(col)) if col[i][0] == 'C']

    res = []
    for i in put_strike:
        if i not in res:
            res.append(i)
    put_strike = np.array(res, dtype=float)

    return put_strike


def plot_future_correlation(exchange1, variety1, exchange2, variety2, start_time='2022-01-01', end_time='2099-01-01'):
    path1 = os.path.join(future_price_dir, exchange1, variety1+'.csv')
    df1 = pd.read_csv(path1, header=[0,1])
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    price1 = np.array(df1['c2']['close'], dtype=float)
    w = np.where(price1 > 1)[0]
    t1 = t1[w]
    price1 = price1[w]

    path2 = os.path.join(future_price_dir, exchange2, variety2+'.csv')
    df2 = pd.read_csv(path2, header=[0,1])
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    price2 = np.array(df2['c2']['close'], dtype=float)
    w = np.where(price2 > 1)[0]
    t2 = t2[w]
    price2 = price2[w]

    t1, price1 = get_period_data(t1, price1, start_time, end_time)
    t2, price2 = get_period_data(t2, price2, start_time, end_time)
    idx1 = np.isin(t1, t2)
    idx2 = np.isin(t2, t1)
    t1 = t1[idx1]
    price1 = price1[idx1]
    t2 = t2[idx2]
    price2 = price2[idx2]

    n = 250
    slope, intercept, r, _, _ = linregress(price1[-n:], price2[-n:])
    fig1 = figure(frame_width=650, frame_height=650)
    fig1.circle(x=price1[-n:], y=price2[-n:], color="purple", legend_label='近一年'+', x = '+variety1+', y = '+variety2+', y = '+str(round(slope,3))+'*x +' + str(round(intercept,3))+', r^2 = ' + str(round(r*r,3)))
    yy = price1[-n:] * slope + intercept
    fig1.line(x=price1[-n:], y=yy, color="black")
    fig1.legend.location='top_left' 

    slope, intercept, r, _, _ = linregress(price1[-n*2:], price2[-n*2:])
    fig2 = figure(frame_width=650, frame_height=650)
    fig2.circle(x=price1[-n*2:], y=price2[-n*2:], color="purple", legend_label='近两年'+', x = '+variety1+', y = '+variety2+', y = '+str(round(slope,3))+'*x +' + str(round(intercept,3))+', r^2 = ' + str(round(r*r,3)))
    yy = price1[-n*2:] * slope + intercept
    fig2.line(x=price1[-n*2:], y=yy, color="black")
    fig2.legend.location='top_left' 

    show(row(fig1,fig2))
    pass


def plot_metal_stock(variety, name):
    path = os.path.join(data_dir, 'lme_stock'+'.csv')
    lme_df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(lme_df['time'], format='%Y-%m-%d'))

    lme_stock0 = np.array(lme_df[name+'-库存'], dtype=float)
    lme_stock1 = np.array(lme_df[name+'-注册仓单'], dtype=float)
    lme_stock2 = np.array(lme_df[name+'-注销仓单'], dtype=float)
    path = os.path.join(future_price_dir, 'shfe', variety+'_stock'+'.csv')
    stock_df = pd.read_csv(path)
    t2 = pd.DatetimeIndex(pd.to_datetime(stock_df['time'], format='%Y-%m-%d'))
    shfe_stock0 = np.array(stock_df['小计'], dtype=float)
    shfe_stock1 = np.array(stock_df['期货'], dtype=float)
    t3, stock_all = data_add(t1, lme_stock0, t2, shfe_stock1)

    path = path = os.path.join(lme_price_dir, variety+'.csv')
    df = pd.read_csv(path)
    t0 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    cash_bid = np.array(df['cash_bid'], dtype=float)
    cash_ask = np.array(df['cash_ask'], dtype=float)
    M3_bid = np.array(df['3M_bid'], dtype=float)
    M3_ask = np.array(df['3M_ask'], dtype=float)
    diff_03 = (cash_bid + cash_ask - M3_bid - M3_ask)/2

    datas = [[[[t0, cash_bid, name + ' cash bid',''],
               [t0, cash_ask, name + ' cash ask',''],
               [t0, M3_bid, name + ' 3M bid',''],
               [t0, M3_ask, name + ' 3M ask',''],],[],''],

            [[[t0, diff_03, name + ' 0-3价差',''],
               ],[],''],

            [[[t3, stock_all,'SHFE+LME 库存',''],
                        ],[],''],

            [[[t2,shfe_stock0,name+' 库存小计',''],
            [t2,shfe_stock1,name+' 库存期货',''],
            ],[],''],

            [[[t1,lme_stock0,'LME '+name+'-库存',''],
            [t1,lme_stock1,'LME '+name+'-注册仓单',''],
            [t1,lme_stock2,'LME '+name+'-注销仓单',''],
            ],[],''],
            ]
      
    plot_many_figure(datas, max_height=800)    


def plot_exchange_stock(exchange, variety):
    path1 = os.path.join(future_price_dir, exchange, variety+'_stock'+'.csv')
    if not(os.path.exists(path1)):
        return
    stock_df = pd.read_csv(path1)
    t1 = pd.DatetimeIndex(pd.to_datetime(stock_df['time'], format='%Y-%m-%d'))
    stock0 = np.array(stock_df['小计'], dtype=float)
    stock1 = np.array(stock_df['期货'], dtype=float)

    path2 = os.path.join(future_price_dir, exchange, variety+'.csv')
    fut_df = pd.read_csv(path2, header=[0,1])
    t2 = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    dominant_contract_price = np.array(fut_df['index']['close'])

    datas = [[[[t2, dominant_contract_price, variety+' 指数',''],
               ],[],''],
               
            [[[t1, stock0,'库存小计',''],[t1, stock1,'库存期货',''],
                        ],[],''],
            ]
    plot_many_figure(datas)  


# def plot_mean_std(t1, data1, name1, t2, data2, name2, T, start_time='2000-01-01', end_time='2099-01-01'):
def plot_mean_std(datas1, datas2, T, max_height=425, start_time='2000-01-01', end_time='2099-01-01', ret=False):
    time.sleep(0.25)
    L1 = len(datas1)
    fig_list = list()
    for i in range(L1):
        if (i==0):
            fig_list.append(figure(frame_width=1400, frame_height=max_height//L1, tools=TOOLS, x_axis_type = "datetime"))
        else:
            fig_list.append(figure(frame_width=1400, frame_height=max_height//L1, tools=TOOLS, x_range=fig_list[0].x_range, x_axis_type = "datetime"))

        t11, data11 = get_period_data(datas1[i][0], datas1[i][1], start_time, end_time, remove_nan=True)
        t3, mean = moving_average(t11, data11, T=T) 
        t3, std = moving_std(t11, data11, T=T)

        fig_list[i].y_range = Range1d(np.nanmin(data11) - abs(np.nanmin(data11))*0.05, np.nanmax(data11) + abs(np.nanmax(data11))*0.05)
        fig_list[i].line(t11, data11, line_width=2, line_color='black', legend_label=datas1[i][2])
        fig_list[i].line(t3, mean+2*std, line_width=2, color='orange', line_dash='dashed', legend_label='+2std')
        fig_list[i].line(t3, mean+1*std, line_width=2, color='yellow', line_dash='dashed', legend_label='+1std')
        fig_list[i].line(t3, mean+0*std, line_width=2, color='gray', line_dash='dashed', legend_label='mean')
        fig_list[i].line(t3, mean-1*std, line_width=2, color='lightblue', line_dash='dashed', legend_label='-1std')
        fig_list[i].line(t3, mean-2*std, line_width=2, color='blue', line_dash='dashed', legend_label='-2std')
        fig_list[i].xaxis[0].ticker.desired_num_ticks = 20
        fig_list[i].legend.location='top_left'

    L2 = len(datas2)
    fig2_list = list()
    for i in range(L2):
        fig2_list.append(figure(frame_width=1400, frame_height=200, tools=TOOLS, x_range=fig_list[0].x_range, x_axis_type = "datetime", y_axis_location="right"))
        t22, data22 = get_period_data(datas2[i][0], datas2[i][1], start_time, end_time, remove_nan=True)
        fig2_list[i].line(t22, data22, line_width=2, line_color='black', legend_label=datas2[i][2])
        fig2_list[i].xaxis[0].ticker.desired_num_ticks = 20
        fig2_list[i].legend.location='top_left'

    fig_list = fig_list + fig2_list
    if ret == False:
        show(column(fig_list))
    else:
        return fig_list


def plot_one_figure(datas, title=None, start_time='2000-01-01', end_time='2099-01-01'):
    time.sleep(0.5)
    L = len(datas)
    z0_list = list()

    fig = figure(frame_width=1400, frame_height=680, tools=TOOLS, title=title, x_axis_type = "datetime")
    for i in range(L):
        z0_list.append(get_period_data(datas[i][0], datas[i][1], start_time, end_time, remove_nan=True))
        fig.line(z0_list[i][0], z0_list[i][1], line_width=2, line_color=many_colors[i], legend_label=datas[i][2])

    fig.xaxis[0].ticker.desired_num_ticks = 20
    fig.legend.click_policy="hide"
    fig.legend.location='top_left'
    show(fig)


# def parse_string(s):
#     parsed_dict = {}
#     w1 = s.find('style=')
#     if (w1 >= 0):
#         w2 = s[w1:].find(',')
#         if (w2 >= 0):
#             r = s[w1+6:w2]
#         else:
#             r = s[w1+6:]
#         parsed_dict['style'] = r

#     w1 = s.find('color=')
#     if (w1 >= 0):
#         w2 = s[w1:].find(',')
#         if (w2 >= 0):
#             r = s[w1+6:w2]
#         else:
#             r = s[w1+6:]
#         parsed_dict['color'] = r

#     w1 = s.find('width=')
#     if (w1 >= 0):
#         w2 = s[w1:].find(',')
#         if (w2 >= 0):
#             r = s[w1+6:w2]
#         else:
#             r = s[w1+6:]
#         parsed_dict['width'] = r

#     w1 = s.find('visible=')
#     if (w1 >= 0):
#         w2 = s[w1:].find(',')
#         if (w2 >= 0):
#             r = s[w1+8:w2]
#         else:
#             r = s[w1+8:]

#         if r == 'True':
#             r = True
#         else:
#             r = False
#         parsed_dict['visible'] = r

#     return parsed_dict


def parse_string(s):
    s = s.strip()
    ss = s.split(',')

    parsed_dict = {}
    for opt in ss:
        z = opt.split('=')
        if len(z) == 2:
            if z[0] == 'visible':
                if z[1] == 'True':
                    z[1] = True
                else:
                    z[1] = False
            
            parsed_dict[z[0]] = z[1]

    return parsed_dict


def list_min_max(z):
    _min = 999999999
    _max = -999999999
    for i in range(len(z)):
        try:
            tmp = np.nanmax(z[i][1])
            if tmp > _max:
                _max = tmp

            tmp = np.nanmin(z[i][1])
            if tmp < _min:
                _min = tmp
        except:
            continue

    return _min, _max


def plot_daily_data_seasonality(t, data, name, start_time, end_time='2100-01-01'):
    t1, data1 = get_period_data(t, data, start=start_time, end=end_time, remove_nan=True)

    t_dict = {}
    for i in range(len(t1)):
        s = datetime.datetime.strftime(t1[i],'%m-%d')
        if not(s in t_dict):
            t_dict[s] = []
        t_dict[s].append(data1[i])

    # average
    for s in t_dict:
        t_dict[s] = sum(t_dict[s])/len(t_dict[s])

    t_list = list(t_dict.keys())
    data2 = []
    for i in range(len(t_list)):
        data2.append(t_dict[t_list[i]])
        # 2020年有02-29
        t_list[i] = pd.to_datetime('2020-' + t_list[i], format='%Y-%m-%d')

    t_array = np.array(t_list)
    data2 = np.array(data2)
    idx = np.argsort(t_array)
    
    datas = [[[[t_array[idx], data2[idx], name, '']],
            [],'']]
    plot_many_figure(datas)


def plot_many_figure(datas, max_height=660, start_time='1995-01-01', end_time='2100-01-01', ret=False, x_is_time=True):
    time.sleep(0.5)
    L1 = len(datas)
    fig_list = list()

    for i in range(L1):
        left_list = list()
        right_list = list()
        if x_is_time == True:
            if (i==0):
                fig_list.append(figure(frame_width=1400, frame_height=max_height//L1, tools=TOOLS, title=datas[i][2], x_axis_type = "datetime"))
            else:
                fig_list.append(figure(frame_width=1400, frame_height=max_height//L1, tools=TOOLS, title=datas[i][2], x_range=fig_list[0].x_range, x_axis_type = "datetime"))
        else:
            if (i==0):
                fig_list.append(figure(frame_width=1400, frame_height=max_height//L1, tools=TOOLS, title=datas[i][2]))
            else:
                fig_list.append(figure(frame_width=1400, frame_height=max_height//L1, tools=TOOLS, title=datas[i][2], x_range=fig_list[0].x_range))
        
        c = 0
        for k in range(len(datas[i][0])):
            if len(datas[i][0][k][0]) == 0:
                left_list.append('')
                continue

            if x_is_time == True:
                left_list.append(get_period_data(datas[i][0][k][0], datas[i][0][k][1], start_time, end_time, remove_nan=True))
            else:
                left_list.append((datas[i][0][k][0], datas[i][0][k][1]))
     
            parsed_dict = parse_string(datas[i][0][k][3])
            if 'width' in parsed_dict:
                width = int(parsed_dict['width'])
            else:
                width = 2

            if 'visible' in parsed_dict:
                visible = parsed_dict['visible']
            else:
                visible = True

            color = many_colors[c]
            if 'color' in parsed_dict:
                color = parsed_dict['color']

            if 'style' in parsed_dict:
                style = parsed_dict['style']
                if style == 'vbar':
                    fig_list[i].varea(x=left_list[k][0], y1=0, y2=left_list[k][1], fill_color='gray', visible=visible, legend_label=datas[i][0][k][2], level='underlay')
                if style == 'quad':
                    gap = np.nanmin(left_list[k][0][1:] - left_list[k][0][:-1])/5
                    fig_list[i].quad(left=left_list[k][0]-gap, right=left_list[k][0]+gap, bottom=0, top=left_list[k][1], fill_color='gray', visible=visible, legend_label=datas[i][0][k][2], level='underlay')
                if style == 'dot_line':
                    fig_list[i].line(left_list[k][0], left_list[k][1], line_width=width, line_color=color, visible=visible, legend_label=datas[i][0][k][2])
                    fig_list[i].circle(x=left_list[k][0], y=left_list[k][1], color=color, legend_label=datas[i][0][k][2])

            else:
                fig_list[i].line(left_list[k][0], left_list[k][1], line_width=width, line_color=color, visible=visible, legend_label=datas[i][0][k][2])
            c = c + 1

        if (len(datas[i][1]) > 0):
            if len(datas[i][1][0][0]) == 0:
                continue

            _min, _max = list_min_max(left_list)
            fig_list[i].y_range = Range1d(_min - abs(_max-_min)*0.05, _max + abs(_max-_min)*0.05)

            for k in range(len(datas[i][1])):
                if x_is_time == True:
                    right_list.append(get_period_data(datas[i][1][k][0], datas[i][1][k][1], start_time, end_time, remove_nan=True))
                else:
                    right_list.append((datas[i][1][k][0], datas[i][1][k][1]))
 
                if (k == 0):
                    y_column2_name = 'y2'

                parsed_dict = parse_string(datas[i][1][k][3])
                if 'width' in parsed_dict:
                    width = int(parsed_dict['width'])
                else:
                    width = 2

                if 'visible' in parsed_dict:
                    visible = parsed_dict['visible']
                else:
                    visible = True

                color = many_colors[c]
                if 'color' in parsed_dict:
                    color = parsed_dict['color']

                if 'style' in parsed_dict:
                    style = parsed_dict['style']
                    if style == 'vbar':
                        fig_list[i].varea(x=right_list[k][0], y1=0, y2=right_list[k][1], fill_color='gray', visible=visible, legend_label=datas[i][1][k][2], y_range_name='y2', level='underlay')
                    if style == 'quad':
                        gap = np.nanmin(right_list[k][0][1:] - right_list[k][0][:-1])/5
                        fig_list[i].quad(left=right_list[k][0]-gap, right=right_list[k][0]+gap, bottom=0, top=right_list[k][1], fill_color='gray', visible=visible, legend_label=datas[i][1][k][2], level='underlay')
                    if style == 'dot_line':
                        fig_list[i].line(right_list[k][0], right_list[k][1], line_width=width, line_color=color, visible=visible, legend_label=datas[i][1][k][2], y_range_name='y2')
                        fig_list[i].circle(x=right_list[k][0], y=right_list[k][1], color=color, legend_label=datas[i][1][k][2], y_range_name='y2')

                else:
                    fig_list[i].line(right_list[k][0], right_list[k][1], line_width=width, line_color=color, visible=visible, legend_label=datas[i][1][k][2], y_range_name='y2')
                c = c + 1

            _min, _max = list_min_max(right_list)
            fig_list[i].extra_y_ranges = {
                y_column2_name: Range1d(
                    start = _min - abs(_max-_min)*0.05,
                    end = _max + abs(_max-_min)*0.05,
                ),
            }

        if (len(datas[i][1]) > 0):
            fig_list[i].add_layout(LinearAxis(y_range_name="y2"), 'right')
        fig_list[i].xaxis[0].ticker.desired_num_ticks = 20
        fig_list[i].legend.click_policy="hide"
        fig_list[i].legend.location='top_left'

    if (ret == True):
        return fig_list
    else:
        show(column(fig_list))


def plot_two_axis(t_left, data_left, name_left, t_right, data_right, name_right, title, start_time, end_time):
    L = len(t_left)
    z0_list = list()
    z1_list = list()
    fig_list = list()
    for i in range(L):
        z0_list.append(get_period_data(t_left[i], data_left[i], start_time, end_time, remove_nan=True))
        if (len(t_right) > 0):
            z1_list.append(get_period_data(t_right[i], data_right[i], start_time, end_time, remove_nan=True))
        if (i==0):
            fig_list.append(figure(frame_width=1400, frame_height=700//L, tools=TOOLS, title=title[i], x_axis_type = "datetime"))
        else:
            fig_list.append(figure(frame_width=1400, frame_height=700//L, tools=TOOLS, title=title[i], x_range=fig_list[0].x_range, x_axis_type = "datetime"))
        fig_list[i].line(z0_list[i][0], z0_list[i][1], line_width=2, line_color='orange', legend_label=name_left[i])
        fig_list[i].y_range = Range1d(np.min(z0_list[i][1]) - abs(np.min(z0_list[i][1]))*0.05, np.max(z0_list[i][1]) + abs(np.max(z0_list[i][1]))*0.05)

        if (len(t_right) > 0):
            y_column2_name = 'y2'
            fig_list[i].extra_y_ranges = {
                y_column2_name: Range1d(
                    start=np.min(z1_list[i][1]) - abs(np.min(z1_list[i][1]))*0.05,
                    end=np.max(z1_list[i][1]) + abs(np.max(z1_list[i][1]))*0.05,
                ),
            }
            fig_list[i].line(z1_list[i][0], z1_list[i][1], line_width=2, color='blue', y_range_name=y_column2_name, legend_label=name_right[i])
            fig_list[i].add_layout(LinearAxis(y_range_name="y2"), 'right')
            fig_list[i].xaxis[0].ticker.desired_num_ticks = 20
            fig_list[i].legend.click_policy="hide"
            fig_list[i].legend.location='top_left'

    show(column(fig_list))


# 基差
def plot_basis(exchange, variety, adjust=1, start_time='2005-01-01', end_time='2099-01-01'):
    path1 = os.path.join(future_price_dir, exchange, variety+'.csv')
    df1 = pd.read_csv(path1, header=[0,1])
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    index_close = np.array(df1.loc[:, pd.IndexSlice['index', 'close']], dtype=float)
    c1_close = np.array(df1.loc[:, pd.IndexSlice['c1', 'close']], dtype=float)
    c2_close = np.array(df1.loc[:, pd.IndexSlice['c2', 'close']], dtype=float)
    c3_close = np.array(df1.loc[:, pd.IndexSlice['c3', 'close']], dtype=float)

    try:
        path2 = os.path.join(future_price_dir, exchange, variety+'_spot'+'.csv')
        df2 = pd.read_csv(path2)
        t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
        dom = np.array(df2['dominant_contract_price'], dtype=float)
        spot = np.array(df2['spot_price'], dtype=float)*adjust # 现货单位 -> 期货单位
    except:
        return

    name = variety
    datas = [[[[t1,c1_close,name+' c1'],[t2,spot,name+' 现货']],[],''],
             [[[t1,c2_close,name+' c2'],[t2,spot,name+' 现货']],[],''],
             [[[t1,c3_close,name+' c3'],[t2,spot,name+' 现货']],[],''],
             [[[t2,dom,name+' 主力'],[t2,spot,name+' 现货']],[],''],
             [[[t1,index_close,name+' 指数'],[t2,spot,name+' 现货']],[],'']]

    L1 = len(datas)
    fig_list = list()

    for i in range(L1):
        left_list = list()
        if (i==0):
            fig_list.append(figure(frame_width=1400, frame_height=700//L1, tools=TOOLS, title=datas[i][2], x_axis_type = "datetime"))
        else:
            fig_list.append(figure(frame_width=1400, frame_height=700//L1, tools=TOOLS, title=datas[i][2], x_range=fig_list[0].x_range, x_axis_type = "datetime"))


        t, v = data_sub(datas[i][0][1][0], datas[i][0][1][1], datas[i][0][0][0], datas[i][0][0][1])
        y_column2_name = 'y2'
        fig_list[i].extra_y_ranges = {
            y_column2_name: Range1d(
                start=np.nanmin(v) - abs(np.nanmin(v))*0.05,
                end=np.nanmax(v) + abs(np.nanmax(v))*0.05,
            ),
        }
        fig_list[i].varea(x=t, y1=0, y2=v, fill_color='gray', legend_label='现货-期货', y_range_name='y2')

        c = 0
        for k in range(len(datas[i][0])):
            left_list.append(get_period_data(datas[i][0][k][0], datas[i][0][k][1], start_time, end_time, remove_nan=True))
            fig_list[i].line(left_list[k][0], left_list[k][1], line_width=2, line_color=many_colors[c], legend_label=datas[i][0][k][2])
            c = c + 1
        fig_list[i].y_range = Range1d(np.nanmin(left_list[0][1]) - abs(np.nanmin(left_list[0][1]))*0.05, np.nanmax(left_list[1][1]) + abs(np.nanmax(left_list[1][1]))*0.05)

        fig_list[i].add_layout(LinearAxis(y_range_name="y2"), 'right')
        fig_list[i].xaxis[0].ticker.desired_num_ticks = 20
        fig_list[i].legend.click_policy="hide"
        fig_list[i].legend.location='top_left'

    show(column(fig_list))


# 画季节图
def plot_seasonality(t, data, start_year=None, end_year=2100, title=''):
    time.sleep(0.1)
    if start_year == None:
        start_year = t[0].year
    fig1 = figure(frame_width=1400, frame_height=600, tools=TOOLS, x_axis_type = "datetime", title=title)
    i = 0

    start_time = datetime.datetime(start_year, 1, 1).strftime('%Y-%m-%d')
    end_time = datetime.datetime(end_year+1, 1, 1).strftime('%Y-%m-%d')
    time_, data_ = get_period_data(t, data, start_time, end_time, remove_nan=True)

    end_year = time_[-1].year
    start_year = end_year
    while (1):
        start_time = datetime.datetime(start_year, 1, 1)
        end_time = datetime.datetime(start_year+1, 1, 1)
        year_idx = np.where((start_time <= time_) & (time_ < end_time))[0]
        # print(start_time, len(year_idx))
        if len(year_idx) == 0:
            if (start_time < time_[0]):
                break
            else:
                start_year -= 1
                continue

        t = []
        for k in range(len(year_idx)):
            t.append(time_[year_idx][k].replace(year=2000))
        # print(data[year_idx])

        if start_year == end_year:
            # last year
            last_year_idx = year_idx.copy()
            last_year_t = t.copy()
        else:
            fig1.line(t, data_[year_idx], line_width=1, color=many_colors[i], legend_label=str(start_year))
            fig1.circle(x=t, y=data_[year_idx], color=many_colors[i], legend_label=str(start_year))
            i += 1

        fig1.line(last_year_t, data_[last_year_idx], line_width=3, color='black', legend_label=str(end_year))
        fig1.circle(x=last_year_t, y=data_[last_year_idx], color='black', legend_label=str(end_year))

        start_year -= 1

    fig1.xaxis[0].ticker.desired_num_ticks = 15
    fig1.legend.click_policy="hide"
    fig1.legend.location='top_left' 
    fig1.left[0].formatter.use_scientific = False
    fig1.xaxis.formatter = DatetimeTickFormatter(months=["%m"])
    show(fig1)


def plot_candle(t, o, h, l, c, ret=False):
    inc = c >= o
    dec = o > c
    w = 12*60*60*1000 # half day in ms

    TOOLS = "pan,wheel_zoom,box_zoom,reset,save"

    p = figure(x_axis_type="datetime", tools=TOOLS, frame_width=1400, frame_height=220, title = "")
    p.xaxis.major_label_orientation = math.pi/4
    p.grid.grid_line_alpha=0.3

    p.segment(t, h, t, l, color="black")
    p.vbar(t[inc], w, o[inc], c[inc], fill_color="green", line_color="black")
    p.vbar(t[dec], w, o[dec], c[dec], fill_color="red", line_color="black")
    p.xaxis[0].ticker.desired_num_ticks = 30
    
    if ret == False:
        show(p)
    else:
        return p


def plot_position(t00, data00, data00_name, t01=None, data01=None, data01_name=None, \
                  ts=[], longs=[], shorts=[], names=[], period=52):
    if (t01 is None):
        fig1 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right", toolbar_location='right')
        fig1.line(t00, data00, color='black', line_width=2, legend_label=data00_name)
        fig1.xaxis[0].ticker.desired_num_ticks = 20
    else:
        fig1 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right", toolbar_location='right')
        fig1.y_range = Range1d(np.min(data00)*0.9, np.max(data00)*1.1)
        fig1.line(t00, data00, line_width=2, color='black', legend_label=data00_name)
        fig1.xaxis[0].ticker.desired_num_ticks = 20
        y_column2_name = 'y2'
        fig1.extra_y_ranges = {
            y_column2_name: Range1d(
                start=np.min(data01)*0.9,
                end=np.max(data01)*1.1,
            ),
        }
        fig1.line(t01, data01, line_width=2, color='blue', y_range_name=y_column2_name, legend_label=data01_name)
        fig1.add_layout(LinearAxis(y_range_name="y2"), 'left')
    fig1.legend.click_policy="hide"
    fig1.legend.location='top_left'

    fig_list = list()
    all_zero = False
    for i in range(len(ts)):
        all_zero = (shorts[i] == 0).all()
        pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, ts[i], longs[i]-shorts[i], period)
        fig_list.append(figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime"))
        if not all_zero:
            fig_list[i].varea(x=ts[i], y1=0, y2=longs[i]-shorts[i], fill_color='gray', legend_label=names[i]+' 净多头')
        else:
            fig_list[i].varea(x=ts[i], y1=0, y2=longs[i], fill_color='gray', legend_label='总持仓')

        fig_list[i].vbar(x=ts[i], bottom=0, top=longs[i]-shorts[i], width=0.05, color='dimgray')
        fig_list[i].varea(x=ts[i], y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
        fig_list[i].varea(x=ts[i], y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
        if not all_zero:
            fig_list[i].line(ts[i], longs[i], line_width=2, color='red', legend_label=names[i]+' 多头')
            fig_list[i].line(ts[i], shorts[i], line_width=2, color='green', legend_label=names[i]+' 空头')
        
        fig_list[i].xaxis[0].ticker.desired_num_ticks = 20
        fig_list[i].legend.location='top_left'

    fig_list = [fig1] + fig_list
    show(column(fig_list))


def plot_many_position(fut):
    for i in range(len(fut)):
        if (fut[i][1] == 'sc'):
            continue

        try:
            path = os.path.join(future_position_dir, fut[i][0], fut[i][1]+'.csv')
            df = pd.read_csv(path, header=[0,1,2]).fillna('0')
            df.drop_duplicates(inplace=True)
            t1 = pd.DatetimeIndex(pd.to_datetime(df['time']['time']['time'], format='%Y-%m-%d'))
        except:
            return
        
        if (fut[i][0] == 'czce'):
            s = ['1']
        else:
            s = ['1','2','3','4','5']
        top5_L = np.array(df.loc[:, pd.IndexSlice[s, 'top5', 'long_open_interest']], dtype=float)
        top5_S = np.array(df.loc[:, pd.IndexSlice[s, 'top5', 'short_open_interest']], dtype=float)
        top10_L = np.array(df.loc[:, pd.IndexSlice[s, 'top10', 'long_open_interest']], dtype=float)
        top10_S = np.array(df.loc[:, pd.IndexSlice[s, 'top10', 'short_open_interest']], dtype=float)
        top15_L = np.array(df.loc[:, pd.IndexSlice[s, 'top15', 'long_open_interest']], dtype=float)
        top15_S = np.array(df.loc[:, pd.IndexSlice[s, 'top15', 'short_open_interest']], dtype=float)
        top20_L = np.array(df.loc[:, pd.IndexSlice[s, 'top20', 'long_open_interest']], dtype=float)
        top20_S = np.array(df.loc[:, pd.IndexSlice[s, 'top20', 'short_open_interest']], dtype=float)

        top5_L_sum = np.sum(top5_L, axis=1)
        top5_S_sum = np.sum(top5_S, axis=1)
        top10_L_sum = np.sum(top10_L, axis=1)
        top10_S_sum = np.sum(top10_S, axis=1)
        top15_L_sum = np.sum(top15_L, axis=1)
        top15_S_sum = np.sum(top15_S, axis=1)
        top20_L_sum = np.sum(top20_L, axis=1)
        top20_S_sum = np.sum(top20_S, axis=1)

        path2 = os.path.join(future_price_dir, fut[i][0], fut[i][1]+'.csv')
        fut_df = pd.read_csv(path2, header=[0,1])
        t2 = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
        price_cny = np.array(fut_df['index']['close'], dtype=float)
        volume = np.array(fut_df['index']['vol'], dtype=float)
        oi = np.array(fut_df['index']['oi'], dtype=float)
        zeros = np.zeros_like(oi)

        time.sleep(0.25)
        ts = [t2, t1, t1, t1, t1]
        longs = [oi, top20_L_sum, top5_L_sum, top15_L_sum, top10_L_sum]
        shorts = [zeros, top20_S_sum, top5_S_sum, top15_S_sum, top10_S_sum]
        names = ['', 'top20', 'top5', 'top15', 'top10']
        plot_position(t2, price_cny, '指数:'+fut[i][2], ts=ts, longs=longs, shorts=shorts, names=names, period=250)

        time.sleep(0.25)
        datas1 = [[t1, top20_L_sum-top20_S_sum, 'top20 net long'],
                  [t1, top10_L_sum-top10_S_sum, 'top10 net long'],
                  [t1, top5_L_sum-top5_S_sum, 'top5 net long']]
        datas2 = [[t2, price_cny, '指数:'+fut[i][2]],
                  [t2, oi, '总持仓:'+fut[i][2]],
                  [t2, volume, '总成交量:'+fut[i][2]],]
        plot_mean_std(datas1, datas2, T=int(250*2))

        # _, top5_L_pct = data_div(t1, top5_L_sum, t2, oi)
        # _, top5_S_pct = data_div(t1, top5_S_sum, t2, oi)
        # _, top20_L_pct = data_div(t1, top20_L_sum, t2, oi)
        # t3, top20_S_pct = data_div(t1, top20_S_sum, t2, oi)
    
        # time.sleep(0.25)
        # datas1 = [[t3, top20_L_pct, 'top20 long / total oi'],
        #           [t3, top20_S_pct, 'top20 short / total oi'],
        #           [t3, top5_L_pct, 'top5 long / total oi'],
        #           [t3, top5_S_pct, 'top5 short / total oi']]
        # datas2 = [[t2, price_cny, '指数:'+fut[i][2]]]
        # plot_mean_std(datas1, datas2, T=int(250*2))


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

            k = k + 1

    if (start == 0 and end == 0):
        return None, None, None, None, None

    t = t1[start:end+1]
    # print(t)
    # print(len(t), len(open))
    return t, open, high, low, close


# data1 - data2
def data_sub(time1, data1, time2, data2, replace=np.nan):
    if np.isnan(replace):
        idx1 = np.logical_not(np.isnan(data1))
        t1 = time1[idx1]
        d1 = data1[idx1]
        idx2 = np.logical_not(np.isnan(data2))
        t2 = time2[idx2]
        d2 = data2[idx2]
    else:
        idx1 = np.isnan(data1)
        t1 = time1.copy()
        d1 = data1.copy()
        d1[idx1] = replace
        idx2 = np.isnan(data2)
        t2 = time2.copy()
        d2 = data2.copy()
        d2[idx2] = replace

    idx1 = np.isin(t1, t2)
    idx2 = np.isin(t2, t1)
    t1 = t1[idx1]
    d1 = d1[idx1]
    d2 = d2[idx2]

    return t1, (d1-d2)

# data1 + data2
def data_add(time1, data1, time2, data2, replace=np.nan):
    if np.isnan(replace):
        idx1 = np.logical_not(np.isnan(data1))
        t1 = time1[idx1]
        d1 = data1[idx1]
        idx2 = np.logical_not(np.isnan(data2))
        t2 = time2[idx2]
        d2 = data2[idx2]
    else:
        idx1 = np.isnan(data1)
        t1 = time1.copy()
        d1 = data1.copy()
        d1[idx1] = replace
        idx2 = np.isnan(data2)
        t2 = time2.copy()
        d2 = data2.copy()
        d2[idx2] = replace

    idx1 = np.isin(t1, t2)
    idx2 = np.isin(t2, t1)
    t1 = t1[idx1]
    d1 = d1[idx1]
    d2 = d2[idx2]

    return t1, (d1+d2)

# data1 * data2
def data_mul(time1, data1, time2, data2, replace=np.nan):
    if np.isnan(replace):
        idx1 = np.logical_not(np.isnan(data1))
        t1 = time1[idx1]
        d1 = data1[idx1]
        idx2 = np.logical_not(np.isnan(data2))
        t2 = time2[idx2]
        d2 = data2[idx2]
    else:
        idx1 = np.isnan(data1)
        t1 = time1.copy()
        d1 = data1.copy()
        d1[idx1] = replace
        idx2 = np.isnan(data2)
        t2 = time2.copy()
        d2 = data2.copy()
        d2[idx2] = replace

    idx1 = np.isin(t1, t2)
    idx2 = np.isin(t2, t1)
    t1 = t1[idx1]
    d1 = d1[idx1]
    d2 = d2[idx2]

    return t1, (d1*d2)

# data1 / data2
def data_div(time1, data1, time2, data2, replace=np.nan):
    if np.isnan(replace):
        idx1 = np.logical_not(np.isnan(data1))
        t1 = time1[idx1]
        d1 = data1[idx1]
        idx2 = np.logical_not(np.isnan(data2))
        t2 = time2[idx2]
        d2 = data2[idx2]
    else:
        idx1 = np.isnan(data1)
        t1 = time1.copy()
        d1 = data1.copy()
        d1[idx1] = replace
        idx2 = np.isnan(data2)
        t2 = time2.copy()
        d2 = data2.copy()
        d2[idx2] = replace

    idx1 = np.isin(t1, t2)
    idx2 = np.isin(t2, t1)
    t1 = t1[idx1]
    d1 = d1[idx1]
    d2 = d2[idx2]

    return t1, (d1/d2)

def fill_data(time, data):
    L = len(data)
    d = np.nan
    for i in range(L):
        if np.isnan(data[i]):
            data[i] = d
        else:
            d = data[i]

    return time, data

def fill_month_daily_avg_data(time, data):
    L = len(data)
    new_data = data.copy()
    start = 0
    for i in range(L):
        if np.isnan(new_data[i]):
            pass
        else:
            days = calendar.monthrange(time[i].year, time[i].month)[1]
            new_data[start:i+1] = new_data[i]/days
            start = i+1

    new_data[start:] = new_data[start]
    return time, new_data


# 计算相关性
def correlation(time1, data1, time2, data2):
    idx1 = np.logical_not(np.isnan(data1))
    t1 = time1[idx1]
    d1 = data1[idx1]
    idx2 = np.logical_not(np.isnan(data2))
    t2 = time2[idx2]
    d2 = data2[idx2]

    idx1 = np.isin(t1, t2)
    idx2 = np.isin(t2, t1)
    d1 = d1[idx1]
    d2 = d2[idx2]

    return np.corrcoef(d1, d2)[0,1]

def position_price_correlation(t_price, price, t_position, position, period=52):
    idx1 = np.isin(t_price, t_position)
    idx2 = np.isin(t_position, t_price)
    t1 = t_price[idx1]
    data1 = price[idx1]
    t2 = t_position[idx2]
    data2 = position[idx2]
    # print(t1)
    # print(data1)
    # print('---------------------------------------------')
    # print(t2)
    # print(data2)

    data1_chg_pct = data1.copy()
    data1_chg_pct[1:] = data1[1:]/data1[:-1] - 1
    data1_chg_pct[0] = 0
    data2_chg_abs = data2.copy()
    data2_chg_abs[1:] = data2[1:]-data2[:-1]
    data2_chg_abs[0] = 0

    data1_chg_bin = data1.copy()
    data1_chg_bin[1:] = data1[1:]>data1[:-1]
    data1_chg_bin[0] = False
    data1_chg_bin = data1_chg_bin.astype(float) * 2 - 1
    data2_chg_bin = data2.copy()
    data2_chg_bin[1:] = data2[1:]>data2[:-1]
    data2_chg_bin[0] = False
    data2_chg_bin = data2_chg_bin.astype(float) * 2 - 1

    try:
        pct_corr_6m = correlation(t1[-period//2:], data1_chg_pct[-period//2:], t2[-period//2:], data2_chg_abs[-period//2:])
        bin_corr_6m = correlation(t1[-period//2:], data1_chg_bin[-period//2:], t2[-period//2:], data2_chg_bin[-period//2:])
    except:
        pct_corr_6m = 0
        bin_corr_6m = 0

    try:
        pct_corr_1y = correlation(t1[-period:], data1_chg_pct[-period:], t2[-period:], data2_chg_abs[-period:])
        bin_corr_1y = correlation(t1[-period:], data1_chg_bin[-period:], t2[-period:], data2_chg_bin[-period:])
    except:
        pct_corr_1y = 0
        bin_corr_1y = 0

    try:
        pct_corr_2y = correlation(t1[-period*2:], data1_chg_pct[-period*2:], t2[-period*2:], data2_chg_abs[-period*2:])
        bin_corr_2y = correlation(t1[-period*2:], data1_chg_bin[-period*2:], t2[-period*2:], data2_chg_bin[-period*2:])
    except:
        pct_corr_2y = 0
        bin_corr_2y = 0

    try:
        pct_corr_3y = correlation(t1[-period*3:], data1_chg_pct[-period*3:], t2[-period*3:], data2_chg_abs[-period*3:])
        bin_corr_3y = correlation(t1[-period*3:], data1_chg_bin[-period*3:], t2[-period*3:], data2_chg_bin[-period*3:])
    except:
        pct_corr_3y = 0
        bin_corr_3y = 0

    return round(pct_corr_6m,3),round(bin_corr_6m,3),round(pct_corr_1y,3),round(bin_corr_1y,3),round(pct_corr_2y,3),round(bin_corr_2y,3),round(pct_corr_3y,3),round(bin_corr_3y,3)


def plot_circle(datas, width=400, height=400, start_time='2000-01-01', end_time='2100-01-01', ret = False):
    time.sleep(0.25)
    t1, data1 = get_period_data(datas[0][0], datas[0][1], start_time, end_time, remove_nan=True)
    t2, data2 = get_period_data(datas[1][0], datas[1][1], start_time, end_time, remove_nan=True)

    idx1 = np.isin(t1, t2)
    idx2 = np.isin(t2, t1)

    x = data1[idx1]
    y = data2[idx2]

    slope, intercept, r, _, _ = linregress(x, y)
    fig = figure(frame_width=width, frame_height=height)
    fig.circle(x=x, y=y, color="black", legend_label='x='+datas[0][2] + ', y='+datas[1][2] + ', r^2='+str(round(r*r,3)) + ', slope='+str(round(slope,2)))
    fig.circle(x=x[-1], y=y[-1], line_width=6, color="red", legend_label=t1[idx1][-1].strftime('%Y-%m-%d'))

    yy = x * slope + intercept
    fig.line(x=x, y=yy, color="black")
    fig.legend.location='top_left' 

    if ret == False:
        show(fig)
    else:
        return fig


def wind_edb_excel_to_csv(name):
    # 读 excel
    excel_path = os.path.join(data_dir, 'excel', name+'.xlsx')
    df = pd.read_excel(excel_path)
    # 改列名, 第一列的名字是 time
    df.rename(columns={'指标名称':'time'}, inplace=True)
    # 删除最后没有数据的两行
    df.drop([len(df)-2,len(df)-1], axis=0, inplace=True)
    # 写入 csv
    csv_path = os.path.join(data_dir, name+'.csv')
    df.to_csv(csv_path, encoding='utf-8', index=False)

def wind_kline_excel_to_csv(name):
    # 读 excel
    excel_path = os.path.join(data_dir, 'excel', name+'.xlsx')
    df = pd.read_excel(excel_path)
    # 改列名, 第一列的名字是 time
    df.rename(columns={'日期':'time'}, inplace=True)
    # 删除前两列
    df.drop(['代码','名称'], axis=1, inplace=True)
    # 删除最后没有数据的两行
    df.drop([len(df)-2,len(df)-1], axis=0, inplace=True)
    # 写入 csv
    csv_path = os.path.join(data_dir, name+'.csv')
    df.to_csv(csv_path, encoding='utf-8', index=False)

# 利率期货数据
def wind_kline_excel_to_csv2(name):
    # 读 excel
    excel_path = os.path.join(data_dir, 'excel', name+'.xlsx')
    df = pd.read_excel(excel_path)
    # 改列名, 第一列的名字是 time
    df.rename(columns={'时间':'time'}, inplace=True)
    # 删除前两列
    df.drop(['代码','名称','日期'], axis=1, inplace=True)
    # 删除最后没有数据的两行
    df.drop([len(df)-2,len(df)-1], axis=0, inplace=True)
    # 写入 csv
    csv_path = os.path.join(data_dir, name+'.csv')
    df.to_csv(csv_path, encoding='utf-8', index=False)

def wind_bond_excel_to_csv1(name):
    # 读 excel
    excel_path = os.path.join(data_dir, 'excel', name+'.xlsx')
    df = pd.read_excel(excel_path, header=None)
    # 删除第一列的序号
    df.drop([0], axis=1, inplace=True)
    # 合并第一行和第二行
    for i in range(df.shape[1]):
        if str(df.iloc[0, i]) != 'nan':
            prefix = df.iloc[0, i].split('(')[0]
            df.iloc[1, i] = prefix + ':' + df.iloc[1, i] + '(亿元)'
    # 删除第一行
    df.drop([0], axis=0, inplace=True)
    # 没有的数据用 0 填充
    df = df.fillna('0')
    # 删除最后没有数据的两行
    df.drop([len(df)-1,len(df)-0], axis=0, inplace=True)
    # 时间逆序
    df.iloc[1:] = df.iloc[-1:0:-1]
    # 写入 csv
    csv_path = os.path.join(data_dir, name+'.csv')
    df.to_csv(csv_path, index=False, header=None)

def wind_bond_excel_to_csv2(name):
    # 读 excel
    excel_path = os.path.join(data_dir, 'excel', name+'.xlsx')
    df = pd.read_excel(excel_path, header=None)
    # 删除第一列的序号
    df.drop([0], axis=1, inplace=True)
    # 没有的数据用 0 填充
    df = df.fillna('0')
    # 删除最后没有数据的两行
    df.drop([len(df)-2,len(df)-1], axis=0, inplace=True)
    # 时间逆序
    df.iloc[1:] = df.iloc[-1:0:-1]
    # 写入 csv
    csv_path = os.path.join(data_dir, name+'.csv')
    df.to_csv(csv_path, index=False, header=None)

# 可能需要手动删除第二行
def choice_edb_excel_to_csv(name):
    # 读 excel
    excel_path = os.path.join(data_dir, 'excel', name+'.xlsx')
    df = pd.read_excel(excel_path)
    # 改列名, 第一列的名字是 time
    df.rename(columns={'指标名称':'time'}, inplace=True)
    # 删除最后没有数据的六行和(第二行)
    df.drop([0,len(df)-6,len(df)-5,len(df)-4,len(df)-3,len(df)-2,len(df)-1], axis=0, inplace=True)
    # 写入 csv
    csv_path = os.path.join(data_dir, name+'.csv')
    df.to_csv(csv_path, encoding='utf-8', index=False)

def choice_kline_to_csv(name, name2):
    # 读 excel
    excel_path = os.path.join(data_dir, 'excel', name+'.xls')
    df = pd.read_excel(excel_path)
    # 改列名, 第三列的名字是 time
    df.rename(columns={'交易时间':'time', '开盘价':'open','最高价':'high','最低价':'low','收盘价':'close'}, inplace=True)
    # 删除最后没有数据的六行和(第二行)
    df.drop([len(df)-6,len(df)-5,len(df)-4,len(df)-3,len(df)-2,len(df)-1], axis=0, inplace=True)
    # 删除前两列
    df = df[['time','open','high','low','close']]
    # 写入 csv
    csv_path = os.path.join(cfd_dir, name2+'.csv')
    df.to_csv(csv_path, encoding='utf-8', index=False)

# 
def mysteel_edb_excel_to_csv(name):
    # 读 excel
    excel_path = os.path.join(data_dir, 'excel', name+'.xls')
    df = pd.read_excel(excel_path)
    # 删除第二行
    df.drop([0], axis=0, inplace=True)
    # 改列名, 第一列的名字是 time
    df.rename(columns={'指标名称':'time'}, inplace=True)

    df = df.replace('-','')
    # 写入 csv
    csv_path = os.path.join(data_dir, name+'.csv')
    df.to_csv(csv_path, encoding='utf-8', index=False)


def reorder_info(exchange):
    directory = os.path.join(option_price_dir, exchange)
    for _, _, files in os.walk(directory):
        for file in files:
            if ('_info' in file):
                print(file)
                path = os.path.join(option_price_dir, exchange, file)
                df = pd.read_csv(path)
                df['time'] = pd.to_datetime(df['time'])
                df.sort_values(by='time', axis=0, ascending=True, inplace=True)
                df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
                df.to_csv(path, encoding='utf-8', index=False)

def drop_dup(exchange):
    directory = os.path.join(option_price_dir, exchange)
    for _, _, files in os.walk(directory):
        for file in files:
            print(file)
            path = os.path.join(option_price_dir, exchange, file)
            if ('info' in file):
                df = pd.read_csv(path)
            else:
                df = pd.read_csv(path, header=[0,1,2])
            df = df.drop_duplicates()
            df.to_csv(path, encoding='utf-8', index=False)

def drop_last_line(directory):
    for _, _, files in os.walk(directory):
        for file in files:
            if not ('spot' in file):
                print(file)
                path = os.path.join(directory, file)
                df = pd.read_csv(path, header=[])
                L = len(df)
                df.drop(L-1, axis=0, inplace=True)
                df.to_csv(path, encoding='utf-8', index=False)
                # return

def change_column_name():
    directory = os.path.join(option_price_dir, 'czce')
    for _, _, files in os.walk(directory):
        for file in files:
            if not ('info' in file):
                print(file)
                path = os.path.join(directory, file)
                df = pd.read_csv(path, header=[0,1,2])
                z = df.columns.values.tolist()
                r0 = list()  
                r1 = list()
                r2 = list()

                for i in range(len(z)):
                    if ('C' in z[i][0]):
                        r0.append('C')
                    elif ('P' in z[i][0]):
                        r0.append('P')
                    else:
                        r0.append('time')
                    r1.append(z[i][1])
                    r2.append(z[i][2])

                df.columns = [r0, r1, r2]
                df.to_csv(path, index=False)


# test
if __name__=="__main__":
    # mysteel_edb_excel_to_csv('mysteel_coal')
    # mysteel_edb_excel_to_csv('mysteel_price')

    # choice_kline_to_csv('K线导出_scm_30分钟数据', 'SC0_intraday')
    # choice_kline_to_csv('K线导出_AU0_30分钟数据', 'AU0_intraday')
    # choice_kline_to_csv('K线导出_AG0_30分钟数据', 'AG0_intraday')
    # choice_kline_to_csv('K线导出_CU0_30分钟数据', 'CU0_intraday')
    # choice_kline_to_csv('K线导出_AL0_30分钟数据', 'AL0_intraday')
    # choice_kline_to_csv('K线导出_ZN0_30分钟数据', 'ZN0_intraday')

    choice_edb_excel_to_csv('进出口')
    choice_edb_excel_to_csv('房地产')
    # choice_edb_excel_to_csv('到期')
    # # choice_edb_excel_to_csv('杠杆率')
    # # choice_edb_excel_to_csv('利率')
    # # choice_edb_excel_to_csv('就业')
    # # choice_edb_excel_to_csv('社会融资规模')
    # # choice_edb_excel_to_csv('铁矿')
    # # choice_edb_excel_to_csv('期货资金')
    choice_edb_excel_to_csv('钢铁利润')
    # # choice_edb_excel_to_csv('汇率')
    # # choice_edb_excel_to_csv('货币供应量')
    # choice_edb_excel_to_csv('铜')
    # # choice_edb_excel_to_csv('风险溢价')
    # # choice_edb_excel_to_csv('通胀')
    choice_edb_excel_to_csv('中美PMI')
    choice_edb_excel_to_csv('G4')
    choice_edb_excel_to_csv('地铁')
    choice_edb_excel_to_csv('情绪')
    # choice_edb_excel_to_csv('LME03')
    # choice_edb_excel_to_csv('猪价格')
    



    # choice_edb_exScsv('企业债')
    # choice_edb_excel_to_csv('同业存单')
    # choice_edb_excel_to_csv('商业银行普通金融债')
    # choice_edb_excel_to_csv('地方政府债')
    # choice_edb_excel_to_csv('政策性金融债')
    # choice_edb_excel_to_csv('证券公司短期融资券')

    # wind_kline_excel_to_csv2('FF2312')

    # wind_kline_excel_to_csv('cu2109_1min')
 
    pass

