import os
import time
import numpy as np
import pandas as pd
from utils import *
from cftc import *


def update_crypto_data():
    code = [
            ['CBBTCUSD', 'BTC'],
            ['CBETHUSD', 'ETH'],
            ['CBBCHUSD', 'BCH'],
            ['CBLTCUSD', 'LTC'],
    ]
    name_code = {'crypto': code}
    update_fred_data(name_code, data_dir)


LOOKINTOBITCOIN_DATANAME = [
    'mvrv_zscore',
    'unrealised_profit_loss',
    'puell_multiple',
    'reserve_risk',
    'cvdd',
    'realized_price',
    'rhodl_ratio',

    'vdd_multiple',
    'hodl_waves',
    'rcap_hodl_waves',
    'whale_watching',
    'bdd',  # coin days destroyed
    'bdd_supply_adjusted',
]


# https://www.lookintobitcoin.com/charts/mvrv-zscore/
def update_lookintobitcoin_data():
    se = requests.session()

    # example
    # url = 'https://www.lookintobitcoin.com/django_plotly_dash/app/mvrv_zscore/_dash-update-component'
    URL = 'https://www.lookintobitcoin.com/django_plotly_dash/app/{}/_dash-update-component'
    payload = {"output":"chart.figure","outputs":{"id":"chart","property":"figure"},"inputs":[{"id":"url","property":"pathname","value":"/charts/relative-unrealized-profit--loss/"}],"changedPropIds":["url.pathname"]}

    HEADERS = {'Host': 'www.lookintobitcoin.com',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            #    'Accept-Encoding': 'gzip, deflate, br',
               'Content-Type': 'application/json',
               'Content-Length': '181',
               'Cookie':'csrftoken=pUbxeOYQLUK2oT7ULqJroQIUP1siMAJr;sessionid=0qezz4nv8ikf2kuyy3bptbk54eroh66v; cf_clearance=sxglDg7snwgfY6FM3.oOOA_sI3OGFYvNI8KsJdrtgo0-1698898797-0-1-6ece6fda.ec9d9f82.39cf303a-0.2.1698898797',
    }

    for name in LOOKINTOBITCOIN_DATANAME:
        url = URL.format(name)
        print(name)
        while (1):
            try:
                r = se.post(url, headers=HEADERS, json=payload)
                data_json = r.json()
                break
            except Exception as e:
                print(e)
                time.sleep(5)

        datas = data_json['response']['chart']['figure']['data']
        df = pd.DataFrame()
        for i in range(len(datas)):
            data = datas[i]
            if 'customdata' in data:
                temp_df = pd.DataFrame()
                temp_df['time'] = data['x'][:len(data['y'])]
                temp_df[data['name']] = data['y']

                if (df.empty):
                    df = temp_df.copy()
                else:
                    df = pd.merge(df, temp_df, on='time', how='outer')

        path = os.path.join(btc_dir, name+'.csv')
        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            old_df.sort_values(by = 'time', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df.to_csv(path, encoding='utf-8', index=False)  


# BTC持仓
def plot_crypto_position():
    path = os.path.join(data_dir, 'crypto'+'.csv')
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))

    btc = np.array(df['BTC'], dtype=float)
    cftc_plot_financial(t, btc, 'BTC', code='133741', inst_name='CME:BTC')

    eth = np.array(df['ETH'], dtype=float)
    cftc_plot_financial(t, eth, 'ETH', code='146021', inst_name='CME:ETH')


def read_btc_data(name):
    path = os.path.join(btc_dir, name+'.csv')
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    cols = df.columns.tolist()
    data = {}
    for i in range(1, len(cols)):
        data[cols[i]] = np.array(df[cols[i]], dtype=float)

    return t, data

######################### PLOT #########################
def plot_mvrv_zscore():
    t, data = read_btc_data('mvrv_zscore')

    datas = [
             [[[t,data['Z-Score'],'Z-Score','color=orange'],
               [t,data['MVRV'],'MVRV','color=red,visible=False'],
              ],
              [[t,np.log10(1+data['Market Cap']),'Market Cap (log10)','color=black'],
               [t,np.log10(1+data['Realized Cap']),'Realized Cap (log10)','color=blue'],],'MVRV Z-Score'],
    ]
    plot_many_figure(datas, start_time='2013-01-01')


def plot_nupl():
    t, data = read_btc_data('unrealised_profit_loss')

    datas = [
             [[[t,np.log10(1+data['BTC Price']),'BTC Price (log10)','color=black'],
              ],
              [[t,data['Net Unrealised Profit / Loss (NUPL)'],'Net Unrealised Profit / Loss (NUPL)','color=orange'],
               ],'NUPL'],
    ]
    plot_many_figure(datas, start_time='2013-01-01')


def plot_bdd():
    t, data = read_btc_data('bdd')
    t1, cdd_7dma = moving_average(t, data['CDD (raw data)'], 7)

    datas = [
             [[[t,np.log10(1+data['BTC Price']),'BTC Price (log10)','color=black'],
              ],
              [
               [t,data['CDD (30dma)'],'CDD (30dma)','color=orange'],
               [t,data['CDD (90dma)'],'CDD (90dma)','color=blue'],
               ],'Coin Days Destroyed'],

             [[[t1,cdd_7dma,'CDD (7dma)','color=red'],
              ],
              [],''],
    ]
    plot_many_figure(datas, start_time='2013-01-01')


    t, data = read_btc_data('bdd_supply_adjusted')
    t1, cdd_7dma = moving_average(t, data['Supply Adjusted CDD (raw data)'], 7)

    datas = [
             [[[t,np.log10(1+data['BTC Price']),'BTC Price (log10)','color=black'],
              ],
              [
               [t,data['Supply Adjusted CDD (30dma)'],'Supply Adjusted CDD (30dma)','color=orange'],
               [t,data['Supply Adjusted CDD (90dma)'],'Supply Adjusted CDD (90dma)','color=blue'],
               ],'Coin Days Destroyed Supply Adjusted'],

             [[[t1,cdd_7dma,'Supply Adjusted CDD (7dma)','color=red'],
              ],
              [],''],
    ]
    plot_many_figure(datas, start_time='2013-01-01')


def plot_vdd_multiple():
    t, data = read_btc_data('vdd_multiple')

    datas = [
             [[[t,np.log10(1+data['BTC Price']),'BTC Price (log10)','color=black'],
              ],
              [
               [t,data['VDD Multiple'],'VDD Multiple','color=orange'],
               ],'Value Days Destroyed (VDD) Multiple'],
    ]
    plot_many_figure(datas, start_time='2013-01-01')


def plot_rhodl_ratio():
    t, data = read_btc_data('rhodl_ratio')

    datas = [
             [[[t,np.log10(1+data['BTC Price']),'BTC Price (log10)','color=black'],
              ],
              [[t,np.log10(1+data['RHODL Ratio']),'RHODL Ratio','color=orange'],
               ],'RHODL Ratio'],
    ]
    plot_many_figure(datas, start_time='2013-01-01')


def plot_whale_watching():
    t, data = read_btc_data('whale_watching')

    datas = [
             [[[t,np.log10(1+data['BTC Price']),'BTC Price (log10)','color=black'],],
              [],'Whale Watching'],

             [[[t,data['10yr+'],'10yr+','style=quad'],
              ],
              [],''],

             [[[t,data['7-9yr'],'7-9yr','style=quad'],
              ],
              [],''],

             [[[t,data['5-7yr'],'5-7yr','style=quad'],
              ],
              [],''],

             [[[t,data['4-5yr'],'4-5yr','style=quad'],
              ],
              [],''],
    ]
    plot_many_figure(datas, start_time='2017-01-01')


def plot_btc_vs_us_debt():
    bill = ['Bill_4W', 'Bill_8W', 'Bill_13W', 'Bill_26W', 'Bill_52W']
    note = ['Note_2Y', 'Note_3Y', 'Note_5Y', 'Note_7Y', 'Note_10Y']
    bond = ['Bond_20Y', 'Bond_30Y']

    df_dict = {}
    for security in bill+note+bond:
        path = os.path.join(treasury_auction_dir, security+'.csv')
        df = pd.read_csv(path).fillna(0)
        df.drop_duplicates(subset=['period_end_date'], keep='last', inplace=True)

        t = pd.DatetimeIndex(pd.to_datetime(df['auction_date'], format='%Y-%m-%d'))
        end_t = pd.DatetimeIndex(pd.to_datetime(df['period_end_date'], format='%Y-%m-%d'))
        df_dict[security] = [t, end_t, df]

    bill_sum = None
    bill_sum_t = None
    note_sum = None
    note_sum_t = None

    for security in bill:
        if bill_sum is None:
            bill_sum_t = df_dict[security][1] 
            bill_sum = np.array(df_dict[security][2]['offering_amt'])
        else:
            bill_sum_t, bill_sum = data_add(bill_sum_t, bill_sum, df_dict[security][1], np.array(df_dict[security][2]['offering_amt'], dtype=float))
    bill_sum /= 1000000000

    for security in note:
        if note_sum is None:
            note_sum_t = df_dict[security][1] 
            note_sum = np.array(df_dict[security][2]['offering_amt'])
        else:
            note_sum_t, note_sum = data_add(note_sum_t, note_sum, df_dict[security][1], np.array(df_dict[security][2]['offering_amt'], dtype=float))
    note_sum /= 1000000000

    path = os.path.join(data_dir, 'crypto'+'.csv')
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    btc = np.array(df['BTC'], dtype=float)
    eth = np.array(df['ETH'], dtype=float)

    # frn+note
    datas = [
             [[],[],''],
             [[],[],''],
    ]
    for security in bill:
        datas[0][0].append([df_dict[security][0], np.array(df_dict[security][2]['offering_amt'], dtype=float)/10000000000, security, 'style=dot_line'])
    datas[0][1].append([t, btc, 'BTC', 'color=black'])
    datas[1][0].append([bill_sum_t, bill_sum, 'bill offering amount', 'color=blue'])
    datas[1][1].append([t, btc, 'BTC', 'color=black'])
    plot_many_figure(datas, start_time='2017-05-01')


    # frn+note
    datas = [
             [[],[],''],
             [[],[],''],
             [[],[],''],
    ]
    for security in note:
        datas[0][0].append([df_dict[security][0], np.array(df_dict[security][2]['offering_amt'], dtype=float)/10000000000, security, 'style=dot_line'])

    datas[0][1].append([t, btc, 'BTC', 'color=black'])
    datas[1][0].append([note_sum_t, note_sum, 'note offering amount', 'color=blue'])
    datas[1][1].append([t, btc, 'BTC', 'color=black']) 
    datas[2][0].append([note_sum_t, note_sum, 'note offering amount', 'color=blue'])
    datas[2][1].append([t, eth, 'ETH', 'color=black']) 
    plot_many_figure(datas, start_time='2017-05-01')


def plot_btc_vs_fed_balance_sheet():
    path = os.path.join(data_dir, 'crypto'+'.csv')
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    btc = np.array(df['BTC'], dtype=float)
    eth = np.array(df['ETH'], dtype=float)

    path = os.path.join(fed_dir, 'Factors Affecting Reserve Balances of Depository Institutions'+'.csv')
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    total_asset = np.array(df['Assets: Other Factors Supplying Reserve Balances: Total factors supplying reserve funds: Wednesday level'], dtype=float)
    rrp = np.array(df['Liabilities and Capital: Liabilities: Reverse repurchase agreements: Wednesday level'], dtype=float)
    tga = np.array(df['Liabilities and Capital: Liabilities: Deposits with F.R. Banks, other than reserve balances: U.S. Treasury, General Account: Wednesday level'], dtype=float)

    z = total_asset - rrp - tga
    datas = [
             [[[t,btc,'BTC','color=black'],
              ],
              [[t1,z,'FED BALANCE SHEET - RRP - TGA','color=blue'],],''],
    ]
    plot_many_figure(datas, start_time='2017-06-01')


def plot_eth_vs_btc():
    path = os.path.join(data_dir, 'crypto'+'.csv')
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    btc = np.array(df['BTC'], dtype=float)
    eth = np.array(df['ETH'], dtype=float)

    ratio = eth/btc
    datas = [
             [[[t,eth,'ETH',''],
              ],
              [[t,btc,'BTC',''],],''],

             [[[t,ratio,'ETH / BTC',''],
              ],
              [],''],
    ]
    plot_many_figure(datas, start_time='2017-06-01')

######################### PLOT #########################

if __name__=="__main__":
    update_lookintobitcoin_data()
    # plot_mvrv_zscore()
    # plot_nupl()
    # plot_bdd()
    # plot_vdd_multiple()
    # plot_rhodl_ratio()
    # plot_whale_watching()


    # update_crypto_data()
    # # plot_crypto_position()
    # plot_eth_vs_btc()
    # plot_btc_vs_us_debt()
    # plot_btc_vs_fed_balance_sheet()

    pass
