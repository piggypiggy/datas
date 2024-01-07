import os
import re
import time
import numpy as np
import pandas as pd
import bs4
import datetime
import requests
from utils import *
from io import StringIO, BytesIO

def update_fomc_calendar():
    se = requests.session()
    url = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
    FED_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Host": "www.federalreserve.gov",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36",
    }

    while (1):
        try:
            r = se.get(url, verify=False, headers=FED_HEADERS, timeout=10)
            break
        except Exception as e:
            print(e)
            time.sleep(5)

    soup = bs4.BeautifulSoup(r.text, 'html.parser')
    divs = soup.find_all(name='div', class_="panel panel-default")
    t = []
    for div in divs:
        d = div.find(name='div', class_='panel-heading')
        year = d.get_text()[:4]
        months = div.find_all(name='div', class_=re.compile('fomc-meeting__month'))
        days = div.find_all(name='div', class_=re.compile('fomc-meeting__date'))

        if not('cancelled' in days):
            L = len(months)
            for i in range(L):
                month = months[i].get_text()
                if '/' in month:
                    month = month.split('/')[1].upper()
                    month = month_dict[month]
                else:
                    month = month[:3].upper()
                    month = month_dict[month]

                day = days[i].get_text()
                day = day.replace('*', '').split(' (')[0]
                if '-' in day:
                    day = day.split('-')[1]
                else:
                    day = day.split(' ')[0]
                if len(day) == 1:
                    day = '0' + day

                t.append(year + '-' + month + '-' + day)

    df = pd.DataFrame(columns=['time'], data=t)
    path = os.path.join(interest_rate_dir, 'fomc_calendar'+'.csv')

    if os.path.exists(path):
        old_df = pd.read_csv(path)
        old_df = pd.concat([old_df, df], axis=0)
        old_df.drop_duplicates(subset=['time'], keep='last', inplace=True) # last
        old_df['time'] = pd.to_datetime(old_df['time'], format='%Y-%m-%d')
        old_df.sort_values(by='time', axis=0, ascending=True, inplace=True)
        old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
        old_df.to_csv(path, encoding='utf-8', index=False)
    else:
        df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
        df.sort_values(by = 'time', inplace=True)
        df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
        df.to_csv(path, encoding='utf-8', index=False)


def get_zq_df_line_data(temp_zq_df):
    contracts = []
    settles = []
    i = 0
    while (1):
        i += 1
        cn = 'c'+str(i)
        try:
            settle = temp_zq_df[cn]['settle']
            if np.isnan(settle):
                break
            contracts.append(temp_zq_df[cn]['inst_id'])
            settles.append(settle)
        except:
            break

    return contracts, settles


def get_pre_month_last_day_effr(effr_t, effr, year, month):
    pre_month_lasy_day_dt = get_pre_month_last_day(year, month)
    while (1):
        idx = np.where(effr_t == pre_month_lasy_day_dt)[0]
        if len(idx) > 0:
            i = idx[0]
            pre_month_lasy_day_effr = effr[i]
            break
        pre_month_lasy_day_dt -= pd.Timedelta(days=1)

    return pre_month_lasy_day_dt, pre_month_lasy_day_effr


def fomc_meeting_day_this_month_since(dt, fomc_t):
    idx = np.where(fomc_t >= dt)[0]
    if len(idx) == 0:
        return None
    else:
        idx = idx[0]
        if fomc_t[idx].month == dt.month:
            return fomc_t[idx]
        else:
            return None


def calculate_effr_expectation():
    path = os.path.join(future_price_dir, 'cme', 'ZQ'+'.csv')
    zq_df = pd.read_csv(path, header=[0,1])
    zq_t = pd.DatetimeIndex(pd.to_datetime(zq_df['time']['time'], format='%Y-%m-%d'))

    path = os.path.join(interest_rate_dir, 'fomc_calendar'+'.csv')
    fomc_df = pd.read_csv(path)
    fomc_t = pd.DatetimeIndex(pd.to_datetime(fomc_df['time'], format='%Y-%m-%d'))
    effr_change_t = fomc_t + pd.Timedelta(days=1)

    path = os.path.join(interest_rate_dir, 'federal_fund_rate'+'.csv')
    effr_df = pd.read_csv(path)
    effr_t = pd.DatetimeIndex(pd.to_datetime(effr_df['time'], format='%Y-%m-%d'))
    # effr1 = np.array(effr_df['Effective Federal Funds Rate'], dtype=float)
    effr = np.array(effr_df['Federal Funds Effective Rate'], dtype=float)
    w = np.where(np.isnan(effr) == False)[0]
    effr[w] = effr[w]
    effr_t = effr_t[w]
    # w = np.where(np.isnan(effr) == False)[0]
    # effr_t = effr_t[w]
    # effr = effr[w]

    # ########## expiry time dict ##########
    # expiry_time_dict = {}
    # for i in range(len(expiry_time)):
    #     expiry_time_dict['ZQ'+ym[i]] = pd.to_datetime(expiry_time[i], format='%Y-%m-%d')
    # temp_zq_df = zq_df.loc[len(zq_t)-1,:]
    # contracts, _ = get_zq_df_line_data(temp_zq_df)
    # for contract in contracts:
    #     if not(contract in expiry_time_dict):
    #         expiry_time_dict[contract] = get_last_friday(int('20'+contract[2:4]), int(contract[4:]))
    # # print(expiry_time_dict)

    ###########################################
    path = os.path.join(interest_rate_dir, 'effr_expectation'+'.csv')
    if not os.path.exists(path):
        start_time = '2023-07-01'
        start_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')
    else:
        df = pd.read_csv(path, header=[0,1])
        t = pd.DatetimeIndex(pd.to_datetime(df['time']['time'], format='%Y-%m-%d'))
        start_time_dt = t[-1] + pd.Timedelta(days=1)

    zq_i = np.where(zq_t >= start_time_dt)[0]
    if len(zq_i) == 0:
        return
    zq_i = zq_i[0]
    ######
    zq_i -= 5
    ######

    ########################################
    while (zq_i < len(zq_t)):
        dt = zq_t[zq_i]
        t = dt.strftime('%Y-%m-%d')
        col1 = ['time']
        col2 = ['time']
        data = [t]
        n = 0

        # time
        day = dt.day
        month = dt.month
        year = dt.year
        print(t)

        preday_month = zq_t[zq_i-1].month

        # check zq time and effr time
        dt_minus1 = dt - pd.Timedelta(days=1)
        if not(dt_minus1 in effr_t):
            zq_i += 1
            continue

        # settle
        temp_zq_df = zq_df.loc[zq_i,:]
        contracts, settles = get_zq_df_line_data(temp_zq_df)
        zq_i += 1

        ########## current month ##########
        _, pre_month_lasy_day_effr = get_pre_month_last_day_effr(effr_t, effr, year, month)

        # fomc meeting day
        fomc_meeting_dt = fomc_meeting_day_this_month_since(dt, fomc_t)

        month_end_day_dt = get_month_last_day(year, month)
        month_end_day = month_end_day_dt.strftime('%Y-%m-%d')
        month_days = calendar.monthrange(year, month)[-1]
        k = 0
        if month != preday_month:  # month first day
            if dt in effr_change_t:
                continue

        if (fomc_meeting_dt is None) or (fomc_meeting_dt == month_end_day_dt):
            begin_day = t
            rate = 100 - settles[k]
            n += 1
            col1 += [str(n), str(n)]
            col2 += ['time', 'rate']
            data += [begin_day, round(rate,4)]

            n += 1
            col1 += [str(n), str(n)]
            col2 += ['time', 'rate']
            data += [month_end_day, round(rate,4)]

            pre_month_lasy_day_effr = rate
        else:
            # has fomc meeting not at lastday
            meeting_day = fomc_meeting_dt.day
            if day > 1:
                idx = np.where(np.logical_and(
                                    (datetime.datetime(year=year, month=month, day=1) <= effr_t), 
                                    (effr_t <= datetime.datetime(year=year, month=month, day=day-1))))[0]
                before_avg_rate = np.average(effr[idx])
            else:
                before_avg_rate = pre_month_lasy_day_effr

            begin_day = datetime.datetime(year=year, month=month, day=day).strftime('%Y-%m-%d')
            n += 1
            col1 += [str(n), str(n)]
            col2 += ['time', 'rate']
            data += [begin_day, round(before_avg_rate,4)]

            if (datetime.datetime(year=year, month=month, day=day) < fomc_meeting_dt):       
                end_day = fomc_meeting_dt.strftime('%Y-%m-%d')
                n += 1
                col1 += [str(n), str(n)]
                col2 += ['time', 'rate']
                data += [end_day, round(before_avg_rate,4)]

            # after fomc
            avg_rate = 100 - settles[k]
            after_avg_rate = (avg_rate*month_days - before_avg_rate*meeting_day) / (month_days - meeting_day)

            begin_day = datetime.datetime(year=year, month=month, day=meeting_day+1).strftime('%Y-%m-%d')
            n += 1
            col1 += [str(n), str(n)]
            col2 += ['time', 'rate']
            data += [begin_day, round(after_avg_rate,4)]

            end_day = datetime.datetime(year=year, month=month, day=month_days).strftime('%Y-%m-%d')
            n += 1
            col1 += [str(n), str(n)]
            col2 += ['time', 'rate']
            data += [end_day, round(after_avg_rate,4)]

            pre_month_lasy_day_effr = after_avg_rate


        ########## later months ##########
        for k in range(1, len(contracts)):
            contract = contracts[k]
            year = int('20'+contract[2:4])
            month = int(contract[4:])
            day = 1
            dt = datetime.datetime(year=year, month=month, day=day)
            t = dt.strftime('%Y-%m-%d')
            fomc_meeting_dt = fomc_meeting_day_this_month_since(dt, fomc_t)

            month_end_day_dt = get_month_last_day(year, month)
            month_end_day = month_end_day_dt.strftime('%Y-%m-%d')
            month_days = calendar.monthrange(year, month)[-1]

            if (fomc_meeting_dt is None) or (fomc_meeting_dt == month_end_day_dt):
                begin_day = t
                rate = 100 - settles[k]
                n += 1
                col1 += [str(n), str(n)]
                col2 += ['time', 'rate']
                data += [begin_day, round(rate,4)]

                n += 1
                col1 += [str(n), str(n)]
                col2 += ['time', 'rate']
                data += [month_end_day, round(rate,4)]

                pre_month_lasy_day_effr = rate
            else:
                # has fomc meeting not at lastday
                meeting_day = fomc_meeting_dt.day
                # before fomc
                before_avg_rate = pre_month_lasy_day_effr
                begin_day = datetime.datetime(year=year, month=month, day=1).strftime('%Y-%m-%d')
                n += 1
                col1 += [str(n), str(n)]
                col2 += ['time', 'rate']
                data += [begin_day, round(before_avg_rate,4)]
                
                end_day = fomc_meeting_dt.strftime('%Y-%m-%d')
                n += 1
                col1 += [str(n), str(n)]
                col2 += ['time', 'rate']
                data += [end_day, round(before_avg_rate,4)]

                # after fomc
                avg_rate = 100 - settles[k]
                after_avg_rate = (avg_rate*month_days - before_avg_rate*meeting_day) / (month_days - meeting_day)

                # print(year, month, meeting_day)
                begin_day = datetime.datetime(year=year, month=month, day=meeting_day+1).strftime('%Y-%m-%d')
                n += 1
                col1 += [str(n), str(n)]
                col2 += ['time', 'rate']
                data += [begin_day, round(after_avg_rate,4)]

                end_day = datetime.datetime(year=year, month=month, day=month_days).strftime('%Y-%m-%d')
                n += 1
                col1 += [str(n), str(n)]
                col2 += ['time', 'rate']
                data += [end_day, round(after_avg_rate,4)]

                pre_month_lasy_day_effr = after_avg_rate


        df = pd.DataFrame(columns=[col1,col2], data=[data])
        path = os.path.join(interest_rate_dir, 'effr_expectation'+'.csv')
        if os.path.exists(path):
            old_df = pd.read_csv(path, header=[0,1])
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=[('time','time')], keep='last', inplace=True) # last
            old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            old_df.sort_values(by = ('time','time'), inplace=True)
            old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df.to_csv(path, encoding='utf-8', index=False)


def plot_effr_expectation():
    path = os.path.join(interest_rate_dir, 'effr_expectation'+'.csv')
    df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(df['time']['time'], format='%Y-%m-%d'))

    L = len(t)
    # z = [0, 1, 2, 3, 4, 5, 10, 15, 20]
    z = [0, 1, 3, 5, 10]
    datas = [[[],[],'']]
    for i in z:
        temp_df = df.loc[L-1-i, :]
        k = 0
        temp_t = []
        temp_data = []
        while (1):
            k += 1
            try:
                if not np.isnan(temp_df[str(k)]['rate']):
                    temp_t.append(pd.to_datetime(temp_df[str(k)]['time'], format='%Y-%m-%d'))
                    temp_data.append(temp_df[str(k)]['rate'])
                else:
                    break
            except:
                break

        datas[0][0].append([np.array(temp_t), np.array(temp_data), t[L-1-i].strftime('%Y-%m-%d'), ''])

    path = os.path.join(interest_rate_dir, 'federal_fund_rate'+'.csv')
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    effr = np.array(df['Federal Funds Effective Rate'], dtype=float)
    datas[0][0].append([t, effr, 'EFFR', 'color=black'])

    plot_many_figure(datas, start_time='2017-01-01')


######### fed balance sheet H.4.1 #########
# example url
# H41_URL = 'https://www.federalreserve.gov/datadownload/Output.aspx?rel=H41&series=7951a85bb48c5cc679a40e18f2d718bd&lastobs=&from=10/01/2023&to=11/15/2023&filetype=csv&label=include&layout=seriescolumn'

H41_SERIES = {
    '7951a85bb48c5cc679a40e18f2d718bd': 'Factors Affecting Reserve Balances of Depository Institutions',
    '7c037361d7d4efc82b17dcd09ff94755': 'Memorandum Items',
    '476ff974a596a080dcdf50b68e9e4449': 'Maturity Distribution of Securities, Loans, and Selected Other Assets and Liabilities',
    '851de028e02a877bdfbfcfa6402d8c08': 'Supplemental Information on Mortgage-Backed Securities',
    '734c5de46015881d6f0213c006ec985d': 'Information on Principal Accounts of Credit Facilities LLCs',
    '522d41432ac812f80e55915e4fa50ca7': 'Consolidated Statement of Condition of All Federal Reserve Banks',
    'd8c555bc285493540550bf0fc2ed5f02': 'Statement of Condition of Each Federal Reserve Bank',
    'c22d8b33b4728f25d2f5b2ad29ce5bbc': 'Collateral Held against Federal Reserve Notes, Federal Reserve Agents Accounts',
}

def update_fed_balance_sheet():
    se = requests.session()
    H41_URL = 'https://www.federalreserve.gov/datadownload/Output.aspx?rel=H41&series={}&lastobs=&from={}&to={}&filetype=csv&label=include&layout=seriescolumn'
    FED_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Host": "www.federalreserve.gov",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
        'Cookie': 'Peace & Love',
    }

    earlist_time = '2002-01-01'

    for series_id in H41_SERIES:
        name = H41_SERIES[series_id]
        path = os.path.join(fed_dir, name+'.csv')
        if os.path.exists(path):
            df = pd.read_csv(path)
            t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
            start_time_dt = t[-1] + pd.Timedelta(days=1)
        else:
            start_time_dt = pd.to_datetime(earlist_time, format='%Y-%m-%d')

        now = datetime.datetime.now()
        
        while (start_time_dt <= now):
            end_time_dt = start_time_dt + pd.Timedelta(days=365)
            url = H41_URL.format(series_id, start_time_dt.strftime('%m/%d/%Y'), end_time_dt.strftime('%m/%d/%Y'))
            while (1):
                try:
                    print(name, start_time_dt, end_time_dt)
                    r = se.get(url, headers=FED_HEADERS, timeout=30)
                    df = pd.read_csv(StringIO(r.text))
                    break
                except Exception as e:
                    print(e)
                    time.sleep(15)
            
            start_time_dt = end_time_dt

            if len(df) <= 5:
                continue

            df.rename(columns={"Series Description":"time"}, inplace=True)
            df = df.loc[5:,]
            # print(df)

            if os.path.exists(path):
                old_df = pd.read_csv(path)
                old_df = pd.concat([old_df, df], axis=0)
                old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
                old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
                old_df.sort_values(by = 'time', inplace=True)
                old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
                old_df.to_csv(path, encoding='utf-8', index=False)
            else:
                df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
                df.sort_values(by = 'time', inplace=True)
                df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
                df.to_csv(path, encoding='utf-8', index=False)


def update_onrrp_data():
    code = [ 
          ['RRPONTSYD', 'Overnight Reverse Repurchase Agreements: Treasury Securities Sold by the Federal Reserve in the Temporary Open Market Operations'],
          ['RRPONTTLD', 'Overnight Reverse Repurchase Agreements: Total Securities Sold by the Federal Reserve in the Temporary Open Market Operations'],
          ['RRPONTSYAWARD', 'Overnight Reverse Repurchase Agreements Award Rate: Treasury Securities Sold by the Federal Reserve in the Temporary Open Market Operations'],
        ]

    name_code = {'onrrp': code}
    update_fred_data(name_code, fed_dir)



if __name__=="__main__":
    # update_fomc_calendar()

    # calculate_effr_expectation()

    # plot_effr_expectation()

    # update_fed_balance_sheet()
    # update_onrrp_data()

    pass
