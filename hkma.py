import os
import requests
import pandas as pd
import datetime
import numpy as np
from utils import *
import calendar

############ 香港金融管理局 ############
# https://www.hkma.gov.hk/chi/data-publications-and-research/data-and-statistics/monthly-statistical-bulletin/

HKMA_NAME_URL = {
    'hibor': ['er-ir/hk-interbank-ir-daily?segment=hibor.fixing&offset={}'],
    'market_operation': ['monetary-operation/market-operation-daily?offset={}'],
    'USDHKD': ['er-ir/hkd-fer-daily?offset={}'],
    'deposit': ['banking/customer-deposits-by-type-cny?offset={}',
                'banking/customer-deposits-by-currency?offset={}'],
    'money_supply': ['money/supply-components-all?offset={}'],
    'discount_window_rate': ['monetary-operation/disc-win-liquid-adj-win-rates-daily?offset={}'],
    'exchange_fund_balance_sheet': ['ef-fc-resv-assets/ef-bal-sheet-abridged?offset={}'],
}

def get_month_last_day(t):
    year = int(t[:4])
    month = int(t[5:7])
    last_day = calendar.monthrange(year, month)[-1]
    dt_last_day = datetime.datetime(year, month, last_day)
    return dt_last_day.strftime('%Y-%m-%d')

def get_quarter_last_day(t):
    year = int(t[:4])
    quarter = t[6]
    month = int(quarter)*3
    last_day = calendar.monthrange(year, month)[-1]
    dt_last_day = datetime.datetime(year, month, last_day)
    return dt_last_day.strftime('%Y-%m-%d')


def update_hkma_data():
    earlist_time = '2000-01-01'
    BASE_URL = 'https://api.hkma.gov.hk/public/market-data-and-statistics/monthly-statistical-bulletin/'
    se = requests.session()
    HKMA_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                    'Host': 'api.hkma.gov.hk'}

    for name in HKMA_NAME_URL:
        path = os.path.join(hkma_dir, name+'.csv')
        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_t = pd.DatetimeIndex(pd.to_datetime(old_df['time'], format='%Y-%m-%d'))
            start_time_dt = old_t[-1] + pd.Timedelta(days=1)
        else:
            old_df = pd.DataFrame()
            start_time_dt = pd.to_datetime(earlist_time, format='%Y-%m-%d')

        URL_LIST = HKMA_NAME_URL[name]
        df = pd.DataFrame()
        for u in URL_LIST:
            URL = BASE_URL + u
            offset = 0
            _df = pd.DataFrame()
            while (1):
                time.sleep(0.25)
                url = URL.format(str(offset))
                offset += 100

                while (1):
                    try:
                        r = se.get(url, headers=HKMA_HEADERS)
                        break
                    except Exception as e:
                        print(e)
                        time.sleep(5)

                data_json = r.json()
                temp_df = pd.DataFrame(data_json["result"]['records'])
                if len(temp_df) == 0:
                    break

                temp_df.rename(columns={"end_of_day":"time", 'end_of_month':'time', 'end_of_quarter':'time'}, inplace=True)
                print(name, temp_df.loc[0, 'time'] + ' - ' + temp_df.loc[len(temp_df)-1, 'time'])
                _df = pd.concat([_df, temp_df], axis=0)

                dt = pd.to_datetime(temp_df.loc[len(temp_df)-1, 'time'])
                if dt <= start_time_dt:
                    break

            _df.reset_index(inplace=True, drop=True)
            t = _df.loc[0, 'time']
            if len(t) == 7:
                if t[5].isdigit():   # yyyy-mm
                    _df['time'] = _df['time'].apply(get_month_last_day)
                else:     # yyyy-qq
                    _df['time'] = _df['time'].apply(get_quarter_last_day)

            # print(_df)
            if df.empty:
                df = _df.copy()
            else:
                df = pd.merge(df, _df, on='time', how='outer')

        old_df = pd.concat([old_df, df], axis=0)
        old_df.drop_duplicates(subset=['time'], keep='last', inplace=True) # last
        old_df['time'] = pd.to_datetime(old_df['time'], format='%Y-%m-%d')
        old_df.sort_values(by='time', axis=0, ascending=True, inplace=True)
        old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
        old_df.to_csv(path, encoding='utf-8', index=False)


if __name__=="__main__":
    update_hkma_data()


    pass



