import os
import requests
import pandas as pd
import datetime
import numpy as np
from utils import *
from io import StringIO, BytesIO
from nasdaq import *
from cftc import *


TREASURY_NAME_URL = {
    'us_yield_curve': 'https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/{}/all?field_tdr_date_value={}&type=daily_treasury_yield_curve&page&_format=csv',
    'us_bill_rate': 'https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/{}/all?type=daily_treasury_bill_rates&field_tdr_date_value={}&page&_format=csv',
    'us_real_rate': 'https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/{}/all?field_tdr_date_value={}&type=daily_treasury_real_yield_curve&page&_format=csv'
}

def update_treasury_yield():
    ############ 美国财政部 ############
    earlist_time = '2000-01-01'
    current_year = datetime.datetime.now().year
    se = requests.session()
    US_TREASURY_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                           'Host': 'home.treasury.gov'}

    for name in TREASURY_NAME_URL:
        path = os.path.join(interest_rate_dir, name+'.csv')
        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_t = pd.DatetimeIndex(pd.to_datetime(old_df['time'], format='%Y-%m-%d'))
            start_time_dt = old_t[-1]
            start_year = start_time_dt.year - 1
            old_df['time'] = pd.to_datetime(old_df['time'], format='%Y-%m-%d')
        else:
            old_df = pd.DataFrame()
            start_time_dt = pd.to_datetime(earlist_time, format='%Y-%m-%d')
            start_year = 2000

        df = pd.DataFrame()
        while (start_year <= current_year):
            url = TREASURY_NAME_URL[name].format(str(start_year), str(start_year))
            start_year += 1

            while (1):
                try:
                    r = se.get(url, headers=US_TREASURY_HEADERS, timeout=10)
                    break
                except Exception as e:
                    print(e)
                    time.sleep(5)

            if (len(r.text) == 0):
                continue

            temp_df = pd.read_csv(StringIO(r.text))
            if len(temp_df) == 0:
                break

            temp_df.rename(columns={'Date': 'time',
                                    "1 Mo":"1M", "2 Mo":"2M", "3 Mo":"3M", "4 Mo":"4M", "6 Mo":"6M", 
                                    "1 Yr":"1Y", "2 Yr":"2Y", "3 Yr":"3Y", "5 Yr":"5Y", "7 Yr":"7Y",
                                    "10 Yr":"10Y", "20 Yr":"20Y", "30 Yr":"30Y",
                                    '5 YR':'5Y', '7 YR':'7Y', '10 YR':'10Y', '20 YR':'20Y', '30 YR':'30Y', }, inplace=True)

            df = pd.concat([df, temp_df], axis=0)
            print(name, start_year-1)
            
        df['time'] = pd.to_datetime(df['time'], format='%m/%d/%Y')
        old_df = pd.concat([old_df, df], axis=0)
        old_df.drop_duplicates(subset=['time'], keep='last', inplace=True) # last
        old_df.sort_values(by='time', axis=0, ascending=True, inplace=True)
        old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
        old_df.to_csv(path, encoding='utf-8', index=False)


def update_us_bond_term_premium():
    code = [ 
          ['THREEFYTP1', 'Term Premium on a 1 Year Zero Coupon Bond'],
          ['THREEFYTP2', 'Term Premium on a 2 Year Zero Coupon Bond'],
          ['THREEFYTP3', 'Term Premium on a 3 Year Zero Coupon Bond'],
          ['THREEFYTP4', 'Term Premium on a 4 Year Zero Coupon Bond'],
          ['THREEFYTP5', 'Term Premium on a 5 Year Zero Coupon Bond'],
          ['THREEFYTP6', 'Term Premium on a 6 Year Zero Coupon Bond'],
          ['THREEFYTP7', 'Term Premium on a 7 Year Zero Coupon Bond'],
          ['THREEFYTP8', 'Term Premium on a 8 Year Zero Coupon Bond'],
          ['THREEFYTP9', 'Term Premium on a 9 Year Zero Coupon Bond'],
          ['THREEFYTP10', 'Term Premium on a 10 Year Zero Coupon Bond'],
        ]

    name_code = {'us_bond_term_premium': code}
    update_fred_data(name_code, interest_rate_dir)


def update_us_yield_spread():
    code = [
            ['BAMLC0A0CM', 'US Corporate Spread'],
            ['BAMLC0A1CAAA', 'AAA US Corporate Spread'],
            ['BAMLC0A2CAA', 'AA US Corporate Spread'],
            ['BAMLC0A3CA', 'A US Corporate Spread'],
            ['BAMLC0A4CBBB', 'BBB US Corporate Spread'],
            ['BAMLH0A1HYBB', 'BB US Corporate Spread'],
            ['BAMLH0A2HYB', 'B US Corporate Spread'],
            ['BAMLH0A0HYM2', 'US High Yield Spread'],
            ['BAMLH0A3HYC', 'CCC AND LOWER US Corporate Spread'],
            ['BAMLEMCBPIOAS', 'Emerging Markets Corporate Spread'],
            ['BAMLH0A0HYM2EY', 'US High Yield Effective Yield'],
            ['AAAFF', 'Moody Seasoned Aaa Corporate Bond Minus Federal Funds Rate'],
            ['BAMLHE00EHYIOAS', 'ICE BofA Euro High Yield Index Option-Adjusted Spread'],

            ['MORTGAGE30US', '30-Year Fixed Rate Mortgage Average in the United States'],
    ]

    name_code = {'us_yield_spread': code}
    update_fred_data(name_code, interest_rate_dir)


def update_us_breakeven_inflation_rate():
    code = [
            ['T10YIE', '10-Year Breakeven Inflation Rate'],
            ['T5YIE', '5-Year Breakeven Inflation Rate'],
            ['T5YIFR', '5-Year, 5-Year Forward Inflation Expectation Rate'],
    ]

    name_code = {'us_breakeven_inflation_rate': code}
    update_fred_data(name_code, interest_rate_dir)


def update_us_inflation_expectation():
    code = [
            ['EXPINF1YR', '1Y'],
            ['EXPINF2YR', '2Y'],
            ['EXPINF3YR', '3Y'],
            ['EXPINF5YR', '5Y'],
            ['EXPINF7YR', '7Y'],
            ['EXPINF10YR', '10Y'],
            ['EXPINF10YR', '20Y'],
            ['EXPINF10YR', '30Y'],
    ]

    name_code = {'us_inflation_expectation': code}
    update_fred_data(name_code, interest_rate_dir)


def update_us_real_rate_expectation():
    code = [
            ['REAINTRATREARAT1MO', '1M'],
            ['REAINTRATREARAT1YE', '1Y'],
            ['REAINTRATREARAT10Y', '10Y'],
    ]

    name_code = {'us_real_rate_expectation': code}
    update_fred_data(name_code, interest_rate_dir)


def update_federal_fund_rate():
    code = [
            ['EFFR', 'Effective Federal Funds Rate'],
            ['DFF', 'Federal Funds Effective Rate'],
            ['EFFRVOL', 'Effective Federal Funds Volume'],
            ['DFEDTARU', 'Federal Funds Target Range - Upper Limit'],
            ['DFEDTARL', 'Federal Funds Target Range - Lower Limit'],
            ['SOFR', 'Secured Overnight Financing Rate'],
            ['SOFRVOL', 'Secured Overnight Financing Volume'],
            ['SOFR30DAYAVG', '30-Day Average SOFR'],
            ['SOFR90DAYAVG', '90-Day Average SOFR'],
            ['SOFRINDEX', 'SOFR Index'],
            ['OBFR', 'Overnight Bank Funding Rate'],
            ['IORB', 'Interest Rate on Reserve Balances'],
            ['DPRIME', 'Bank Prime Loan Rate'],
    ]

    name_code = {'federal_fund_rate': code}
    update_fred_data(name_code, interest_rate_dir)


# https://www.zerohedge.com/markets/verge-epic-showdown-demand-tlt-calls-explodes-record-treasury-futures-shorts-hit-all-time
def plot_us_treasury_fut_opt_volume_oi():
    plot_nasdaq_option_datas('TLT')

    path = os.path.join(interest_rate_dir, 'us_yield_curve'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    us02y = np.array(df['2Y'], dtype=float)
    us05y = np.array(df['5Y'], dtype=float)
    us10y = np.array(df['10Y'], dtype=float)

    cftc_plot_financial(t, us02y, 'US02Y', code='042601', inst_name='CBOT:US02Y')
    cftc_plot_financial(t, us05y, 'US05Y', code='044601', inst_name='CBOT:US05Y')
    cftc_plot_financial(t, us10y, 'US10Y', code='043602', inst_name='CBOT:US10Y')


def update_all_us_rate():
    # # yield curve
    update_treasury_yield()

    # term premium
    # https://fred.stlouisfed.org/series/THREEFYTP10
    update_us_bond_term_premium()

    # yield spred
    update_us_yield_spread()

    # breakeven inflation rate
    update_us_breakeven_inflation_rate()

    # effr, sofr, obfr, ...
    update_federal_fund_rate()


if __name__=="__main__":
    # plot_us_treasury_fut_opt_volume_oi()

    update_all_us_rate()

    # update_us_inflation_expectation()
    # update_us_real_rate_expectation()

    # TODO: https://fsapps.fiscal.treasury.gov/dts/issues/2019/1?sortOrder=desc#FY2019

