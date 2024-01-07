import os
import requests
import pandas as pd
import datetime
import numpy as np
from utils import *
from io import StringIO, BytesIO


def to_weekend(x):
    dt = pd.to_datetime(x, format='%Y-%m-%d')
    dt = dt + pd.Timedelta(days=(6-dt.weekday()))
    t = dt.strftime('%Y-%m-%d')
    return t

def to_monthend(x):
    dt = pd.to_datetime(x, format='%Y-%m-%d')
    last_day = calendar.monthrange(dt.year, dt.month)[-1]
    dt = dt + pd.Timedelta(days=(last_day-dt.day))
    t = dt.strftime('%Y-%m-%d')
    return t

def update_treasury_auction_data():
    se = requests.session()
    US_TREASURY_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                           'Host': 'api.fiscaldata.treasury.gov'}
    URL = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query?filter=record_date:gt:{}&format=csv&page[size]={}'

    earlist_time = '2000-01-01'
    page_size = 100

    while (1):
        path = os.path.join(treasury_auction_dir, 'auction.csv')
        if os.path.exists(path):
            old_df = pd.read_csv(path)
            t = pd.DatetimeIndex(pd.to_datetime(old_df['record_date'], format='%Y-%m-%d'))
            start_time_dt = t[-1] - pd.Timedelta(days=20)
            start_time = start_time_dt.strftime('%Y-%m-%d')
        else:
            start_time = earlist_time

        url = URL.format(start_time, str(page_size))
        print('TREASURY AUCTION FROM', start_time)
        r = se.get(url, headers=US_TREASURY_HEADERS)
        auction_df = pd.read_csv(StringIO(r.text))
        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, auction_df], axis=0)
            old_df.drop_duplicates(subset=['auction_date', 'security_type', 'security_term'], keep='last', inplace=True)
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            auction_df.to_csv(path, encoding='utf-8', index=False)


        security_type = np.array(auction_df['security_type'], dtype=str)
        # print(auction_df.columns.tolist())
        tips = np.array(auction_df['inflation_index_security'], dtype=str)
        floating_rate = np.array(auction_df['floating_rate'], dtype=str)
        df_dict = {}

        # Bill
        idx = np.where(security_type == 'Bill')[0]
        for i in idx:
            temp_df = pd.DataFrame(auction_df.loc[i]).T
            temp_df.reset_index(inplace=True, drop=True)
            security_term = temp_df.loc[0, 'security_term']
            if temp_df.loc[0, 'cash_management_bill_cmb'] == 'No':
                name = 'Bill' + '_' + security_term.split('-')[0] + 'W'
            elif temp_df.loc[0, 'cash_management_bill_cmb'] == 'Yes':
                name = 'CMB'
            else:
                continue

            weekend = pd.to_datetime(temp_df['auction_date'], format='%Y-%m-%d')
            for i in range(len(weekend)):
                weekend[i] = weekend[i] + pd.Timedelta(days=(6-weekend[i].weekday()))
            
            temp_df['period_end_date'] = temp_df['auction_date']
            temp_df['period_end_date'] = temp_df['period_end_date'].apply(to_weekend)

            temp_df = temp_df[['auction_date', 'period_end_date', 
                               'security_type', 'security_term', 'security_term_day_month', 'security_term_week_year',
                            'reopening', 'original_issue_date', 'original_security_term',
                            'announcemt_date', 'record_date', 'issue_date', 'maturity_date',
                            'price_per100', 
                            'allocation_pctage', 'avg_med_discnt_rate', 
                            'high_discnt_rate', 'high_investment_rate', 'high_price', 'low_discnt_rate',
                            'bid_to_cover_ratio',
                            'currently_outstanding', 'est_pub_held_mat_by_type_amt',
                            'total_accepted', 'total_tendered',
                            'offering_amt', 'comp_accepted', 'comp_tendered', 'noncomp_accepted',
                            'direct_bidder_accepted', 'direct_bidder_tendered', 'indirect_bidder_accepted', 'indirect_bidder_tendered',
                            'primary_dealer_accepted', 'primary_dealer_tendered', 'soma_accepted', 'soma_tendered', 'soma_holdings',
                            'treas_retail_accepted', 'fima_noncomp_accepted', 'fima_noncomp_tendered', 
                            ]]

            if name in df_dict:
                df_dict[name] = pd.concat([df_dict[name], temp_df], axis=0)
            else:
                df_dict[name] = temp_df.copy()


        idx = np.where(security_type == 'Note')[0]
        tips_idx = np.where(tips == 'Yes')[0]
        frn_idx = np.where(floating_rate == 'Yes')[0]
        note_idx = np.setdiff1d(idx, tips_idx)
        note_idx = np.setdiff1d(note_idx, frn_idx)

        # Note
        for i in note_idx:
            temp_df = pd.DataFrame(auction_df.loc[i]).T
            temp_df.reset_index(inplace=True, drop=True)
            try:
                original_security_term = temp_df.loc[0, 'original_security_term']
                name = 'Note' + '_' + original_security_term.split('-')[0] + 'Y'
            except:
                continue

            temp_df['period_end_date'] = temp_df['auction_date']
            temp_df['period_end_date'] = temp_df['period_end_date'].apply(to_monthend)

            temp_df = temp_df[['auction_date', 'period_end_date', 
                               'security_type', 'security_term', 'security_term_day_month', 'security_term_week_year',
                            'reopening', 'original_issue_date', 'original_security_term',
                            'announcemt_date', 'record_date', 'issue_date', 'maturity_date',
                            'strippable',
                            'price_per100', 
                            'allocation_pctage', 'avg_med_yield', 
                            'high_yield', 'high_price', 'int_rate', 'low_yield',
                            'bid_to_cover_ratio',
                            'currently_outstanding', 'est_pub_held_mat_by_type_amt',
                            'total_accepted', 'total_tendered',
                            'offering_amt', 'comp_accepted', 'comp_tendered', 'noncomp_accepted',
                            'direct_bidder_accepted', 'direct_bidder_tendered', 'indirect_bidder_accepted', 'indirect_bidder_tendered',
                            'primary_dealer_accepted', 'primary_dealer_tendered', 'soma_accepted', 'soma_tendered', 'soma_holdings',
                            'treas_retail_accepted', 'fima_noncomp_accepted', 'fima_noncomp_tendered', 
                            ]]

            if name in df_dict:
                df_dict[name] = pd.concat([df_dict[name], temp_df], axis=0)
            else:
                df_dict[name] = temp_df.copy()


        # Tips
        for i in tips_idx:
            temp_df = pd.DataFrame(auction_df.loc[i]).T
            temp_df.reset_index(inplace=True, drop=True)
            try:
                original_security_term = temp_df.loc[0, 'original_security_term']
                name = 'Tips' + '_' + original_security_term.split('-')[0] + 'Y'
            except:
                continue

            temp_df['period_end_date'] = temp_df['auction_date']
            temp_df['period_end_date'] = temp_df['period_end_date'].apply(to_monthend)

            temp_df = temp_df[['auction_date', 'period_end_date',
                               'security_type', 'security_term', 'security_term_day_month', 'security_term_week_year',
                            'reopening', 'original_issue_date', 'original_security_term',
                            'announcemt_date', 'record_date', 'issue_date', 'maturity_date',
                            'strippable',
                            'price_per100', 'adj_price', 'unadj_price',
                            'cpi_base_reference_period', 'index_ratio_on_issue_date',
                            'ref_cpi_on_dated_date', 'ref_cpi_on_issue_date', 'tiin_conversion_factor_per1000',
                            'allocation_pctage', 'avg_med_yield', 
                            'high_yield', 'high_price', 'int_rate', 'low_yield',
                            'bid_to_cover_ratio',
                            'currently_outstanding', 'est_pub_held_mat_by_type_amt',
                            'total_accepted', 'total_tendered',
                            'offering_amt', 'comp_accepted', 'comp_tendered', 'noncomp_accepted',
                            'direct_bidder_accepted', 'direct_bidder_tendered', 'indirect_bidder_accepted', 'indirect_bidder_tendered',
                            'primary_dealer_accepted', 'primary_dealer_tendered', 'soma_accepted', 'soma_tendered', 'soma_holdings',
                            'treas_retail_accepted', 'fima_noncomp_accepted', 'fima_noncomp_tendered', 
                            ]]

            if name in df_dict:
                df_dict[name] = pd.concat([df_dict[name], temp_df], axis=0)
            else:
                df_dict[name] = temp_df.copy()


        # Floating rate note
        for i in frn_idx:
            temp_df = pd.DataFrame(auction_df.loc[i]).T
            temp_df.reset_index(inplace=True, drop=True)
            try:
                original_security_term = temp_df.loc[0, 'original_security_term']
                name = 'FRN' + '_' + original_security_term.split('-')[0] + 'Y'
            except:
                continue

            temp_df['period_end_date'] = temp_df['auction_date']
            temp_df['period_end_date'] = temp_df['period_end_date'].apply(to_monthend)

            temp_df = temp_df[['auction_date', 'period_end_date', 
                               'security_type', 'security_term', 'security_term_day_month', 'security_term_week_year',
                            'reopening', 'original_issue_date', 'original_security_term',
                            'announcemt_date', 'record_date', 'issue_date', 'maturity_date',
                            'strippable',
                            'price_per100', 
                            'allocation_pctage', 'avg_med_discnt_margin', 
                            'frn_index_determination_date', 'frn_index_determination_rate', 
                            'high_discnt_margin', 'high_price', 'low_discnt_margin', 'spread',
                            'bid_to_cover_ratio',
                            'currently_outstanding', 'est_pub_held_mat_by_type_amt',
                            'total_accepted', 'total_tendered',
                            'offering_amt', 'comp_accepted', 'comp_tendered', 'noncomp_accepted',
                            'direct_bidder_accepted', 'direct_bidder_tendered', 'indirect_bidder_accepted', 'indirect_bidder_tendered',
                            'primary_dealer_accepted', 'primary_dealer_tendered', 'soma_accepted', 'soma_tendered', 'soma_holdings',
                            'treas_retail_accepted', 'fima_noncomp_accepted', 'fima_noncomp_tendered', 
                            ]]

            if name in df_dict:
                df_dict[name] = pd.concat([df_dict[name], temp_df], axis=0)
            else:
                df_dict[name] = temp_df.copy()

       
        # Bond
        bond_idx = np.where(security_type == 'Bond')[0]
        for i in bond_idx:
            temp_df = pd.DataFrame(auction_df.loc[i]).T
            temp_df.reset_index(inplace=True, drop=True)
            try:
                original_security_term = temp_df.loc[0, 'original_security_term']
                name = 'Bond' + '_' + original_security_term.split('-')[0] + 'Y'
            except:
                continue

            temp_df['period_end_date'] = temp_df['auction_date']
            temp_df['period_end_date'] = temp_df['period_end_date'].apply(to_monthend)

            temp_df = temp_df[['auction_date', 'period_end_date', 
                               'security_type', 'security_term', 'security_term_day_month', 'security_term_week_year',
                            'reopening', 'original_issue_date', 'original_security_term',
                            'announcemt_date', 'record_date', 'issue_date', 'maturity_date',
                            'strippable',
                            'price_per100', 
                            'allocation_pctage', 'avg_med_yield', 
                            'high_yield', 'high_price', 'int_rate', 'low_yield',
                            'bid_to_cover_ratio',
                            'currently_outstanding', 'est_pub_held_mat_by_type_amt',
                            'total_accepted', 'total_tendered',
                            'offering_amt', 'comp_accepted', 'comp_tendered', 'noncomp_accepted',
                            'direct_bidder_accepted', 'direct_bidder_tendered', 'indirect_bidder_accepted', 'indirect_bidder_tendered',
                            'primary_dealer_accepted', 'primary_dealer_tendered', 'soma_accepted', 'soma_tendered', 'soma_holdings',
                            'treas_retail_accepted', 'fima_noncomp_accepted', 'fima_noncomp_tendered', 
                            ]]

            if name in df_dict:
                df_dict[name] = pd.concat([df_dict[name], temp_df], axis=0)
            else:
                df_dict[name] = temp_df.copy()


        for name in df_dict:
            print('AUCTION', name)
            path = os.path.join(treasury_auction_dir, name+'.csv')
            df = df_dict[name]
            df.replace("/", '-', inplace=True)
            if os.path.exists(path):
                old_df = pd.read_csv(path)
                old_df = pd.concat([old_df, df], axis=0)
                old_df.drop_duplicates(subset=['auction_date'], keep='last', inplace=True)
                old_df['auction_date'] = old_df['auction_date'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
                old_df.sort_values(by = 'auction_date', inplace=True)
                old_df['auction_date'] = old_df['auction_date'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
                old_df.to_csv(path, encoding='utf-8', index=False)
            else:
                df['auction_date'] = df['auction_date'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
                df.sort_values(by = 'auction_date', inplace=True)
                df['auction_date'] = df['auction_date'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
                df.to_csv(path, encoding='utf-8', index=False)

        if len(auction_df) < page_size:
            break


def plot_treasury_auction_data():
    bill = ['Bill_4W', 'Bill_8W', 'Bill_13W', 'Bill_17W', 'Bill_26W', 'Bill_52W']
    cmb = ['CMB']
    frn = ['FRN_2Y']
    note = ['Note_2Y', 'Note_3Y', 'Note_5Y', 'Note_7Y', 'Note_10Y']
    bond = ['Bond_20Y', 'Bond_30Y']
    tips = ['Tips_5Y', 'Tips_10Y', 'Tips_20Y', 'Tips_30Y']

    df_dict = {}
    for security in bill+cmb+frn+note+bond+tips:
        path = os.path.join(treasury_auction_dir, security+'.csv')
        df = pd.read_csv(path)

        t = pd.DatetimeIndex(pd.to_datetime(df['auction_date'], format='%Y-%m-%d'))
        end_t = pd.DatetimeIndex(pd.to_datetime(df['period_end_date'], format='%Y-%m-%d'))
        df_dict[security] = [t, end_t, df]


    ### offering amount ###
    # bill+cmb
    datas = [
             [[],[],''],
    ]
    for security in bill+cmb:
        datas[0][0].append([df_dict[security][0], np.array(df_dict[security][2]['offering_amt'], dtype=float)/10000000000, security, 'style=dot_line'])
    plot_many_figure(datas)

    # frn+note
    datas = [
             [[],[],''],
    ]
    for security in frn+note:
        datas[0][0].append([df_dict[security][0], np.array(df_dict[security][2]['offering_amt'], dtype=float)/10000000000, security, 'style=dot_line'])
    plot_many_figure(datas)

    # bond
    datas = [
             [[],[],''],
    ]
    for security in bond:
        datas[0][0].append([df_dict[security][0], np.array(df_dict[security][2]['offering_amt'], dtype=float)/10000000000, security, 'style=dot_line'])
    plot_many_figure(datas)

    # tips
    datas = [
             [[],[],''],
    ]
    for security in tips:
        datas[0][0].append([df_dict[security][0], np.array(df_dict[security][2]['offering_amt'], dtype=float)/10000000000, security, 'style=dot_line'])
    plot_many_figure(datas)


    ### bid to cover ratio
    # bill
    datas = []
    for security in bill+cmb:
        datas.append(
            [[[df_dict[security][0], np.array(df_dict[security][2]['offering_amt'], dtype=float)/10000000000, security, 'style=dot_line']],
             [[df_dict[security][0], np.array(df_dict[security][2]['bid_to_cover_ratio'], dtype=float), 'bid to cover ratio', 'style=dot_line']],''],
        )
    plot_many_figure(datas, max_height=1500)

    # frn+note
    datas = []
    for security in frn+note:
        datas.append(
            [[[df_dict[security][0], np.array(df_dict[security][2]['offering_amt'], dtype=float)/10000000000, security, 'style=dot_line']],
             [[df_dict[security][0], np.array(df_dict[security][2]['bid_to_cover_ratio'], dtype=float), 'bid to cover ratio', 'style=dot_line']],''],
        )
    plot_many_figure(datas, max_height=1500)

    # bond
    datas = []
    for security in bond:
        datas.append(
            [[[df_dict[security][0], np.array(df_dict[security][2]['offering_amt'], dtype=float)/10000000000, security, 'style=dot_line']],
             [[df_dict[security][0], np.array(df_dict[security][2]['bid_to_cover_ratio'], dtype=float), 'bid to cover ratio', 'style=dot_line']],''],
        )
    plot_many_figure(datas, max_height=700)

    # tips
    datas = []
    for security in tips:
        datas.append(
            [[[df_dict[security][0], np.array(df_dict[security][2]['offering_amt'], dtype=float)/10000000000, security, 'style=dot_line']],
             [[df_dict[security][0], np.array(df_dict[security][2]['bid_to_cover_ratio'], dtype=float), 'bid to cover ratio', 'style=dot_line']],''],
        )
    plot_many_figure(datas, max_height=700)


def plot_onrrp_data():
    path = os.path.join(fed_dir, 'onrrp'+'.csv')
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    onrrp_amount = np.array(df['Overnight Reverse Repurchase Agreements: Treasury Securities Sold by the Federal Reserve in the Temporary Open Market Operations'], dtype=float)

    path = os.path.join(fed_dir, 'Factors Affecting Reserve Balances of Depository Institutions'+'.csv')
    df = pd.read_csv(path)
    t2 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    tga = np.array(df['Liabilities and Capital: Liabilities: Deposits with F.R. Banks, other than reserve balances: U.S. Treasury, General Account: Wednesday level'], dtype=float)

    datas = [
             [[[t1,onrrp_amount,'ONRRP $bn','color=black'],
               [t2,tga/1000,'TGA $bn','']
              ],
              [],''],
    ]
    plot_many_figure(datas, start_time='2015-01-01')


if __name__=="__main__":
    update_treasury_auction_data()

    # plot_treasury_auction_data()

    # plot_onrrp_data()




