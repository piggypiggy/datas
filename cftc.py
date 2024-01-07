import os
import time
import requests
import zipfile
import numpy as np
import pandas as pd
import datetime
from bokeh.io import output_file, show
from bokeh.layouts import column
from bokeh.plotting import figure
from bokeh.models import LinearAxis, Range1d, VBar, NumeralTickFormatter
from scipy.stats import linregress
from utils import *
from datetime import date
from io import StringIO, BytesIO


def cot_year(year = 2020, cot_report_type = "legacy_fut"):    
    '''Downloads the selected COT report historical data for a single year
    from the cftc.gov webpage as zip file, unzips the downloaded folder and returns
    the cot data as DataFrame.
    For the current year selection, please note: updates by the CFTC occur typically weekly.
    Once the documents update by CFTC occured, the updated data can be accessed through 
    this function. The cot_report_type must match one of the following.

    COT report types:  
    "legacy_fut" as report type argument selects the Legacy futures only report,
    "legacy_futopt" the Legacy futures and options report,
    "supplemental_futopt" the Sumpplemental futures and options reports,
    "disaggregated_fut" the Disaggregated futures only report, 
    "disaggregated_futopt" the COT Disaggregated futures and options report, 
    "traders_in_financial_futures_fut" the Traders in Financial Futures futures only report, and 
    "traders_in_financial_futures_fut" the Traders in Financial Futures futures and options report. 
    
    Args:
        cot_report_type (str): selection of the COT report type. Defaults to "legacy_fut" (Legacy futures only report).
        cot_year(int) = year specification as YYYY
    
    Returns:
        A DataFrame with differing variables (depending on the selected report type). 
        
    Raises:
        ValueError: Raises an exception and returns the argument options.'''    
    print("Selected:", cot_report_type)
    try: 
        if cot_report_type== "legacy_fut": 
           rep = "deacot"
       
        elif cot_report_type == "legacy_futopt": 
           rep = "deahistfo"
      
        elif cot_report_type == "supplemental_futopt": 
           rep = "dea_cit_txt_"
 
        elif cot_report_type == "disaggregated_fut": 
           rep = "fut_disagg_txt_"

        elif cot_report_type == "disaggregated_futopt": 
           rep = "com_disagg_txt_"

        elif cot_report_type == "traders_in_financial_futures_fut": 
           rep = "fut_fin_txt_"

        elif cot_report_type == "traders_in_financial_futures_futopt": 
           rep = "com_fin_txt_"

    except ValueError:    
        print("""Input needs to be either:
                "legacy_fut", "legacy_futopt", supplemental_futopt",
                "disaggregated_fut", "disaggregated_futopt", 
                "traders_in_financial_futures_fut" or
                "traders_in_financial_futures_futopt" """)

    cot_url = "https://cftc.gov/files/dea/history/" + rep + str(year) + ".zip"
    CFTC_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                    'Host': 'www.cftc.gov',
                    'Cookie': '__cf_bm=q6yu4ElD9FzbKh5eQCqQXeJX61i9tODwmzEmOghOf44-1699692869-0-AaChK4jx52aSquoZu6+pwUBlvL2DUrzVC4oU3ldDg9u9VJx3/TYRIx47zdlbQKY5TGUEkcCJXJBJplRJyJIPWHA='}
    while (1):
        try:
            print(cot_url)
            r = requests.get(cot_url, headers=CFTC_HEADERS, timeout=30)
            break
        except Exception as e:
           print(e)
           time.sleep(5)

    z = zipfile.ZipFile(BytesIO(r.content))
    name = z.namelist()[0]
    # df = pd.read_csv(txt, low_memory=False)  
    df = pd.read_csv(z.open(name), delimiter=',')
    df.replace("\"", '', inplace=True)
    print("Downloaded single year data from:", year)
    return df



def concat_data1():
    path = os.path.join(cur_dir, 'cftc_fin_2017.csv')
    df = pd.read_csv(path)

    path = os.path.join(cur_dir, 'cftc_fin_2018.csv')
    df1 = pd.read_csv(path)   
    df = df.append(df1, ignore_index=True)

    path = os.path.join(cur_dir, 'cftc_fin_2019.csv')
    df1 = pd.read_csv(path)   
    df = df.append(df1, ignore_index=True)

    path = os.path.join(cur_dir, 'cftc_fin_2020.csv')
    df1 = pd.read_csv(path)   
    df = df.append(df1, ignore_index=True)

    path = os.path.join(cur_dir, 'cftc_fin_2021.csv')
    df1 = pd.read_csv(path)   
    df = df.append(df1, ignore_index=True)

    path = os.path.join(cur_dir, 'cftc_fin_2022.csv')
    df1 = pd.read_csv(path)   
    df = df.append(df1, ignore_index=True)

    path = os.path.join(cur_dir, 'cftc_fin_2023.csv')
    df1 = pd.read_csv(path)   
    df = df.append(df1, ignore_index=True)

    csv_path = os.path.join(future_position_dir, 'traders_in_financial_futures_futopt' + '.csv')
    df.to_csv(csv_path, encoding='utf-8', index=False)

def concat_data2():
    path = os.path.join(cur_dir, 'cftc_disaggregated_futopt_2017.csv')
    df = pd.read_csv(path)
    df.replace('.',0,inplace=True)

    path = os.path.join(cur_dir, 'cftc_disaggregated_futopt_2018.csv')
    df1 = pd.read_csv(path)   
    df1.replace('.',0,inplace=True)
    df = df.append(df1, ignore_index=True)

    path = os.path.join(cur_dir, 'cftc_disaggregated_futopt_2019.csv')
    df1 = pd.read_csv(path)   
    df1.replace('.',0,inplace=True)
    df = df.append(df1, ignore_index=True)

    path = os.path.join(cur_dir, 'cftc_disaggregated_futopt_2020.csv')
    df1 = pd.read_csv(path)   
    df1.replace('.',0,inplace=True)
    df = df.append(df1, ignore_index=True)

    path = os.path.join(cur_dir, 'cftc_disaggregated_futopt_2021.csv')
    df1 = pd.read_csv(path)   
    df1.replace('.',0,inplace=True)
    df = df.append(df1, ignore_index=True)

    path = os.path.join(cur_dir, 'cftc_disaggregated_futopt_2022.csv')
    df1 = pd.read_csv(path)   
    df1.replace('.',0,inplace=True)
    df = df.append(df1, ignore_index=True)

    path = os.path.join(cur_dir, 'cftc_disaggregated_futopt_2023.csv')
    df1 = pd.read_csv(path)   
    df1.replace('.',0,inplace=True)
    df = df.append(df1, ignore_index=True)

    csv_path = os.path.join(future_position_dir, 'disaggregated_futopt' + '.csv')
    df.to_csv(csv_path, encoding='utf-8', index=False)

def concat_data3():
    path = os.path.join(cur_dir, 'legacy_futopt_2017.csv')
    df = pd.read_csv(path)
    df.replace('.',0,inplace=True)

    path = os.path.join(cur_dir, 'legacy_futopt_2018.csv')
    df1 = pd.read_csv(path)   
    df1.replace('.',0,inplace=True)
    df = df.append(df1, ignore_index=True)

    path = os.path.join(cur_dir, 'legacy_futopt_2019.csv')
    df1 = pd.read_csv(path)   
    df1.replace('.',0,inplace=True)
    df = df.append(df1, ignore_index=True)

    path = os.path.join(cur_dir, 'legacy_futopt_2020.csv')
    df1 = pd.read_csv(path)   
    df1.replace('.',0,inplace=True)
    df = df.append(df1, ignore_index=True)

    path = os.path.join(cur_dir, 'legacy_futopt_2021.csv')
    df1 = pd.read_csv(path)   
    df1.replace('.',0,inplace=True)
    df = df.append(df1, ignore_index=True)

    path = os.path.join(cur_dir, 'legacy_futopt_2022.csv')
    df1 = pd.read_csv(path)   
    df1.replace('.',0,inplace=True)
    df = df.append(df1, ignore_index=True)

    path = os.path.join(cur_dir, 'legacy_futopt_2023.csv')
    df1 = pd.read_csv(path)   
    df1.replace('.',0,inplace=True)
    df = df.append(df1, ignore_index=True)

    csv_path = os.path.join(future_position_dir, 'legacy_futopt' + '.csv')
    df.to_csv(csv_path, encoding='utf-8', index=False)


def update_cftc_data1():
    csv_path = os.path.join(future_position_dir, 'traders_in_financial_futures_futopt' + '.csv')
    df = pd.read_csv(csv_path)

    df_new = cot_year(year = date.today().year, cot_report_type = 'traders_in_financial_futures_futopt')
    df = df._append(df_new, ignore_index=True)
    df = df.drop_duplicates()

    csv_path = os.path.join(future_position_dir, 'traders_in_financial_futures_futopt' + '.csv')
    df.to_csv(csv_path, encoding='utf-8', index=False)

def update_cftc_data2():
    csv_path = os.path.join(future_position_dir, 'disaggregated_futopt' + '.csv')
    df = pd.read_csv(csv_path)

    df_new = cot_year(year = date.today().year, cot_report_type = 'disaggregated_futopt')
    df_new.replace('.',0,inplace=True)
    csv_path = os.path.join(future_position_dir, 'tmp' + '.csv')
    df_new.to_csv(csv_path, encoding='utf-8', index=False)
    df_new = pd.read_csv(csv_path)

    df = df._append(df_new, ignore_index=True)
    df = df.drop_duplicates()

    csv_path = os.path.join(future_position_dir, 'disaggregated_futopt' + '.csv')
    df.to_csv(csv_path, encoding='utf-8', index=False)

def update_cftc_data3():
    csv_path = os.path.join(future_position_dir, 'legacy_futopt' + '.csv')
    df = pd.read_csv(csv_path)

    df_new = cot_year(year = date.today().year, cot_report_type = 'legacy_futopt')
    df_new.replace('.',0,inplace=True)
    csv_path = os.path.join(future_position_dir, 'tmp' + '.csv')
    df_new.to_csv(csv_path, encoding='utf-8', index=False)
    df_new = pd.read_csv(csv_path)

    df = df._append(df_new, ignore_index=True)
    df = df.drop_duplicates()

    csv_path = os.path.join(future_position_dir, 'legacy_futopt' + '.csv')
    df.to_csv(csv_path, encoding='utf-8', index=False)

def get_cftc_financial_position_data(name=None, code=None, exact_name=False):
    csv_path = os.path.join(future_position_dir, 'traders_in_financial_futures_futopt' + '.csv')
    df = pd.read_csv(csv_path)

    if (name != None):
        if (exact_name == True):
            df1 = df[df['Market_and_Exchange_Names'] == name]
        else:
            df1 = df[df['Market_and_Exchange_Names'].str.contains(name, na=True)]
    elif (code != None):
        df1 = df[df['CFTC_Contract_Market_Code'] == code]

    df1 = df1.sort_values(by = 'As_of_Date_In_Form_YYMMDD')

    t = pd.DatetimeIndex(pd.to_datetime(df1['Report_Date_as_YYYY-MM-DD'], format='%Y-%m-%d'))
    dealer_long = np.array(df1['Dealer_Positions_Long_All'], dtype=float)
    dealer_short = np.array(df1['Dealer_Positions_Short_All'], dtype=float)
    dealer_spread = np.array(df1['Dealer_Positions_Spread_All'], dtype=float)

    asset_mgr_long = np.array(df1['Asset_Mgr_Positions_Long_All'], dtype=float)
    asset_mgr_short = np.array(df1['Asset_Mgr_Positions_Short_All'], dtype=float)
    asset_mgr_spread = np.array(df1['Asset_Mgr_Positions_Spread_All'], dtype=float)

    lev_money_long = np.array(df1['Lev_Money_Positions_Long_All'], dtype=float)
    lev_money_short = np.array(df1['Lev_Money_Positions_Short_All'], dtype=float)
    lev_money_spread = np.array(df1['Lev_Money_Positions_Spread_All'], dtype=float)

    other_rept_long = np.array(df1['Other_Rept_Positions_Long_All'], dtype=float)
    other_rept_short = np.array(df1['Other_Rept_Positions_Short_All'], dtype=float)
    other_rept_spread = np.array(df1['Other_Rept_Positions_Spread_All'], dtype=float)

    none_rept_long = np.array(df1['NonRept_Positions_Long_All'], dtype=float)
    none_rept_short = np.array(df1['NonRept_Positions_Short_All'], dtype=float)

    return t, dealer_long, dealer_short, dealer_spread, asset_mgr_long, asset_mgr_short, asset_mgr_spread, \
            lev_money_long, lev_money_short, lev_money_spread, other_rept_long, other_rept_short, other_rept_spread, \
            none_rept_long, none_rept_short

def get_cftc_disaggregated_position_data(name=None, code=None, exact_name=False):
    csv_path = os.path.join(future_position_dir, 'disaggregated_futopt' + '.csv')
    df = pd.read_csv(csv_path)

    if (name != None):
        if (exact_name == True):
            df1 = df[df['Market_and_Exchange_Names'] == name]
        else:
            df1 = df[df['Market_and_Exchange_Names'].str.contains(name, na=True)]
    elif (code != None):
        df1 = df[df['CFTC_Contract_Market_Code'] == code]

    df1 = df1.sort_values(by = 'As_of_Date_In_Form_YYMMDD')

    t = pd.DatetimeIndex(pd.to_datetime(df1['Report_Date_as_YYYY-MM-DD'], format='%Y-%m-%d'))
    prod_merc_long = np.array(df1['Prod_Merc_Positions_Long_All'], dtype=float)
    prod_merc_short = np.array(df1['Prod_Merc_Positions_Short_All'], dtype=float)

    swap_long = np.array(df1['Swap_Positions_Long_All'], dtype=float)
    swap_short = np.array(df1['Swap__Positions_Short_All'], dtype=float)
    swap_spread = np.array(df1['Swap__Positions_Spread_All'], dtype=float)

    m_money_long = np.array(df1['M_Money_Positions_Long_All'], dtype=float)
    m_money_short = np.array(df1['M_Money_Positions_Short_All'], dtype=float)
    m_money_spread = np.array(df1['M_Money_Positions_Spread_All'], dtype=float)

    other_rept_long = np.array(df1['Other_Rept_Positions_Long_All'], dtype=float)
    other_rept_short = np.array(df1['Other_Rept_Positions_Short_All'], dtype=float)
    other_rept_spread = np.array(df1['Other_Rept_Positions_Spread_All'], dtype=float)

    none_rept_long = np.array(df1['NonRept_Positions_Long_All'], dtype=float)
    none_rept_short = np.array(df1['NonRept_Positions_Short_All'], dtype=float)

    return t, prod_merc_long, prod_merc_short, swap_long, swap_short, swap_spread, \
            m_money_long, m_money_short, m_money_spread, other_rept_long, other_rept_short, other_rept_spread, \
            none_rept_long, none_rept_short


def get_cftc_legacy_position_data(name=None, code=None, exact_name=False):
    csv_path = os.path.join(future_position_dir, 'legacy_futopt' + '.csv')
    df = pd.read_csv(csv_path)

    if (name != None):
        if (exact_name == True):
            df1 = df[df['Market and Exchange Names'] == name]
        else:
            df1 = df[df['Market and Exchange Names'].str.contains(name, na=True)]
    elif (code != None):
        df1 = df[df['CFTC Contract Market Code'] == code]

    df1 = df1.sort_values(by = 'As of Date in Form YYMMDD')

    t = pd.DatetimeIndex(pd.to_datetime(df1['As of Date in Form YYYY-MM-DD'], format='%Y-%m-%d'))
    noncom_long = np.array(df1['Noncommercial Positions-Long (All)'], dtype=float)
    noncom_short = np.array(df1['Noncommercial Positions-Short (All)'], dtype=float)
    noncom_spread = np.array(df1['Noncommercial Positions-Spreading (All)'], dtype=float)

    com_long = np.array(df1['Commercial Positions-Long (All)'], dtype=float)
    com_short = np.array(df1['Commercial Positions-Short (All)'], dtype=float)

    return t, noncom_long, noncom_short, noncom_spread, com_long, com_short


def cftc_plot_disaggregated(t00, data00, data00_name, t01=None, data01=None, data01_name=None, code=None, inst_name=''):
    if (t01 is None):
        fig1 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
        fig1.line(t00, data00, color='black', line_width=2, legend_label=data00_name)
        fig1.xaxis[0].ticker.desired_num_ticks = 20
    else:
        fig1 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
        fig1.y_range = Range1d(np.min(data00)*0.9, np.max(data00)*1.1)
        fig1.line(t00, data00, line_width=2, color='black', legend_label=data00_name)
        fig1.xaxis[0].ticker.desired_num_ticks = 20
        y_column2_name = 'y2'
        fig1.extra_y_ranges = {
            y_column2_name: Range1d(
                start=np.min(data01)*0.95,
                end=np.max(data01)*1.05,
            ),
        }
        fig1.line(t01, data01, line_width=2, color='blue', y_range_name=y_column2_name, legend_label=data01_name)
        fig1.add_layout(LinearAxis(y_range_name="y2"), 'left')
    fig1.legend.click_policy="hide"
    fig1.legend.location='top_left'

    # 仓位
    t1, prod_merc_long, prod_merc_short, swap_long, swap_short, swap_spread, \
            m_money_long, m_money_short, m_money_spread, other_rept_long, other_rept_short, other_rept_spread, \
            none_rept_long, none_rept_short = get_cftc_disaggregated_position_data(code=code)

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, prod_merc_long-prod_merc_short)
    fig2 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig2.varea(x=t1, y1=0, y2=prod_merc_long-prod_merc_short, fill_color='gray', legend_label=inst_name+' 期货期权 Prod_Merc 净多头')
    fig2.vbar(x=t1, bottom=0, top=prod_merc_long-prod_merc_short, width=0.05, color='dimgray')
    fig2.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig2.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig2.line(t1, prod_merc_long, line_width=2, color='red', legend_label=inst_name+' 期货期权 Prod_Merc 多头')
    fig2.line(t1, prod_merc_short, line_width=2, color='green', legend_label=inst_name+' 期货期权 Prod_Merc 空头')
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.location='top_left'

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, swap_long-swap_short)
    fig3 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig3.varea(x=t1, y1=0, y2=swap_long-swap_short, fill_color='gray', legend_label=inst_name+' 期货期权 Swap 净多头')
    fig3.vbar(x=t1, bottom=0, top=swap_long-swap_short, width=0.05, color='dimgray')
    fig3.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig3.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig3.line(t1, swap_long, line_width=2, color='red', legend_label=inst_name+' 期货期权 Swap 多头')
    fig3.line(t1, swap_short, line_width=2, color='green', legend_label=inst_name+' 期货期权 Swap 空头')
    fig3.xaxis[0].ticker.desired_num_ticks = 20
    fig3.legend.location='top_left'

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, m_money_long-m_money_short)
    fig4 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig4.varea(x=t1, y1=0, y2=m_money_long-m_money_short, fill_color='gray', legend_label=inst_name+' 期货期权 M_Money 净多头')
    fig4.vbar(x=t1, bottom=0, top=m_money_long-m_money_short, width=0.05, color='dimgray')
    fig4.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig4.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig4.line(t1, m_money_long, line_width=2, color='red', legend_label=inst_name+' 期货期权 M_Money 多头')
    fig4.line(t1, m_money_short, line_width=2, color='green', legend_label=inst_name+' 期货期权 M_Money 空头')
    fig4.xaxis[0].ticker.desired_num_ticks = 20
    fig4.legend.location='top_left'

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, other_rept_long-other_rept_short)
    fig5 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig5.varea(x=t1, y1=0, y2=other_rept_long-other_rept_short, fill_color='gray', legend_label=inst_name+' 期货期权 Other Rept 净多头')
    fig5.vbar(x=t1, bottom=0, top=other_rept_long-other_rept_short, width=0.05, color='dimgray')
    fig5.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig5.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig5.line(t1, other_rept_long, line_width=2, color='red', legend_label=inst_name+' 期货期权 Other Rept 多头')
    fig5.line(t1, other_rept_short, line_width=2, color='green', legend_label=inst_name+' 期货期权 Other Rept 空头')
    fig5.xaxis[0].ticker.desired_num_ticks = 20
    fig5.legend.location='top_left'

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, none_rept_long-none_rept_short)
    fig6 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig6.varea(x=t1, y1=0, y2=none_rept_long-none_rept_short, fill_color='gray', legend_label=inst_name+' 期货期权 None Rept 净多头')
    fig6.vbar(x=t1, bottom=0, top=none_rept_long-none_rept_short, width=0.05, color='dimgray')
    fig6.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig6.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig6.line(t1, none_rept_long, line_width=2, color='red', legend_label=inst_name+' 期货期权 None Rept 多头')
    fig6.line(t1, none_rept_short, line_width=2, color='green', legend_label=inst_name+' 期货期权 None Rept 空头')
    fig6.xaxis[0].ticker.desired_num_ticks = 20
    fig6.legend.location='top_left'

    show(column(fig1,fig4,fig2,fig3,fig5,fig6))
    time.sleep(0.25)

    # if (t01 is None):
    #     fig1 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
    #     fig1.line(t00, data00, color='black', line_width=2, legend_label=data00_name)
    #     fig1.xaxis[0].ticker.desired_num_ticks = 20
    # else:
    #     fig1 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
    #     fig1.y_range = Range1d(np.min(data00)*0.9, np.max(data00)*1.1)
    #     fig1.line(t00, data00, line_width=2, color='orange', legend_label=data00_name)
    #     fig1.xaxis[0].ticker.desired_num_ticks = 20
    #     y_column2_name = 'y2'
    #     fig1.extra_y_ranges = {
    #         y_column2_name: Range1d(
    #             start=np.min(data01)*0.95,
    #             end=np.max(data01)*1.05,
    #         ),
    #     }
    #     fig1.line(t01, data01, line_width=2, color='blue', y_range_name=y_column2_name, legend_label=data01_name)
    #     fig1.add_layout(LinearAxis(y_range_name="y2"), 'left')

    # pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, swap_spread)
    # fig3 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    # fig3.varea(x=t1, y1=0, y2=swap_spread, fill_color='gray', legend_label=inst_name+' 期货期权 Swap Spread')
    # fig3.vbar(x=t1, bottom=0, top=swap_spread, width=0.05, color='dimgray')
    # fig3.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    # fig3.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    # fig3.xaxis[0].ticker.desired_num_ticks = 20

    # pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, m_money_spread)
    # fig4 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    # fig4.varea(x=t1, y1=0, y2=m_money_spread, fill_color='gray', legend_label=inst_name+' 期货期权 M_Money Spread')
    # fig4.vbar(x=t1, bottom=0, top=m_money_spread, width=0.05, color='dimgray')
    # fig4.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    # fig4.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    # fig4.xaxis[0].ticker.desired_num_ticks = 20

    # pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, other_rept_spread)
    # fig5 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    # fig5.varea(x=t1, y1=0, y2=other_rept_spread, fill_color='gray', legend_label=inst_name+' 期货期权 Other Rept Spread')
    # fig5.vbar(x=t1, bottom=0, top=other_rept_spread, width=0.05, color='dimgray')
    # fig5.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    # fig5.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    # fig5.xaxis[0].ticker.desired_num_ticks = 20

    # show(column(fig1,fig4,fig3,fig5))
    # time.sleep(0.25)


    t2, noncom_long, noncom_short, noncom_spread, com_long, com_short = \
        get_cftc_legacy_position_data(code=code)
    
    if (t01 is None):
        fig1 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
        fig1.line(t00, data00, color='black', line_width=2, legend_label=data00_name)
        fig1.xaxis[0].ticker.desired_num_ticks = 20
    else:
        fig1 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
        fig1.y_range = Range1d(np.min(data00)*0.9, np.max(data00)*1.1)
        fig1.line(t00, data00, line_width=2, color='black', legend_label=data00_name)
        fig1.xaxis[0].ticker.desired_num_ticks = 20
        y_column2_name = 'y2'
        fig1.extra_y_ranges = {
            y_column2_name: Range1d(
                start=np.min(data01)*0.95,
                end=np.max(data01)*1.05,
            ),
        }
        fig1.line(t01, data01, line_width=2, color='blue', y_range_name=y_column2_name, legend_label=data01_name)
        fig1.add_layout(LinearAxis(y_range_name="y2"), 'left')
    fig1.legend.click_policy="hide"
    fig1.legend.location='top_left'

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t2, noncom_long-noncom_short)
    fig2 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig2.varea(x=t2, y1=0, y2=noncom_long-noncom_short, fill_color='gray', legend_label=inst_name+' 期货期权 Noncommercial 净多头')
    fig2.vbar(x=t1, bottom=0, top=noncom_long-noncom_short, width=0.05, color='dimgray')
    fig2.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig2.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig2.line(t2, noncom_long, line_width=2, color='red', legend_label=inst_name+' 期货期权 Noncommercial 多头')
    fig2.line(t2, noncom_short, line_width=2, color='green', legend_label=inst_name+' 期货期权 Noncommercial 空头')
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.location='top_left'

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t2, com_long-com_short)
    fig3 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig3.varea(x=t2, y1=0, y2=com_long-com_short, fill_color='gray', legend_label=inst_name+' 期货期权 Commercial 净多头')
    fig3.vbar(x=t1, bottom=0, top=com_long-com_short, width=0.05, color='dimgray')
    fig3.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig3.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig3.line(t2, com_long, line_width=2, color='red', legend_label=inst_name+' 期货期权 Commercial 多头')
    fig3.line(t2, com_short, line_width=2, color='green', legend_label=inst_name+' 期货期权 Commercial 空头')
    fig3.xaxis[0].ticker.desired_num_ticks = 20
    fig3.legend.location='top_left'

    show(column(fig1,fig2,fig3))



def cftc_plot_financial(t00, data00, data00_name, t01=None, data01=None, data01_name=None, code=None, inst_name=''):
    t1, dealer_long, dealer_short, dealer_spread, asset_mgr_long, asset_mgr_short, asset_mgr_spread, \
            lev_money_long, lev_money_short, lev_money_spread, other_rept_long, other_rept_short, other_rept_spread, \
            none_rept_long, none_rept_short = get_cftc_financial_position_data(code=code)

    if (t01 is None):
        fig1 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
        fig1.line(t00, data00, color='black', line_width=2, legend_label=data00_name)
        fig1.xaxis[0].ticker.desired_num_ticks = 20
    else:
        fig1 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
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

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, dealer_long-dealer_short)
    fig2 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig2.varea(x=t1, y1=0, y2=dealer_long-dealer_short, fill_color='gray', legend_label=inst_name+' 期货期权 Dealer 净多头')
    fig2.vbar(x=t1, bottom=0, top=dealer_long-dealer_short, width=0.05, color='dimgray')
    fig2.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig2.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig2.line(t1, dealer_long, line_width=2, color='red', legend_label=inst_name+' 期货期权 Dealer 多头')
    fig2.line(t1, dealer_short, line_width=2, color='green', legend_label=inst_name+' 期货期权 Dealer 空头')
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.location='top_left'

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, asset_mgr_long-asset_mgr_short)
    fig3 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig3.varea(x=t1, y1=0, y2=asset_mgr_long-asset_mgr_short, fill_color='gray', legend_label=inst_name+' 期货期权 Asset Mgr 净多头')
    fig3.vbar(x=t1, bottom=0, top=asset_mgr_long-asset_mgr_short, width=0.05, color='dimgray')
    fig3.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig3.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig3.line(t1, asset_mgr_long, line_width=2, color='red', legend_label=inst_name+' 期货期权 Asset Mgr 多头')
    fig3.line(t1, asset_mgr_short, line_width=2, color='green', legend_label=inst_name+' 期货期权 Asset Mgr 空头')
    fig3.xaxis[0].ticker.desired_num_ticks = 20
    fig3.legend.location='top_left'

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, lev_money_long-lev_money_short)
    fig4 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig4.varea(x=t1, y1=0, y2=lev_money_long-lev_money_short, fill_color='gray', legend_label=inst_name+' 期货期权 Leveraged Funds 净多头')
    fig4.vbar(x=t1, bottom=0, top=lev_money_long-lev_money_short, width=0.05, color='dimgray')
    fig4.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig4.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig4.line(t1, lev_money_long, line_width=2, color='red', legend_label=inst_name+' 期货期权 Leveraged Funds 多头')
    fig4.line(t1, lev_money_short, line_width=2, color='green', legend_label=inst_name+' 期货期权 Leveraged Funds 空头')
    fig4.xaxis[0].ticker.desired_num_ticks = 20
    fig4.legend.location='top_left'

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, other_rept_long-other_rept_short)
    fig5 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig5.varea(x=t1, y1=0, y2=other_rept_long-other_rept_short, fill_color='gray', legend_label=inst_name+' 期货期权 Other Rept 净多头')
    fig5.vbar(x=t1, bottom=0, top=other_rept_long-other_rept_short, width=0.05, color='dimgray')
    fig5.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig5.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig5.line(t1, other_rept_long, line_width=2, color='red', legend_label=inst_name+' 期货期权 Other Rept 多头')
    fig5.line(t1, other_rept_short, line_width=2, color='green', legend_label=inst_name+' 期货期权 Other Rept 空头')
    fig5.xaxis[0].ticker.desired_num_ticks = 20
    fig5.legend.location='top_left'

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, none_rept_long-none_rept_short)
    fig6 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig6.varea(x=t1, y1=0, y2=none_rept_long-none_rept_short, fill_color='gray', legend_label=inst_name+' 期货期权 None Rept 净多头')
    fig6.vbar(x=t1, bottom=0, top=none_rept_long-none_rept_short, width=0.05, color='dimgray')
    fig6.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig6.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig6.line(t1, none_rept_long, line_width=2, color='red', legend_label=inst_name+' 期货期权 None Rept 多头')
    fig6.line(t1, none_rept_short, line_width=2, color='green', legend_label=inst_name+' 期货期权 None Rept 空头')
    fig6.xaxis[0].ticker.desired_num_ticks = 20
    fig6.legend.location='top_left'

    show(column(fig1,fig4,fig2,fig3,fig5,fig6))
    time.sleep(0.25)

    # if (t01 is None):
    #     fig1 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
    #     fig1.line(t00, data00, color='black', line_width=2, legend_label=data00_name)
    #     fig1.xaxis[0].ticker.desired_num_ticks = 20
    # else:
    #     fig1 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
    #     fig1.y_range = Range1d(np.min(data00)*0.9, np.max(data00)*1.1)
    #     fig1.line(t00, data00, line_width=2, color='orange', legend_label=data00_name)
    #     fig1.xaxis[0].ticker.desired_num_ticks = 20
    #     y_column2_name = 'y2'
    #     fig1.extra_y_ranges = {
    #         y_column2_name: Range1d(
    #             start=np.min(data01)*0.9,
    #             end=np.max(data01)*1.1,
    #         ),
    #     }
    #     fig1.line(t01, data01, line_width=2, color='blue', y_range_name=y_column2_name, legend_label=data01_name)
    #     fig1.add_layout(LinearAxis(y_range_name="y2"), 'left')

    # pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, dealer_spread)
    # fig2 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    # fig2.varea(x=t1, y1=0, y2=dealer_spread, fill_color='gray', legend_label=inst_name+' 期货期权 Dealer Spread')
    # fig2.vbar(x=t1, bottom=0, top=dealer_spread, width=0.05, color='dimgray')
    # fig2.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    # fig2.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    # fig2.xaxis[0].ticker.desired_num_ticks = 20

    # pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, asset_mgr_spread)
    # fig3 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    # fig3.varea(x=t1, y1=0, y2=asset_mgr_spread, fill_color='gray', legend_label=inst_name+' 期货期权 Asset Mgr Spread')
    # fig3.vbar(x=t1, bottom=0, top=asset_mgr_spread, width=0.05, color='dimgray')
    # fig3.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    # fig3.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    # fig3.xaxis[0].ticker.desired_num_ticks = 20

    # pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, lev_money_spread)
    # fig4 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    # fig4.varea(x=t1, y1=0, y2=lev_money_spread, fill_color='gray', legend_label=inst_name+' 期货期权 Leveraged Funds Spread')
    # fig4.vbar(x=t1, bottom=0, top=lev_money_spread, width=0.05, color='dimgray')
    # fig4.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    # fig4.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    # fig4.xaxis[0].ticker.desired_num_ticks = 20

    # pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, other_rept_spread)
    # fig5 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    # fig5.varea(x=t1, y1=0, y2=other_rept_spread, fill_color='gray', legend_label=inst_name+' 期货期权 Other Rept Spread')
    # fig5.vbar(x=t1, bottom=0, top=other_rept_spread, width=0.05, color='dimgray')
    # fig5.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    # fig5.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    # fig5.xaxis[0].ticker.desired_num_ticks = 20

    # show(column(fig1,fig4,fig2,fig3,fig5))
    # time.sleep(0.25)

    t2, noncom_long, noncom_short, noncom_spread, com_long, com_short = \
        get_cftc_legacy_position_data(code=code)
    
    if (t01 is None):
        fig1 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
        fig1.line(t00, data00, color='black', line_width=2, legend_label=data00_name)
        fig1.xaxis[0].ticker.desired_num_ticks = 20
    else:
        fig1 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
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

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t2, noncom_long-noncom_short)
    fig2 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig2.varea(x=t2, y1=0, y2=noncom_long-noncom_short, fill_color='gray', legend_label=inst_name+' 期货期权 Noncommercial 净多头')
    fig2.vbar(x=t2, bottom=0, top=noncom_long-noncom_short, width=0.05, color='dimgray')
    fig2.varea(x=t2, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig2.varea(x=t2, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig2.line(t2, noncom_long, line_width=2, color='red', legend_label=inst_name+' 期货期权 Noncommercial 多头')
    fig2.line(t2, noncom_short, line_width=2, color='green', legend_label=inst_name+' 期货期权 Noncommercial 空头')
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.location='top_left'

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t2, com_long-com_short)
    fig3 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig3.varea(x=t2, y1=0, y2=com_long-com_short, fill_color='gray', legend_label=inst_name+' 期货期权 Commercial 净多头')
    fig3.vbar(x=t2, bottom=0, top=com_long-com_short, width=0.05, color='dimgray')
    fig3.varea(x=t2, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig3.varea(x=t2, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig3.line(t2, com_long, line_width=2, color='red', legend_label=inst_name+' 期货期权 Commercial 多头')
    fig3.line(t2, com_short, line_width=2, color='green', legend_label=inst_name+' 期货期权 Commercial 空头')
    fig3.xaxis[0].ticker.desired_num_ticks = 20
    fig3.legend.location='top_left'

    show(column(fig1,fig2,fig3))



if __name__=="__main__":
    # concat_data1()
    # concat_data2()
    # concat_data3()

    update_cftc_data1()
    update_cftc_data2()
    update_cftc_data3()
    pass






