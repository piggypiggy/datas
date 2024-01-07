import os
import requests
import pandas as pd
import datetime
import numpy as np
import sys
from utils import *
from bokeh.io import output_file, show
from bokeh.layouts import column, row, gridplot, layout
from bokeh.plotting import figure
from bokeh.models import LinearAxis, Range1d, VBar, DatetimeTickFormatter
from scipy.stats import linregress

# [inst_id, auction]
# inst_ids_info = [['sc2402', 1], ['sc2403', 1],
#                  ['au2402', 1], ['au2404', 1],
#                  ['ag2402', 1], ['ag2404', 1], 
#                  ['zn2401', 1], ['zn2402', 1], 
#                  ['al2401', 1], ['al2402', 1], 
#                  ['cu2401', 1], ['cu2402', 1],
#                  ['CF405', 0],
#                 ]

inst_ids_info = [['sc2402', 1],
                 ['au2402', 1], ['au2404', 1],
                 ['ag2402', 1], ['ag2404', 1], 
                 ['CF405', 0],
                ]

def fit_price():
    for inst_id_info in inst_ids_info:
        time.sleep(0.25)
        inst_id = inst_id_info[0]
        if inst_id[1].isdigit():
            variety = inst_id[0]
        else:
            variety = inst_id[0:2]
        for exchange in exchange_dict:
            if variety in exchange_dict[exchange]:
                break

        path = os.path.join(future_price_dir, exchange, variety+'.csv')
        if not os.path.exists(path):
            print("error", path)
            exit()
        fut_df = pd.read_csv(path, header=[0,1])
        fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))

        path = os.path.join(option_price_dir, exchange, inst_id+'.csv')
        if not os.path.exists(path):
            print("error", path)
            exit()
        opt_df = pd.read_csv(path, header=[0,1,2])
        opt_t = pd.DatetimeIndex(pd.to_datetime(opt_df['time']['time']['time'], format='%Y-%m-%d'))

        fut_price = None
        temp_fut_df = fut_df.loc[len(fut_df)-1, :]
        cs = ['c1','c2','c3','c4','c5','c6','c7','c8','c9']
        for c in cs:
            if (temp_fut_df[c]['inst_id'] == inst_id):
                fut_price = temp_fut_df[c]['close']
                break
        if fut_price is None:
            print('fut_price is None')
            exit()


        col = opt_df.columns.tolist()
        res = [(col[i][1]) for i in range(len(col)) if col[i][0] == 'P']
        strikes_str = []
        for i in res:
            if i not in strikes_str:
                strikes_str.append(i)
        strikes_str = np.array(strikes_str, dtype=str)
        strikes_dbl = np.array(strikes_str, dtype=float)
        sort = np.argsort(strikes_dbl)
        strikes_str = strikes_str[sort]

        strike = []
        call_strike = []
        call_price = []
        call_iv = []
        call_delta = []
        put_strike = []
        put_price = []
        put_iv = []
        put_delta = []

        for strike_str in strikes_str:
            strike = float(strike_str)
            if ((strike / fut_price) > 0.96) and ((strike / fut_price) < 1.15):
                call_strike.append(strike)
                call_price.append(opt_df.loc[len(opt_df)-1, pd.IndexSlice['C', strike_str, 'close']])
                call_delta.append(opt_df.loc[len(opt_df)-1, pd.IndexSlice['C', strike_str, 'delta_c']])
                call_iv.append(opt_df.loc[len(opt_df)-1, pd.IndexSlice['C', strike_str, 'imp_vol_c']])
            if ((strike / fut_price) > 0.85) and ((strike / fut_price) < 1.04):
                put_strike.append(strike)
                put_price.append(opt_df.loc[len(opt_df)-1, pd.IndexSlice['P', strike_str, 'close']])
                put_delta.append(opt_df.loc[len(opt_df)-1, pd.IndexSlice['P', strike_str, 'delta_c']])
                put_iv.append(opt_df.loc[len(opt_df)-1, pd.IndexSlice['P', strike_str, 'imp_vol_c']])

        call_strike = np.array(call_strike, dtype=float)
        call_price = np.array(call_price, dtype=float)
        call_delta = np.array(call_delta, dtype=float)
        call_iv = np.array(call_iv, dtype=float)

        put_strike = np.array(put_strike, dtype=float)
        put_price = np.array(put_price, dtype=float)
        put_delta = np.array(put_delta, dtype=float)
        put_iv = np.array(put_iv, dtype=float)

        print(call_strike)
        print(call_price)
        print(call_delta)
        print(call_iv)
        # fig1 = figure(frame_width=1400, frame_height=300, tools=TOOLS, title=inst_id)
        # fig1.line(call_strike, call_price, line_width=2, line_color='red', legend_label='CALL PRICE')
        # fig1.circle(x=call_strike, y=call_price, color='red', legend_label='CALL PRICE')
        # fig1.line(p_strike, p_price, line_width=2, line_color='darkgreen', legend_label='PUT PRICE')
        # fig1.circle(x=p_strike, y=p_price, color='darkgreen', legend_label='PUT PRICE')

        # fig2 = figure(frame_width=1400, frame_height=300, tools=TOOLS, x_range=fig1.x_range)
        # fig2.line(c_strike, c_delta, line_width=2, line_color='red', legend_label='CALL DELTA')
        # fig2.circle(x=c_strike, y=c_delta, color='red', legend_label='CALL DELTA')
        # fig2.line(p_strike, p_delta, line_width=2, line_color='darkgreen', legend_label='PUT DELTA')
        # fig2.circle(x=p_strike, y=p_delta, color='darkgreen', legend_label='PUT DELTA')

        # show(column(fig1, fig2))


        datas = [[[[call_strike, call_price, 'CALL PRICE', 'color=red,style=dot_line'],],
                  [[call_strike, call_delta, 'CALL DELTA', 'color=orange,style=dot_line'],],inst_id],

                [[[put_strike, put_price, 'PUT PRICE', 'color=darkgreen,style=dot_line'],],
                 [[put_strike, put_delta, 'CALL DELTA', 'color=blue,style=dot_line'],],''],
                ]
        plot_many_figure(datas, max_height=660, x_is_time=False)
       
        datas = [[[[call_strike, call_price, 'CALL PRICE', 'color=red,style=dot_line'],],
                  [[call_strike, call_iv, 'CALL IV', 'color=orange,style=dot_line'],],inst_id],

                [[[put_strike, put_price, 'PUT PRICE', 'color=darkgreen,style=dot_line'],],
                 [[put_strike, put_iv, 'CALL IV', 'color=blue,style=dot_line'],],''],
                ]
        plot_many_figure(datas, max_height=660, x_is_time=False)


if __name__=="__main__":

    fit_price()

    pass
