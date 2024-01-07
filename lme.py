import os
import bs4
import requests
import re
import pandas as pd
import datetime
import time
import numpy as np
import akshare as ak
from openpyxl import load_workbook
from bokeh.io import output_file, show
from bokeh.layouts import column
from bokeh.plotting import figure
from bokeh.models import LinearAxis, Range1d, VBar, NumeralTickFormatter
from scipy.stats import linregress
from utils import *
from selenium import webdriver
from io import StringIO, BytesIO


metals = ['Copper',
          'Aluminium', 
          'Lead',
          'Zinc',
          'Nickel',
          'Tin']

metals_file_name = ['MiFID Weekly COTR Report CA',
          'MiFID Weekly COTR Report AH', 
          'MiFID Weekly COTR Report PB',
          'MiFID Weekly COTR Report ZS',
          'MiFID Weekly COTR Report NI',
          'MiFID Weekly COTR Report SN']

metals_url = [
              'https://www.lme.com/en/Market-data/Reports-and-data/Commitments-of-traders/Copper',
              'https://www.lme.com/en/Market-data/Reports-and-data/Commitments-of-traders/Aluminium',
              'https://www.lme.com/en/Market-data/Reports-and-data/Commitments-of-traders/LME-Lead',
              'https://www.lme.com/en/Market-data/Reports-and-data/Commitments-of-traders/LME-Zinc',
              'https://www.lme.com/en/Market-data/Reports-and-data/Commitments-of-traders/LME-Nickel',
              'https://www.lme.com/en/Market-data/Reports-and-data/Commitments-of-traders/LME-Tin']

# position 和 price 的 cookie 不一样
cookies = 'cf_clearance=nsYyoTsXuzawgaYFD_UbBwgUxaK52Cns5IOwOttc38c-1698914131-0-1-eb4b155b.4a2d83d5.bc4a4abb-250.0.0; lme#lang=en; ASP.NET_SessionId=p3f0ihgprz4jzgjcvmr0nzb1; shell#lang=en; __cf_bm=RVxy_dsrwh_bSTshGK0S0rK9JwF5LuFpjaOJL2Ec0Zc-1699529866-0-AfXKTMcrNHdL93ALpEPrMR8BAuPHzKTpCSj+yjhlkuEWjIb2dK7/2jw1dUOs3WvMppls82lxT32rQhBnnMTRSWk='
data = 'a8a20d87e06401fd2880a357a4ecf539448348976494393dbbfb58148e724079=4w9vQFkZPt1lz5ecA5DeF3zd5qNRAWyKQH16zHRl2Pk-1698914131-0-Ab369f5uD4Yuz029x6Q69hYviJHad5feiaqAiRMt5x6GF-WhOA3-gjMCU347iV3XmSMOp7raiF1prIXY7upDFOd4xpOuwb76V3qC33VJhkqrNfI3FAxDkC4wddhWaGa2In5Obe0fOyVXXMMaAZC3jpbPyKMlHCDfJjomwGkPto1tMdD1LB-1dxATpqjUmxLydcX1he94et9Ik6Ggwi-IQjTpbjD77RVjQmCjiTizaEKnlDuWM41EbsV7h9XOekhRTxTDY4_t7tP2SXamM55cJ51bajrDZMHj60XW_OVt0WlFtLpXmQL4RCq05G5ROo2SCAqKQaffrLCGfcozzIowA1EGlg7dYRI7DZtARQTyGcCOBJERCnNoqqvFCSOSjzq5wp7oOuUBO3WO8UL-KMoyjAbK8h8nZnMfRiKsgN3JrLi3Lw0i5-uTyFXgGg4osFtayoo5SOVhShX7ZhD_9n_FDXnIuvX_BzsHCpPVN7dlaLUG74l3xshAEWdWQ0Cmj8pkw5sxlxC-unr_hYukcAeLhC_PvqgwoOGuze57OK1UfeYlXazipbcMWJ-CUesWqgmTY9pSyaaCsfP586Lt4pAKamYi6H62NhCZwOsQpzIdj9tW4tJ5okOqY9gFrYr0giHXjOGrONliGPXu4hgA4T5RprUu_nmLUS8f-Nhgllrb4JZfKdxemA7AEU9sMkbKrU8exqs7QB7l5iEZnetiGwlDXhpbXOcyVVaUQjd2FoYXhFP-7bsf4C4dJ8VVlEDfuOnyEo50uJQ38aT-6xGx3dmFFNmscTMl9NE_BoBSHMJe_KFiaIyA79cHcViSw4OvvtyXMt_7em6LKhwt1Vs800YhhG-pm6PDqBasyPVDwfdzdbUZTMDCPBpTPTx-pK_NXacKRRBZ3BmP00ie9t-UlKdGWdRPMeiJxR7viwh1x5Dlif3E14wJWyWHKSeXUC_fkYvEZu1WZZA1L-NsvTyMmUSMLei_4PzqmAT6ONTLnX0BKA-u4qoGSKZOjJHQYakKwGdksMb2WditL0kmeOLizEL2IP5dLIG4bWJgxVsmZ-JFT-RCCjXpIn6lPFN6TKUo7gMVITzVC4ppjJ50PpNk3O0DOfbFoxvdC1Io_EDq3GqAQw7hwr23LNgcHRbiJSyDTIfeuyeOCYCZHLPsm08EZ-2FGirGV2xMuMEo-LlSZnGEoivL1_GfKTJeYQmsLmi_iuJbF4fZ0UGwcVkt1v_k33KIWaHRMkWE6HM6nbpJ32Dpc-7SuXeGAIRF6xOo3-ZSg1OaNpmLgodDDHz8codf4thgrWjb_2U9BkhebTSN5RPawpqcTtBp50QezMMW8lISxqBkfcM83WWvDnaO1eilnGXptxDeI3P68A2TcIkfYfDMBZ1n8VT9QQBXZzg8yfAhYKfj86wriQUOt1kjyurAHhpGmqW27p6Mpy-bVqCYkcqsHE1Aa7qN3ITXYiM1ie-A-FZINoSI1gDcAx63EDQKq5BcswcrLnAQdOvbc37uyMxA3zP8KlgeqdrAyIEgdegGY72MmmmTKLXDRQ-Pyi4vjb5mHRXlyNnKyQ2m98yDjMQg-C8nlODGwhNcgLSvodlWtYvI5nmaJptlMaImge4BiVkboFTmwkf8tcXciiB1sRUq3j3n1dJnyveTNoHZR9-6b_vhpXP0doQOwKkp_Tpps2b95QsKUFuW0go70NaHth9LddMTQ94jzl6IaRUSyqKN8B_B9WtfxWjgJG-3Qbw46EhUJ3e5xL9TTRNpQMJZmEKI8RXyiFsTywTKHSfB-Pzax_0C9Clbg2iGOqs4vKofaMoY9g7BGNi4P8wZSMI4EaV_D0uc_-3OMEfsrFo3X7AXBRzytFZxEkpY_vzEXgz2rhT2bflzuqpfuyryo2FVkUycAte4mXaXxJvpmtJOTX30NOtvykvLnv71PAETsCwCV7v95MOsWCKSnYTLoy0l1XTyZsmT3VP5PUlivdGCPRpeZRZFUYd-oLhCcpCNskW8w3_myaGcJhhEA9AywbuB1LS-7PHXVwo44kSsoKhiYbm4501Irgsr-UeFLY2F7c0ca1qK0Bul3Za9nxT1eK16vHLant1n9brxCCc3rMulfNs1jWp0fud2XJ-bdZ8R5k_6bwyefArxooYyET8lPIVI06oegv1FEaY7zfuFrZe2hJQ2n_e5bir7KOUoL_bmh3VKvOhR0Eea1LkKkKEQhM4XacSWv8BJBR6dMsTSgCsc_ASLtWGC9W4uP_vYNpJ2IVYc6GeG8sjrmBOHiJ76uSAjAOr4V2lA_LSkcWYfGHMqojwOTEQAvlqNWjGSQ-xTPR9PaOQFiIzQHynrh9d8--g2NHcfCX0v4ApqzBVgDXch6o69NtG3NFBN4m51FpxRDCelKzu7loMHLtc2o4698gxzXaPHNadfLLWXvklCHDMIWqUIr13eA-HoBHGbBGtlEMpTDzCSskhgpCy7V7o4S6wLQlOAIprwSEpiByUFR45G9264KnEGemIYbsOC0NFEUmVaGVFKNRZHuWXeb2wXXRCI8kRB91Dw6k5eP46CiNkGoj_SAA0oPXHwrnjYLxqixvcUITTmbILePG7JpJG71rOa36lpLlgWkeEqoBqaRg3ec8_sgSvgQAMEpOhx19NZddO9ZRQlmPcSphWscKZhCX_3fziJaz1gzs12bgK2DaXE-ydien5BzJSExB7chTzGrPGc5FlQu2yWCzFdtu3iAqQ3byIYNLvg3gtEesimILSjFhEQlCdehxBHuDXwe4_EnntLmgNCu7zWsGlMLi47Qc36lrWyhwr6paBtv7YOBkB28wnsR3iQhnCheI1Dohr5caMDIjLyxw-wVJVBOkRKobXvUCuFWYPEk8TT6Y-myWuKxLIBmv3p6eBSWXjlNhkpL3PsVTq6ZYgbq5Q8y-idcszWb_biAtxcXy6B7plE7CUoAB5Zm0PSpwy7hXxVo1fhfT8ysKp_DiYpkCIXim_qzu0F0NpazzUoO_06H1LleNzdqIrRHd7YR545BHNeLnM5Qn6HiJ__Gws6er1fogFKxu4xb9QGI9-ukNc-nOM-ET9lTa57bOLg6RJksaM-tTJK3izWwMQmnzp7uq1z9XGIqrjmiyIwHmRppQjWO3kMSjB1YOYrbh1pZdn931xZ9pZn6lE2WNBrzCc45Q61fEdKJADbxiEINrIFRHlyz2ydEVEJ7kbcNmMTNw0eY05B_IiBG3dwphduk3_SVe_57s5zvrCaEWcgwXC0hz_WAsF6jsg_jyt9QKQ5l16EXng4JFlsCF7jyS0EGcJ-sWiUYgzFMtw77RBHa0_u6geXNm4xZvqjhis5eVeZa8JPDXV7dJ7N7fV-6Iue_vJu4bOfGxHUUBrRAnr64OCn_XnW_GQtkH_KDAVkYt1YCNcBvAnIo1Ss9MsjCWtyll9kOGeEKutM58Car6zuZFQcm9ReaKy832fkcARfeCsBpb0hp08rr1oduSzsbSPzNB31Zb9bHD21Xy7TRlMLkNxCtO7QJwhj6rVpWY08mIi4F5O1vpWlH3Q8Y_lZ0ikzwdGjmVCYskOuCOafN5OYSWYpoOV5XQ4sb_85pNzzbsHc9KpStHFWxcb5H6qrRFbnCXa6q9G6ayk3EOgn6XaHhFpqwcpn1POhC01zo8nKu_sbDCX-EOtc_rM_RrRmowxdKGN9oJBGpPyVMIJ6_ugg4KHw936fx8igInvVqNDLp2c-vtMNn1oLrKnp8M5KHzkLDV9GMdZBsdHpiKtzTpToC1WPl0wL6tcEu95DcoybkGjWI-AXsT7h7W0cYN_7RFbnOSC1QQLXcXWs3iprJhM0qf2oW66BvB0F67QznpqbqSTMxrtpN8Bn-1R9Yq2z0wLl6CrZcqm_REf1hWny9--eJIPz8-fDdU6q8PYiPaceKwM-6POIY7PuneClEIb47nU0TZACXJd4Zo8DGDqWqTjQVN3vBQMEootovq09snVu37VR_Kaz_B5GgRVaQQN8d29UT-oxVEG6fa0C4QC_kLA0QZOR&62e3e305f64a43d5aa8b7d701fd23e3764697bb7cc66ec0b2b14c0f820d759bb=0e7864555aad31605c8f69b115cc7441&346272ed459c4cfff56e23394af751a82e3a6ff5ff686de4a81ae62a80c294a3=NqgzCEIgAzsG-1-81fb0b67085ece54&156b5ed5826c089f5cd9968d5678424d2c73dd972f4624c2b6a21570aeaabbef=1ed6d11cf3171977caaaa996c443376d%7C%7B%22managed_clearance%22%3A%22i%22%7D'

def init_lme_position_data():
    for i, metal in enumerate(metals): # loop through each metal  
        path = os.path.join(future_position_dir, 'lme', metal + '_LME' + '.csv')
        df = pd.read_csv(path)

        html_text = requests.get(metals_url[i]).text    
        soup = bs4.BeautifulSoup(html_text, 'lxml')
        body = soup.find_all(class_ = "link-download")

        for k in range(len(body)-1, -1, -1):
            match_string = re.search("Commitments of Traders Report -(.*)", str(body[k])).group(1) 
            print(match_string)
            date = re.search("(\d{2})", match_string).group(1).strip()
            month = re.search("\d{2}\s(\w*)", match_string).group(1).strip()
            year = re.search("(\d{4})", match_string).group(1).strip()
            if ((int(year) == 2020) or ((int(year) == 2021) and (month == 'January') and (int(date) <= 15))):
                date_on_report = datetime.datetime.strptime(f"{date}-{month}-{year}","%d-%B-%Y").date()
                download_link = f"https://www.lme.com{body[k]['href']}" # download link for latest file 
                while (1):
                    try:   
                        r = requests.get(download_link, allow_redirects=True) # download the excel file
                        break
                    except:
                        pass
                open('file.xlsx', 'wb').write(r.content)
                # time.sleep(0.25)
                z = load_workbook('file.xlsx')
                z.save('file.xls')
                print(z)
                report_data = pd.read_excel('file.xls')
                print(report_data)
            else:
                date_on_report = datetime.datetime.strptime(f"{date}-{month}-{year}","%d-%B-%Y").date()
                download_link = f"https://www.lme.com{body[k]['href']}" # download link for latest file 
                while (1):
                    try:   
                        r = requests.get(download_link, allow_redirects=True) # download the excel file
                        break
                    except:
                        pass
                open('file.xls', 'wb').write(r.content)

                report_data = pd.read_excel('file.xls')
                print(report_data)

            relevant_data = report_data.iloc[8:17, 3:11].T
            relevant_data_in_one_row = pd.DataFrame(relevant_data.to_numpy().flatten()).T
            relevant_data_in_one_row.columns = df.columns[1:]
            relevant_data_in_one_row['Date'] = date_on_report #add the report date   
            df = pd.concat([df, relevant_data_in_one_row], ignore_index=True)      
            os.remove('file.xls')

        # df = df.sort_values(by = 'Date')
        df.to_csv(path, encoding='utf-8', index=False)


def update_lme_position_data():
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Host": "www.lme.com",
        "Proxy-Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    }

    option = webdriver.FirefoxOptions()
    option.add_argument('--headless')
    # 实例化webdriver对象
    driver = webdriver.Firefox(options=option)
    driver.get('https://www.lme.com/Market-data/Reports-and-data/Commitments-of-traders')
    cookie = driver.get_cookies()
    driver.close()
    driver.quit()

    c = ''
    for i in range(len(cookie)):
        c += cookie[i]['name']
        c += '='
        c += cookie[i]['value']
        c += ';'
    headers['Cookie'] = c


    for i, metal in enumerate(metals): # loop through each metal  
        path = os.path.join(future_position_dir, 'lme', metal + '.csv')
        df = pd.read_csv(path)

        t = pd.DatetimeIndex(pd.to_datetime(df['Date'], format='%Y-%m-%d'))
        most_recent_time = t[-1]

        proxy = {'https':'127.0.0.1:8889'}
        proxy = None
        html_text = requests.get(metals_url[i], headers=headers, proxies=proxy).text   
        # print(html_text) 
        # return
        soup = bs4.BeautifulSoup(html_text, 'lxml')
        body = soup.find_all(class_ = "link-download")
        j = 0
        while (j < len(body)):
            match_string = re.search("Commitments of Traders Report -(.*)", str(body[j])).group(1) 
            date = re.search("(\d{2})", match_string).group(1).strip()
            month = re.search("\d{2}\s*(\w*)", match_string).group(1).strip()
            year = re.search("(\d{4})", match_string).group(1).strip()
            date_on_report = datetime.datetime.strptime(f"{date}-{month}-{year}","%d-%B-%Y").date()
            # 需要更新多少个report
            if (date_on_report <= most_recent_time.date()):
                break
            j = j + 1

        for k in range(j):
            match_string = re.search("Commitments of Traders Report -(.*)", str(body[k])).group(1) 
            date = re.search("(\d{2})", match_string).group(1).strip()
            month = re.search("\d{2}\s*(\w*)", match_string).group(1).strip()
            year = re.search("(\d{4})", match_string).group(1).strip()
            date_on_report = datetime.datetime.strptime(f"{date}-{month}-{year}","%d-%B-%Y").date()
            print(metal, date_on_report)
            download_link = f"https://www.lme.com{body[k]['href']}" # download link for latest file    
            r = requests.get(download_link, allow_redirects=True) # download the excel file
            open('file.xls', 'wb').write(r.content)
            
            report_data = pd.read_excel('file.xls')
            relevant_data = report_data.iloc[8:17, 3:11].T
            relevant_data_in_one_row = pd.DataFrame(relevant_data.to_numpy().flatten()).T
            relevant_data_in_one_row.columns = df.columns[1:]
            relevant_data_in_one_row['Date'] = date_on_report #add the report date   
            df = pd.concat([df, relevant_data_in_one_row], ignore_index=True)    
            os.remove('file.xls')  
              
        df.to_csv(path, encoding='utf-8', index=False)


def update_lme_position_data_from_file():
    for i, metal in enumerate(metals): # loop through each metal
        print(metal)
        path = os.path.join(future_position_dir, 'lme', metal + '.csv')
        df = pd.read_csv(path)

        xlsx_name = metals_file_name[i]
        directory = os.path.join(data_dir, 'excel')
        for _, _, files in os.walk(directory):
            for file in files:
                if ((xlsx_name in file)):
                    xlsx_path = os.path.join(directory, file)
                    break
        t = file[len(file)-12:len(file)-4]

        date_on_report = (datetime.datetime.strptime(f"{t[0:2]}-{t[2:4]}-{t[4:]}","%d-%m-%Y") - pd.Timedelta(days=4)).date()
        report_data = pd.read_excel(xlsx_path)
        relevant_data = report_data.iloc[8:17, 3:11].T
        relevant_data_in_one_row = pd.DataFrame(relevant_data.to_numpy().flatten()).T
        relevant_data_in_one_row.columns = df.columns[1:]
        relevant_data_in_one_row['Date'] = date_on_report #add the report date   
        df = pd.concat([df, relevant_data_in_one_row], ignore_index=True) 
        df.to_csv(path, encoding='utf-8', index=False)     


def get_lme_position_data(code):
    csv_path = os.path.join(future_position_dir, 'lme', code + '.csv')
    df = pd.read_csv(csv_path)

    t = pd.DatetimeIndex(pd.to_datetime(df['Date'], format='%Y-%m-%d'))
    ci_long_risk = np.array(df['CI Long Risk'], dtype=float)
    ci_long_other = np.array(df['CI Long Other'], dtype=float)
    ci_long_total = np.array(df['CI Long Total'], dtype=float)
    ci_short_risk = np.array(df['CI Short Risk'], dtype=float)
    ci_short_other = np.array(df['CI Short Other'], dtype=float)
    ci_short_total = np.array(df['CI Short Total'], dtype=float)

    if_long_risk = np.array(df['IF Long Risk'], dtype=float)
    if_long_other = np.array(df['IF Long Other'], dtype=float)
    if_short_risk = np.array(df['IF Short Risk'], dtype=float)
    if_short_other = np.array(df['IF Short Other'], dtype=float)

    ofi_long_risk = np.array(df['OFI Long Risk'], dtype=float)
    ofi_long_other = np.array(df['OFI Long Other'], dtype=float)
    ofi_short_risk = np.array(df['OFI Short Risk'], dtype=float)
    ofi_short_other = np.array(df['OFI Short Other'], dtype=float)

    cu_long_risk = np.array(df['CU Long Risk'], dtype=float)
    cu_long_other = np.array(df['CU Long Other'], dtype=float)
    cu_long_total = np.array(df['CU Long Total'], dtype=float)
    cu_short_risk = np.array(df['CU Short Risk'], dtype=float)
    cu_short_other = np.array(df['CU Short Other'], dtype=float)
    cu_short_total = np.array(df['CU Short Total'], dtype=float)

    long_risk = ci_long_risk + if_long_risk + ofi_long_risk + cu_long_risk
    short_risk = ci_short_risk + if_short_risk + ofi_short_risk + cu_short_risk
    long_other = ci_long_other + if_long_other + ofi_long_other + cu_long_other
    short_other = ci_short_other + if_short_other + ofi_short_other + cu_short_other

    return t, ci_long_total, ci_short_total, if_long_other, if_short_other, ofi_long_other, ofi_short_other, \
            cu_long_total, cu_short_total, long_risk, short_risk, long_other, short_other


def lme_plot_position(t00, data00, data00_name, t01=None, data01=None, data01_name=None, code=None, inst_name=''):
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

    # LME仓位
    t1, ci_long_total, ci_short_total, if_long_other, if_short_other, ofi_long_other, ofi_short_other, \
            cu_long_total, cu_short_total, long_risk, short_risk, long_other, short_other = get_lme_position_data(code=code)
    fig1.legend.click_policy="hide"
    fig1.legend.location='top_left'

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, ci_long_total-ci_short_total)
    fig2 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig2.varea(x=t1, y1=0, y2=ci_long_total-ci_short_total, fill_color='gray', legend_label=inst_name+' Investment Firms or Credit Institutions 净多头')
    fig2.vbar(x=t1, bottom=0, top=ci_long_total-ci_short_total, width=0.05, color='dimgray')
    fig2.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig2.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig2.line(t1, ci_long_total, line_width=2, color='red', legend_label=inst_name+' Investment Firms or Credit Institutions 多头')
    fig2.line(t1, ci_short_total, line_width=2, color='green', legend_label=inst_name+' Investment Firms or Credit Institutions 空头')
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.location='top_left'

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, if_long_other-if_short_other)
    fig3 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig3.varea(x=t1, y1=0, y2=if_long_other-if_short_other, fill_color='gray', legend_label=inst_name+' Investment Funds 净多头')
    fig3.vbar(x=t1, bottom=0, top=if_long_other-if_short_other, width=0.05, color='dimgray')
    fig3.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig3.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig3.line(t1, if_long_other, line_width=2, color='red', legend_label=inst_name+' Investment Funds 多头')
    fig3.line(t1, if_short_other, line_width=2, color='green', legend_label=inst_name+' Investment Funds 空头')
    fig3.xaxis[0].ticker.desired_num_ticks = 20
    fig3.legend.location='top_left'

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, ofi_long_other-ofi_short_other)
    fig4 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig4.varea(x=t1, y1=0, y2=ofi_long_other-ofi_short_other, fill_color='gray', legend_label=inst_name+' Other Financial Institutions 净多头')
    fig4.vbar(x=t1, bottom=0, top=ofi_long_other-ofi_short_other, width=0.05, color='dimgray')
    fig4.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig4.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig4.line(t1, ofi_long_other, line_width=2, color='red', legend_label=inst_name+' Other Financial Institutions 多头')
    fig4.line(t1, ofi_short_other, line_width=2, color='green', legend_label=inst_name+' Other Financial Institutions 空头')
    fig4.xaxis[0].ticker.desired_num_ticks = 20
    fig4.legend.location='top_left'

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, cu_long_total-cu_short_total)
    fig5 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig5.varea(x=t1, y1=0, y2=cu_long_total-cu_short_total, fill_color='gray', legend_label=inst_name+' Commercial Undertakings 净多头')
    fig5.vbar(x=t1, bottom=0, top=cu_long_total-cu_short_total, width=0.05, color='dimgray')
    fig5.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig5.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig5.line(t1, cu_long_total, line_width=2, color='red', legend_label=inst_name+' Commercial Undertakings 多头')
    fig5.line(t1, cu_short_total, line_width=2, color='green', legend_label=inst_name+' Commercial Undertakings 空头')
    fig5.xaxis[0].ticker.desired_num_ticks = 20
    fig5.legend.location='top_left'

    show(column(fig1,fig2,fig3,fig5,fig4))


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

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, long_other-short_other)
    fig2 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig2.varea(x=t1, y1=0, y2=long_other-short_other, fill_color='gray', legend_label=inst_name+' Other 净多头')
    fig2.vbar(x=t1, bottom=0, top=long_other-short_other, width=0.05, color='dimgray')
    fig2.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig2.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig2.line(t1, long_other, line_width=2, color='red', legend_label=inst_name+' Other 多头')
    fig2.line(t1, short_other, line_width=2, color='green', legend_label=inst_name+' Other 空头')
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.location='top_left'

    pct_corr_6m,bin_corr_6m,pct_corr_1y,bin_corr_1y,pct_corr_2y,bin_corr_2y,pct_corr_3y,bin_corr_3y = position_price_correlation(t00, data00, t1, long_risk-short_risk)
    fig3 = figure(frame_width=1400, frame_height=190, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig3.varea(x=t1, y1=0, y2=long_risk-short_risk, fill_color='gray', legend_label=inst_name+' Risk 净多头')
    fig3.vbar(x=t1, bottom=0, top=long_risk-short_risk, width=0.05, color='dimgray')
    fig3.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='pct_corr 6m,1y,2y,3y: '+str(pct_corr_6m)+', '+str(pct_corr_1y)+', '+str(pct_corr_2y)+', '+str(pct_corr_3y))
    fig3.varea(x=t1, y1=0, y2=0, fill_color='gray', legend_label='bin_corr 6m,1y,2y,3y: '+str(bin_corr_6m)+', '+str(bin_corr_1y)+', '+str(bin_corr_2y)+', '+str(bin_corr_3y))
    fig3.line(t1, long_risk, line_width=2, color='red', legend_label=inst_name+' Risk 多头')
    fig3.line(t1, short_risk, line_width=2, color='green', legend_label=inst_name+' Risk 空头')
    fig3.xaxis[0].ticker.desired_num_ticks = 20
    fig3.legend.location='top_left'

    show(column(fig1,fig2,fig3))


##################################### LME PRICE #####################################
def create_lme_price_file(path):
    if not os.path.exists(path):
        c1 = ['time', 'cash', '3M']
        df = pd.DataFrame(columns=c1)
        df.to_csv(path, encoding='utf-8', index=False)
        print('LME PRICE CREATE ' + path)


def update_all_lme_price():
    LME_CU_URL = "https://www.lme.com/api/trading-data/chart-data?datasourceId=39fabad0-95ca-491b-a733-bcef31818b16&startDate={}&endDate={}"
    LME_AL_URL = "https://www.lme.com/api/trading-data/chart-data?datasourceId=dddbc815-1a81-4f35-beed-6a193f4c946a&startDate={}&endDate={}"
    LME_PB_URL = "https://www.lme.com/api/trading-data/chart-data?datasourceId=9f2cf5c9-855d-4f68-939a-387babebe11f&startDate={}&endDate={}"
    LME_ZN_URL = "https://www.lme.com/api/trading-data/chart-data?datasourceId=1a1aca59-3032-4ea6-b22b-18b151514b84&startDate={}&endDate={}"
    LME_NI_URL = "https://www.lme.com/api/trading-data/chart-data?datasourceId=0ab0e715-84cd-41d1-8318-a96070917a43&startDate={}&endDate={}"
    LME_SN_URL = "https://www.lme.com/api/trading-data/chart-data?datasourceId=707be4f9-a4f5-4fe3-8f5b-7bd2886f58e7&startDate={}&endDate={}"

    LME_URLS = {'cu':LME_CU_URL,
                'al':LME_AL_URL,
                'pb':LME_PB_URL,
                'zn':LME_ZN_URL,
                'ni':LME_NI_URL,
                'sn':LME_SN_URL,
    }

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Host": "www.lme.com",
        "Proxy-Connection": "keep-alive",
        'Content-Length': '0',
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    }

    # option = webdriver.FirefoxOptions()
    # option.add_argument('--headless')
    # # 实例化webdriver对象
    # driver = webdriver.Firefox(options=option)
    # driver.get('https://www.lme.com/en/Metals/Non-ferrous/LME-Aluminium#Price+graphs')
    # cookie = driver.get_cookies()
    # driver.close()

    # c = ''
    # for i in range(len(cookie)):
    #     c += cookie[i]['name']
    #     c += '='
    #     c += cookie[i]['value']
    #     c += ';'
    # headers['Cookie'] = c

    current_time_dt = datetime.datetime.now()
    earlist_time_dt = datetime.datetime(year=current_time_dt.year-5, month=current_time_dt.month, day=current_time_dt.day)
    earlist_time = earlist_time_dt.strftime('%Y-%m-%d')

    for variety in LME_URLS:
        path = os.path.join(lme_price_dir, variety+'.csv')
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
                    last_line_dt = pd.to_datetime(last_line[:10], format='%Y-%m-%d')
                    start_time_dt = last_line_dt + pd.Timedelta(days=1)
                    start_time = start_time_dt.strftime('%Y%m%d')
                except:
                    start_time = earlist_time
                    start_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')
                print('LME PRICE UPDATE ' + path)
        else:
            c1 = ['time', 'cash_bid', 'cash_ask', '3M_bid', '3M_ask']
            df = pd.DataFrame(columns=c1)
            df.to_csv(path, encoding='utf-8', index=False)
            print('LME PRICE CREATE ' + path)
            start_time = earlist_time
            start_time_dt = pd.to_datetime(start_time, format='%Y-%m-%d')

        while (start_time_dt < current_time_dt):
            end_time_dt = datetime.datetime(start_time_dt.year+2, 1, 1) # 一次拿4年的数据
            if (end_time_dt > current_time_dt):
                end_time_dt = current_time_dt
            start_time = start_time_dt.strftime('%Y-%m-%d')
            end_time = end_time_dt.strftime('%Y-%m-%d')

            url = LME_URLS[variety].format(start_time, end_time)
            print(url)
            r = requests.get(url, headers=headers)
            if (r.status_code == 200):
                data_json = r.json()
                # print(data_json)
                temp_df1 = pd.DataFrame(data_json["Labels"])
                temp_df2 = pd.DataFrame(data_json["Datasets"][0]['Data'])
                temp_df3 = pd.DataFrame(data_json["Datasets"][1]['Data'])
                temp_df4 = pd.DataFrame(data_json["Datasets"][2]['Data'])
                temp_df5 = pd.DataFrame(data_json["Datasets"][3]['Data'])
                df = pd.concat([temp_df1, temp_df2, temp_df3, temp_df4, temp_df5], axis=1)
                df.columns = ['time', 'cash_bid', 'cash_ask', '3M_bid', '3M_ask']
                df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%d/%m/%Y'))
                df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))

                if (len(df) > 0):
                    path = os.path.join(lme_price_dir, variety+'.csv')
                    old_df = pd.read_csv(path)
                    old_df = pd.concat([old_df, df], axis=0)
                    old_df.drop_duplicates(subset=['time'], inplace=True)
                    old_df.to_csv(path, encoding='utf-8', index=False)
            else:
                print('status_code ==', r.status_code)

            start_time_dt = end_time_dt + pd.Timedelta(days=1)

def get_lme_future_stock():
    path = os.path.join(data_dir, 'lme_stock'+'.csv')
    lme_stock_df = ak.macro_euro_lme_stock()
    lme_stock_df.rename_axis("time", inplace=True)
    print('LME stock')
    lme_stock_df.to_csv(path, encoding='utf-8')


def update_lme_option_data():
    se = requests.session()
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Host": "www.lme.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    }
    url = 'https://www.lme.com/en/Market-data/Reports-and-data/Open-interest/Exchange-open-interest'
    
    path = os.path.join(option_price_dir, 'lme', 'CA_info'+'.csv')
    if os.path.exists(path):
        df = pd.read_csv(path)
        tt = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
        start_time_dt = tt[-1] + pd.Timedelta(days=1)
    else:
        tt = []

    option = webdriver.FirefoxOptions()
    option.add_argument('--headless')
    # 实例化webdriver对象
    driver = webdriver.Firefox(options=option)
    driver.get(url)
    cookie = driver.get_cookies()

    soup = bs4.BeautifulSoup(driver.page_source, 'html.parser')
    driver.close()
    driver.quit()
    aa = soup.find_all(name='a', string=re.compile('Exchange Open Interest - base'))
    for a in aa:
        url = a['href']
        if 'OPI' in url:
            s = url.split('EOI-base/')[1][:8]
            t = s[:4] + '-' + s[4:6] + '-' + s[6:]

        if '---base---' in url:
            s = url.split('---base---')[1].split('.')[0]
            s = s.replace('_', '')
            t = pd.to_datetime(s, format='%d-%B-%Y').strftime('%Y-%m-%d')

        dt = pd.to_datetime(t, format='%Y-%m-%d')
        if dt in tt:
            continue

        while (1):
            try:
                r = se.get('https://www.lme.com/'+url, headers=headers)
                eoi_df = pd.read_csv(StringIO(r.text), dtype=str)
                underlying = np.array(eoi_df['UNDERLYING'], dtype=str)
                currency = np.array(eoi_df['CURRENCY'], dtype=str)
                contract_type = np.array(eoi_df['CONTRACT_TYPE'], dtype=str)
                opt_type = np.array(eoi_df['SUB_CONTRACT_TYPE'], dtype=str)
                expiry_month = np.array(eoi_df['FORWARD_MONTH'], dtype=str)
                strike = np.array(eoi_df['STRIKE'], dtype=str)
                volume = np.array(eoi_df['TURNOVER'], dtype=float)
                oi = np.array(eoi_df['OPEN_INTEREST'], dtype=float)

                break
            except Exception as e:
                print(e)
                time.sleep(15)

        for variety in ['CA', 'AH', 'PB', 'ZS', 'NI', 'SN']:
            print(t, variety, 'Option')
            z = np.logical_and(underlying == variety, currency == 'USD')
            z = np.logical_and(z, contract_type == 'LMEOption')
            w = np.where(z)[0]

            if len(w) == 0:
                continue

            data_dict = {}
            for i in w:
                if expiry_month[i] in data_dict:
                    data_dict[expiry_month[i]][0] += [volume[i], oi[i]]
                    data_dict[expiry_month[i]][1] += [opt_type[i], opt_type[i]]
                    data_dict[expiry_month[i]][2] += [strike[i], strike[i]]
                    data_dict[expiry_month[i]][3] += ['volume', 'oi']
                else:
                    data_dict[expiry_month[i]] = [[t, volume[i], oi[i]], ['time', opt_type[i], opt_type[i]], ['time', strike[i], strike[i]], ['time', 'volume', 'oi']]

            names = []
            ois = []
            for m in data_dict:
                name = variety + m[2:]
                path = os.path.join(option_price_dir, 'lme', name+'.csv')
                df = pd.DataFrame(columns=[data_dict[m][1], data_dict[m][2], data_dict[m][3]], data=[data_dict[m][0]])

                # oi
                names += [name]
                z1 = np.logical_and(z, expiry_month == m)
                w = np.where(z1)[0]
                ois += [np.sum(oi[w])]

                if os.path.exists(path):
                    old_df = pd.read_csv(path, header=[0,1,2])
                    old_df = pd.concat([old_df, df], axis=0)
                    old_df.drop_duplicates(subset=[('time','time','time')], keep='last', inplace=True) # last
                    old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
                    old_df.sort_values(by = ('time','time','time'), inplace=True)
                    old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
                    old_df.to_csv(path, encoding='utf-8', index=False)
                else:
                    df.to_csv(path, encoding='utf-8', index=False)

            names = np.array(names, dtype=str)
            ois = np.array(ois, dtype=float)
            sort = np.argsort(ois)[::-1]
            names = names[sort]
            ois = ois[sort]

            name_string = ''
            for name in names:
                name_string += name
                name_string += ','
            info_df = pd.DataFrame(columns=['time', 'inst_ids'], data=[[t, name_string]])

            path = os.path.join(option_price_dir, 'lme', variety+'_info'+'.csv')
            # print(df)
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



def update_lme_option_indicator():
    for variety in ['CA', 'AH', 'PB', 'ZS', 'NI', 'SN']:
        path = os.path.join(option_price_dir, 'lme', variety+'_info'+'.csv')
        if not(os.path.exists(path)):
            continue

        # 最大成交量
        path = os.path.join(option_price_dir, 'lme', variety+'_info'+'.csv')
        if not(os.path.exists(path)):
            return
        info_df = pd.read_csv(path)
        info_t = pd.DatetimeIndex(pd.to_datetime(info_df['time'], format='%Y-%m-%d'))
        # inst_ids
        # maxoi_contracts = np.array(info_df['dom1'])

        path = os.path.join(option_price_dir, 'lme', variety+'_indicator'+'.csv')
        if os.path.exists(path):
            ind_df = pd.read_csv(path)
            ind_t = pd.DatetimeIndex(pd.to_datetime(ind_df['time'], format='%Y-%m-%d'))
            start_idx = np.where(info_t == ind_t[-1])[0][0] + 1
        else:
            start_idx = 0

        #
        opt_dict = {}
        _df = pd.DataFrame()
        for i in range(start_idx, len(info_df)):
            print(variety, info_t[i])
            df = pd.DataFrame()
            t = info_t[i]
            df['time'] = [t.strftime('%Y-%m-%d')]
            df['total_put_volume'] = [0]
            df['total_call_volume'] = [0]
            df['total_put_oi'] = [0]
            df['total_call_oi'] = [0]

            df['top1_total_put_volume'] = [0]
            df['top1_total_call_volume'] = [0]
            df['top1_total_put_oi'] = [0]
            df['top1_total_call_oi'] = [0]

            df['top3_total_put_volume'] = [0]
            df['top3_total_call_volume'] = [0]
            df['top3_total_put_oi'] = [0]
            df['top3_total_call_oi'] = [0]

            df['top5_total_put_volume'] = [0]
            df['top5_total_call_volume'] = [0]
            df['top5_total_put_oi'] = [0]
            df['top5_total_call_oi'] = [0]

            df['top10_total_put_volume'] = [0]
            df['top10_total_call_volume'] = [0]
            df['top10_total_put_oi'] = [0]
            df['top10_total_call_oi'] = [0]

            inst_ids = info_df.loc[i, 'inst_ids'].split(',')
            inst_ids.remove('')

            L1 = len(info_t)

            n = 0
            for inst_id in inst_ids:
                n += 1
                if (inst_id == ''):
                    continue
                if not(inst_id in opt_dict):
                    try:
                        path3 = os.path.join(option_price_dir, 'lme', inst_id+'.csv')
                    except:
                        continue
                    opt_df = pd.read_csv(path3, header=[0,1,2]).fillna(0)
                    opt_t = pd.DatetimeIndex(pd.to_datetime(opt_df['time']['time']['time'], format='%Y-%m-%d'))
                    strike = get_full_strike_price(opt_df)
                    opt_dict[inst_id] = [opt_df, opt_t, strike]

                opt_df = opt_dict[inst_id][0]
                opt_t = opt_dict[inst_id][1]
                strike = opt_dict[inst_id][2]

                try:
                    w = np.where(opt_t == info_t[i])[0][0]
                except:
                    print('opt_t == info_t[i], ', inst_id, info_t[i])
                    continue
                temp_df = opt_df.loc[w,:]

                # volume
                try:
                    put_volume = temp_df.loc[pd.IndexSlice['P', :, 'volume']].sum()
                except:
                    put_volume = 0

                try:
                    call_volume = temp_df.loc[pd.IndexSlice['C', :, 'volume']].sum()
                except:
                    call_volume = 0

                df['total_put_volume'] += put_volume
                df['total_call_volume'] += call_volume
                if n <= 1:
                    df['top1_total_put_volume'] += put_volume
                    df['top1_total_call_volume'] += call_volume
                if n <= 3:
                    df['top3_total_put_volume'] += put_volume
                    df['top3_total_call_volume'] += call_volume
                if n <= 5:
                    df['top5_total_put_volume'] += put_volume
                    df['top5_total_call_volume'] += call_volume
                if n <= 10:
                    df['top10_total_put_volume'] += put_volume
                    df['top10_total_call_volume'] += call_volume

                # oi
                try:
                    put_oi = temp_df.loc[pd.IndexSlice['P', :, 'oi']].sum()
                except:
                    put_oi = 0

                try:
                    call_oi = temp_df.loc[pd.IndexSlice['C', :, 'oi']].sum()
                except:
                    call_oi = 0

                df['total_put_oi'] += put_oi
                df['total_call_oi'] += call_oi
                if n <= 1:
                    df['top1_total_put_oi'] += put_oi
                    df['top1_total_call_oi'] += call_oi
                if n <= 3:
                    df['top3_total_put_oi'] += put_oi
                    df['top3_total_call_oi'] += call_oi
                if n <= 5:
                    df['top5_total_put_oi'] += put_oi
                    df['top5_total_call_oi'] += call_oi
                if n <= 10:
                    df['top10_total_put_oi'] += put_oi
                    df['top10_total_call_oi'] += call_oi

            _df = pd.concat([_df, df], axis=0)

        path = os.path.join(option_price_dir, 'lme', variety+'_indicator'+'.csv')
        # print(df)
        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, _df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='first', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            old_df.sort_values(by = 'time', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            _df.to_csv(path, encoding='utf-8', index=False)  


def plot_lme_option_data(var):
    VARIETY_DICT = {
        'cu': 'CA',
        "al": 'AH',
        'pb': 'PB',
        'zn': 'ZS',
        'ni': 'NI',
        'sn': 'SN',
    }

    if not(var in VARIETY_DICT):
        return

    variety = VARIETY_DICT[var]

    path = os.path.join(option_price_dir, 'lme', variety+'_indicator.csv')
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    total_put_oi = np.array(df['total_put_oi'], dtype=float)
    total_call_oi = np.array(df['total_call_oi'], dtype=float)
    total_put_volume = np.array(df['total_put_volume'], dtype=float)
    total_call_volume = np.array(df['total_call_volume'], dtype=float)

    top1_total_put_oi = np.array(df['top1_total_put_oi'], dtype=float)
    top1_total_call_oi = np.array(df['top1_total_call_oi'], dtype=float)
    top1_total_put_volume = np.array(df['top1_total_put_volume'], dtype=float)
    top1_total_call_volume = np.array(df['top1_total_call_volume'], dtype=float)

    top3_total_put_oi = np.array(df['top3_total_put_oi'], dtype=float)
    top3_total_call_oi = np.array(df['top3_total_call_oi'], dtype=float)
    top3_total_put_volume = np.array(df['top3_total_put_volume'], dtype=float)
    top3_total_call_volume = np.array(df['top3_total_call_volume'], dtype=float)

    top5_total_put_oi = np.array(df['top5_total_put_oi'], dtype=float)
    top5_total_call_oi = np.array(df['top5_total_call_oi'], dtype=float)
    top5_total_put_volume = np.array(df['top5_total_put_volume'], dtype=float)
    top5_total_call_volume = np.array(df['top5_total_call_volume'], dtype=float)

    top10_total_put_oi = np.array(df['top10_total_put_oi'], dtype=float)
    top10_total_call_oi = np.array(df['top10_total_call_oi'], dtype=float)
    top10_total_put_volume = np.array(df['top10_total_put_volume'], dtype=float)
    top10_total_call_volume = np.array(df['top10_total_call_volume'], dtype=float)
    
    
    CFD_DICT = {
        'cu': 'COPPER',
        "al": 'ALUMINUM',
        'pb': 'LEAD',
        'zn': 'ZINC',
        'ni': 'NICKLE',
        'sn': 'TIN',
    }
    path = os.path.join(cfd_dir, CFD_DICT[var]+'_CFD.csv')
    df1 = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    price = np.array(df1['close'], dtype=float)


    datas = [
             [[[t1,price,CFD_DICT[var]+' CFD','color=black']],
              [[t,total_put_oi/total_call_oi,'total_oi pcr','color=blue']],''],

             [[[t,total_put_oi,'total_put_oi','color=darkgreen'],
               [t,total_call_oi,'total_call_oi','color=red']],
              [[t,total_put_oi-total_call_oi,'total_put_oi-total_call_oi','style=vbar'],],''],

             [[[t1,price,CFD_DICT[var]+' CFD','color=black']],
              [[t,total_put_volume/total_call_volume,'total_volume pcr','color=blue']],''],

             [[[t,total_put_volume,'total_put_volume','color=darkgreen'],
               [t,total_call_volume,'total_call_volume','color=red']],
              [[t,total_put_volume-total_call_volume,'total_put_volume-total_call_volume','style=vbar'],],''],

             [[[t,top3_total_put_oi,'top3_total_put_oi','color=darkgreen'],
               [t,top3_total_call_oi,'top3_total_call_oi','color=red']],
              [[t,top3_total_put_oi-top3_total_call_oi,'top3_total_put_oi-top3_total_call_oi','style=vbar'],],''],

    ]
    plot_many_figure(datas, start_time='2020-08-01')


if __name__=="__main__":
    # plot_lme_option_data('pb')
    # update_lme_option_indicator()
    # update_lme_option_data()
    # update_all_lme_price()
    update_lme_position_data()
    # update_lme_position_data_from_file()

    pass
