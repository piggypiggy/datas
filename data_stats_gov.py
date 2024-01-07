import os
import requests
import pandas as pd
import datetime
import numpy as np
from utils import *
import json


# 国家统计局

headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Host": "data.stats.gov.cn",
        "Proxy-Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
        "Sec-Fetch-Site": "same-origin"
    }

# '按行业分工业企业主要经济指标2012-2017': [["A020N01","工业企业单位数"], ["A020N02","工业亏损企业单位数"], ["A020N03","工业企业流动资产合计"], ["A020N04","工业企业应收账款"],
#             ["A020N05","工业企业存货"], ["A020N06","工业企业产成品存货"], ["A020N07","工业企业资产总计"], ["A020N08","工业企业负债合计"],
#             ["A020N09","工业企业营业收入"], ["A020N0A","工业企业主营业务收入"], ["A020N0B","工业企业营业成本"], ["A020N0C","工业企业主营业务成本"],
#             ["A020N0D","工业企业主营业务税金及附加"], ["A020N0E","工业企业销售费用"], ["A020N0F","工业企业管理费用"], ["A020N0G","工业企业财务费用"],
#             ["A020N0H","工业企业利息支出"], ["A020N0I","工业企业利润总额"], ["A020N0J","工业亏损企业亏损总额"], ["A020N0K","工业企业应交增值税"]],


name_codes_detail_hgyd = {
    'CPI': [['A010101', 'CPI同比 2016-'], ['A010103', '食品CPI同比'], 
            ['A010104', '城市CPI同比 2016-'], ['A010106', '城市食品CPI同比'],
            ['A010107', '农村CPI同比 2016-'], ['A010109', '农村食品CPI同比'],
            ['A010201', 'CPI累计同比 2016-'], ['A010203', '食品CPI累计同比'], 
            ['A010204', '城市CPI累计同比 2016-'], ['A010206', '城市食品CPI累计同比'],
            ['A010207', '农村CPI累计同比 2016-'], ['A010209', '农村食品CPI累计同比'],
            ['A010301', 'CPI环比 2016-'], ['A010303', '食品CPI环比'], 
            ['A010304', '城市CPI环比 2016-'], ['A010306', '城市食品CPI环比'],
            ['A010307', '农村CPI环比 2016-'], ['A010309', '农村食品CPI环比'],],

    # 商品零售价格指数
    'RPI': [['A010401', 'RPI同比'], ['A010402', '城市RPI同比'], ['A010403', '农村RPI同比'],
            ['A010501', 'RPI累计同比'], ['A010502', '城市RPI累计同比'], ['A010503', '农村RPI累计同比'],
            ['A010601', 'RPI环比'], ['A010602', '城市RPI环比'], ['A010603', '农村RPI环比'],],

    'PPIRM': [['A010701', 'PPIRM同比'], ['A010702', 'PPIRM累计同比'], ['A010703', 'PPIRM环比']],

    'PPI': [['A010801', 'PPI同比'], ['A010802', 'PPI生产资料同比'], ['A010803', 'PPI生活资料同比'],
            ['A010804', 'PPI累计同比'], ['A010805', 'PPI生产资料累计同比'], ['A010806', 'PPI生活资料累计同比'],
            ['A010807', 'PPI环比'], ['A010808', 'PPI生产资料环比'], ['A010809', 'PPI生活资料环比'],
            ['A010B', 'PPI分行业同比 2018-'], ['A010E', 'PPI分行业累计同比 2018-'], ['A010F', 'PPI分行业环比 2018-']],

    # '能源主要产品产量': [['A030101', '原煤'], ['A030102', '原油'], ['A030103', '天然气'], ['A030104', '煤层气'],
    #                    ['A030105', '液化天然气'], ['A030106', '原油加工量'], ['A030107', '汽油'], ['A030108', '煤油'],
    #                    ['A030109', '柴油'], ['A03010A', '燃料油'], ['A03010B', '石脑油'], ['A03010C', '液化石油气'],
    #                    ['A03010D', '石油焦'], ['A03010E', '石油沥青'], ['A03010F', '焦炭'], ['A03010G', '发电量'],
    #                    ['A03010H', '火力发电量'], ['A03010I', '水力发电量'], ['A03010J', '核能发电量'], ['A03010K', '风力发电量'],
    #                    ['A03010L', '太阳能发电量'], ['A03010M', '煤气'],], 

    # '工业增加值': [['A0201', '工业增加值'], ['A0202', '按经济类型分'], ['A020P', '按三大门类分']],    
    # '按工业大类分工业增加值': [['A0205', '按工业大类分2018-']],    

    # '主要工业产品产量': [['A020901', '铁矿石原矿'], ['A020902', '磷矿石（折含五氧化二磷30％）'], ['A020903', '原盐'], ['A020904', '饲料'],
    #                    ['A020905', '精制食用植物油'], ['A020906', '成品糖'], ['A020907', '鲜、冷藏肉'], ['A020908', '乳制品'],
    #                    ['A020909', '白酒（折65度，商品量）'], ['A02090A', '啤酒'], ['A02090B', '葡萄酒'], ['A02090C', '饮料'],
    #                    ['A02090D', '卷烟'], ['A02090E', '纱'], ['A02090F', '布'], ['A02090G', '蚕丝及交织机织物（含蚕丝≥30％）'],
    #                    ['A02090H', '机制纸及纸板（外购原纸加工除外）'], ['A02090I', '新闻纸'], ['A02090J', '硫酸（折100％）'], ['A02090K', '烧碱（折100％）'],
    #                    ['A02090L', '纯碱（碳酸钠）'], ['A02090M', '乙烯'], ['A02090N', '农用氮、磷、钾化学肥料（折纯）'], ['A02090O', '化学农药原药（折有效成分100％）'],
    #                    ['A02090P', '初级形态塑料'], ['A02090Q', '合成橡胶'], ['A02090R', '合成洗涤剂'], ['A02090S', '化学药品原药'],
    #                    ['A02090T', '中成药'], ['A02090U', '化学纤维'], ['A02090V', '合成纤维'], ['A02090W', '橡胶轮胎外胎'],
    #                    ['A02090X', '塑料制品'], ['A02090Y', '水泥'], ['A02090Z', '平板玻璃'], ['A020910', '钢化玻璃'],
    #                    ['A020911', '夹层玻璃'], ['A020912', '中空玻璃'], ['A020913', '生铁'], ['A020914', '粗钢'],
    #                    ['A020915', '钢材'], ['A020916', '钢筋'], ['A020917', '线材（盘条）'], ['A020918', '冷轧薄板'],
    #                    ['A020919', '中厚宽钢带'], ['A02091A', '焊接钢管'], ['A02091B', '铁合金'], ['A02091C', '氧化铝'],
    #                    ['A02091D', '十种有色金属'], ['A02091E', '精炼铜（电解铜）'], ['A02091F', '铅'], ['A02091G', '锌'],
    #                    ['A02091H', '原铝（电解铝）'], ['A02091I', '铝合金'], ['A02091J', '铜材'], ['A02091K', '铝材'],
    #                    ['A02091L', '金属集装箱'], ['A02091M', '工业锅炉'], ['A02091N', '发动机'], ['A02091O', '金属切削机床'],
    #                    ['A02091P', '金属成形机床'], ['A02091Q', '电梯、自动扶梯及升降机'], ['A02091R', '电动手提式工具'], ['A02091S', '包装专用设备'],
    #                    ['A02091T', '复印和胶版印制设备'], ['A02092T', '挖掘铲土运输机械'], ['A02091U', '挖掘机'], ['A02091V', '水泥专用设备'],
    #                    ['A02091W', '金属冶炼设备'], ['A02091X', '饲料生产专用设备'], ['A02091Y', '大型拖拉机'], ['A02091Z', '中型拖拉机'],
    #                    ['A020920', '小型拖拉机'], ['A020921', '大气污染防治设备'], ['A020922', '工业机器人'], ['A02092U', '服务机器人'],
    #                    ['A020923', '汽车'], ['A020924', '基本型乘用车（轿车）'], ['A020925', '运动型多用途乘用车（SUV）'], ['A020926', '载货汽车'],
    #                    ['A02092W', '新能源汽车'], ['A020927', '铁路机车'], ['A020928', '动车组'], ['A020929', '民用钢质船舶'],
    #                    ['A02092A', '发电机组（发电设备）'], ['A02092B', '交流电动机'], ['A02092C', '光缆'], ['A02092D', '锂离子电池'],
    #                    ['A02092E', '太阳能电池（光伏电池）'], ['A02092F', '家用电冰箱（家用冷冻冷藏箱）'], ['A02092G', '家用冷柜（家用冷冻箱）'], ['A02092H', '房间空气调节器'],
    #                    ['A02092I', '家用洗衣机'], ['A02092J', '电子计算机整机'], ['A02092K', '微型计算机设备'], ['A02092L', '程控交换机'],
    #                    ['A02092M', '移动通信基站设备'], ['A02092N', '传真机'], ['A02092O', '移动通信手持机（手机）'], ['A02092X', '智能手机'],
    #                    ['A02092P', '彩色电视机'], ['A02092Q', '集成电路'], ['A02092R', '光电子器件'], ['A02092V', '智能手表'],
    #                    ['A02092S', '电工仪器仪表']],    

    # '工业企业主要经济指标': [['A020A', '工业企业'], ['A020B', '国有工业企业'], ['A020C', '集体工业企业'], ['A020D', '股份合作工业企业'],
    #                        ['A020E', '股份制工业企业'], ['A020F', '外商及港澳台投资工业企业'], ['A020G', '其它工业企业'], ['A020H', '国有控股工业企业'],
    #                        ['A020I', '大中型工业企业'], ['A020J', '大中型国有控股工业企业'], ['A020K', '私营工业企业']],

    # '按行业分工业企业主要经济指标': [["A020O01", "工业企业单位数"], ["A020O02", "工业亏损企业单位数"], ["A020O03", "工业企业流动资产合计"], ["A020O0N", "工业企业应收票据及应收账款"],
    #                               ["A020O04", "工业企业应收账款"], ["A020O05", "工业企业存货"], ["A020O06", "工业企业产成品存货"], ["A020O07", "工业企业资产总计"],
    #                               ["A020O08", "工业企业负债合计"], ["A020O0P", "工业企业所有者权益合计"], ["A020O09", "工业企业营业收入"], ["A020O0A", "工业企业主营业务收入"],
    #                               ["A020O0B", "工业企业营业成本"], ["A020O0C", "工业企业主营业务成本"], ["A020O0D", "工业企业主营业务税金及附加"], ["A020O0E", "工业企业销售费用"],
    #                               ["A020O0F", "工业企业管理费用"], ["A020O0G", "工业企业财务费用"], ["A020O0O", "工业企业利息费用"], ["A020O0H", "工业企业利息支出"],
    #                               ["A020O0I", "工业企业投资收益"], ["A020O0J", "工业企业营业利润"], ["A020O0K", "工业企业利润总额"], ["A020O0L", "工业亏损企业亏损总额"],
    #                               ["A020O0M", "工业企业应交增值税"], ["A020O0Q", "工业企业平均用工人数"]],

    # '固定资产投资': [['A0401', '固定资产投资概况'], ['A0402', '固定资产投资增速'], ['A0404', '按登记注册类型分'], ['A0406', '资金来源'], ['A040A', '计划总投资']],

    # '按行业分民间固定资产投资增速': [['A0405', '按行业分民间固定资产投资增速']],

    # '按行业分固定资产投资增速': [['A0403', '按行业分固定资产投资增速 2018-']],

    # '服务业生产指数': [['A0501', '服务业生产指数']],

    # '城镇调查失业率': [['A0E01', '城镇调查失业率']],

    # '房地产(统计局)': [["A0601", "房地产开发投资情况"], ["A0602", "房地产开发投资实际到位资金"], ["A0603", "房地产土地开发与销售情况"], ["A0604", "房地产施工、竣工面积"],
    #             ["A0605", "商品住宅施工、竣工面积"], ["A0606", "办公楼施工、竣工面积"], ["A0607", "商业营业用房施工、竣工面积"], ["A0608", "商品房销售面积"],
    #             ["A0609", "商品房销售额"], ["A060A", "商品住宅销售面积"], ["A060B", "商品住宅销售额"], ["A060C", "办公楼销售面积"],
    #             ["A060D", "办公楼销售额"], ["A060E", "商业营业用房销售面积"], ["A060F", "商业营业用房销售额"]],

    # '社会消费品零售总额': [['A0701', '社会消费品零售总额'], ['A0702', '按经营地分'], ['A0703', '按消费类型分'], ['A0707', '按零售业态分'],
    #                      ['A0706', '网上零售额']],

    # '限上单位商品零售类值': [["A070401", "粮油、食品、饮料、烟酒类"], ["A070402", "服装鞋帽、针、纺织品类"], ["A070403", "化妆品类"], ["A070404", "金银珠宝类"],
    #                         ["A070405", "日用品类"], ["A070406", "体育、娱乐用品类"], ["A070407", "书报杂志类"], ["A070408", "家用电器和音像器材类"],
    #                         ["A070409", "中西药品类"], ["A07040A", "文化办公用品类"], ["A07040B", "家具类"], ["A07040C", "通讯器材类"],
    #                         ["A07040D", "石油及制品类"], ["A07040E", "建筑及装潢材料类"], ["A07040F", "汽车类"], ["A07040G", "其他"]],

    '交通运输': [['A0901', '货物运输量'], ['A0902', '货物周转量'], ['A0903', '旅客运输量'], ['A0904', '旅客周转量']],

    '中国制造业PMI': [['A0B01', '制造业PMI'],],
    '中国非制造业PMI': [['A0B02', '非制造业PMI'],],
    '中国综合PMI': [['A0B03', '综合PMI产出指数'],],

}


name_codes_detail_csyd = {
    '70个大中城市住宅销售价格指数': [['A0108', '70个大中城市住宅销售价格指数']], 

    # '36个城市居民消费价格分类指数': [['A0101', '36个城市居民消费价格分类指数2016- 同比'], 
    #                                ['A0102', '36个城市居民消费价格分类指数2016- 累计同比'],
    #                                ['A0103', '36个城市居民消费价格分类指数2016- 环比']], 

    # '36个城市居民消费价格分类指数-2015': [['A0104', '36个城市居民消费价格分类指数-2015 同比'], 
    #                                ['A0105', '36个城市居民消费价格分类指数-2015 累计同比'],
    #                                ['A0106', '36个城市居民消费价格分类指数-2015 环比']], 
}


name_codes_detail_fsyd = {
    '31个省居民消费价格分类指数': [['A010101', '31个省居民消费价格分类指数2016- 同比'],
                                 ['A010107', '31个省居民消费价格分类指数2016- 环比'],
                                 ['A010103', '31个省食品类居民消费价格指数 同比'],
                                 ['A010109', '31个省食品类居民消费价格指数 环比'],], 

    # '31个省居民消费价格分类指数_old': [['A010102', '31个省居民消费价格分类指数-2016 同比'],
    #                              ['A010108', '31个省居民消费价格分类指数-2016 环比'],
    #                              ['A010103', '31个省食品类居民消费价格指数 同比'],
    #                              ['A010109', '31个省食品类居民消费价格指数 环比'],], 
}

name_codes_detail_hgjd = {
    'GDP': [['A0101', 'GDP'], ['A0102', 'GDP(不变价)'], ['A0103', '国内生产总值指数'],
            ['A0104', '国内生产总值环比增长速度'], ['A0105', '三大需求贡献率'], ['A0106', '三次产业贡献率'],
            ['A0107', '服务货物占比']],

    '人民生活': [['A0501', '居民人均可支配收入'], ['A0502', '城镇居民人均可支配收入'], ['A0503', '农村居民人均可支配收入'],
            ['A0504', '居民人均消费支出'], ['A0505', '城镇居民人均消费支出'], ['A0506', '农村居民人均消费支出'],
            ['A0507', '农村外出务工']],
}


# 国家统计局
def update_all_gov_data(name_codes_detail, freq):
    se = requests.session()
    earlist_time = '200501-'

    # get cookies
    if freq == 'hgyd':
        url = 'https://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode=hgyd&rowcode=sj&colcode=zb&wds=[]&dfwds=[{"wdcode":"zb","valuecode":"A0B03"}]'+'&k='+str(int(time.time()*1000))
    if freq == 'hgjd':
        url = 'https://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode=hgjd&rowcode=sj&colcode=zb&wds=[]&dfwds=[{"wdcode":"zb","valuecode":"A0107"}]'+'&k='+str(int(time.time()*1000))

    r = se.get(url, verify=False, headers=headers)
    cookies = r.cookies

    for name in name_codes_detail:
        path = os.path.join(nbs_dir, name+'.csv')
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
                    last_line_dt = pd.to_datetime(last_line[:7], format='%Y-%m')
                    start_time = last_line_dt.strftime('%Y%m')
                    start_time = start_time + '-'
                except:
                    start_time = earlist_time
                print('GOV DATA UPDATE ' + path)
        else:
            print('GOV DATA CREATE ' + path)
            start_time = earlist_time

        cols_set = set()
        df = pd.DataFrame()

        # set start time
        if freq == 'hgyd':
            url = 'https://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode=hgyd&rowcode=sj&colcode=zb&wds=[]&dfwds=[{"wdcode":"sj","valuecode":'+'"'+start_time+'"'+'}]'+'&k='+str(int(time.time()*1000))
        if freq == 'hgjd':
            url = 'https://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode=hgjd&rowcode=sj&colcode=zb&wds=[]&dfwds=[{"wdcode":"sj","valuecode":'+'"'+start_time+'"'+'}]'+'&k='+str(int(time.time()*1000))
   
        r = se.get(url, verify=False, headers=headers, cookies=cookies)
        print(name+': start_time =', start_time)
    
        for codes_detail in name_codes_detail[name]:
            time.sleep(1)
            if freq == 'hgyd':
                url = 'https://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode=hgyd&rowcode=sj&colcode=zb&wds=[]&dfwds=[{"wdcode":"zb","valuecode":'+'"'+codes_detail[0]+'"'+'}]'+'&k='+str(int(time.time()*1000))
            if freq == 'hgjd':
                url = 'https://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode=hgjd&rowcode=sj&colcode=zb&wds=[]&dfwds=[{"wdcode":"zb","valuecode":'+'"'+codes_detail[0]+'"'+'}]'+'&k='+str(int(time.time()*1000))

            while (1):
                try:
                    r = se.get(url, verify=False, headers=headers, cookies=cookies)
                    s = r.content.decode('utf-8')
                    z = json.loads(s)
                    break
                except Exception as e:
                    print(e)
                    time.sleep(15)

            print(codes_detail)

            if (r.status_code == 200):
                # print(z)
                L = len(z['returndata']['datanodes'])
                datas = []
                codes = []
                ts = []
                for i in range(L):
                    if (z['returndata']['datanodes'][i]['data']['hasdata'] == False):
                        datas.append(np.nan)
                    else:
                        datas.append(z['returndata']['datanodes'][i]['data']['data'])

                    codes.append(z['returndata']['datanodes'][i]['wds'][0]['valuecode'])
                    t = z['returndata']['datanodes'][i]['wds'][1]['valuecode']
                    if freq == 'hgjd':
                        t = t.replace('A', '03')
                        t = t.replace('B', '06')
                        t = t.replace('C', '09')
                        t = t.replace('D', '12')
                    ts.append(t)
                datas = np.array(datas)
                codes = np.array(codes)
                ts = np.array(ts)

                code_name = {}
                for i in range(len(z['returndata']['wdnodes'][0]['nodes'])):
                    code_name[z['returndata']['wdnodes'][0]['nodes'][i]['code']] = z['returndata']['wdnodes'][0]['nodes'][i]['cname']

                temp_df = pd.DataFrame()
                where = np.where(codes == codes[0])[0]
                temp_df['time'] = ts[where]
                for code in (set(codes)):
                    where = np.where(codes == code)[0]
                    data = datas[where]
                    temp_df[code_name[code]] = data

                if (len(temp_df['time'][0]) == 6):
                    temp_df['time'] = temp_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y%m'))
                    temp_df.sort_values(by = 'time', inplace=True)
                    temp_df['time'] = temp_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m'))
                else:
                    temp_df['time'] = temp_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y%m%d'))
                    temp_df.sort_values(by = 'time', inplace=True)
                    temp_df['time'] = temp_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))

                cols = temp_df.columns.to_list()
                for col in cols:
                    if (col in cols_set) and (col != 'time'):
                        temp_df.drop(col, axis=1, inplace=True)
                    else:
                        cols_set.add(col)

                if (df.empty):
                    df = temp_df.copy()
                else:
                    df = pd.merge(df, temp_df, on='time', how='outer')

        # print(df)
        if (len(df) > 0):
            path = os.path.join(nbs_dir, name+'.csv')
            if os.path.exists(path):
                old_df = pd.read_csv(path)
                old_df = pd.concat([old_df, df], axis=0)
                old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
                c = old_df.columns.tolist()
                c.pop(0)  # 不考虑'time'
                # 删除全na的行
                old_df.dropna(how='all', subset=c, inplace=True)
                old_df.to_csv(path, encoding='utf-8', index=False)
            else:
                c = df.columns.tolist()
                c.pop(0)  # 不考虑'time'
                # 删除全na的行
                df.dropna(how='all', subset=c, inplace=True)
                df.to_csv(path, encoding='utf-8', index=False)


# 城市月度 分省月度
def update_all_gov_data_csyd_fsyd(name_codes_detail, type):
    session = requests.session()
    earlist_time = '200501'
        # earlist_time = '201601'
    # get cookies
    url = 'https://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode='+type+'&rowcode=reg&colcode=zb&wds=[{"wdcode":"sj","valuecode":"202306"}]&dfwds=[{"wdcode":"zb","valuecode":"A0108"}]'+'&k='+str(int(time.time()*1000))

    r = session.get(url, verify=False, headers=headers)
    cookies = r.cookies

    for name in name_codes_detail:
        path = os.path.join(nbs_dir, name+'.csv')
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
                    last_line_dt = pd.to_datetime(last_line[:7], format='%Y-%m')
                    start_time = last_line_dt.strftime('%Y%m')
                    start_time = start_time
                except:
                    start_time = earlist_time
                print('GOV DATA UPDATE ' + path)
        else:
            print('GOV DATA CREATE ' + path)
            start_time = earlist_time

        if start_time != earlist_time:
            start_time_dt = pd.to_datetime(start_time, format='%Y%m')
            if start_time_dt.month == 12:
                start_time_dt = datetime.datetime(year=start_time_dt.year+1, month=1, day=1)
            else:
                start_time_dt = datetime.datetime(year=start_time_dt.year, month=start_time_dt.month+1, day=1)  
        else:
            start_time_dt = pd.to_datetime(start_time, format='%Y%m')          
        current_time_dt = datetime.datetime.now()

        print(name+': start_time =', start_time)
        while(start_time_dt < current_time_dt):
            df = pd.DataFrame()
            start_time = start_time_dt.strftime(format='%Y%m')
            for codes_detail in name_codes_detail[name]:
                time.sleep(0.5)
                zb_dict = {}
                reg_dict = {}
                url = 'https://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode='+type+'&rowcode=reg&colcode=zb&wds=[{"wdcode":"sj","valuecode":'+'"'+start_time+'"'+'}]&dfwds=[{"wdcode":"zb","valuecode":'+'"'+codes_detail[0]+'"'+'}]'+'&k='+str(int(time.time()*1000))

                r = session.get(url, verify=False, headers=headers, cookies=cookies)
                print(codes_detail, start_time)

                if (r.status_code == 200):
                    s = r.content.decode('utf-8')
                    z = json.loads(s)
                    if (z['returndata']['hasdatacount'] == 0):
                        continue
                    # print(z)
                    c1 = ['time']
                    c2 = ['time']

                    L1 = len(z['returndata']['wdnodes'][1]['nodes'])
                    L2 = len(z['returndata']['wdnodes'][0]['nodes'])
                    for i in range(L1):
                        code1 = z['returndata']['wdnodes'][1]['nodes'][i]['code']
                        name1 = z['returndata']['wdnodes'][1]['nodes'][i]['cname']
                        reg_dict[code1] = name1
                        for k in range(L2):
                            code2 = z['returndata']['wdnodes'][0]['nodes'][k]['code']
                            name2 = z['returndata']['wdnodes'][0]['nodes'][k]['cname']
                            zb_dict[code2] = name2
                            c2.append(name2)
                            c1.append(name1)

                    temp_df = pd.DataFrame(columns=[c1,c2])
                    start_time1 = start_time_dt.strftime(format='%Y-%m')
                    temp_df.loc[0, pd.IndexSlice['time','time']] = start_time1
                    L = len(z['returndata']['datanodes'])
                    for i in range(L):
                        zb = z['returndata']['datanodes'][i]['wds'][0]['valuecode']
                        reg = z['returndata']['datanodes'][i]['wds'][1]['valuecode']
                        if (z['returndata']['datanodes'][i]['data']['hasdata'] == False):
                            temp_df.loc[0, pd.IndexSlice[reg_dict[reg],zb_dict[zb]]] = np.nan
                        else:
                            temp_df.loc[0, pd.IndexSlice[reg_dict[reg],zb_dict[zb]]] = z['returndata']['datanodes'][i]['data']['data']

                    if (df.empty):
                        df = temp_df.copy()
                    else:
                        df = pd.merge(df, temp_df, on=[('time','time')], how='outer')

            # print(df)
            if (len(df) > 0):
                path = os.path.join(nbs_dir, name+'.csv')
                if os.path.exists(path):
                    old_df = pd.read_csv(path, header=[0,1])
                    old_df = pd.concat([old_df, df], axis=0)
                    old_df.drop_duplicates(subset=[('time','time')], keep='last', inplace=True)
                    old_df.to_csv(path, encoding='utf-8', index=False)
                else:
                    df.to_csv(path, encoding='utf-8', index=False)

            if start_time_dt.month == 12:
                start_time_dt = datetime.datetime(year=start_time_dt.year+1, month=1, day=1)
            else:
                start_time_dt = datetime.datetime(year=start_time_dt.year, month=start_time_dt.month+1, day=1)   


def concat_data(replace, path1, path2, path3):
    keys = []
    for key in replace:
        keys.append(key)
    
    df1 = pd.read_csv(path1)
    cols = df1.columns.to_list()
    for i in range(len(cols)):
        col = cols[i]
        for k in range(len(keys)):
            key = keys[k]
            if key in col:
                col = col.replace(key, replace[key])
                cols[i] = col

    df1.columns = cols

    df2 = pd.read_csv(path2)

    df3 = pd.concat([df1, df2], axis=0)
    df3.to_csv(path3, encoding='utf-8', index=False)


# # 分工业大类工业增加值
# replace = {
#     '开采辅助活动': '开采专业及辅助性活动',
#     '木材加工及木、竹、藤、棕、草制品业': '木材加工和木、竹、藤、棕、草制品业',
#     '造纸及纸制品业': '造纸和纸制品业',
#     '石油加工、炼焦及核燃料加工业': '石油、煤炭及其他燃料加工业', 
#     '化学原料及化学制品制造业': '化学原料和化学制品制造业', 
#     '黑色金属冶炼及压延加工业': '黑色金属冶炼和压延加工业', 
#     '有色金属冶炼及压延加工业': '有色金属冶炼和压延加工业', 
#     '电气机械及器材制造业': '电气机械和器材制造业',
#     '通信设备、计算机及其他电子设备制造业': '计算机、通信和其他电子设备制造业',
#     '电力、热力的生产和供应业': '电力、热力生产和供应业',
# }

# # 分行业工业企业主要利润指标
# replace = {
#     '开采辅助活动': '开采专业及辅助性活动',
#     '石油加工、炼焦和核燃料加工业': '石油、煤炭及其他燃料加工业', 
# }

# # 按行业分固定资产投资增速
# replace = {
#     '开采辅助活动': '开采专业及辅助性活动',
#     '石油加工、炼焦和核燃料加工业': '石油、煤炭及其他燃料加工业加工业', 
#     '科学研究、技术服务和地质勘查业': '科学研究和技术服务业',
#     '环境管理业': '生态保护和环境治理业',
#     '居民服务和其他服务业': '居民服务、修理和其他服务业',
#     '卫生、社会保障和社会福利业': '卫生和社会工作',
#     '公共管理和社会组织': '公共管理、社会保障和社会组织',
# }


def concat():
    replace = {
        '开采辅助活动': '开采专业及辅助性活动',
        '酒、饮料和精制茶制造业': '酒、饮料及精制茶制造业', 
        '石油加工、炼焦和核燃料加工业': '石油、煤炭及其他燃料加工业',
    }
    path1 = os.path.join(nbs_dir, 'PPI-2014'+'.csv')
    path2 = os.path.join(nbs_dir, 'PPI-2018'+'.csv')
    path3 = os.path.join(nbs_dir, 'TEMP3'+'.csv')
    concat_data(replace, path1, path2, path3)

    # replace = {
    #     '开采辅助活动': '开采专业及辅助性活动',
    #     '石油加工、炼焦和核燃料加工业': '石油、煤炭及其他燃料加工业', 
    # }
    # path1 = os.path.join(nbs_dir, 'TEMP'+'.csv')
    # path2 = os.path.join(nbs_dir, '按行业分工业企业主要经济指标'+'.csv')
    # path3 = os.path.join(nbs_dir, 'TEMP3'+'.csv')
    # concat_data(replace, path1, path2, path3)

if __name__=="__main__":
    # update_all_gov_data(name_codes_detail_hgyd, 'hgyd')  # 月度
    update_all_gov_data_csyd_fsyd(name_codes_detail_csyd, 'csyd')  # 城市月度
    # update_all_gov_data_csyd_fsyd(name_codes_detail_fsyd, 'fsyd')  # 分省月度

    # update_all_gov_data(name_codes_detail_hgjd, 'hgjd')  # 季度

    pass


