import datetime
import time
import numpy as np
import json
import requests
import pandas as pd
from pytdx.hq import TdxHq_API

top_n_amount = 50
top_n_amplitude = 25
chengben_distance_percent = 2.5
ma_distance_percent = 0.8
ma1 = 5
ma2 = 8

def get_jisilu_cookie():
    url = "https://www.jisilu.cn/account/ajax/login_process/"

    payload = "return_url=https%3A%2F%2Fwww.jisilu.cn%2Fdata%2Fcbnew%2F&user_name=1951073d8f611cace9cde076c3c383b0&password=f9c676190f7099fb77a7bc2aa903b4e3&net_auto_login=1&_post_type=ajax&aes=1"
    headers = {
        'Connection': 'keep-alive',
        'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://www.jisilu.cn',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://www.jisilu.cn/account/login/',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    }

    response = requests.request("POST", url, headers=headers, data=payload, timeout=2)

    print(response.cookies.get_dict())

    with open(f'./cookie.json', 'w') as f:
        f.write(json.dumps(response.cookies.get_dict()))


def get_jisilu_detail(cookies):
    url = "https://www.jisilu.cn/webapi/cb/list_new/"

    headers = {
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
        'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
        'Accept': 'application/json, text/plain, */*',
        'Init': '1',
        'Columns': '1,2,3,5,6,11,12,14,15,16,29,30,32,34,35,44,46,47,52,53,54,56,57,58,59,60,62,63,67,70',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://www.jisilu.cn/web/data/cb/list',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    }

    response = requests.get(url, headers=headers, cookies=cookies, timeout=2)
    return response.json()


def get_jisilu_data():
    with open('./cookie.json', 'r') as f:
        cookies = f.read()
    cookies = eval(cookies) if isinstance(cookies, str) else cookies
    jRes = get_jisilu_detail(cookies)
    return pd.DataFrame(jRes['data'])


def xunlongjue(df):
    columns = df.columns.to_list()
    df = df.sort_values(by='datetime', ascending=True)
    df['var2'] = df['low'].shift(1)
    df['var3'] = (abs(df['low'] - df['var2'])).ewm(alpha=1/3).mean() / np.maximum(df['low'] - df['var2'], 0).ewm(alpha=1/3).mean() * 100
    df['var4'] = (df['close'] * 1.3 != 0) * (df['var3'] * 10.0) + (df['close'] * 1.3 == 0) * (df['var3'] / 10.0)
    df['var4'] = df['var4'].ewm(alpha=1/2).mean()
    df['var5'] = df['low'].rolling(30).min()
    df['var6'] = df['var4'].rolling(30).max()
    df['var8'] = (df['low'] <= df['var5']) * ((df['var4'] + df['var6'] * 2) / 2) + (df['low'] > df['var5']) * 0
    df['var8'] = df['var8'].ewm(alpha=1/2).mean() / 618.0
    df['var8'] = (df['var8'] > 0.1) * df['var8']
    columns.append('var8')
    return df[columns].fillna(0.0)


def get_minute_candles(api, market_code, stock_code, more_days):
    df = pd.DataFrame(api.get_security_bars(9, market_code, stock_code, 0, more_days))
    start_datetime = datetime.datetime.strptime(df.head(1)['datetime'].values[0], '%Y-%m-%d %H:%M')
    start_datetime = start_datetime.replace(hour=9, minute=30, second=0, microsecond=0)

    i = 0
    df = pd.DataFrame()
    while df.empty or datetime.datetime.strptime(df.head(1)['datetime'].values[0], '%Y-%m-%d %H:%M') > start_datetime:
        df = pd.concat([pd.DataFrame(api.get_security_bars(7, market_code, stock_code, i, 800)), df], axis=0)
        df = df.sort_values(by=['year', 'month', 'day', 'hour', 'minute']).reset_index(drop=True)
        i += 800
    df['datetime'] = pd.to_datetime(df['datetime'])
    return df[df['datetime'] >= start_datetime].reset_index(drop=True)


def get_intraday_xishu(df):
    if df.empty:
        return [0.0, 0.0]
    df = df.reset_index(drop=True)
    # print(df)
    min_value = df['low'].min()
    min_id = df['low'].idxmin()
    df = df.iloc[min_id:, :]
    max_value = df['high'].max()
    last_value = df['close'].iloc[-1]
    # print(min_value, max_value, last_value)

    if max_value == min_value:
        return [0.0, 0.0]
    else:
        return [(last_value - min_value) / (max_value - min_value), max_value / min_value - 1]


def get_bond_list():
    # 从集思录当中获取可转债数据
    while True:
        try:
            bond_list = get_jisilu_data()
            if bond_list.shape[0] <= 30:
                get_jisilu_cookie()
                bond_list = get_jisilu_data()

            bond_list['市场代码'] = 0
            bond_list.loc[bond_list['market_cd'].str.startswith('sh'), '市场代码'] = 1
            bond_list = bond_list[['market_cd', '市场代码', 'bond_id', 'bond_nm', 'price', 'volume', 'turnover_rt',
                                   'stock_id', 'stock_nm', 'sincrease_rt', 'svolume', 'premium_rt', 'bond_nm_tip']]
            bond_list.columns = ['市场', '市场代码', '转债代码', '转债名称', '转债价格', '转债成交额(亿元)', '转债换手率(%)', '正股代码',
                                 '正股名称', '正股涨幅(%)', '正股成交额(亿元)', '溢价率(%)', '转债赎回信息']
            bond_list['转债代码'] = bond_list['转债代码'].astype('object')
            bond_list['转债换手率(%)'] = bond_list['转债换手率(%)'].astype('double')
            bond_list['转债成交额(亿元)'] = bond_list['转债成交额(亿元)'].astype('double') / 10000
            bond_list['正股成交额(亿元)'] = bond_list['正股成交额(亿元)'].astype('double') / 10000
            bond_list = bond_list[~bond_list['转债名称'].str.contains('EB')]
            bond_list = bond_list[~bond_list['转债赎回信息'].str.contains('最后交易日')]
            bond_list.loc[bond_list['转债赎回信息'].str.contains('不提前赎回'), '转债赎回信息'] = '不赎回'
            bond_list.loc[bond_list['转债赎回信息'].str.contains('不行使'), '转债赎回信息'] = '不赎回'
            bond_list['警示信息'] = ''
            bond_list.loc[bond_list['转债赎回信息'].str.contains('强赎') & (bond_list['溢价率(%)'] > 10.0), '警示信息'] = '强赎 + 高溢价'
            bond_list.loc[bond_list['溢价率(%)'] > 10.0, '警示信息'] = '高溢价'
            bond_list = bond_list.sort_values(by=['转债成交额(亿元)'], ascending=False)
            break
        except Exception as e:
            print(e)
            pass
    return bond_list


def get_stock_market(stock_code):
    if stock_code.startswith('6'):
        return 1
    else:
        return 0


if __name__ == '__main__':
    pd.set_option('max_columns', None)
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    pd.set_option('display.width', 1000)

    # 获取转债全列表存在ZXG.blk 直接导入通达信自选股
    bond_list = get_bond_list()
    bond_list['通达信代码'] = bond_list['市场代码'].astype('str') + bond_list['转债代码'].astype('str')
    bond_list[['通达信代码']].to_csv('D:/new_jyplug1/T0002/blocknew/ZXG.blk', header=False, index=False)

    bond_list = bond_list.sort_values(by=['转债成交额(亿元)'], ascending=False)
    bond_list = bond_list.reset_index(drop=True).iloc[0:top_n_amount, :]
    api = TdxHq_API()
    with api.connect('119.147.212.81', 7709):
        t = []
        for i in range(bond_list.shape[0]):
            t.append((bond_list.iloc[i, 1], bond_list.iloc[i, 2]))
        df = pd.DataFrame(api.get_security_quotes(t))
        df = df[['code', 'last_close', 'high', 'low']]
        df.columns = ['转债代码', '前收盘价', '高', '低']
        df['转债代码'] = df['转债代码'].astype('object')
        df['振幅%'] = (df['高'] - df['低']) / df['前收盘价'] * 100.0
        df = df[['转债代码', '振幅%']]

        df0 = []
        for code in t:
            temp = pd.DataFrame(api.get_security_bars(1, code[0], str(code[1]), 0, ma2))
            temp = temp.sort_values(by=['year', 'month', 'day', 'hour', 'minute'], ascending=False).reset_index(drop=True)
            df0.append([str(code[1]), temp.iloc[0:ma1, :]['close'].mean(), temp['close'].mean()])
        df0 = pd.DataFrame(df0, columns=['转债代码', '短期均线', '长期均线'])
        df = df.merge(df0, how='inner', left_on='转债代码', right_on='转债代码')
    bond_list = bond_list.merge(df, how='inner', left_on='转债代码', right_on='转债代码')
    bond_list = bond_list.sort_values(by=['振幅%'], ascending=False)
    bond_list = bond_list.reset_index(drop=True).iloc[0:top_n_amplitude, :]

    chengbenxian = pd.read_csv('data1_chengbenxian.csv', encoding='gbk')
    chengbenxian['转债代码'] = chengbenxian['转债代码'].str.split('.').str[0]
    chengbenxian = chengbenxian.merge(bond_list, how='inner', left_on='转债代码', right_on='转债代码')
    chengbenxian['距离成本%'] = abs(chengbenxian['转债价格'] / chengbenxian['成本0.3'] - 1) * 100.0
    chengbenxian['均线距离%'] = abs(chengbenxian['短期均线'] / chengbenxian['长期均线'] - 1) * 100.0
    chengbenxian = chengbenxian[chengbenxian['距离成本%'] < chengben_distance_percent]
    chengbenxian = chengbenxian[chengbenxian['均线距离%'] < ma_distance_percent]
    chengbenxian = chengbenxian.drop_duplicates(subset=['转债代码'])
    chengbenxian = chengbenxian.sort_values(by=['转债成交额(亿元)'], ascending=False).reset_index(drop=True)
    chengbenxian[['通达信代码']].to_csv('D:/new_jyplug1/T0002/blocknew/JXKZZ.blk', header=False, index=False)
    chengbenxian = chengbenxian[['转债代码', '转债名称', '转债成交额(亿元)', '振幅%', '转债换手率(%)', '正股代码', '正股名称', '正股涨幅(%)', '正股成交额(亿元)', '溢价率(%)', '转债赎回信息', '警示信息', '距离成本%', '均线距离%', '成本0.3']]
    print(chengbenxian)
