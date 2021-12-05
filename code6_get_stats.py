import pandas as pd
import datetime
from pytdx.hq import TdxHq_API


def get_tdx_data(api, code, high_datetime):
    high_datetime = datetime.datetime.strptime(high_datetime, '%Y-%m-%d %H:%M:%S')
    if code.split('.')[1] == 'SZ':
        market = 0
    else:
        market = 1

    i = 0
    results = pd.DataFrame()
    while True:
        results = pd.concat([pd.DataFrame(api.get_security_bars(1, market, str(code.split('.')[0]), i, 800)), results], axis=0)
        i += 800
        if high_datetime.strftime('%Y-%m-%d %H:%M') in results['datetime'].to_list():
            break
    results['datetime'] = pd.to_datetime(results['datetime'])
    results = results[results['datetime'] >= high_datetime].reset_index(drop=True)
    return results


if __name__ == '__main__':
    df = pd.read_csv('data1_chengbenxian.csv', encoding='gbk')
    code_list = df['转债代码'].drop_duplicates().to_list()
    # code_list = ['128093.SZ']

    output = []
    api = TdxHq_API()
    with api.connect('119.147.212.81', 7709):
        for code in code_list:
            temp = df[df['转债代码'] == code].copy().reset_index(drop=True)
            candles = get_tdx_data(api, code, temp['最高点日期'][0])
            high = temp['最高点'][0]
            high_pos = candles.iloc[0, :]['datetime']
            low = temp['最高点'][0]
            low_pos = candles.iloc[0, :]['datetime']
            last_low = temp['最高点'][0]
            for i in range(candles.shape[0]):
                if i == candles.shape[0] - 1:
                    break
                if candles.iloc[i, :]['high'] > high:
                    high = candles.iloc[i, :]['high']
                    high_pos = candles.iloc[i, :]['datetime']
                if candles.iloc[i, :]['low'] < low or high_pos >= low_pos:
                    low = candles.iloc[i, :]['low']
                    low_pos = candles.iloc[i, :]['datetime']
                chengben = (high - low) * 0.3 + low
                condition = candles.iloc[i, :]['high'] > chengben
                condition = condition and (temp[(temp['成本0.3'] >= low) & (temp['成本0.3'] <= high)].shape[0] > 1)
                condition = condition and (candles.iloc[i, :]['low'] > last_low)

                if condition:
                    rate_of_return = (candles.iloc[i, :]['high'] / low - 1) * 100.0
                    line_count = temp[(temp['成本0.3'] >= low) & (temp['成本0.3'] <= high)].shape[0]
                    span = candles.iloc[i, :]['datetime'] - low_pos
                    output.append([code, low_pos, candles.iloc[i, :]['datetime'], span, line_count, rate_of_return])
                    print(code, low_pos, candles.iloc[i, :]['datetime'], span, line_count, rate_of_return)
                    high = candles.iloc[i, :]['high']
                    low = candles.iloc[i, :]['high']
                    high_pos = candles.iloc[i, :]['datetime']
                    low_pos = candles.iloc[i, :]['datetime']

                last_low = candles.iloc[i, :]['low']
        pd.DataFrame(output, columns=['转债代码', '低点日期', '反弹日期', '时长', '击穿次数', '最大收益']).to_csv('data6_stats.csv', encoding='gbk', index=False)


