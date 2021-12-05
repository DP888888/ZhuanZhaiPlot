import struct
import datetime
from pytdx.hq import TdxHq_API
import pandas as pd
import subprocess

backtest_flag = 0  # 程序会把线画在最高点下方
tdx_dir = "D:/new_jyplug1/"


def modify_tdx_line_bytes(b, code, price, start_time, end_time, parallel=True):
    b = bytearray(b)
    price = float(price)
    if code.split('.')[1] == 'SZ':
        market = 0
    else:
        market = 1

    start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M')
    end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M')
    start_time_date_num = (start_time.year - 2004) * 2048 + start_time.month * 100 + start_time.day
    start_time_time_num = start_time.hour * 60 + start_time.minute
    end_time_date_num = (end_time.year - 2004) * 2048 + end_time.month * 100 + end_time.day
    end_time_time_num = end_time.hour * 60 + end_time.minute
    b[0] = ord(struct.pack('B', market))
    b[1:7] = code.split('.')[0].encode()
    b[86:90] = struct.pack('f', price)
    b[90:94] = struct.pack('f', price)
    if parallel:
        b[101] = ord(struct.pack('B', 46))
    else:
        b[101] = ord(struct.pack('B', 10))
    b[105:107] = struct.pack('H', end_time_date_num)
    b[107:109] = struct.pack('H', end_time_time_num)
    b[109:113] = struct.pack('f', price)
    b[121:125] = struct.pack('f', price)
    b[117:119] = struct.pack('H', start_time_date_num)
    b[119:121] = struct.pack('H', start_time_time_num)
    return bytes(b)


def read_tdx_line_bytes(b):
    market = ord(b[0:1])
    if market == 0:
        market = 'SZ'
    else:
        market = 'SH'
    code = b[1:7].decode() + '.' + market
    price = struct.unpack('f', b[86:90])[0]

    start_time_date_num = struct.unpack('H', b[105:107])[0]
    start_time_time_num = struct.unpack('H', b[107:109])[0]
    year = start_time_date_num // 2048 + 2004
    month = start_time_date_num % 2048 // 100
    day = start_time_date_num % 2048 % 100
    hour = start_time_time_num // 60
    minute = start_time_time_num % 60
    start_time = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute).strftime('%Y-%m-%d %H:%M')

    end_time_date_num = struct.unpack('H', b[117:119])[0]
    end_time_time_num = struct.unpack('H', b[119:121])[0]
    year = end_time_date_num // 2048 + 2004
    month = end_time_date_num % 2048 // 100
    day = end_time_date_num % 2048 % 100
    hour = end_time_time_num // 60
    minute = end_time_time_num % 60
    end_time = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute).strftime('%Y-%m-%d %H:%M')
    if struct.pack('B', b[101])[0] == 46:
        parallel = True
    else:
        parallel = False
    return [code, price, start_time, end_time, parallel]


if __name__ == '__main__':
    pd.set_option('max_columns', None)
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    pd.set_option('display.width', 1000)

    df = []
    with open(tdx_dir + "T0002/tdxline.dat", "rb") as file:
        while True:
            try:
                data = file.read(378)
                if not data:
                    break
                d = read_tdx_line_bytes(data)
                d.append(data)
                df.append(d)
            except (ValueError,) as e:
                print(e)
                print(data)
                continue
    df = pd.DataFrame(df, columns=['转债代码', '价格', '起始时间', '终止时间', '手划线', '原始数据'])
    df = df[df['手划线']]
    df = df.sort_values(by=['转债代码', '起始时间'])

    chengbenxian = pd.DataFrame()
    api = TdxHq_API()
    with api.connect('119.147.212.81', 7709):
        temp = pd.DataFrame(api.get_index_bars(9, 1, '000001', 0, 10))
        last_trade_date = temp.iloc[temp.shape[0] - 2, :]['datetime']
        code_list = df['转债代码'].drop_duplicates().to_list()
        for code in code_list:
            lines = df[df['转债代码'] == code].copy()
            if code.split('.')[1] == 'SZ':
                market = 0
            else:
                market = 1
            start_time = lines.head(1)['起始时间'].values[0]
            start_date = datetime.datetime.strptime(lines.head(1)['起始时间'].values[0], '%Y-%m-%d %H:%M')

            t = 0
            candles = pd.DataFrame()
            while candles.empty or datetime.datetime.strptime(candles.head(1)['datetime'].values[0], '%Y-%m-%d %H:%M') >= start_date:
                temp = pd.DataFrame(api.get_security_bars(9, market, str(code.split('.')[0]), t, 800))
                if temp.empty:
                    break
                candles = pd.concat([temp, candles], axis=0)
                candles = candles.sort_values(by=['year', 'month', 'day', 'hour', 'minute']).reset_index(drop=True)
                t += 800
            candles['datetime'] = pd.to_datetime(candles['datetime'])
            candles = candles[candles['datetime'] >= start_date].reset_index(drop=True)
            candles = candles.set_index('datetime')
            high = candles['high'].max()
            high_datetime = candles['high'].idxmax()
            lines['最高点'] = high
            lines['最高点日期'] = high_datetime
            lines['成本0.3'] = lines['价格'] + (lines['最高点'] - lines['价格']) * 0.3
            chengbenxian = pd.concat([chengbenxian, lines], axis=0)
            print(market, code, start_time, start_date, high)
        chengbenxian = chengbenxian.reset_index(drop=True)
        chengbenxian.to_csv('data1_chengbenxian.csv', encoding='gbk', index=False)

    b = b''
    start_time = (datetime.datetime.strptime(last_trade_date, '%Y-%m-%d %H:%M')).replace(hour=10, minute=0, second=0, microsecond=0)
    end_time = start_time.replace(hour=15, minute=0, second=0, microsecond=0)
    start_time = start_time.strftime('%Y-%m-%d %H:%M')
    end_time = end_time.strftime('%Y-%m-%d %H:%M')
    for i in range(chengbenxian.shape[0]):
        temp = chengbenxian.iloc[i, :]
        s = (datetime.datetime.strptime(str(chengbenxian.iloc[i, :]['最高点日期']), '%Y-%m-%d %H:%M:%S'))
        e = s.replace(hour=15, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M')
        s = s.replace(hour=10, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M')
        if backtest_flag:
            start_time = s
            end_time = e
        b += temp['原始数据']
        b += modify_tdx_line_bytes(temp['原始数据'], temp['转债代码'], temp['成本0.3'], start_time, end_time, parallel=False)
    with open(tdx_dir + "T0002/tdxline.dat", "wb") as file:
        file.write(b)

    subprocess.call([tdx_dir + 'tdxw.exe'])
