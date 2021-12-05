import pandas as pd



if __name__ == '__main__':
    pd.set_option('max_columns', None)
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    pd.set_option('display.width', 1000)

    df = pd.read_csv('data6_stats.csv', encoding='gbk')
    counts = df[['击穿次数']].value_counts()
    counts = counts / counts.sum() * 100.0
    print(counts)

    print(df.sort_values(by=['击穿次数'], ascending=False).iloc[0:20, :])

    print(df.sort_values(by=['最大收益'], ascending=False).iloc[0:20, :])

