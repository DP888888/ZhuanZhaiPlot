import statistics
import pandas as pd


# 可调参数
price = [150.9, 148.2, 145.7, 143.4, 140.6]
expected_margin = -0.2
positions = []


def get_position(n):
    data = [0] * n
    get_position_helper(data, 100, 1, n)


def get_position_helper(data, end, index, r):  # 递归搜索所有的加仓可能性
    if index == r:
        results = []
        for j in range(index):  # 计算各个价格反弹的时候收益率（按全部资金记）
            total = 0.0
            cost = 0.0
            revenue = 0.0
            for k in range(j+1):
                total += data[k]
                cost += data[k] * price[k]
                if j - 1 >= 0:
                    revenue += data[k] * price[j - 1]
                else:
                    revenue += data[k] * price[0]
            if cost == 0.0:
                results.append(0.0)
            else:
                results.append((revenue / cost - 1) * total)

        if cost != 0:
            m = statistics.mean(results[1:])
            s = statistics.stdev(results[1:])
            positions.append([data.copy(), results.copy(), m, s, m / s])
            print(data)
        return

    total = 0.0
    cost = 0.0
    revenue = 0.0
    for k in range(index):
        total += data[k]
        cost += data[k] * price[k]
        if index - 2 >= 0:
            revenue += data[k] * price[index - 2]
        else:
            revenue += data[k] * price[0]

    if cost != 0 and (revenue / cost - 1) * total < expected_margin:  # 剪枝条件
        # print('-----', data[0:index], revenue / cost)
        return

    if index == r - 1:
        data[index] = 100 - sum(data[0:index])
        get_position_helper(data, 0, index + 1, r)
    else:
        i = 0
        while i <= end:
            data[index] = i
            new_end = 100 - sum(data[0:index+1])
            get_position_helper(data, new_end, index + 1, r)
            i += 2


if __name__ == '__main__':
    get_position(len(price))
    df = pd.DataFrame(positions)
    df = df.sort_values(df.columns[-1], ascending=False).reset_index(drop=True)  # 选取在每个反弹点盈亏比都尽量好的加仓方式
    df = df.round(3)
    print()
    for i in range(5):
        print(df.iloc[i, 0], pd.Series(df.iloc[i, 1]).round(3).to_list(), df.iloc[i, 2], df.iloc[i, 3], df.iloc[i, 4])

