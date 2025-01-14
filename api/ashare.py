import tushare as ts
import pandas as pd
import concurrent.futures
import time

class TushareDataHandler:
    def __init__(self, start_date, end_date, frequency='D', token=None):
        """
        :param start_date: 开始日期，格式 YYYYMMDD
        :param end_date: 结束日期，格式 YYYYMMDD
        :param frequency: 数据频率，默认为日线 'D'
        :param token: Tushare 的个人认证 Token
        """
        self.start_date = start_date
        self.end_date = end_date
        self.frequency = frequency
        
        if token:
            ts.set_token(token)
        self.pro = ts.pro_api()

    def get_index_list(self, index_code):
        """
        获取指定指数的成分股列表。
        :param index_code: 指数代码，例如 '000016.SH'（上证50）
        :return: 成分股代码列表
        """
        index_weights = self.pro.index_weight(index_code=index_code, start_date=self.start_date, end_date=self.end_date)
        unique_stock_list = list(set(index_weights['con_code'].tolist()))  # 使用 set 去重
        
        return unique_stock_list
    
    def get_index_prices_from_tushare(self, index_code):
        """
        获取指定指数的日线行情数据，重新整理为多重索引的 DataFrame。
        :param index_code: 指数代码，例如 '000016.SH'（上证50）
        :param fields: 需要获取的字段，例如 ['open', 'high', 'low', 'close', 'vol']。
        :return: 多重索引结构的价格数据 DataFrame
        """

        benchmark_data = self.pro.index_daily(ts_code=index_code, start_date=self.start_date, end_date=self.end_date)
        
        # 按日期排序（通常从最新日期到过去，需要升序计算净值变化）
        benchmark_data['trade_date'] = pd.to_datetime(benchmark_data['trade_date'])
        benchmark_data = benchmark_data.sort_values(by='trade_date')

        benchmark_data.set_index('trade_date', inplace=True)

        # 计算累积净值变化
        benchmark_data['benchmark_net_value'] = (1 + benchmark_data['pct_chg'] / 100).cumprod()  # 累积乘积
        benchmark_data.loc[benchmark_data.index[0], 'benchmark_net_value'] = 1.0

        return benchmark_data[['ts_code', 'benchmark_net_value']]
    
    
    def get_prices_from_tushare(self, stock_list, fields=['close'], sleep_time=0):
        """
        获取多只股票的日线行情数据，重新整理为多重索引的 DataFrame。
        :param stock_list: 股票列表（个股 TS 代码列表，例如 ['600000.SH', '000001.SZ']）
        :param fields: 需要获取的字段，例如 ['open', 'high', 'low', 'close', 'vol']。
        :return: 多重索引结构的价格数据 DataFrame
        """
        
        field_mapping = {
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'vol': 'Volume',
        }
        mapped_fields = [field_mapping[field] for field in fields]  # 将字段名称映射为规范的大写形式
        
        all_data = []
        num = 0
        print("开始获取行情数据...")
        # 遍历股票列表
        for stock in stock_list:
            # 获取单只股票的日线数据
            df = self.pro.daily(ts_code=stock, start_date=self.start_date, end_date=self.end_date)
            df['ts_code'] = stock
            all_data.append(df)

            if sleep_time > 0:
                time.sleep(sleep_time)
            print(num)
            num += 1
        print("数据获取完成！")

        # 合并所有股票的数据
        combined_data = pd.concat(all_data)
        combined_data['trade_date'] = pd.to_datetime(combined_data['trade_date'])
        combined_data.set_index(['trade_date', 'ts_code'], inplace=True)

        # 选择需要的字段
        combined_data = combined_data[fields]
        combined_data.rename(columns=field_mapping, inplace=True)

        # 转换为多重索引结构，列为 (Symbol, 属性)
        new_structure_data = {}
        for stock in combined_data.index.get_level_values('ts_code').unique():
            for field in mapped_fields:
                key = (stock, field)
                series = combined_data.loc[combined_data.index.get_level_values('ts_code') == stock, field]
                series = series.droplevel('ts_code')  # 移除 ts_code，只有日期作为索引
                new_structure_data[key] = series

        # 转换为 DataFrame
        new_structure_df = pd.DataFrame(new_structure_data)

        new_structure_df = new_structure_df.sort_index(ascending=True)  # 按日期升序排列

        return new_structure_df
    
    # 直接调取数据会有个问题，就是可能会不让一次性拉太多数据，尤其是在跑全市场的话
    def get_prices_from_tushare_parallel(self, stock_list, fields=['close']):
        """
        获取多只股票的日线行情数据，重新整理为多重索引的 DataFrame。
        :param stock_list: 股票列表（个股 TS 代码列表，例如 ['600000.SH', '000001.SZ']）
        :param fields: 需要获取的字段，例如 ['open', 'high', 'low', 'close', 'vol']。
        :return: 多重索引结构的价格数据 DataFrame
        """
        
        field_mapping = {
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'vol': 'Volume',
        }
        mapped_fields = [field_mapping[field] for field in fields]  # 将字段名称映射为规范的大写形式
        
        print("开始获取行情数据...")
        # 一次性读取所有股票数据
        df_all = self.pro.daily(ts_code=','.join(stock_list), start_date=self.start_date, end_date=self.end_date)

        if df_all.empty:
            print("API 未返回任何数据，请检查输入参数！")
            return None

        print("数据获取完成！")
        
        # 处理数据
        df_all['trade_date'] = pd.to_datetime(df_all['trade_date'])
        df_all.set_index(['trade_date', 'ts_code'], inplace=True)
        # 选择需要的字段
        df_all = df_all[fields]
        df_all.rename(columns=field_mapping, inplace=True)

        # 转换为多重索引结构，列为 (Symbol, 属性)
        new_structure_data = {}
        for stock in df_all.index.get_level_values('ts_code').unique():
            for field in mapped_fields:
                key = (stock, field)
                series = df_all.loc[df_all.index.get_level_values('ts_code') == stock, field]
                series = series.droplevel('ts_code')  # 移除 ts_code，只有日期作为索引
                new_structure_data[key] = series
        # 转换为 DataFrame
        new_structure_df = pd.DataFrame(new_structure_data)
        new_structure_df = new_structure_df.sort_index(ascending=True)  # 按日期升序排列

        return new_structure_df
    

    def get_factors_from_tushare(self, stock_list, factors=['total_mv'], sleep_time=0):
        """
        获取多只股票的指定因子数据，重新整理为多重索引的 DataFrame。
        :param stock_list: 股票列表（个股 TS 代码列表）
        :param factors: 因子字段列表，例如 ['pe_ttm', 'pb', 'market_cap']。
        :return: 多重索引结构的因子数据 DataFrame
        """

        all_data = []
        factors_with_date = factors + ['trade_date'] # 得加上时间
        print("开始获取因子数据...")

        for stock in stock_list:
            # tushare提供的财务接口: income, balancesheet, cashflow, forecast, express
            # 每日数据（非离散）: daily_basic
            df = self.pro.daily_basic(ts_code=stock, start_date=self.start_date, end_date=self.end_date, fields=factors_with_date)
            df['ts_code'] = stock
            all_data.append(df)

            if sleep_time > 0:
                time.sleep(sleep_time)
                
        print("数据获取完成！")

        combined_data = pd.concat(all_data)

        combined_data['trade_date'] = pd.to_datetime(combined_data['trade_date'])

        combined_data.set_index(['trade_date', 'ts_code'], inplace=True)

        # 选择需要的因子字段
        combined_data = combined_data[factors]

        new_structure_data = {}
        for stock in combined_data.index.get_level_values('ts_code').unique():
            for field in factors:
                key = (stock, field)
                series = combined_data.loc[combined_data.index.get_level_values('ts_code') == stock, field]
                series = series.droplevel('ts_code')
                new_structure_data[key] = series

        # 转换为 DataFrame
        new_structure_df = pd.DataFrame(new_structure_data)
        new_structure_df = new_structure_df.sort_index(ascending=True)  # 按日期升序排列

        return new_structure_df
    
    def get_factors_from_tushare_parallel(self, stock_list, factors=['total_mv']):
        """
        获取多只股票的指定因子数据，重新整理为多重索引的 DataFrame。
        :param stock_list: 股票列表（个股 TS 代码列表）。
        :param factors: 因子字段列表，例如 ['pe_ttm', 'pb', 'market_cap']。
        :return: 多重索引结构的因子数据 DataFrame。
        """
        
        factors_with_date = factors + ['trade_date', 'ts_code']  # 确保返回因子的同时包含交易日期
        print("开始获取因子数据...")
        
        # 一次性读取所有股票的因子数据
        df_all = self.pro.daily_basic(ts_code=','.join(stock_list), start_date=self.start_date, end_date=self.end_date, fields=factors_with_date)

        if df_all.empty:
            print("API 未返回任何数据，请检查输入参数！")
            return None

        print("数据获取完成！")
        
        # 数据处理
        df_all['trade_date'] = pd.to_datetime(df_all['trade_date'])
        df_all.set_index(['trade_date', 'ts_code'], inplace=True)
        # 选择需要的因子字段
        df_all = df_all[factors]
        
        # 转换为多重索引结构，列为 (Symbol, 因子)
        new_structure_data = {}
        for stock in df_all.index.get_level_values('ts_code').unique():
            for factor in factors:
                key = (stock, factor)  # 多重索引的列格式 (Symbol, 因子)
                series = df_all.loc[df_all.index.get_level_values('ts_code') == stock, factor]
                series = series.droplevel('ts_code')  # 移除 ts_code，只保留日期索引
                new_structure_data[key] = series

        # 转换为 DataFrame
        new_structure_df = pd.DataFrame(new_structure_data)
        new_structure_df = new_structure_df.sort_index(ascending=True)  # 按日期升序排列

        return new_structure_df