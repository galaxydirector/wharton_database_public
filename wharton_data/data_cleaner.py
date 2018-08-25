import pandas as pd
import numpy as np
import datetime as dt
from tqdm import tqdm
from pandas import DatetimeIndex
import time

import pprint
pp = pprint.PrettyPrinter(indent=4)

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f ms' % (method.__name__, (te - ts) * 1000))
        return result
    return timed

class DataCleaner(object):
    """docstring for DataCleaner"""
    standard_time_format_len = len("00:00:00.000000")
    def __init__(self):
        pass        

    def validate_data_completeness(self, df, time_column_name, f_name=None, validate_market_data_only=True):
        if validate_market_data_only:
            df_to_validate = df.between_time('9:30', '16:00', include_end=False)

            try:
                assert len(df_to_validate) == 23400
            except AssertionError as e:
                if f_name is not None:
                    print("In file {}".format(f_name))
                # df_to_validate.to_csv('~/Desktop/faulty_data.csv')
                print(df_to_validate)
                print('There are some seconds missing in the market data! Expected {} Actual {}'.format(23400, len(df_to_validate)))
                raise e
        else:
            start_time = df[time_column_name].iloc[0]
            end_time = df[time_column_name].iloc[-1]

            try:
                assert len(df)-1 == (end_time - start_time).seconds
            except AssertionError as e:
                if f_name is not None:
                    print("In file {}".format(f_name))
                print(df)
                print('There are some seconds missing in the data! Expected {} Actual {}'.format((end_time - start_time).seconds, len(resampled_data)))
                raise e

    def clean_time_column(self, data):
        """ Remove any time entries not having the format
        %h:%m:%s.%f.

        Actual pattern matching is too expensive so 
        we just check the length of the time.
        
        Returns the data with replaced time column.
        """
        return data.assign(TIME = [i+".000000"  if len(i) < self.standard_time_format_len else i for i in data['TIME']])


    def set_date_with_time_as_index(self, data):
        date_column = data['DATE']
        time_column = data['TIME']

        a_random_type = type(date_column.iloc[2])
        if   a_random_type is np.int64: date_column = date_column.apply(lambda x: str(x)).apply(lambda s: "{}-{}-{}".format(s[:4], s[4:6], s[6:8]))
        elif a_random_type is dt.date : date_column = date_column.apply(lambda x: dt.datetime.strftime(x,'%Y-%m-%d'))

        return data.assign(DATETIME = pd.to_datetime(date_column + ' ' + time_column, format='%Y-%m-%d %H:%M:%S'))\
                    .drop(columns=['DATE','TIME'])\
                    .set_index('DATETIME')\
                    .sort_index()

    def temporal_downsampling(self, data, scale, fill_holes_in_time=True):
        data = data.resample(scale).mean()
        if fill_holes_in_time: data = data.fillna(method='ffill')
        data = data.dropna()

        return data

    def accumulate_trading_volume(self, data, scale):
        data = data.resample(scale).sum()

        try:
            assert data.notna().all()
        except AssertionError as e:
            print('`accumulated_size` has NaN value. Why?')
            print(data[data.isna()])

        return data

    def test_for_nan(self, data):
        try:
            assert data.notna().all().all()
        except AssertionError as e:
            print('Merged resampled data from %s has NaN value. Why?'%data['SYMBOL'].head(1))
            with pd.option_context('display.max_rows', None, 'display.max_columns', 5):
                data.to_csv('~/Desktop/faulty_data.csv')
                print(data)
                print(data[data.isna().any()])
            data = self.drop_na(data)

    def add_symbol_column(self, data, stock_symbol):
        data = data.assign(SYMBOL=stock_symbol)
        return data

    def add_datetime_column(self, data):
        return data.reset_index()

    def drop_na(self, data):
        return data.dropna()

    def reset_col_name(self, df, name_pattern, new_name):
        col = df.filter(regex=(name_pattern))
        old_column_name = col.columns[0]
        
        if new_name != old_column_name:
            df[new_name] = col
            df = df.drop([old_column_name], axis=1)

        return df

    def unify_column_names(self, df):
        df.columns = df.columns.str.upper()
        
        if 'SYM_ROOT' in df.columns:
            df.rename(index=str, columns={'SYM_ROOT': 'SYMBOL', 'TIME_M': 'TIME'}, inplace=True)

        return df[['DATE', 'TIME', 'SYMBOL', 'PRICE', 'SIZE']]

    def get_time_from_datetime_index(self, data, i):
        try:
            return data.index[i].strftime("%H:%M:%S")
        except:
            return

    def separate_market_and_otc_data(self, data):
        before_market_end_time = '09:30:00'
        before_market_start_time = None
        i = 0
        while before_market_start_time==None:
            before_market_start_time = self.get_time_from_datetime_index(data, i)
            i += 1


        after_market_start_time = '16:00:00'
        after_market_end_time = None
        i = -1
        while after_market_end_time==None:
            after_market_end_time = self.get_time_from_datetime_index(data, i)
            i -= 1

        before_market_data = data.between_time(before_market_start_time, before_market_end_time, include_end=False)
        market_data        = data.between_time('9:30', "16:00", include_end=False)
        after_market_data  = data.between_time(after_market_start_time, after_market_end_time)

        return before_market_data, market_data, after_market_data


    def _fill_missing_seconds(self, before_market, market):
        """ Using forward filling and backward filling to fill any missing seconds at the start and end of the market

        """
        if len(market) < 23400:
            start_dt = "{} 09:30:00".format(market.index[0].strftime("%Y-%m-%d %H:%M:%S").split(' ')[0])
            filled_date_range = pd.date_range(start_dt, periods=23400, freq='S')
            market = market.reindex(filled_date_range)

            market['SIZE'] = market['SIZE'].fillna(0).astype(int)

            if len(before_market['PRICE']) > 0:
                market['PRICE'] = market['PRICE'].fillna(method='ffill')
                market['PRICE'] = market['PRICE'].fillna(before_market['PRICE'].iloc[-1])
            else:
                market['PRICE'] = market['PRICE'].fillna(method='bfill')
                market['PRICE'] = market['PRICE'].fillna(method='ffill')


            try:
                assert len(market) == 23400
            except AssertionError as e:
                print("Oops! Market data is still under length. Acutal length: {}".format(len(market)))

        return market

    def post_process(self, data, data_symbol):
        data = self.unify_column_names(data)

        
        try:
            assert data.SYMBOL.nunique()==1, 'More than one symbol is extracted. Why?'
            symbol = data['SYMBOL'].iloc[0]
            assert data_symbol == symbol
        except AssertionError as e:
            print("symbol is not unique or it does not match the requested symbol!")
            print("Expected %s, Acutal %s"%(data_symbol, symbol))
            
            return pd.DataFrame(), 1

        #print('[DEBUG] Data types of date and time columns are `%s`.' % data[['DATE', 'TIME']].dtypes)
        #print('[DEBUG] A sample of time is of type `%s`.' % type(data.TIME.sample(1).iloc[0]))
        data = self.set_date_with_time_as_index(data)
        before_market_price_data = data['PRICE']

        before_market_data, market_data, after_market_data = self.separate_market_and_otc_data(data)
        market_resampled_price  = self.temporal_downsampling(market_data['PRICE'], '1S')        
        market_accumulated_size = self.accumulate_trading_volume(market_data['SIZE'], '1S')

        before_market_resampled_price  = self.temporal_downsampling(before_market_data['PRICE'], '1S', fill_holes_in_time=False)        
        before_market_accumulated_size = self.accumulate_trading_volume(before_market_data['SIZE'], '1S')

        after_market_resampled_price  = self.temporal_downsampling(after_market_data['PRICE'], '1S', fill_holes_in_time=False)        
        after_market_accumulated_size = self.accumulate_trading_volume(after_market_data['SIZE'], '1S')

        resampled_market_data = pd.concat([market_resampled_price, market_accumulated_size], axis=1)

        resampled_before_market_data = pd.concat(
            [before_market_resampled_price, before_market_accumulated_size], 
            axis=1
        ).dropna()


        resampled_after_market_data = pd.concat(
            [after_market_resampled_price, after_market_accumulated_size], 
            axis=1
        ).dropna()

        resampled_market_data = self._fill_missing_seconds(resampled_before_market_data, resampled_market_data)
        # resampled_data = pd.concat(
        #     [resampled_before_market_data,
        #      resampled_market_data,
        #      resampled_after_market_data
        #     ], axis=0
        # ).dropna()
                
        resampled_data = self.add_symbol_column(resampled_market_data, symbol)

        self.validate_data_completeness(resampled_data, 'DATETIME')
        self.test_for_nan(resampled_data)
        resampled_data = self.add_datetime_column(resampled_data)

        return resampled_data, 0
