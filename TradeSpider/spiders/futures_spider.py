import os
from datetime import datetime
from zipfile import ZipFile

import pandas as pd
import scrapy

from TradeSpider.utils import get_spider_root, get_conn, read_sql


# scrapy crawl quotes -a tag=humor


class FuturesSpider(scrapy.Spider):
    name = 'futures_tick'
    data_dir = os.path.join(get_spider_root(), 'data')

    def start_requests(self):
        urls = [
            'https://www.taifex.com.tw/cht/3/dlFutPrevious30DaysSalesData'
        ]
        for url in urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response: scrapy.http.Response, **kwargs):
        ed = datetime.strptime(getattr(self, 'execution_date', None), '%Y%m%d')

        if ed is None:
            raise AttributeError('execution_date attribute should be given.')

        rows = response.xpath("//table[@class='table_c']")[1].xpath("tr")
        for row in rows[1:]:
            dt = row.xpath("td/text()")[1].get()
            dt = datetime.strptime(dt, '%Y/%m/%d')
            if ed == dt:
                url = row.xpath('td/input/@onclick')[1].get()

                start = url.find("'")
                end = url.rfind("'")
                url = url[start + 1:end]
                return response.follow(url, callback=self.download)

    def download(self, response):
        ed = datetime.strptime(getattr(self, 'execution_date', None), '%Y%m%d')

        file_path = os.path.join(self.data_dir, f'{ed.strftime("%Y%m%d")}_futures.zip')

        # download file
        with open(os.path.join(self.data_dir, file_path), 'wb') as file:
            file.write(response.body)
        self.logger.info('Download data')

        # unzip file
        with ZipFile(file_path, 'r') as zipfile:
            zipfile.extractall(self.data_dir)
        self.logger.info('Unzip data')

        bulk_file_path = self.resample()
        self.bulk_insert(bulk_file_path)

    def resample(self):
        ed = datetime.strptime(getattr(self, 'execution_date', None), '%Y%m%d')
        ed = ed.strftime('%Y_%m_%d')

        cols = ['txn_date', 'commodity_id', 'expired_date', 'txn_time', 'price',
                'volume', 'near_price', 'far_price', 'call_auction']

        input_filepath = os.path.join(self.data_dir, f'Daily_{ed}.csv')
        df = pd.read_csv(input_filepath, skiprows=1, names=cols, encoding='big5', dtype=str)
        df['txn_dt'] = pd.to_datetime(df['txn_date'] + ' ' + df['txn_time'])

        df.set_index('txn_dt', inplace=True)
        df.sort_values(['commodity_id', 'expired_date'], inplace=True)

        df['commodity_id'] = df['commodity_id'].str.strip()
        df['expired_date'] = df['expired_date'].str.strip()
        df['price'] = df['price'].astype(float)
        df['volume'] = df['volume'].astype(int)

        # re-sample time series
        open_price = df.groupby(['commodity_id', 'expired_date']).resample('1min')['price'].first().fillna(
            method='ffill').rename('open_price')
        high_price = df.groupby(['commodity_id', 'expired_date']).resample('1min')['price'].max().fillna(
            method='ffill').rename('high_price')
        low_price = df.groupby(['commodity_id', 'expired_date']).resample('1min')['price'].min().fillna(
            method='ffill').rename('low_price')
        close_price = df.groupby(['commodity_id', 'expired_date']).resample('1min')['price'].last().fillna(
            method='ffill').rename('close_price')
        volume = df.groupby(['commodity_id', 'expired_date']).resample('1min')['volume'].sum().fillna(
            method='ffill').rename('volume')

        txn_df = pd.merge(open_price, high_price, left_index=True, right_index=True)
        txn_df = txn_df.merge(low_price, left_index=True, right_index=True)
        txn_df = txn_df.merge(close_price, left_index=True, right_index=True)
        txn_df = txn_df.merge(volume, left_index=True, right_index=True)

        txn_df.reset_index(['commodity_id', 'expired_date'], inplace=True)

        output_filepath = os.path.join(self.data_dir, f'futures_1min_{ed}.csv')
        txn_df.to_csv(output_filepath)
        self.logger.info('resample data to 1min')
        return output_filepath

    def bulk_insert(self, file_path):
        bulk_sql = f'''
        truncate table futures.txn_stage;
        
        COPY futures.txn_stage
            FROM '{file_path}'
            (HEADER TRUE, FORMAT CSV, ENCODING 'UTF8');
        '''

        txn_sql = read_sql('txn')

        with get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(bulk_sql)
                cursor.execute(txn_sql)
            conn.commit()
        self.logger.info('Bulk insert data')
        'ee'
