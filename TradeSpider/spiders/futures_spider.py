import logging
import os
from datetime import datetime
from zipfile import ZipFile

import pandas as pd
import scrapy
import psycopg2

from TradeSpider.utils import get_spider_root

logging.getLogger('airflow.task')


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

        self.bulk_insert()

    def bulk_insert(self):
        ed = datetime.strptime(getattr(self, 'execution_date', None), '%Y%m%d')
        file_path = os.path.join(self.data_dir, f'Daily_{ed.strftime("%Y_%m_%d")}.csv')

        row_df = pd.read_csv(file_path, encoding='big5', dtype=str)
        row_df = row_df.iloc[:, :-3]
        row_df.to_csv(file_path, index=None)
        self.logger.info('Clean data')

        sql = f'''
        truncate table ods.futures.tw_futures_txn_stage;

        COPY ods.futures.tw_futures_txn_stage
            FROM '{file_path}'
            (HEADER TRUE, FORMAT CSV, ENCODING 'UTF8');
        '''
        conn_args = dict(
            host='localhost',
            user='leochen',
            # password=conn.password,
            dbname='ods',
            port='5432',
        )
        with psycopg2.connect(**conn_args) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
            conn.commit()
        self.logger.info('Bulk insert data')
