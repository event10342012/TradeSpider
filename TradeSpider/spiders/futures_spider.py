import logging
import os
from datetime import datetime
from zipfile import ZipFile

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
        rows = response.xpath("//tr[@class='color']")
        for row in rows:
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

        # unzip file
        with ZipFile(file_path, 'r') as zipfile:
            zipfile.extractall(self.data_dir)

        self.bulk_insert()

    def bulk_insert(self):
        ed = datetime.strptime(getattr(self, 'execution_date', None), '%Y%m%d')
        file_path = os.path.join(self.data_dir, f'Daily_{ed.strftime("%Y_%m_%d")}.csv')
        sql = f'''
        COPY futures.tw_futures_txn
            FROM '{file_path}'
            (HEADER TRUE, FORMAT CSV, ENCODING 'big5');
        '''
        conn_args = dict(
            host='localhost',
            user='leochen',
            # password=conn.password,
            dbname='trading',
            port='5432',
        )
        with psycopg2.connect(**conn_args) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
            conn.commit()
