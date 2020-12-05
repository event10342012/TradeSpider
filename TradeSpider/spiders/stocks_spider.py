import os

import pandas_datareader.data as web
import typer
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine
from tqdm import tqdm

from utils import get_conn

load_dotenv(find_dotenv())


def get_stocks_id():
    with get_conn() as conn:
        with conn.cursor() as cursor:
            sql = "select commodity_id from stock.commodity where market_type = '上市'"
            cursor.execute(sql)
            rows = cursor.fetchall()
    rows = [row[0] for row in rows]
    return rows


def get_stock_data(stock_id: str, start=None, end=None):
    if start and end:
        df = web.get_data_yahoo(f'{stock_id}.tw', start=start, end=end)
    else:
        df = web.get_data_yahoo(f'{stock_id}.tw')

    col_map = {
        'Date': 'date',
        'High': 'high_price',
        'Low': 'low_price',
        'Open': 'open_price',
        'Close': 'close_price',
        'Volume': 'volume',
        'Adj Close': 'adj_close_price'
    }

    df.reset_index(inplace=True)
    df['commodity_id'] = stock_id
    df.rename(col_map, axis=1, inplace=True)

    host = os.environ.get('db_host')
    user = os.environ.get('user')
    db = os.environ.get('dbname')
    engine = create_engine(f'postgresql+psycopg2://{user}@{host}/{db}')
    df.to_sql('txn', engine, schema='stock', if_exists='append', index=False)


def main(sd: str = None, ed: str = None):
    stocks = get_stocks_id()
    for stock_id in tqdm(stocks):
        try:
            get_stock_data(stock_id, start=sd, end=ed)
        except Exception as e:
            print(stock_id, e.args)


if __name__ == '__main__':
    typer.run(main)
