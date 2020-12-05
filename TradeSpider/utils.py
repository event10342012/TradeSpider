import os

import psycopg2
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


def get_spider_root():
    return os.path.dirname(os.path.abspath(__file__))


def get_conn():
    return psycopg2.connect(
        host=os.environ.get('db_host'),
        user=os.environ.get('user'),
        dbname=os.environ.get('dbname'),
        port='5432'
    )


def read_sql(sql_name: str):
    sql_dir = os.path.join(get_spider_root(), 'sql', f'{sql_name}.sql')
    with open(sql_dir) as file:
        sql = file.read()
    return sql
