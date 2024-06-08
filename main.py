import requests_html
import psycopg2
import json
from psycopg2.extras import execute_values
from config import DB_CONFIG
import signal
import sys
import time


def connect_to_db():
    try:
        connection = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        return connection
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None


def generate_insert_query(table_name, columns, values_list):
    columns_str = ', '.join(columns)
    values_placeholder = ', '.join(['%s'] * len(values_list))
    query = f"INSERT INTO {table_name} ({columns_str}) VALUES {values_placeholder} ON CONFLICT (ad_code) DO NOTHING"
    return query, values_list


def insert_ad_data(connection, ads_list):
    s = 0
    table_name = "ads"
    columns = ["ad_data", "ad_code"]
    cursor = connection.cursor()
    query, values = generate_insert_query(table_name, columns, ads_list)
    cursor.execute(query, values)
    if cursor.rowcount == 0:
        s += 1
    if s >= 3:
        connection.commit()
        cursor.close()
        return "Ads are repeating..."
    connection.commit()
    cursor.close()


def scrape_bama_data(url):
    connection = connect_to_db()
    if connection is None:
        return
    try:
        session = requests_html.HTMLSession()
        j = 0
        while True:
            try:
                r = session.get(f'{url}?pageIndex={j}')
                r.raise_for_status()
                js = r.json()
                ads_list = []
                ads = js['data']['ads']
                if not ads:
                    break
                for i in ads:
                    if i['type'] == 'ad':
                        code = i['detail']['code']
                        i = json.dumps(i)
                        ads_list.append((i, code))
                    else:
                        continue
                insert_ad_data(connection, ads_list)
                if j == 10:
                    break
                j += 1
            except KeyboardInterrupt:
                print("KeyboardInterrupt detected. Exiting...")
                sys.exit(0)
    finally:
        if connection:
            connection.close()


def sigterm_handler(signum, frame):
    print("SIGTERM received. Exiting...")
    sys.exit(0)


signal.signal(signal.SIGTERM, sigterm_handler)
bama_url = 'https://bama.ir/cad/api/search'
scrape_bama_data(bama_url)
