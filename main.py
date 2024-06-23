import requests_html
import psycopg2
import json
import signal
import sys
from dotenv import load_dotenv
import os

load_dotenv('.env')


def connect_to_db():
    try:
        connection = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
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
    table_name = "raw_ads"
    columns = ["raw_data", "ad_code"]
    cursor = connection.cursor()
    query, values = generate_insert_query(table_name, columns, ads_list)
    cursor.execute(query, values)
    connection.commit()
    cursor.close()
    return cursor.rowcount


def scrape_bama_data(url):
    connection = connect_to_db()
    if connection is None:
        return
    try:
        session = requests_html.HTMLSession()
        j = 0
        s = 0
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
                result = insert_ad_data(connection, ads_list)
                if result == 0:
                    s += 1
                else:
                    s = 0
                print('page:', j)
                print('repeat counter:', s)
                print('Insert result:', result)
                print('----------------------')
                if s >= 3:
                    print("3 repeated pages with zero result detected. Exiting loop.")
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
