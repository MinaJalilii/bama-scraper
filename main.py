import requests_html
<<<<<<< HEAD
<<<<<<< HEAD
import psycopg2
import json
from psycopg2.extras import execute_values
from config import DB_CONFIG
import signal
import sys
import time
=======
import psycopg2
import json
import signal
import sys
from dotenv import load_dotenv
import os

load_dotenv('.env')
>>>>>>> origin/feature/create-dotenv


def connect_to_db():
    try:
        connection = psycopg2.connect(
<<<<<<< HEAD
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
=======
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
>>>>>>> origin/feature/create-dotenv
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
<<<<<<< HEAD
    table_name = "ads"
    columns = ["ad_data", "ad_code"]
=======
    table_name = "raw_ads"
    columns = ["raw_data", "ad_code"]
>>>>>>> origin/feature/create-dotenv
    cursor = connection.cursor()
    query, values = generate_insert_query(table_name, columns, ads_list)
    cursor.execute(query, values)
    connection.commit()
    cursor.close()
<<<<<<< HEAD
=======
    return cursor.rowcount
>>>>>>> origin/feature/create-dotenv


def scrape_bama_data(url):
    connection = connect_to_db()
    if connection is None:
        return
    try:
        session = requests_html.HTMLSession()
<<<<<<< HEAD
=======


def scrape_bama_data(url):
    try:
        session = requests_html.HTMLSession()
        encountered_ads = []
        repeated_ads_count = 0
>>>>>>> origin/feature/base-codes
        j = 0
        while True:
            try:
                r = session.get(f'{url}?pageIndex={j}')
<<<<<<< HEAD
=======
        j = 0
        s = 0
        while True:
            try:
                r = session.get(f'{url}?pageIndex={j}')
>>>>>>> origin/feature/create-dotenv
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
<<<<<<< HEAD
                insert_ad_data(connection, ads_list)
                if j == 10:
=======
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
>>>>>>> origin/feature/create-dotenv
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
<<<<<<< HEAD
=======
            except requests_html.HTTPError as e:
                print(f"HTTP error occurred: {e}")
            r.raise_for_status()
            js = r.json()
            ads = js['data']['ads']
            if not ads:
                break
            for i in ads:
                if i['type'] == 'ad':
                    code = i['detail']['code']
                    if code in encountered_ads:
                        repeated_ads_count += 1
                        # If the count reaches 36, stop scraping
                        if repeated_ads_count >= 36:
                            break
                    else:
                        repeated_ads_count = 0
                        encountered_ads.append(code)
                else:
                    continue
            j += 1
    except requests_html.RequestException as e:
        print(f"Request error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


>>>>>>> origin/feature/base-codes
=======
>>>>>>> origin/feature/create-dotenv
bama_url = 'https://bama.ir/cad/api/search'
scrape_bama_data(bama_url)
