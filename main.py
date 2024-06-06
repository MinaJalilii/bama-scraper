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


def insert_ad_data(connection, ads_list):
    try:
        cursor = connection.cursor()
        query = "INSERT INTO ads (ad_data, ad_code) VALUES (%s, %s)"
        # Extract ad_data and ad_code from ads_list
        records = [(json.dumps(ad['ad_data']), ad['ad_code']) for ad in ads_list]
        # Execute the query with executemany
        result = cursor.executemany(query, records)
        cursor.close()
        print(result)
    except Exception as e:
        print(f"Error inserting data: {e}")


def scrape_bama_data(url):
    connection = connect_to_db()
    if connection is None:
        return
    # start_time = time.time()
    try:
        session = requests_html.HTMLSession()
        j = 0
        while True:
            try:
                r = session.get(f'{url}?pageIndex={j}')
                r.raise_for_status()
                js = r.json()
                ads_list = []
                ads = js['data']['ads']  # [ 0, 1 , ... 30]
                if not ads:
                    break
                for i in ads:  # data0 , data1 , ... , data30
                    if i['type'] == 'ad':
                        code = i['detail']['code']  # code0, code1
                        ads_list.append({'ad_data': i, 'ad_code': code})
                    else:
                        continue
                insert_ad_data(connection, ads_list)
                if j == 5:
                    break
                j += 1
            except KeyboardInterrupt:
                print("KeyboardInterrupt detected. Exiting...")
                sys.exit(0)
            except Exception as e:
                print(f"An error occurred: {e}")
                break
    finally:
        # end_time = time.time()
        # execution_time = end_time - start_time
        # print(f"Execution time: {execution_time} seconds")
        if connection:
            connection.close()


# def sigterm_handler(signum, frame):
#     print("SIGTERM received. Exiting...")
#     sys.exit(0)


# signal.signal(signal.SIGTERM, sigterm_handler)
bama_url = 'https://bama.ir/cad/api/search'
scrape_bama_data(bama_url)
