import requests_html
import psycopg2
import json
from config import DB_CONFIG
import signal
import sys


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


def insert_ad_data(connection, ad_data):
    try:
        cursor = connection.cursor()
        # Insert the ad_data as a JSONB object
        cursor.execute(
            "INSERT INTO ads (ad_data) VALUES (%s)",
            [json.dumps(ad_data)]
        )
        connection.commit()
        cursor.close()
    except Exception as e:
        print(f"Error inserting data: {e}")


def sigterm_handler(signum, frame):
    print("SIGTERM received. Exiting...")
    sys.exit(0)


def scrape_bama_data(url):
    connection = connect_to_db()
    if connection is None:
        return
    try:
        session = requests_html.HTMLSession()
        encountered_ads = []
        repeated_ads_count = 0
        j = 0
        while True:
            try:
                r = session.get(f'{url}?pageIndex={j}')
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
                            if repeated_ads_count >= 36:
                                break
                        else:
                            repeated_ads_count = 0
                            encountered_ads.append(code)
                            insert_ad_data(connection, i)
                    else:
                        continue
                print(j)
                if j == 10:
                    break
                j += 1
            except KeyboardInterrupt:
                print("KeyboardInterrupt detected. Exiting...")
                sys.exit(0)
            except Exception as e:
                print(f"An error occurred: {e}")
                break
    finally:
        if connection:
            connection.close()


signal.signal(signal.SIGTERM, sigterm_handler)
bama_url = 'https://bama.ir/cad/api/search'
scrape_bama_data(bama_url)
