<<<<<<< Updated upstream
=======
import requests_html
import psycopg2
import json
from config import DB_CONFIG


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
                        insert_ad_data(connection, i)
                else:
                    continue
            j += 1
    except requests_html.RequestException as e:
        print(f"Request error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if connection:
            connection.close()


bama_url = 'https://bama.ir/cad/api/search'
scrape_bama_data(bama_url)
>>>>>>> Stashed changes
