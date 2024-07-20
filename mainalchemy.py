from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from requests_html import HTMLSession
import json
import signal
import sys
from models import *


def connect_to_db():
    try:
        return Session
    except SQLAlchemyError as e:
        print(f"Error connecting to the database: {e}")
        return None


def insert_ad_data(session, ads_list):
    try:
        insert_stmt = pg_insert(RawAd).values(ads_list)
        on_conflict_stmt = insert_stmt.on_conflict_do_nothing(
            index_elements=['ad_code', 'version']
        )
        result = session.execute(on_conflict_stmt)
        session.commit()
        return result.rowcount
    except SQLAlchemyError as e:
        print(f"Error inserting data: {e}")
        session.rollback()
        return


def scrape_bama_data(url):
    session = connect_to_db()
    if session is None:
        return
    try:
        html_session = HTMLSession()
        j = 0
        s = 0
        while True:
            try:
                r = html_session.get(f'{url}?pageIndex={j}')
                r.raise_for_status()
                js = r.json()
                ads_list = []
                ads = js['data']['ads']
                if not ads:
                    break
                for ad in ads:
                    if ad['type'] == 'ad':
                        code = ad['detail']['code']
                        ads_list.append({'raw_data': ad, 'ad_code': code})
                    else:
                        continue
                result = insert_ad_data(session, ads_list)
                if result == 0:
                    s += 1
                else:
                    s = 0
                print('page:', j)
                print('repeat counter:', s)
                print('Insert result:', result)
                print('----------------------')
                if s >= 5:
                    print("5 repeated pages with zero result detected. Exiting loop.")
                    break
                j += 1
            except KeyboardInterrupt:
                print("KeyboardInterrupt detected. Exiting...")
                sys.exit(0)
    finally:
        if session:
            session.close()


def sigterm_handler(signum, frame):
    print("SIGTERM received. Exiting...")
    sys.exit(0)


signal.signal(signal.SIGTERM, sigterm_handler)
bama_url = 'https://bama.ir/cad/api/search'
scrape_bama_data(bama_url)
