import requests_html
import json
import signal
import sys
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from mainalchemy import RawAd

load_dotenv('.env')

DATABASE_URL = os.getenv("DB_URL")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()


def fetch_latest_ad_by_code(session, ad_code):
    latest_ad = session.query(RawAd).filter(RawAd.ad_code == ad_code).order_by(RawAd.version.desc()).first()
    return latest_ad


def insert_updated_ad(session, new_ad, new_version):
    new_record = RawAd(
        raw_data=new_ad,
        ad_code=new_ad['detail']['code'],
        version=new_version
    )
    session.add(new_record)
    print(f"updated {new_ad['detail']['code']}")
    session.commit()


def update_ad_data(session, ads_list):
    for new_ad in ads_list:
        ad_code = new_ad['detail']['code']
        latest_ad = fetch_latest_ad_by_code(session, ad_code)
        if latest_ad:
            if json.dumps(latest_ad.raw_data, sort_keys=True) != json.dumps(new_ad, sort_keys=True):
                new_version = latest_ad.version + 1
                insert_updated_ad(session, new_ad, new_version)


def update_bama_data(url):
    session = Session()
    try:
        requests_session = requests_html.HTMLSession()
        page = 0
        while True:
            try:
                response = requests_session.get(f'{url}?pageIndex={page}')
                response.raise_for_status()
                data = response.json()
                ads = data['data']['ads']

                if not ads:
                    break

                ads_list = [ad for ad in ads if ad['type'] == 'ad']
                update_ad_data(session, ads_list)

                print('Page:', page)
                page += 1
            except KeyboardInterrupt:
                print("KeyboardInterrupt detected. Exiting...")
                sys.exit(0)
            except Exception as e:
                print(f"Error fetching or processing data: {e}")
                break
    finally:
        session.close()


def sigterm_handler(signum, frame):
    print("SIGTERM received. Exiting...")
    sys.exit(0)


signal.signal(signal.SIGTERM, sigterm_handler)
bama_url = 'https://bama.ir/cad/api/search'
update_bama_data(bama_url)
