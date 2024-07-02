import requests_html
import json
import signal
import sys
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, Column, Integer, BigInteger, Text, DateTime, UniqueConstraint
from sqlalchemy.dialects.postgresql import insert, JSONB
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import func

load_dotenv('.env')

DATABASE_URL = os.getenv('DB_URL')

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()


class RawAd(Base):
    __tablename__ = 'raw_ads'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    raw_data = Column(JSONB, nullable=False)
    ad_code = Column(Text, nullable=False)
    process_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    version = Column(Integer, nullable=False, default=0)

    __table_args__ = (UniqueConstraint('ad_code', 'version', name='unique_ad_code_version'),)


Base.metadata.create_all(engine)


def insert_ad_data(session, ads_list):
    stmt = insert(RawAd).values(ads_list).on_conflict_do_nothing(index_elements=['ad_code', 'version'])
    result = session.execute(stmt)
    session.commit()
    return result.rowcount


def scrape_bama_data(url):
    session = Session()
    try:
        requests_session = requests_html.HTMLSession()
        page = 0
        repeat_counter = 0
        while True:
            try:
                response = requests_session.get(f'{url}?pageIndex={page}')
                response.raise_for_status()
                data = response.json()
                ads = data['data']['ads']

                if not ads:
                    break

                ads_list = []
                for ad in ads:
                    if ad['type'] == 'ad':
                        ad_code = ad['detail']['code']
                        ad = json.dumps(ad)
                        ads_list.append({'raw_data': ad, 'ad_code': ad_code})

                result = insert_ad_data(session, ads_list)
                if result == 0:
                    repeat_counter += 1
                else:
                    repeat_counter = 0

                print('Page:', page)
                print('Repeat counter:', repeat_counter)
                print('Insert result:', result)
                print('----------------------')

                if repeat_counter >= 3:
                    print("3 repeated pages with zero result detected. Exiting loop.")
                    break

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
scrape_bama_data(bama_url)
