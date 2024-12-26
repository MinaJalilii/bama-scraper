import requests
import requests_html
import json
import signal
import sys
from models import RawAd, Session
from custom_loggers import info_logger


def fetch_latest_ad_by_code(session, ad_code):
    latest_ad = (
        session.query(RawAd)
        .filter(RawAd.ad_code == ad_code)
        .order_by(RawAd.version.desc())
        .first()
    )
    return latest_ad


def insert_updated_ad(session, new_ad, new_version):
    new_record = RawAd(
        raw_data=new_ad, ad_code=new_ad["detail"]["code"], version=new_version
    )
    session.add(new_record)
    info_logger.info(f"Updated {new_ad['detail']['code']} to version {new_version}")
    print(f"Updated {new_ad['detail']['code']} to version {new_version}")
    session.commit()


def update_ad_data(session, ads_list):
    for new_ad in ads_list:
        ad_code = new_ad["detail"]["code"]
        latest_ad = fetch_latest_ad_by_code(session, ad_code)
        if latest_ad:
            if json.dumps(latest_ad.raw_data, sort_keys=True) != json.dumps(
                new_ad, sort_keys=True
            ):
                new_version = latest_ad.version + 1
                insert_updated_ad(session, new_ad, new_version)


def update_bama_data(url):
    try:
        requests_session = requests_html.HTMLSession()
        page = 0
        while True:
            try:
                info_logger.info("Start updating ads....")
                response = requests_session.get(f"{url}?pageIndex={page}")
                response.raise_for_status()
                data = response.json()
                ads = data["data"]["ads"]

                if not ads:
                    print(f"No more ads found. Exiting loop.")
                    break

                ads_list = [ad for ad in ads if ad["type"] == "ad"]
                if not ads_list:
                    print(f"No more 'ad' type ads found. Exiting loop.")
                    break

                update_ad_data(Session, ads_list)

                print("Page:", page)
                page += 1
            except KeyboardInterrupt:
                print("KeyboardInterrupt detected. Exiting...")
                sys.exit(0)
            except requests.exceptions.RequestException as e:
                print(f"Request error: {e}. Retrying page {page}...")
                continue
            except Exception as e:
                print(f"Error fetching or processing data: {e}")
                break
    finally:
        Session.close()
        info_logger.info("Finish updating ads....")


def sigterm_handler(signum, frame):
    print("SIGTERM received. Exiting...")
    sys.exit(0)


signal.signal(signal.SIGTERM, sigterm_handler)
bama_url = "https://bama.ir/cad/api/search"
update_bama_data(bama_url)
