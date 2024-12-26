import requests_html
from sqlalchemy.dialects.postgresql import insert
from models import *
from custom_loggers import *


def parse_vehicles(url):
    html_session = requests_html.HTMLSession()
    r = html_session.get(url)
    r.raise_for_status()
    js = r.json()
    info_logger.info("Starting vehicle parsing process.")

    try:
        for brand in js["data"]:
            if brand["type"] == "brand" and "items" in brand:
                brand_title_fa = brand.get("title", "")
                brand_value_en = brand.get("value", "")
                make_stmt = insert(Make).values(
                    make=brand_value_en,
                    default_price=None
                ).on_conflict_do_nothing(index_elements=['make'])
                Session.execute(make_stmt)
                Session.commit()
                for item in brand["items"]:
                    if item["type"] == "model":
                        model_fa = item.get("title", "").replace(brand_title_fa, "").strip()
                        model_en = item.get("value", "").replace(f"{brand_value_en},", "").strip()
                        keywords_model = item.get("keywords", "")
                        title_en = brand_value_en + '-' + model_en
                        title_fa = brand_title_fa + '-' + model_fa

                        car_stmt = insert(Car).values(
                            make_fa=brand_title_fa,
                            make_en=brand_value_en,
                            model_fa=model_fa,
                            model_en=model_en,
                            keywords=keywords_model,
                            title_fa=title_fa,
                            title_en=title_en
                        ).on_conflict_do_nothing(index_elements=['make_fa', 'model_fa'])
                        Session.execute(car_stmt)
                        info_logger.info(f"One vehicle added: {title_fa}")
                        print(f"One vehicle added: {title_fa}")

        Session.commit()
        info_logger.info("Vehicle parsing completed successfully.")
        print("Vehicle parsing completed successfully.")

    except Exception as e:
        Session.rollback()
        error_logger.error(f"Error occurred during vehicle parsing: {e}", exc_info=True)

    finally:
        Session.close()


bama_url = 'https://bama.ir/cad/api/filter/vehicle'
parse_vehicles(bama_url)
