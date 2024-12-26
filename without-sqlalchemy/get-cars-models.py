import requests_html
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv('../.env')


def parse_vehicles(url):
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )
    cursor = conn.cursor()
    session = requests_html.HTMLSession()
    r = session.get(url)
    r.raise_for_status()
    js = r.json()
    for brand in js["data"]:
        if brand["type"] == "brand" and "items" in brand:
            brand_title_fa = brand.get("title", "")
            brand_value_en = brand.get("value", "")

            for item in brand["items"]:
                if item["type"] == "model":
                    model_fa = item.get("title", "").replace(brand_title_fa, "").strip()
                    model_en = item.get("value", "").replace(f"{brand_value_en},", "").strip()
                    keywords_model = item.get("keywords", "")
                    title_en = brand_value_en + ' ' + model_en
                    title_fa = brand_title_fa + ' ' + model_fa
                    cursor.execute(
                        """INSERT INTO cars (make_fa, make_en, model_fa, model_en, keywords, title_fa, title_en) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (brand_title_fa, brand_value_en, model_fa, model_en, keywords_model, title_fa, title_en)
                    )
    conn.commit()
    cursor.close()
    conn.close()


bama_url = 'https://bama.ir/cad/api/filter/vehicle'
parse_vehicles(bama_url)
