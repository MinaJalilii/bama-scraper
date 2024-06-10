import requests_html
import psycopg2
from config import DB_CONFIG


def pars_vehicles(url):
    conn = psycopg2.connect(
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port']
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
                    cursor.execute(
                        """
                        INSERT INTO cars (make_fa, make_en, model_fa, model_en, keywords) 
                        VALUES (%s, %s, %s, %s, %s)""",
                        (brand_title_fa, brand_value_en, model_fa, model_en, keywords_model)
                    )
    conn.commit()
    cursor.close()
    conn.close()


bama_url = 'https://bama.ir/cad/api/filter/vehicle'
pars_vehicles(bama_url)
