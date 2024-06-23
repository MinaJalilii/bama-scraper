import psycopg2
from dotenv import load_dotenv
import os

load_dotenv('.env')
conn = psycopg2.connect(
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT')
)

cursor = conn.cursor()

cursor.execute("SELECT id, raw_data, ad_code FROM raw_ads")

rows = cursor.fetchall()

for row in rows:
    raw_id, raw_data, ad_code = row

    print("ID:", raw_id)
    print("CODE:", ad_code)

    if raw_data is not None:
        # Extract data
        price = int(raw_data.get('price', {}).get('price', '0').replace(',', ''))
        if raw_data.get('dealer') is not None:
            dealer_type = raw_data.get('dealer', {}).get('type')
        else:
            dealer_type = 'شخصی'
        car_title = raw_data.get('detail', {}).get('title', '')
        location = raw_data.get('detail', {}).get('location', '')
        created_at = raw_data.get('detail', {}).get('modified_date', None)
        code = raw_data.get('detail', {}).get('code', '')
        cursor.execute(
            "INSERT INTO ads (car_title, price, location, dealer_type, created_at, code) VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (code) DO NOTHING",
            (car_title, price, location, dealer_type, created_at, code)
        )

conn.commit()
cursor.close()
conn.close()
