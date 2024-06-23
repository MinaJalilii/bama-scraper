from sqlalchemy import create_engine, Column, Integer, Text, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import insert
import requests_html
from config import DB_CONFIG
from custom_loggers import info_logger, error_logger
from dotenv import load_dotenv
import os

load_dotenv('.env')
Base = declarative_base()


class Car(Base):
    __tablename__ = 'cars'
    id = Column(Integer, primary_key=True, autoincrement=True)
    make_en = Column(Text)
    make_fa = Column(Text)
    model_en = Column(Text)
    model_fa = Column(Text)
    min_price = Column(Integer)
    max_price = Column(Integer)
    created_year = Column(Text)
    level_impact = Column(Text)
    keywords = Column(Text)
    title_fa = Column(Text)
    title_en = Column(Text)
    __table_args__ = (
        UniqueConstraint('make_fa', 'model_fa', name='uq_make_model'),
    )


db_url = os.getenv('DB_URL')

engine = create_engine(db_url)
Session = sessionmaker(bind=engine)

Base.metadata.create_all(engine)


def parse_vehicles(url):
    session = Session()
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

                for item in brand["items"]:
                    if item["type"] == "model":
                        model_fa = item.get("title", "").replace(brand_title_fa, "").strip()
                        model_en = item.get("value", "").replace(f"{brand_value_en},", "").strip()
                        keywords_model = item.get("keywords", "")
                        title_en = brand_value_en + '-' + model_en
                        title_fa = brand_title_fa + '-' + model_fa

                        car = Car(
                            make_fa=brand_title_fa,
                            make_en=brand_value_en,
                            model_fa=model_fa,
                            model_en=model_en,
                            keywords=keywords_model,
                            title_fa=title_fa,
                            title_en=title_en
                        )

                        insert_stmt = insert(Car).values(
                            make_fa=brand_title_fa,
                            make_en=brand_value_en,
                            model_fa=model_fa,
                            model_en=model_en,
                            keywords=keywords_model,
                            title_fa=title_fa,
                            title_en=title_en
                        ).on_conflict_do_nothing(index_elements=['make_fa', 'model_fa'])
                        session.execute(insert_stmt)

        session.commit()
        info_logger.info("Vehicle parsing completed successfully.")

    except Exception as e:
        session.rollback()
        error_logger.error(f"Error occurred during vehicle parsing: {e}", exc_info=True)

    finally:
        session.close()


bama_url = 'https://bama.ir/cad/api/filter/vehicle'
parse_vehicles(bama_url)