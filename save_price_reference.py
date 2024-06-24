from sqlalchemy import create_engine, Column, BigInteger, Text, JSON, DateTime, func, ForeignKey, Integer, Float, \
    UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from custom_loggers import info_logger, error_logger
from dotenv import load_dotenv
import os
from get_fields_sqlalchemy import Car, Ad, RawAd, Dealer

load_dotenv('.env')

Base = declarative_base()
db_url = os.getenv('DB_URL')


class PriceReference(Base):
    __tablename__ = 'price_reference'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    make = Column(Text, nullable=False)
    model = Column(Text, nullable=False)
    year = Column(Text, nullable=False)
    avg_price = Column(BigInteger, nullable=False)
    count = Column(BigInteger, nullable=False)
    __table_args__ = (
        UniqueConstraint('make', 'model', 'year', name='uq_make_model_year'),
    )


engine = create_engine(db_url)
Session = sessionmaker(bind=engine)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)
session = Session()


def save_avg_price_reference(session):
    query = session.query(
        func.count(Ad.id),
        func.round(func.avg(Ad.price)).label('average_price'),
        Car.make_en,
        Car.model_en,
        Ad.year
    ).join(Car).filter(
        Ad.price_type == 'with price',
        Ad.price >= 1000000000
    ).group_by(
        Car.make_en,
        Car.model_en,
        Ad.year
    )
    try:
        # min max
        for count, average_price, make_en, model_en, year in query:
            price_ref = PriceReference(
                make=make_en,
                model=model_en,
                year=year,
                avg_price=int(average_price),
                count=count
            )
            session.add(price_ref)
            print(f"added -> {make_en} - {model_en}")

        session.commit()
        info_logger.info("Price references inserted successfully.")
    except Exception as e:
        session.rollback()
        error_logger.error(f"Error occurred: {e}")
    finally:
        session.close()
    # for ad in session.query(Ad).all():
    #     found_result = None
    #     for result in aggregated_results:
    #         if (result.make_en == ad.car.make_en and
    #                 result.model_en == ad.car.model_en and
    #                 result.year == ad.year):
    #             found_result = result
    #             break
    #     if found_result:
    #         level = calculate_level(found_result.average_price)
    #         ad.level = level


save_avg_price_reference(session)
