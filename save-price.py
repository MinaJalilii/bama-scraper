from custom_loggers import *
from sqlalchemy.dialects.postgresql import insert
from models import *


def save_price(session):
    query = (
        session.query(
            func.count(Car.id).label("ad_count"),
            Car.id,
            func.round(func.avg(Ad.price)).label("average_price"),
            func.min(Ad.price).label("min_price"),
            func.max(Ad.price).label("max_price"),
            Ad.year,
        )
        .join(Car)
        .filter(Ad.price_type == "with price", Ad.price >= 1000000000)
        .group_by(Car.id, Ad.year)
    )
    try:
        info_logger.info(f"Start saving prices....")
        for count, car_id, average_price, min_price, max_price, year in query:
            price_stmt = (
                insert(PriceReference)
                .values(
                    count=count,
                    car_id=car_id,
                    avg_price=int(average_price),
                    min_price=int(min_price),
                    max_price=int(max_price),
                    year=year,
                )
                .on_conflict_do_nothing(index_elements=["car_id", "year"])
            )
            session.execute(price_stmt)
            print(f"added -> {car_id} - {year}")
            info_logger.info(f"added -> {car_id} - {year}")
        session.commit()
    except Exception as e:
        session.rollback()
        error_logger.error(f"Error occurred: {e}")
        print(f"error: {e}")
    finally:
        session.close()
        info_logger.info(f"Finish saving prices....")


save_price(Session)
