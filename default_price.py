from sqlalchemy.exc import SQLAlchemyError
from custom_loggers import info_logger
from models import *


def populate_default_prices(session):
    try:
        makes = session.query(Make).all()
        info_logger.info("Start populating default prices...")
        for make in makes:
            avg_price = session.query(
                func.avg(PriceReference.avg_price)
            ).join(Car, PriceReference.car_id == Car.id).filter(
                Car.make_en == make.make
            ).scalar()
            if avg_price:
                make.default_price = int(avg_price)
                print(f"Default price for {make.make}: {make.default_price}")
                info_logger.info(f"Default price for {make.make}: {make.default_price}")
        session.commit()
        info_logger.info("Default prices populated successfully.")
    except SQLAlchemyError as e:
        session.rollback()
        print("Error:", e)
    finally:
        session.close()


populate_default_prices(Session)
