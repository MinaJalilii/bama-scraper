from sqlalchemy.orm import aliased
from custom_loggers import *
from models import *


def calculate_level(avg_price):
    level_price_steps = os.getenv('LEVEL_PRICE_STEPS',
                                  '0,300000000,600000000,1000000000,3000000000,5000000000,7000000000,9000000000,'
                                  '11000000000,13000000000,15000000000,17000000000,19000000000,100000000000')

    levels = [int(step) for step in level_price_steps.split(',')]

    if avg_price is None:
        return None

    for i in range(len(levels) - 1):
        if levels[i] <= avg_price < levels[i + 1]:
            return i + 1

    if avg_price >= levels[-1]:
        return 20

    return 0


def determine_level(session):
    info_logger.info(f"Start determining levels....")
    price_references = aliased(PriceReference)
    makes = aliased(Make)
    ads = session.query(Ad, Car).join(Car, Ad.car_id == Car.id).all()
    for ad, car in ads:
        result = session.query(price_references.avg_price).filter(
            price_references.car_id == car.id,
            price_references.year == ad.year
        ).first()

        if result:
            avg_price = result[0]
            level = calculate_level(avg_price)
            if ad.price == 0 and (ad.price_type == 'negotiable' or ad.price_type == 'installment'):
                accuracy = 0
            else:
                accuracy = 100 - (abs(((ad.price * 100) // avg_price) - 100))
            ad.level = level
            ad.accuracy = accuracy
            print(f"level added")
            info_logger.info(f"level added")
        else:
            result = session.query(makes.default_price).filter(
                makes.make == car.make_en,
            ).first()
            if result:
                default_price = result[0]
                level = calculate_level(default_price)
                ad.level = level
                ad.accuracy = 0
                print(f"level added")
                info_logger.info(f"level added")
            print(
                f"No price_reference record found for make: {car.make_en}, model: {car.model_en}, year: {ad.year}")
            info_logger.info(
                f"No price_reference record found for make: {car.make_en}, model: {car.model_en}, year: {ad.year}")
    session.commit()
    session.close()
    info_logger.info(f"Finish determining levels....")


determine_level(Session)
