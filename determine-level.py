from sqlalchemy.orm import aliased
from custom_loggers import *
from models import *


def calculate_level(avg_price):
    level_price_steps = os.getenv(
        "LEVEL_PRICE_STEPS",
        "0,300000000,600000000,1000000000,3000000000,5000000000,7000000000,9000000000,"
        "11000000000,13000000000,15000000000,17000000000,19000000000,100000000000",
    )

    levels = [int(step) for step in level_price_steps.split(",")]

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
    ads = (
        session.query(Ad, Car)
        .join(Car, Ad.car_id == Car.id)
        .filter((Ad.level.is_(None)) | (Ad.accuracy.is_(None)))
        .all()
    )
    for ad, car in ads:
        avg_price = None

        if ad.level is None or ad.accuracy is None:
            result = (
                session.query(price_references.avg_price)
                .filter(
                    price_references.car_id == car.id, price_references.year == ad.year
                )
                .first()
            )

            if result:
                avg_price = result[0]
            else:
                result = (
                    session.query(makes.default_price)
                    .filter(
                        makes.make == car.make_en,
                    )
                    .first()
                )
                if result:
                    avg_price = result[0]

            if avg_price is None:
                info_logger.info(
                    f"No price reference or default price found for make: {car.make_en}, model: {car.model_en}, year: {ad.year}"
                )
                print(
                    f"No price reference or default price found for make: {car.make_en}, model: {car.model_en}, year: {ad.year}"
                )

        if ad.level is None and avg_price is not None:
            level = calculate_level(avg_price)
            ad.level = level
            print(f"Level added for {ad.code}")
            info_logger.info(f"Level added for {ad.code}")

        if ad.accuracy is None:
            if ad.price == 0 and ad.price_type in ("negotiable", "installment"):
                ad.accuracy = 0
            elif avg_price:
                ad.accuracy = 100 - abs((ad.price * 100 // avg_price) - 100)
            else:
                ad.accuracy = 0
            print(f"Accuracy added for {ad.code}")
            info_logger.info(f"Accuracy added for {ad.code}")
    session.commit()
    session.close()
    info_logger.info(f"Finish determining levels....")


determine_level(Session)
