from datetime import datetime
import pytz
from sqlalchemy import create_engine, Column, BigInteger, Text, JSON, DateTime, func, ForeignKey, Integer, Float, \
    UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker, declarative_base, aliased
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from custom_loggers import info_logger, error_logger
from dotenv import load_dotenv
import os

load_dotenv('.env')

Base = declarative_base()
db_url = os.getenv('DB_URL')


class Car(Base):
    __tablename__ = 'cars'
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    make_en = Column(Text, nullable=False)
    make_fa = Column(Text, nullable=False)
    model_en = Column(Text, nullable=False)
    model_fa = Column(Text, nullable=False)
    keywords = Column(Text)
    title_fa = Column(Text, nullable=False)
    title_en = Column(Text, nullable=False)
    __table_args__ = (
        UniqueConstraint('make_fa', 'model_fa', name='uq_make_model'),
    )


class Dealer(Base):
    __tablename__ = 'dealers'
    id = Column(Integer, primary_key=True)
    bama_id = Column(Integer, nullable=False, unique=True)
    name = Column(Text)
    type = Column(Text)
    score = Column(Float)
    address = Column(Text)
    ad_count = Column(Integer)
    package_type = Column(Text)


class RawAd(Base):
    __tablename__ = 'raw_ads'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    raw_data = Column(JSONB, nullable=False)
    ad_code = Column(Text, nullable=False, unique=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    process_at = Column(DateTime, nullable=True, default=None)


class Ad(Base):
    __tablename__ = 'ads'
    id = Column(BigInteger, primary_key=True, autoincrement=True, nullable=False)
    title = Column(Text, nullable=False)
    price = Column(BigInteger, nullable=False)
    level = Column(Integer, nullable=True)
    accuracy = Column(Integer, nullable=True)
    price_type = Column(Text, nullable=False)
    location = Column(Text, nullable=False)
    dealer_type = Column(Text, nullable=False)
    year = Column(Text, nullable=False)
    mileage = Column(Text, nullable=False)
    ad_image = Column(Text, nullable=True)
    images = Column(JSONB, default=list, nullable=True)
    modified_date = Column(DateTime(timezone=False), nullable=False)
    code = Column(Text, nullable=False, unique=True)
    car_id = Column(BigInteger, ForeignKey('cars.id'), nullable=False)
    dealer_id = Column(BigInteger, ForeignKey('dealers.id'), nullable=True)


class PriceReference(Base):
    __tablename__ = 'price_reference'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    car_id = Column(BigInteger, ForeignKey('cars.id'), nullable=False)
    year = Column(Text, nullable=False)
    avg_price = Column(BigInteger, nullable=False)
    min_price = Column(BigInteger, nullable=False)
    max_price = Column(BigInteger, nullable=False)
    count = Column(Integer, nullable=True)
    __table_args__ = (
        UniqueConstraint('car_id', 'year', name='uq_car_id_year'),
    )


engine = create_engine(db_url)
Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)
session = Session()


def get_car_id(make, model):
    car = session.query(Car.id).filter(Car.make_fa == make, Car.model_fa == model).first()
    if not car:
        return None
    return car.id


def get_dealer_id(bama_id):
    dealer = session.query(Dealer.id).filter(Dealer.bama_id == bama_id).first()
    return dealer.id


def calculate_level(avg_price):
    if 1000000000 <= avg_price < 2000000000:
        return 1
    elif 2000000000 <= avg_price < 3000000000:
        return 2
    elif 3000000000 <= avg_price < 4000000000:
        return 3
    elif 4000000000 <= avg_price < 5000000000:
        return 4
    elif 5000000000 <= avg_price < 6000000000:
        return 5
    elif 6000000000 <= avg_price < 7000000000:
        return 6
    elif 7000000000 <= avg_price < 8000000000:
        return 7
    elif 8000000000 <= avg_price < 9000000000:
        return 8
    elif 9000000000 <= avg_price < 10000000000:
        return 9
    elif avg_price >= 10000000000:
        return 10
    else:
        return 0


def parse_ads(session):
    try:
        raw_ads = session.query(RawAd.id, RawAd.raw_data, RawAd.ad_code).filter(RawAd.process_at.is_(None))
        for raw_ad in raw_ads:
            raw_id, raw_data, ad_code = raw_ad
            if raw_data is not None:
                title = raw_data.get('detail').get('title').split('، ')
                make = title[0]
                model = title[1]
                price = 0
                if raw_data.get('price', {}).get('type') == 'lumpsum':
                    price = int(raw_data.get('price').get('price').replace(',', ''))
                    price_type = 'with price'
                    if price < 1000000000:
                        continue
                elif raw_data.get('price', {}).get('type') == 'installment':
                    price_type = 'installment'
                elif raw_data.get('price', {}).get('type') == 'negotiable':
                    price_type = 'negotiable'
                else:
                    price_type = 'unknown'

                if raw_data.get('dealer') is None:
                    dealer_type = 'شخصی'
                    dealer_id = None
                else:
                    dealer_type = raw_data.get('dealer', {}).get('type')
                    bama_id = raw_data.get('dealer', {}).get('id')
                    dealer_stmt = insert(Dealer).values(
                        bama_id=bama_id,
                        name=raw_data.get('dealer', {}).get('name'),
                        type=raw_data.get('dealer', {}).get('type'),
                        score=raw_data.get('dealer', {}).get('score'),
                        address=raw_data.get('dealer', {}).get('address'),
                        ad_count=raw_data.get('dealer', {}).get('ad_count'),
                        package_type=raw_data.get('dealer', {}).get('package_type')
                    ).on_conflict_do_update(
                        index_elements=['bama_id'],
                        set_={
                            'name': raw_data.get('dealer', {}).get('name'),
                            'type': raw_data.get('dealer', {}).get('type'),
                            'score': raw_data.get('dealer', {}).get('score'),
                            'address': raw_data.get('dealer', {}).get('address'),
                            'ad_count': raw_data.get('dealer', {}).get('ad_count'),
                            'package_type': raw_data.get('dealer', {}).get('package_type')
                        }
                    )
                    session.execute(dealer_stmt)
                    dealer_id = get_dealer_id(bama_id)

                title = raw_data.get('detail', {}).get('title', '')
                location = raw_data.get('detail', {}).get('location', '')
                modified_date = raw_data.get('detail', {}).get('modified_date', None)
                code = raw_data.get('detail', {}).get('code', '')
                year = raw_data.get('detail', {}).get('year', '')
                mileage = raw_data.get('detail', {}).get('mileage', '')
                car_id = get_car_id(make, model)
                ad_image = raw_data.get('detail', {}).get('image', '')
                images = raw_data.get('images', [])
                if not car_id:
                    error_logger.error(f"newcar({title})")
                    print(f"New car {title}")
                    continue
                stmt1 = insert(Ad).values(
                    title=title,
                    price=price,
                    price_type=price_type,
                    location=location,
                    dealer_type=dealer_type,
                    modified_date=modified_date,
                    code=code,
                    car_id=car_id,
                    dealer_id=dealer_id,
                    year=year,
                    mileage=mileage,
                    ad_image=ad_image,
                    images=images
                ).on_conflict_do_nothing(index_elements=['code'])
                session.execute(stmt1)
                info_logger.info(f"ad with code '{code}' parsed..")
                print(f"ad parsed : {code}")
                session.query(RawAd).filter(RawAd.id == raw_id).update({'process_at': datetime.now(pytz.utc)})
        session.commit()

    except SQLAlchemyError as e:
        session.rollback()
        print("Error:", e)
    finally:
        session.close()


def determine_level(session):
    price_references = aliased(PriceReference)
    ads = session.query(Ad, Car).join(Car, Ad.car_id == Car.id).all()

    for ad, car in ads:
        result = session.query(price_references.avg_price).filter(
            price_references.car_id == car.id,
            price_references.year == ad.year
        ).first()

        if result:
            avg_price = result[0]
            level = calculate_level(avg_price)
            accuracy = 100 - (abs(((ad.price * 100) // avg_price) - 100))
            ad.level = level
            ad.accuracy = accuracy
            print(f"level added")
        else:
            print(
                f"No price_reference record found for make: {car.make_en}, model: {car.model_en}, year: {ad.year}")
    session.commit()
    session.close()


def save_price(session):
    query = session.query(
        func.count(Car.id).label('ad_count'),
        Car.id,
        func.round(func.avg(Ad.price)).label('average_price'),
        func.min(Ad.price).label('min_price'),
        func.max(Ad.price).label('max_price'),
        Ad.year
    ).join(Car).filter(
        Ad.price_type == 'with price',
        Ad.price >= 1000000000
    ).group_by(
        Car.id,
        Ad.year
    )
    try:
        for count, car_id, average_price, min_price, max_price, year in query:
            price_stmt = insert(PriceReference).values(
                count=count,
                car_id=car_id,
                avg_price=int(average_price),
                min_price=int(min_price),
                max_price=int(max_price),
                year=year,
            ).on_conflict_do_nothing(index_elements=['car_id', 'year'])
            session.execute(price_stmt)
            print(f"added -> {car_id} - {year}")
        session.commit()
        info_logger.info("Price references inserted successfully.")
    except Exception as e:
        session.rollback()
        error_logger.error(f"Error occurred: {e}")
        print(f"error: {e}")
    finally:
        session.close()


save_price(session)
