from datetime import datetime
import pytz
from sqlalchemy import create_engine, Column, BigInteger, Text, JSON, DateTime, func, ForeignKey, Integer, Float
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from config import DB_CONFIG

Base = declarative_base()
db_url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@" \
         f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)


class Car(Base):
    __tablename__ = 'cars'
    id = Column(Integer, primary_key=True)
    make_en = Column(Text, nullable=False)
    make_fa = Column(Text, nullable=False)
    model_en = Column(Text, nullable=False)
    model_fa = Column(Text, nullable=False)
    min_price = Column(Integer)
    max_price = Column(Integer)
    created_year = Column(Text)
    level_impact = Column(Text)
    keywords = Column(Text)
    title_fa = Column(Text)
    title_en = Column(Text)


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
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    title = Column(Text, nullable=False)
    price = Column(BigInteger, nullable=False)
    price_type = Column(Text, nullable=False)
    location = Column(Text, nullable=False)
    dealer_type = Column(Text, nullable=False)
    modified_date = Column(DateTime(timezone=False), nullable=False)
    code = Column(Text, nullable=False, unique=True)
    car_id = Column(BigInteger, ForeignKey('cars.id'), nullable=False)
    dealer_id = Column(BigInteger, ForeignKey('dealers.id'), nullable=True)


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
            car_id = get_car_id(make, model)
            if not car_id:
                # TODO: log the situation
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
                dealer_id=dealer_id
            ).on_conflict_do_nothing(index_elements=['code'])
            session.execute(stmt1)
            print('one ad parsed..')
            session.query(RawAd).filter(RawAd.id == raw_id).update({'process_at': datetime.now(pytz.utc)})

    session.commit()

except SQLAlchemyError as e:
    session.rollback()
    print("Error:", e)
finally:
    session.close()
