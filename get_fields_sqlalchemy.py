from sqlalchemy import create_engine, Column, BigInteger, Text, JSON, DateTime, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker, declarative_base
from config import DB_CONFIG
from sqlalchemy.dialects.postgresql import insert

Base = declarative_base()
db_url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@" \
         f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)


class RawAd(Base):
    __tablename__ = 'raw_ads'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    raw_data = Column(JSONB, nullable=False)
    ad_code = Column(Text, nullable=False, unique=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())


class Ad(Base):
    __tablename__ = 'ads'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    car_title = Column(Text, nullable=False)
    price = Column(BigInteger, nullable=False)
    location = Column(Text)
    dealer_type = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=False), nullable=False)
    code = Column(Text, nullable=False, unique=True)


session = Session()

try:
    raw_ads = session.query(RawAd.id, RawAd.raw_data, RawAd.ad_code).all()
    for raw_ad in raw_ads:
        raw_id, raw_data, ad_code = raw_ad

        print("ID:", raw_id)
        print("CODE:", ad_code)

        if raw_data is not None:
            price = int(raw_data.get('price', {}).get('price', '0').replace(',', ''))
            if raw_data.get('dealer') is not None:
                dealer_type = raw_data.get('dealer', {}).get('type')
            else:
                dealer_type = 'شخصی'
            car_title = raw_data.get('detail', {}).get('title', '')
            location = raw_data.get('detail', {}).get('location', '')
            created_at = raw_data.get('detail', {}).get('modified_date', None)
            code = raw_data.get('detail', {}).get('code', '')

            stmt = insert(Ad).values(car_title=car_title,
                                     price=price,
                                     location=location,
                                     dealer_type=dealer_type,
                                     created_at=created_at,
                                     code=code).on_conflict_do_nothing(index_elements=['code'])
            session.execute(stmt)
    session.commit()

except Exception as e:
    session.rollback()
    print("Error:", e)
finally:
    session.close()
