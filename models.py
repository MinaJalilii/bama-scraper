from sqlalchemy import (
    create_engine,
    Column,
    BigInteger,
    Text,
    DateTime,
    func,
    ForeignKey,
    Integer,
    Float,
    UniqueConstraint,
    TIMESTAMP,
)
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
import os
from sqlalchemy.dialects.postgresql import JSONB

load_dotenv(".env")

Base = declarative_base()
db_url = os.getenv("DB_URL")


class Car(Base):
    __tablename__ = "cars"
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    make_en = Column(Text, nullable=False)
    make_fa = Column(Text, nullable=False)
    model_en = Column(Text, nullable=False)
    model_fa = Column(Text, nullable=False)
    keywords = Column(Text)
    title_fa = Column(Text, nullable=False)
    title_en = Column(Text, nullable=False)
    __table_args__ = (UniqueConstraint("make_fa", "model_fa", name="uq_make_model"),)


class Make(Base):
    __tablename__ = "makes"

    id = Column(Integer, primary_key=True)
    make = Column(Text, unique=True, nullable=False)
    default_price = Column(BigInteger)


class Dealer(Base):
    __tablename__ = "dealers"
    id = Column(Integer, primary_key=True)
    bama_id = Column(Integer, nullable=False, unique=True)
    name = Column(Text)
    type = Column(Text)
    score = Column(Float)
    address = Column(Text)
    ad_count = Column(Integer)
    package_type = Column(Text)


class RawAd(Base):
    __tablename__ = "raw_ads"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    raw_data = Column(JSONB, nullable=False)
    ad_code = Column(Text, nullable=False)
    process_at = Column(TIMESTAMP, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    version = Column(Integer, nullable=False, default=0)

    __table_args__ = (
        UniqueConstraint("ad_code", "version", name="unique_ad_code_version"),
    )


class Ad(Base):
    __tablename__ = "ads"
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
    car_id = Column(BigInteger, ForeignKey("cars.id"), nullable=False)
    dealer_id = Column(BigInteger, ForeignKey("dealers.id"), nullable=True)


class PriceReference(Base):
    __tablename__ = "price_reference"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    car_id = Column(BigInteger, ForeignKey("cars.id"), nullable=False)
    year = Column(Text, nullable=False)
    avg_price = Column(BigInteger, nullable=False)
    min_price = Column(BigInteger, nullable=False)
    max_price = Column(BigInteger, nullable=False)
    count = Column(Integer, nullable=True)
    __table_args__ = (UniqueConstraint("car_id", "year", name="uq_car_id_year"),)


engine = create_engine(db_url)
Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)
Session = Session()
