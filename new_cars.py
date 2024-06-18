import re
import psycopg2
from config import DB_CONFIG
from sqlalchemy import create_engine, Column, BigInteger, Text, JSON, DateTime, func, ForeignKey, Integer, Float
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError


def read_logs_from_file(log_file):
    pattern = r'newcar\(([^()]*(?:\((?:[^()]*(?:\([^()]*\))?[^()]*)*\))?[^()]*)\)'
    newcar_values = []
    with open(log_file, 'r', encoding='utf-8') as file:
        content = file.read()
        matches = re.findall(pattern, content)
        for match in matches:
            newcar_values.append(match.strip())
    return newcar_values


Base = declarative_base()
db_url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@" \
         f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)


class Car(Base):
    __tablename__ = 'cars'
    id = Column(Integer, primary_key=True)
    make_en = Column(Text)
    make_fa = Column(Text)
    model_en = Column(Text, unique=True, nullable=False)
    model_fa = Column(Text, unique=True, nullable=False)
    min_price = Column(Integer)
    max_price = Column(Integer)
    created_year = Column(Text)
    level_impact = Column(Text)
    keywords = Column(Text)
    title_fa = Column(Text)
    title_en = Column(Text)


Base.metadata.create_all(engine)
session = Session()


def save_to_db(titles):
    session = Session()
    try:
        for i in titles:
            make_fa, model_fa = map(str.strip, i.split('،', 1))
            print(model_fa)
            car = Car(make_fa=make_fa, model_fa=model_fa)
            session.add(car)
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        print(f"An error occurred: {e}")
    finally:
        session.close()


save_to_db(read_logs_from_file('cars_error.log'))
