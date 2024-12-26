from custom_loggers import *
import pytz
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert
from models import *


def get_car_id(make, model):
    car = Session.query(Car.id).filter(Car.make_fa == make, Car.model_fa == model).first()
    if not car:
        return None
    return car.id


def get_dealer_id(bama_id):
    dealer = Session.query(Dealer.id).filter(Dealer.bama_id == bama_id).first()
    return dealer.id


def parse_ads(session):
    try:
        # raw_ads = Session.query(RawAd.id, RawAd.raw_data, RawAd.ad_code).filter(RawAd.process_at.is_(None))
        subquery = session.query(
            RawAd.ad_code,
            func.max(RawAd.version).label('max_version')
        ).group_by(RawAd.ad_code).subquery()

        raw_ads = session.query(RawAd.id, RawAd.raw_data, RawAd.ad_code).join(
            subquery,
            (RawAd.ad_code == subquery.c.ad_code) & (RawAd.version == subquery.c.max_version)
        ).filter(RawAd.process_at.is_(None))
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


parse_ads(Session)
