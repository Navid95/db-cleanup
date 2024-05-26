from sqlalchemy import create_engine
from sqlalchemy import select, delete
from sqlalchemy import Table
from sqlalchemy import MetaData
import logging
import sys

from environ import SQLALCHEMY_DATABASE_URI_UAT

logging.basicConfig(stream=sys.stdout,
                    format='[%(asctime)s,%(msecs)d %(levelname)s]: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S%z',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)
engine = create_engine(SQLALCHEMY_DATABASE_URI_UAT, echo=False, future=True)

metadata_obj = MetaData()
metadata_obj.reflect(bind=engine)

tables = [
    {
        'name': 'cart_product_addon',
        'temp': 'tmp_cart_product_addon_cleanup'
    },
    {
        'name': 'cart_product',
        'temp': 'tmp_cart_product_cleanup_1'
    },
    {
        'name': 'cart_product',
        'temp': 'tmp_cart_product_cleanup_2'
    },
    {
        'name': 'carts',
        'temp': 'tmp_carts_cleanup'
    },
    {
        'name': 'delivery_information',
        'temp': 'tmp_delivery_information_cleanup'
    },
    {
        'name': 'registration_information',
        'temp': 'tmp_registeration_information_cleanup'
    },
    {
        'name': 'transactions',
        'temp': 'tmp_transaction_cleanup'
    },
    {
        'name': 'users',
        'temp': 'tmp_users_cleanup'
    }
]


def select_data(table__: Table, limit_: int = 1000):
    logger.info(
        f'################################################## select_data ##################################################')
    result_list = list()
    logger.info(f'select_data: {table__.name} with limit: {limit_}')
    with engine.connect() as conn:
        stm = select(table__.c.id).limit(limit_)
        result = conn.execute(stm)
        conn.commit()
        trans = result.all()
        result_list.extend(trans)
    logger.info(f'select_data: {table__.name} count of selected rows: {len(trans)}')
    return [x.id for x in result_list]


def delete_data(table__: Table, id_list: list):
    logger.info(
        f'################################################## delete_data ##################################################')
    logger.info(f'delete_data: {table__.name} \t id_list:{id_list}')
    count = 0
    with engine.connect() as conn:
        stm = delete(table__).where(table__.c.id.in_(id_list))
        result = conn.execute(stm)
        conn.commit()
        count += result.rowcount
    logger.info(f'delete_data: {table__.name} row count: {result.rowcount}')


for table in tables:
    try:
        logger.info(
            f'################################################## ROOT ##################################################')
        logger.info(f'reflecting table config: {table}')
        table_ = metadata_obj.tables[table['name']]
        logger.info(f'reflected table: {table_}')
        table_temp = metadata_obj.tables[table['temp']]
        logger.info(f'reflected table_temp: {table_temp}')

        while True:
            id_list = select_data(table_temp)
            if not id_list:
                break
            delete_data(table_, id_list)
            delete_data(table_temp, id_list)
    except Exception as e:
        import traceback
        logger.info(f'EXCEPTION')
        traceback.print_exc()
        continue
