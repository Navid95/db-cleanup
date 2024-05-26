import sqlalchemy.orm
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy import delete
from sqlalchemy import Table
from typing import Any
from typing import Dict
from typing import List
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker
import logging
import sys

logging.basicConfig(stream=sys.stdout,
                    format='[%(asctime)s,%(msecs)d %(levelname)s]: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S%z',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)


class DBManager:
    def __init__(self, tables: List[Dict[str, str]], db_url: str, limit: int = 2000, alchemy_echo: bool = False):
        # variable list
        self.tables: List[Dict[str, str]] = tables
        self.__db_url__: str = db_url
        self.__echo__: bool = alchemy_echo
        self.limit: int = limit
        self.__metadata__: MetaData = None
        self.__session_maker__: sqlalchemy.orm.sessionmaker = None
        self.__engine__: sqlalchemy.engine.base.Engine = None

        #     initializing the variables
        self.__engine__ = create_engine(self.__db_url__, echo=self.__echo__, future=True)
        self.__metadata__ = MetaData()
        self.__metadata__.reflect(bind=self.__engine__)
        self.__session_maker__ = sessionmaker(bind=self.__engine__)

    def __select_data__(self, table_: Table, limit_: int = 1000) -> List[int]:
        logger.info(f'select_data: {table_.name} with limit: {limit_}')
        result_list = list()
        with self.__session_maker__() as session:
            stm = select(table_.c.id).limit(limit_)
            result = session.execute(stm)
            session.commit()
            result_list = result.all()
        logger.info(f'select_data: {table_.name} count of selected rows: {len(result_list)}')
        return [x.id for x in result_list]

    def __delete_data__(self, table__: Table, id_list: list) -> None:
        logger.info(f'delete_data: {table__.name} \t id_list:{id_list}')
        count = 0
        with self.__session_maker__() as session:
            stm = delete(table__).where(table__.c.id.in_(id_list))
            result = session.execute(stm)
            session.commit()
            count += result.rowcount
        logger.info(f'delete_data: {table__.name} row count: {result.rowcount}')

    def run_cleanup(self):
        logger.info(
            f'################################################## run_cleanup ##################################################')
        iteration = 0
        for table in self.tables:
            try:
                iteration += 1
                logger.info(f'Iteration: {iteration}')
                logger.info(f'reflecting table config: {table}')
                table_ = self.__metadata__.tables[table['name']]
                logger.info(f'reflected table: {table_}')
                table_temp = self.__metadata__.tables[table['temp']]
                logger.info(f'reflected table_temp: {table_temp}')

                while True:
                    # TODO: remove commit from private methods and move here
                    id_list = self.__select_data__(table_temp, limit_=self.limit)
                    if not id_list:
                        break
                    self.__delete_data__(table_, id_list)
                    self.__delete_data__(table_temp, id_list)
            except Exception as e:
                import traceback
                logger.info(f'EXCEPTION')
                traceback.print_exc()
                continue
