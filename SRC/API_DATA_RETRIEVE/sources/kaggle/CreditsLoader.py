import ast
import csv
import logging
from typing import Optional, Generator

import fire

from SRC.API_DATA_RETRIEVE.common.mysql import MySQL, DEFAULT_DB, MySQLAuth, BatchObjInserter, BatchValueInserter
from SRC.API_DATA_RETRIEVE.sources.kaggle import sql
from SRC.API_DATA_RETRIEVE.sources.kaggle.base import *

DEFAULT_INSERT_BATCH_SIZE = 1000


class CreditsLoader:
    def __init__(self, mysql_usr: str = '', mysql_pwd: str = '', mysql_host: Optional[str] = None,
                 mysql_port: Optional[int] = None, mysql_db: str = DEFAULT_DB, mysql_helper: Optional[MySQL] = None,
                 log_level: str = logging.INFO):
        logging.basicConfig(level=log_level,
                            format="%(asctime)s : %(threadName)s: %(levelname)s : %(name)s : %(module)s : %(message)s")
        self.mysql = mysql_helper or MySQL(MySQLAuth(mysql_usr, mysql_pwd, mysql_host, mysql_port, mysql_db))

    @staticmethod
    def get_ingestion_ddl() -> List[str]:
        return [ddl.strip() for ddl in sql.TEMP_CREDITS_TABLES_DLLS.split(';') if len(ddl.strip()) > 0]

    @staticmethod
    def get_parsed_credits_generator(credits_file_path: str) -> Generator[Credits, None, None]:
        with open(credits_file_path, newline='') as fp:
            reader = csv.DictReader(fp, delimiter=',', quotechar='"')
            for _, row in enumerate(reader):
                crew = []
                for member in ast.literal_eval(row['crew']):
                    member['movie_id'] = int(row['id'])
                    crew.append(CrewMember.from_dict(member))
                cast = []
                for member in ast.literal_eval(row['cast']):
                    member['movie_id'] = int(row['id'])
                    cast.append(CastMember.from_dict(member))
                yield Credits(
                    int(row['id']),
                    crew,
                    cast
                )

    def load(self, credits_file_path: str, insert_batch_size: int = DEFAULT_INSERT_BATCH_SIZE):
        credits = self.get_parsed_credits_generator(credits_file_path)
        with self.mysql as mysql:
            crsr = mysql.get_cursor()
            departments_inserter = BatchValueInserter(mysql, crsr, insert_batch_size, contract.DEPARTMENT_TABLE,
                                                      contract.DEPARTMENT_COLUMN)
            jobs_inserter = BatchValueInserter(mysql, crsr, insert_batch_size, contract.JOBS_TABLE,
                                               contract.JOB_COLUMN)
            characters_inserter = BatchValueInserter(mysql, crsr, insert_batch_size, contract.CHARACTERS_TABLE,
                                                     contract.CHARACTER_COLUMN, cache_size=200000)
            names_inserter = BatchValueInserter(mysql, crsr, insert_batch_size, contract.NAMES_TABLE,
                                                contract.NAME_COLUMN, cache_size=200000)
            temp_crew_inserter = BatchObjInserter(mysql, crsr, insert_batch_size, f"temp_{contract.CREW_TABLE}")
            temp_cast_inserter = BatchObjInserter(mysql, crsr, insert_batch_size, f"temp_{contract.CAST_TABLE}")

            ddls = self.get_ingestion_ddl()
            logging.info("creating temp tables...")
            for ddl in ddls:
                mysql.execute(ddl, crsr)

            logging.info("iterating over credits...")
            for idx, credit in enumerate(credits):
                if (idx + 1) % 500 == 0:
                    logging.debug(f"handling credit #{idx}")
                departments_inserter.extend([item.department for item in credit.crew])
                jobs_inserter.extend((item.job for item in credit.crew))
                characters_inserter.extend((item.character for item in credit.cast))
                names_inserter.extend((item.name for item in credit.crew))
                names_inserter.extend((item.name for item in credit.cast))
                temp_crew_inserter.extend(credit.crew)
                temp_cast_inserter.extend(credit.cast)

            logging.info("finished iterating over credits, flushing inserters...")
            [inserter.flush() for inserter in
             [departments_inserter, jobs_inserter, characters_inserter, names_inserter, temp_crew_inserter,
              temp_cast_inserter]]

            logging.info("inserting data from temp tables to final tables...")
            inserts = [insert.strip() for insert in sql.FINALIZE_CREDITS_TEMP_TABLES_QUERIES.split(';') if
                       len(insert.strip()) > 0]
            for insert in inserts:
                mysql.execute(insert, crsr)
            crsr.close()
            mysql.commit()

            logging.info('done')


if __name__ == '__main__':
    fire.Fire(CreditsLoader)
