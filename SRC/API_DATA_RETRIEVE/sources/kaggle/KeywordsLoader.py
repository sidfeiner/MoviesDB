import ast
import csv
import logging
from typing import Optional, Generator

import fire

from SRC.API_DATA_RETRIEVE.common.mysql import MySQL, DEFAULT_DB, MySQLAuth, BatchObjInserter, BatchValueInserter
from SRC.API_DATA_RETRIEVE.sources.kaggle import sql
from SRC.API_DATA_RETRIEVE.sources.kaggle.base import *

DEFAULT_INSERT_BATCH_SIZE = 1000


class KeywordsLoader:
    def __init__(self, mysql_usr: str = '', mysql_pwd: str = '', mysql_host: Optional[str] = None,
                 mysql_port: Optional[int] = None, mysql_db: str = DEFAULT_DB, mysql_helper: Optional[MySQL] = None,
                 log_level: str = logging.INFO):
        logging.basicConfig(level=log_level,
                            format="%(asctime)s : %(threadName)s: %(levelname)s : %(name)s : %(module)s : %(message)s")
        self.mysql = mysql_helper or MySQL(MySQLAuth(mysql_usr, mysql_pwd, mysql_host, mysql_port, mysql_db))

    @staticmethod
    def get_ingestion_ddl() -> List[str]:
        return [ddl.strip() for ddl in sql.TEMP_KEYWORDS_TABLES_DLLS.split(';') if len(ddl.strip()) > 0]

    @staticmethod
    def get_parsed_keywords_generator(keywords_file_path: str) -> Generator[Keywords, None, None]:
        with open(keywords_file_path, newline='') as fp:
            reader = csv.DictReader(fp, delimiter=',', quotechar='"')
            for row in reader:
                yield Keywords(
                    int(row['id']),
                    [Keyword.from_dict(d) for d in ast.literal_eval(row['keywords'])]
                )

    def load(self, keywords_file_path: str, insert_batch_size: int = DEFAULT_INSERT_BATCH_SIZE):
        keywords = self.get_parsed_keywords_generator(keywords_file_path)
        with self.mysql as mysql:
            crsr = mysql.get_cursor()
            keywords_inserter = BatchObjInserter(mysql, crsr, insert_batch_size, contract.KEYWORDS_TABLE, 50000)
            keyword_keywords_inserter = BatchObjInserter(mysql, crsr, insert_batch_size,
                                                         f"temp_{contract.MOVIE_KEYWORDS_TABLE}")

            ddls = self.get_ingestion_ddl()
            logging.info("creating temp tables...")
            for ddl in ddls:
                mysql.execute(ddl, crsr)

            logging.info("iterating over keywords...")
            for idx, keyword in enumerate(keywords):
                if (idx + 1) % 500 == 0:
                    logging.debug(f"handling credit #{idx}")
                keywords_inserter.extend(keyword.keywords)
                if keyword.keywords is not None:
                    keyword_keywords_inserter.extend(
                        (MovieKeyword(keyword.movie_id, key.name) for key in keyword.keywords))

            logging.info("finished iterating over keywords, flushing inserters...")
            [inserter.flush() for inserter in
             [keywords_inserter, keyword_keywords_inserter]]

            logging.info("inserting data from temp tables to final tables...")
            inserts = [insert.strip() for insert in sql.FINALIZE_KEYWORDS_TEMP_TABLES_QUERIES.split(';') if
                       len(insert.strip()) > 0]
            for insert in inserts:
                mysql.execute(insert, crsr)
            crsr.close()
            mysql.commit()

            logging.info('done')


if __name__ == '__main__':
    fire.Fire(KeywordsLoader)
