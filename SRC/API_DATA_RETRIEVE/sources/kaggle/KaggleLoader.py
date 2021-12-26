from typing import Optional

import fire

from SRC.API_DATA_RETRIEVE.common.mysql import MySQLAuth, MySQL
import logging

from SRC.API_DATA_RETRIEVE.contract import DEFAULT_DB
from SRC.API_DATA_RETRIEVE.sources.kaggle.CreditsLoader import CreditsLoaderImpl
from SRC.API_DATA_RETRIEVE.sources.kaggle.MovieLoader import MovieLoaderImpl
from SRC.API_DATA_RETRIEVE.sources.kaggle.KeywordsLoader import KeywordsLoaderImpl

DEFAULT_INSERT_BATCH_SIZE = 2000


class KaggleLoader:
    """
    This will parse data from the Kaggle DB, obtained at https://www.kaggle.com/rounakbanik/the-movies-dataset
    We will use data only from 2 tables:
    1) movies_metadata.csv
    2) credits.csv
    3) keywords.csv

    links/ratings tables will be ignored because links aren't important to us, and individual ratings won't
    be treated in our service (only overall rating which appears in the movies_metadata.csv file)
    """

    def __init__(self, mysql_usr: str, mysql_pwd: str, mysql_host: Optional[str] = None,
                 mysql_port: Optional[int] = None, mysql_db: str = DEFAULT_DB, log_level: str = logging.INFO):
        logging.basicConfig(level=log_level,
                            format="%(asctime)s : %(threadName)s: %(levelname)s : %(name)s : %(module)s : %(message)s")
        self.mysql = MySQL(MySQLAuth(mysql_usr, mysql_pwd, mysql_host, mysql_port, mysql_db), is_shared_connection=True)

    def apply_etls(self):
        with self.mysql as mysql:
            crsr = mysql.get_cursor()
            logging.info("running PrepareMatchingKeywords procedure...")
            mysql.execute("CALL PrepareMatchingKeywords()", crsr)

    def load(self, movies_file_path: str, credits_file_path: str, keywords_file_path: str,
             insert_batch_size: int = DEFAULT_INSERT_BATCH_SIZE):
        with self.mysql as mysql:
            movies_loader = MovieLoaderImpl(movies_file=movies_file_path, mysql_helper=mysql)
            credits_loader = CreditsLoaderImpl(credits_file=credits_file_path, mysql_helper=mysql)
            keywords_loader = KeywordsLoaderImpl(keywords_file=keywords_file_path, mysql_helper=mysql)

            logging.info("loading movies...")
            movies_loader.load(insert_batch_size)
            logging.info("loading credits...")
            credits_loader.load(insert_batch_size)
            logging.info("loading keywords...")
            keywords_loader.load(insert_batch_size)

            logging.info("applying etls...")
            self.apply_etls()


if __name__ == '__main__':
    fire.Fire(KaggleLoader)
