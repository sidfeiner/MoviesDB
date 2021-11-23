from typing import Optional

import fire

from SRC.API_DATA_RETRIEVE.common.contract import DEFAULT_DB
from SRC.API_DATA_RETRIEVE.common.mysql import MySQLAuth
import logging

from SRC.API_DATA_RETRIEVE.sources.kaggle.CreditsLoader import CreditsLoader
from SRC.API_DATA_RETRIEVE.sources.kaggle.KeywordsLoader import KeywordsLoader
from SRC.API_DATA_RETRIEVE.sources.kaggle.MovieLoader import MovieLoader

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
        self.mysql_auth = MySQLAuth(mysql_usr, mysql_pwd, mysql_host, mysql_port, mysql_db)

    def load(self, movies_file_path: str, credits_file_path: str, keywords_file_path: str,
             insert_batch_size: int = DEFAULT_INSERT_BATCH_SIZE):
        movies_loader = MovieLoader(self.mysql_auth.usr, self.mysql_auth.pwd, self.mysql_auth.host,
                                    self.mysql_auth.port, self.mysql_auth.db)
        credits_loader = CreditsLoader(self.mysql_auth.usr, self.mysql_auth.pwd, self.mysql_auth.host,
                                       self.mysql_auth.port, self.mysql_auth.db)
        keywords_loader = KeywordsLoader(self.mysql_auth.usr, self.mysql_auth.pwd, self.mysql_auth.host,
                                         self.mysql_auth.port, self.mysql_auth.db)

        logging.info("loading movies...")
        movies_loader.load(movies_file_path, insert_batch_size)
        logging.info("loading credits...")
        credits_loader.load(credits_file_path, insert_batch_size)
        logging.info("loading keywords...")
        keywords_loader.load(keywords_file_path, insert_batch_size)


if __name__ == '__main__':
    fire.Fire(KaggleLoader)
