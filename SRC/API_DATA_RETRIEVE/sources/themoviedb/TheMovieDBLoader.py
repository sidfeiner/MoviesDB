import logging
from typing import Optional

import fire

from SRC.API_DATA_RETRIEVE.common.mysql import MySQL, MySQLAuth
from SRC.API_DATA_RETRIEVE.contract import DEFAULT_DB
from SRC.API_DATA_RETRIEVE.sources.themoviedb.CreditsLoader import CreditsLoaderImpl
from SRC.API_DATA_RETRIEVE.sources.themoviedb.KeywordsLoader import KeywordsLoaderImpl
from SRC.API_DATA_RETRIEVE.sources.themoviedb.MovieLoader import MovieLoaderImpl

DEFAULT_INSERT_BATCH_SIZE = 1000


class TheMovieDBLoader:
    """
        This will query TheMovieDB API at https://api.themoviedb.org/3
        It will bring the movie details, credits (crew/cast) and keywords.
        """

    def __init__(self, mysql_usr: str, mysql_pwd: str, mysql_host: Optional[str] = None,
                 mysql_port: Optional[int] = None, mysql_db: str = DEFAULT_DB, log_level: str = logging.INFO):
        logging.basicConfig(level=log_level,
                            format="%(asctime)s : %(threadName)s: %(levelname)s : %(name)s : %(module)s : %(message)s")
        self.mysql = MySQL(MySQLAuth(mysql_usr, mysql_pwd, mysql_host, mysql_port, mysql_db), is_shared_connection=True)

    def load(self, token: str, insert_batch_size: int = DEFAULT_INSERT_BATCH_SIZE):
        with self.mysql as mysql:
            movies_loader = MovieLoaderImpl(token=token, mysql_helper=mysql)

            logging.info("scraping movies, credits and keywords")
            movies = movies_loader.load(insert_batch_size)
            credits_loader = CreditsLoaderImpl(token=token, movie_ids=(movie.id for movie in movies),
                                               mysql_helper=mysql)
            movie_credits = credits_loader.load(insert_batch_size)

            keywords_loader = KeywordsLoaderImpl(token=token, movie_ids=(credit.movie_id for credit in movie_credits),
                                                 mysql_helper=mysql)
            keywords = keywords_loader.load(insert_batch_size)
            [_ for _ in keywords]  # ensure everything is run


if __name__ == '__main__':
    fire.Fire(TheMovieDBLoader)
