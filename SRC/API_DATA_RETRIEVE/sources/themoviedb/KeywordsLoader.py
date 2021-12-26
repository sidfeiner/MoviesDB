import ast
import csv
import logging
from typing import Generator

import fire
import requests

from SRC.API_DATA_RETRIEVE.common.mysql import MySQL, DEFAULT_DB
from SRC.API_DATA_RETRIEVE.sources import KeywordsLoader
from SRC.API_DATA_RETRIEVE.sources.kaggle.base import *
from SRC.API_DATA_RETRIEVE.sources.themoviedb import base
from SRC.API_DATA_RETRIEVE.sources.themoviedb.base import MOVIES_API_URL

MOVIE_KEYWORDS_URL = f"{MOVIES_API_URL}/movie/{{movie_id}}/keywords"


class KeywordsLoaderImpl(base.TheMovieDBScraper, KeywordsLoader.KeywordsLoader):
    def __init__(self, token: str, movie_ids: Generator[int, None, None], mysql_usr: str = '', mysql_pwd: str = '',
                 mysql_host: Optional[str] = None,
                 mysql_port: Optional[int] = None, mysql_db: str = DEFAULT_DB, mysql_helper: Optional[MySQL] = None,
                 log_level: str = logging.INFO):
        base.TheMovieDBScraper.__init__(self, token=token)
        KeywordsLoader.KeywordsLoader.__init__(self, mysql_usr=mysql_usr, mysql_pwd=mysql_pwd,
                                               mysql_host=mysql_host, mysql_port=mysql_port, mysql_db=mysql_db,
                                               mysql_helper=mysql_helper, log_level=log_level)
        self.movie_ids = movie_ids

    def get_parsed_keywords_generator(self) -> Generator[Keywords, None, None]:
        for movie_id in self.movie_ids:
            keywords_url = self.get_endpoint_url(MOVIE_KEYWORDS_URL.format(movie_id=movie_id))
            logging.debug(f"requesting keywords for movie: {keywords_url}")
            keywords_dict = requests.get(keywords_url).json()
            yield Keywords(
                movie_id,
                [Keyword.from_dict(keyword) for keyword in keywords_dict['keywords']]
            )


if __name__ == '__main__':
    fire.Fire(KeywordsLoader)
