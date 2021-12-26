import ast
import csv
import logging
from typing import Generator

import fire
import requests

from SRC.API_DATA_RETRIEVE.sources import CreditsLoader
from SRC.API_DATA_RETRIEVE.common.mysql import MySQL, DEFAULT_DB
from SRC.API_DATA_RETRIEVE.sources.kaggle.base import *
from SRC.API_DATA_RETRIEVE.sources.themoviedb import base
from SRC.API_DATA_RETRIEVE.sources.themoviedb.base import MOVIES_API_URL

MOVIE_CREDITS_URL = f"{MOVIES_API_URL}/movie/{{movie_id}}/credits"


class CreditsLoaderImpl(base.TheMovieDBScraper, CreditsLoader.CreditsLoader):
    def __init__(self, token: str, movie_ids: Generator[int, None, None], mysql_usr: str = '', mysql_pwd: str = '',
                 mysql_host: Optional[str] = None,
                 mysql_port: Optional[int] = None, mysql_db: str = DEFAULT_DB, mysql_helper: Optional[MySQL] = None,
                 log_level: str = logging.INFO):
        base.TheMovieDBScraper.__init__(self, token=token)
        CreditsLoader.CreditsLoader.__init__(self, mysql_usr=mysql_usr, mysql_pwd=mysql_pwd,
                                             mysql_host=mysql_host, mysql_port=mysql_port, mysql_db=mysql_db,
                                             mysql_helper=mysql_helper, log_level=log_level)
        self.movie_ids = movie_ids

    def get_parsed_credits_generator(self) -> Generator[Credits, None, None]:
        for movie_id in self.movie_ids:
            credits_url = self.get_endpoint_url(MOVIE_CREDITS_URL.format(movie_id=movie_id))
            logging.debug(f"requesting credits for movie: {credits_url}")
            credits_dict = requests.get(credits_url).json()
            cast = []
            for cast_member_dict in credits_dict.get('cast', []):
                cast_member_dict['movie_id'] = movie_id
                cast.append(CastMember.from_dict(cast_member_dict))
            crew = []
            for crew_member_dict in credits_dict.get('crew', []):
                crew_member_dict['movie_id'] = movie_id
                crew.append(CrewMember.from_dict(crew_member_dict))
            yield Credits(movie_id, crew, cast)


if __name__ == '__main__':
    fire.Fire(CreditsLoader)
