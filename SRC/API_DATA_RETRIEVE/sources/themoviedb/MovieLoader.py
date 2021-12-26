import ast
import csv
import logging
from abc import ABC
from typing import Generator

import fire
import requests

from SRC.API_DATA_RETRIEVE.sources import MovieLoader
from SRC.API_DATA_RETRIEVE.common.mysql import MySQL, DEFAULT_DB
from SRC.API_DATA_RETRIEVE.sources.kaggle.base import *
from SRC.API_DATA_RETRIEVE.sources.themoviedb import base
from SRC.API_DATA_RETRIEVE.sources.themoviedb.base import MOVIES_API_URL

DEFAULT_INSERT_BATCH_SIZE = 2000

NOW_PLAYING_URL = f"{MOVIES_API_URL}/movie/now_playing?page={{page_id}}"
MOVIE_DETAILS_URL = f"{MOVIES_API_URL}/movie/{{movie_id}}"


class MovieLoaderImpl(ABC, MovieLoader.MovieLoader, base.TheMovieDBScraper):
    def __init__(self, token: str, mysql_usr: str = '', mysql_pwd: str = '', mysql_host: Optional[str] = None,
                 mysql_port: Optional[int] = None, mysql_db: str = DEFAULT_DB, mysql_helper: Optional[MySQL] = None,
                 log_level: str = logging.INFO):
        base.TheMovieDBScraper.__init__(self, token=token)
        MovieLoader.MovieLoader.__init__(self, mysql_usr=mysql_usr, mysql_pwd=mysql_pwd,
                                         mysql_host=mysql_host, mysql_port=mysql_port, mysql_db=mysql_db,
                                         mysql_helper=mysql_helper, log_level=log_level)

    def parse_json_result(self, json_resp: dict):
        for movie_dict in json_resp['results']:
            details_url = self.get_endpoint_url(MOVIE_DETAILS_URL.format(movie_id=movie_dict['id']))
            logging.debug(f"requesting details for movie: {details_url}")
            movie_details_dict = requests.get(details_url).json()
            yield Movie(
                movie_dict['id'],
                movie_details_dict['adult'],
                None,
                [Genre.from_dict(genre) for genre in movie_details_dict['genres']],
                None,
                movie_details_dict['original_language'],
                movie_details_dict['original_title'],
                movie_details_dict['overview'],
                movie_details_dict['popularity'],
                [Company.from_dict(company) for company in movie_details_dict['production_companies']],
                [Country.from_dict(country) for country in movie_details_dict['production_countries']],
                movie_details_dict['release_date'],
                movie_details_dict['revenue'],
                movie_details_dict['runtime'],
                [Language.from_dict(d) for d in movie_details_dict['spoken_languages']],
                movie_details_dict['status'],
                movie_details_dict['tagline'],
                movie_details_dict['title'],
                movie_details_dict['vote_average'],
                movie_details_dict['vote_count']
            )

    def get_parsed_movies_generator(self) -> Generator[Movie, None, None]:
        url = self.get_endpoint_url(NOW_PLAYING_URL.format(page_id=1))
        resp = requests.get(url).json()
        movies = self.parse_json_result(resp)
        for movie in movies:
            yield movie
        for page_index in range(2, resp['total_pages'] + 1):
            url = self.get_endpoint_url(NOW_PLAYING_URL.format(page_id=page_index))
            resp = requests.get(url).json()
            for movie in self.parse_json_result(resp):
                yield movie


if __name__ == '__main__':
    fire.Fire(MovieLoader)
