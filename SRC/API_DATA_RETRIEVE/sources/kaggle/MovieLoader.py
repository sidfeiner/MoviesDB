import ast
import csv
import logging
from typing import Generator, Any, Sequence

import fire

from SRC.API_DATA_RETRIEVE.sources import MovieLoader
from SRC.API_DATA_RETRIEVE.common.mysql import MySQL, DEFAULT_DB
from SRC.API_DATA_RETRIEVE.sources.kaggle.base import *

DEFAULT_INSERT_BATCH_SIZE = 2000


class MovieLoaderImpl(MovieLoader.MovieLoader):
    def __init__(self, movies_file: str, mysql_usr: str = '', mysql_pwd: str = '', mysql_host: Optional[str] = None,
                 mysql_port: Optional[int] = None, mysql_db: str = DEFAULT_DB, mysql_helper: Optional[MySQL] = None,
                 log_level: str = logging.INFO):
        super().__init__(mysql_usr, mysql_pwd, mysql_host, mysql_port, mysql_db, mysql_helper, log_level)
        self.movies_file = movies_file

    def get_parsed_movies_generator(self) -> Generator[Movie, None, None]:
        failed_rows = 0
        with open(self.movies_file, newline='') as fp:
            reader = csv.DictReader(fp, delimiter=',', quotechar='"')
            for row in reader:
                if not row['id'].isdigit():
                    print(f"id is not number: {row['id']}")
                    failed_rows += 1
                    continue
                yield Movie(
                    int(row['id']),
                    bool(row['adult']),
                    int(row['budget']) if row['budget'] not in ('', '0') else None,
                    [Genre.from_dict(d) for d in ast.literal_eval(row['genres'])] if row['genres'] not in (
                        None,
                        '') else None,
                    row['imdb_id'],
                    row['original_language'],
                    row['original_title'],
                    row['overview'],
                    float(row['popularity']) if row['popularity'] not in (None, '', '0', '0.0') else None,
                    [Company.from_dict(d) for d in ast.literal_eval(row['production_companies'])] if row[
                                                                                                         'production_companies'] not in (
                                                                                                         None,
                                                                                                         '') else None,
                    [Country.from_dict(d) for d in ast.literal_eval(row['production_countries'])] if row[
                                                                                                         'production_countries'] not in (
                                                                                                         None,
                                                                                                         '') else None,
                    row['release_date'] if row['release_date'] not in (None, '') else None,
                    int(row['revenue']) if row['revenue'] not in (None, '', '0') else None,
                    int(row['runtime'].replace('.0', '')) if row['runtime'] is not None and row['runtime'].replace('.0',
                                                                                                                   '') not in (
                                                                 '', '0') else None,
                    [Language.from_dict(d) for d in ast.literal_eval(row['spoken_languages'])] if row[
                                                                                                      'spoken_languages'] not in (
                                                                                                      None,
                                                                                                      '') else None,
                    row['status'],
                    row['tagline'],
                    row['title'],
                    float(row['vote_average']) if row['vote_average'] not in (None, '', '0', '0.0') else None,
                    int(row['vote_count']) if row['vote_count'] not in (None, '', '0') else None
                )
        if failed_rows > 0:
            logging.warning(f"done with {failed_rows} failures")

    def load(self, insert_batch_size: int = DEFAULT_INSERT_BATCH_SIZE) -> Sequence[Movie]:
        gen = super().load(insert_batch_size)
        return [movie for movie in gen]


if __name__ == '__main__':
    fire.Fire(MovieLoader)
