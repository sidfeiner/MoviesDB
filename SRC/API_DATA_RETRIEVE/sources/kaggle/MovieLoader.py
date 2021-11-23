import ast
import csv
import logging
from typing import Optional, Generator

import fire

from SRC.API_DATA_RETRIEVE.common.mysql import MySQL, DEFAULT_DB, MySQLAuth, BatchObjInserter, BatchValueInserter
from SRC.API_DATA_RETRIEVE.sources.kaggle import sql
from SRC.API_DATA_RETRIEVE.sources.kaggle.base import *

DEFAULT_INSERT_BATCH_SIZE = 2000


class MovieLoader:
    def __init__(self, mysql_usr: str, mysql_pwd: str, mysql_host: Optional[str] = None,
                 mysql_port: Optional[int] = None, mysql_db: str = DEFAULT_DB, log_level: str = logging.INFO):
        logging.basicConfig(level=log_level,
                            format="%(asctime)s : %(threadName)s: %(levelname)s : %(name)s : %(module)s : %(message)s")
        self.mysql_auth = MySQLAuth(mysql_usr, mysql_pwd, mysql_host, mysql_port, mysql_db)

    @staticmethod
    def get_ingestion_ddl() -> List[str]:
        return [ddl.strip() for ddl in sql.TEMP_MOVIES_TABLES_DLLS.split(';') if len(ddl.strip()) > 0]

    @staticmethod
    def get_parsed_movies_generator(movies_file_path: str) -> Generator[Movie, None, None]:
        failed_rows = 0
        with open(movies_file_path, newline='') as fp:
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

    def load(self, movies_file_path: str, insert_batch_size: int = DEFAULT_INSERT_BATCH_SIZE):
        movies = self.get_parsed_movies_generator(movies_file_path)
        with MySQL(self.mysql_auth) as mysql:
            crsr = mysql.get_cursor()
            temp_movies_inserter = BatchObjInserter(mysql, crsr, insert_batch_size, f"temp_{contract.MOVIES_TABLE}")
            languages_inserter = BatchObjInserter(mysql, crsr, insert_batch_size, contract.LANGUAGES_TABLE)
            temp_movie_spoken_languages_inserter = BatchObjInserter(mysql, crsr, insert_batch_size,
                                                                    f"temp_{contract.MOVIES_SPOKEN_LANGUAGES_TABLE}")
            countries_inserter = BatchObjInserter(mysql, crsr, insert_batch_size, contract.COUNTRIES_TABLE)
            temp_movie_production_countries_inserter = BatchObjInserter(mysql, crsr, insert_batch_size,
                                                                        f"temp_{contract.MOVIE_PRODUCTION_COUNTRIES_TABLES}")
            production_companies_inserter = BatchObjInserter(mysql, crsr, insert_batch_size,
                                                             contract.PRODUCTION_COMPANIES_TABLE)
            temp_movie_production_companies_inserter = BatchObjInserter(mysql, crsr, insert_batch_size,
                                                                        f"temp_{contract.MOVIE_PRODUCTION_COMPANIES_TABLES}")
            titles_inserter = BatchValueInserter(mysql, crsr, insert_batch_size, contract.TITLES_TABLE,
                                                 contract.TITLE_COLUMN)
            statuses_inserter = BatchValueInserter(mysql, crsr, insert_batch_size, contract.STATUSES_TABLE,
                                                   contract.STATUS_COLUMN)
            genres_inserter = BatchObjInserter(mysql, crsr, insert_batch_size, contract.GENRES_TABLE)
            temp_movie_genres_inserter = BatchObjInserter(mysql, crsr, insert_batch_size,
                                                          f"temp_{contract.MOVIE_GENRES_TABLE}")

            ddls = self.get_ingestion_ddl()
            logging.info("creating temp tables...")
            for ddl in ddls:
                mysql.execute(ddl, crsr)

            logging.info("iterating over movies...")
            for idx, movie in enumerate(movies):
                if (idx + 1) % 500 == 0:
                    logging.debug(f"handling movie #{idx}")
                temp_movies_inserter.append(movie)
                languages_inserter.extend(movie.spoken_languages)
                if movie.spoken_languages is not None:
                    temp_movie_spoken_languages_inserter.extend(
                        (MovieLanguage(movie.id, item.name) for item in movie.spoken_languages))
                countries_inserter.extend(movie.production_countries)
                if movie.production_countries is not None:
                    temp_movie_production_countries_inserter.extend(
                        (MovieProductionCountry(movie.id, item.name) for item in movie.production_countries))
                production_companies_inserter.extend(movie.production_companies)
                if movie.production_companies is not None:
                    temp_movie_production_companies_inserter.extend(
                        (MovieProductionCompany(movie.id, item.name) for item in movie.production_companies))
                titles_inserter.extend([movie.title, movie.original_title])
                statuses_inserter.append(movie.status)
                genres_inserter.extend(movie.genres)
                if movie.genres is not None:
                    temp_movie_genres_inserter.extend((MovieGenre(movie.id, item.name) for item in movie.genres))

            logging.info("finished iterating over movies, flushing inserters...")
            [inserter.flush() for inserter in
             [temp_movies_inserter, languages_inserter, temp_movie_spoken_languages_inserter,
              countries_inserter, temp_movie_production_countries_inserter,
              production_companies_inserter, temp_movie_production_companies_inserter, titles_inserter,
              statuses_inserter, genres_inserter, temp_movie_genres_inserter]]

            logging.info("inserting data from temp tables to final tables...")
            inserts = [insert.strip() for insert in sql.FINALIZE_MOVIES_TEMP_TABLES_QUERIES.split(';') if
                       len(insert.strip()) > 0]
            for insert in inserts:
                mysql.execute(insert, crsr)
            crsr.close()
            mysql.commit()

            logging.info('done')


if __name__ == '__main__':
    fire.Fire(MovieLoader)
