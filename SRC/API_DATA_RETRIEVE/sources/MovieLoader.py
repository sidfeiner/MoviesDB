import logging
from typing import Generator, Any, Sequence

from SRC.API_DATA_RETRIEVE.common.mysql import BatchObjInserter, BatchValueInserter, MySQL, MySQLAuth
from SRC.API_DATA_RETRIEVE.contract import DEFAULT_DB
from SRC.API_DATA_RETRIEVE.sources.kaggle import sql
from SRC.API_DATA_RETRIEVE.sources.kaggle.base import *

DEFAULT_INSERT_BATCH_SIZE = 2000


class MovieLoader:
    def __init__(self, mysql_usr: str = '', mysql_pwd: str = '', mysql_host: Optional[str] = None,
                 mysql_port: Optional[int] = None, mysql_db: str = DEFAULT_DB, mysql_helper: Optional[MySQL] = None,
                 log_level: str = logging.INFO, *args, **kwargs):
        logging.basicConfig(level=log_level,
                            format="%(asctime)s : %(threadName)s: %(levelname)s : %(name)s : %(module)s : %(message)s")
        self.mysql = mysql_helper or MySQL(MySQLAuth(mysql_usr, mysql_pwd, mysql_host, mysql_port, mysql_db))

    @staticmethod
    def get_ingestion_ddl() -> List[str]:
        return [ddl.strip() for ddl in sql.TEMP_MOVIES_TABLES_DLLS.split(';') if len(ddl.strip()) > 0]

    def get_parsed_movies_generator(self) -> Generator[Movie, None, None]:
        raise NotImplementedError

    def load(self, insert_batch_size: int = DEFAULT_INSERT_BATCH_SIZE) -> Sequence[Any]:
        movies = self.get_parsed_movies_generator()
        with self.mysql as mysql:
            crsr = mysql.get_cursor()
            temp_movies_inserter = BatchObjInserter(mysql, crsr, insert_batch_size, f"temp_{contract.MOVIES_TABLE}")
            languages_inserter = BatchObjInserter(mysql, crsr, insert_batch_size, contract.LANGUAGES_TABLE,
                                                  cache_size=200, cache_key_func=lambda x: x.iso_639_1)
            temp_movie_spoken_languages_inserter = BatchObjInserter(mysql, crsr, insert_batch_size,
                                                                    f"temp_{contract.MOVIES_SPOKEN_LANGUAGES_TABLE}")
            countries_inserter = BatchObjInserter(mysql, crsr, insert_batch_size, contract.COUNTRIES_TABLE,
                                                  cache_size=2000, cache_key_func=lambda x: x.iso_3166_1)
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
            genres_inserter = BatchObjInserter(mysql, crsr, insert_batch_size, contract.GENRES_TABLE, cache_size=200)
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
                yield movie

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
