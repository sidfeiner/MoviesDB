from typing import List

from SRC.API_DATA_RETRIEVE import contract
from SRC.API_DATA_RETRIEVE.common.mysql import MySQL


class Validator:
    movies_columns = None
    cast_columns = None
    crew_columns = None
    genres = None
    is_init = False

    @classmethod
    def get(cls, mysql: MySQL):
        cls.init(mysql)
        return cls

    @classmethod
    def init(cls, mysql: MySQL):
        if not cls.is_init:
            cur_db = mysql.fetch_one("select database()")[0]
            cls.movies_columns = [r[0] for r in mysql.query_all("information_schema.columns", ['column_name'],
                                                                {'table_schema': cur_db,
                                                                 'table_name': contract.MOVIES_VIEW})]
            cls.cast_columns = [r[0] for r in mysql.query_all("information_schema.columns", ['column_name'],
                                                              {'table_schema': cur_db,
                                                               'table_name': contract.CAST_VIEW})]
            cls.crew_columns = [r[0] for r in mysql.query_all("information_schema.columns", ['column_name'],
                                                              {'table_schema': cur_db,
                                                               'table_name': contract.CREW_VIEW})]
            cls.genres = [r[0] for r in mysql.query_all(contract.GENRES_TABLE, ['genre'])]
        cls.is_init = True

    @classmethod
    def find_invalid_movies_columns(cls, cols: List[str]) -> List[str]:
        """
        :param cols: columns we want to query
        :return: list of columns that are INVALID
        """
        if cols is None:
            return []
        return [col for col in cols if col not in cls.movies_columns]

    @classmethod
    def find_invalid_cast_columns(cls, cols: List[str]) -> List[str]:
        """
        :param cols: columns we want to query
        :return: list of columns that are INVALID
        """
        if cols is None:
            return []
        return [col for col in cols if col not in cls.cast_columns]

    @classmethod
    def find_invalid_crew_columns(cls, cols: List[str]) -> List[str]:
        """
        :param cols: columns we want to query
        :return: list of columns that are INVALID
        """
        if cols is None:
            return []
        return [col for col in cols if col not in cls.crew_columns]

    @classmethod
    def find_invalid_genres(cls, genres: List[str]) -> List[str]:
        """
        :param genres: genres we want to query
        :return: list of genres that are INVALID
        """
        return [genre for genre in genres if genre not in cls.genres]
