import ast
import csv
from typing import List, Generator

import fire

from SRC.API_DATA_RETRIEVE.sources.serializable import Serializable


class CastMember(Serializable):
    def __init__(self, id: int, name: str, cast_id: int, character: str, gender: int, order: int, credit_id: str,
                 *args, **kwargs):
        self.id = id
        self.name = name
        self.character = character
        self.gender = gender
        self.cast_id = cast_id
        self.credit_id = credit_id
        self.order = order


class CrewMember(Serializable):
    def __init__(self, id: int, name: str, job: str, gender: int, department: str, credit_id: str,
                 *args, **kwargs):
        self.id = id
        self.name = name
        self.job = job
        self.department = department
        self.gender = gender
        self.credit_id = credit_id


class Credits:
    def __init__(self, id: int, crew: List, cast: List):
        self.id = id
        self.crew = crew
        self.cast = cast


class ProductionCompany(Serializable):
    def __init__(self, id: int, name: str, *args, **kwargs):
        self.id = id
        self.name = name


class Country(Serializable):
    def __init__(self, name: str, iso_3166_1: str):
        self.name = name
        self.iso_3166_1 = iso_3166_1


class Language(Serializable):
    def __init__(self, name: str, iso_639_1: str):
        self.name = name
        self.iso_639_1 = iso_639_1


class Genre(Serializable):
    def __init__(self, id: int, name: str):
        self.id = id
        self.name = name


class Movie:
    def __init__(self, id: int, adult: bool, budget: int, genres: List[Genre], imdb_id: str, original_language: str,
                 original_title: str, overview: str, popularity: float, production_companies: List[ProductionCompany],
                 production_countries: List[Country], release_date: str, revenue: int, runtime: int,
                 spoken_languages: List[Language], status: str, tagline: str, title: str, vote_average: float,
                 vote_count: int):
        self.id = id
        self.adult = adult
        self.budget = budget
        self.genres = genres
        self.imdb_id = imdb_id
        self.original_language = original_language
        self.original_title = original_title
        self.overview = overview
        self.popularity = popularity
        self.production_companies = production_companies
        self.production_countries = production_countries
        self.release_date = release_date
        self.revenue = revenue
        self.runtime = runtime
        self.spoken_languages = spoken_languages
        self.status = status
        self.tagline = tagline
        self.title = title
        self.vote_average = vote_average
        self.vote_count = vote_count


class KaggleParser:
    """
    This will parse data from the Kaggle DB, obtained at https://www.kaggle.com/rounakbanik/the-movies-dataset
    We will use data only from 2 tables:
    1) movies_metadata.csv
    2) credits.csv
    3) keywords.csv

    links/ratings tables will be ignored because links aren't important to us, and individual ratings won't
    be treated in our service (only overall rating which appears in the movies_metadata.csv file)
    """

    @staticmethod
    def get_parsed_credits_generator(credits_file_path: str) -> Generator[Credits, None, None]:
        with open(credits_file_path, newline='') as fp:
            reader = csv.DictReader(fp, delimiter=',', quotechar='"')
            for row in reader:
                yield Credits(
                    int(row['id']),
                    [CrewMember.from_dict(d) for d in ast.literal_eval(row['crew'])],
                    [CastMember.from_dict(d) for d in ast.literal_eval(row['cast'])]
                )

    @staticmethod
    def get_parsed_movies_generator(movies_file_path: str) -> Generator[Movie, None, None]:
        with open(movies_file_path, newline='') as fp:
            reader = csv.DictReader(fp, delimiter=',', quotechar='"')
            for row in reader:
                yield Movie(
                    int(row['id']),
                    bool(row['adult']),
                    int(row['budget']),
                    [Genre.from_dict(d) for d in ast.literal_eval(row['genres'])],
                    row['imdb_id'],
                    row['original_language'],
                    row['original_title'],
                    row['overview'],
                    float(row['popularity']),
                    [ProductionCompany.from_dict(d) for d in ast.literal_eval(row['production_companies'])],
                    [Country.from_dict(d) for d in ast.literal_eval(row['production_countries'])],
                    row['release_date'],
                    int(row['revenue']),
                    int(row['runtime'].replace('.0', '')),
                    [Language.from_dict(d) for d in ast.literal_eval(row['spoken_languages'])],
                    row['status'],
                    row['tagline'],
                    row['title'],
                    float(row['vote_average']),
                    int(row['vote_count'])
                )

    def load_credits(self, credits_file_path: str):
        credits = self.get_parsed_credits_generator(credits_file_path)
        for credit in credits:
            print(credit)

    def load_movies(self, movies_file_path: str):
        movies = self.get_parsed_movies_generator(movies_file_path)
        for movie in movies:
            print(movie)


if __name__ == '__main__':
    fire.Fire(KaggleParser)
