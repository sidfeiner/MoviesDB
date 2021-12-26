from typing import List, Dict, Optional

from SRC.API_DATA_RETRIEVE import contract
from SRC.API_DATA_RETRIEVE.common.formats import Serializable, ToDB


def clean_gender(gender: int) -> Optional[int]:
    return gender if gender != 0 else None


class CastMember(Serializable, ToDB):
    def __init__(self, id: int, name: str, cast_id: int, character: str, gender: int, order: int, movie_id: str,
                 *args, **kwargs):
        self.id = id
        self.name = name.strip()
        self.character = character
        self.gender = clean_gender(gender)
        self.cast_id = cast_id
        self.movie_id = movie_id
        self.order = order

    @classmethod
    def export_order(cls) -> List[str]:
        return ['id', 'name', 'character', 'gender', 'cast_id', 'movie_id', 'order']

    @classmethod
    def override_target_names(cls) -> Dict[str, str]:
        return {
            'name': '`name`',
            'character': 'character_name',
            'order': '`order`',
            'id': 'id_in_cast',
            'gender': 'gender_id',
        }


class CrewMember(Serializable, ToDB):
    def __init__(self, id: int, name: str, job: str, gender: int, department: str, movie_id: str,
                 *args, **kwargs):
        self.id = id
        self.name = name.strip()
        self.job = job
        self.department = department
        self.gender = clean_gender(gender)
        self.movie_id = movie_id

    @classmethod
    def override_target_names(cls) -> Dict[str, str]:
        return {
            'name': '`name`',
            'id': 'id_in_crew',
            'gender': 'gender_id',
        }

    @classmethod
    def export_order(cls) -> List[str]:
        return ['id', 'name', 'job', 'department', 'gender', 'movie_id']


class Credits:
    def __init__(self, movie_id: int, crew: List[CrewMember], cast: List[CastMember]):
        self.movie_id = movie_id
        self.crew = crew
        self.cast = cast


class Keyword(Serializable, ToDB):
    def __init__(self, id: int, name: str, *args, **kwargs):
        self.id = id
        self.name = name.strip()

    @classmethod
    def export_order(cls) -> List[str]:
        return ['id', 'name']

    @classmethod
    def override_target_names(cls) -> Dict[str, str]:
        return {
            'name': '`keyword`'
        }


class Keywords:
    def __init__(self, movie_id: int, keywords: List[Keyword]):
        self.movie_id = movie_id
        self.keywords = keywords


class MovieKeyword(ToDB):
    def __init__(self, movie_id: int, keyword: str):
        self.movie_id = movie_id
        self.keyword = keyword

    @classmethod
    def export_order(cls) -> List[str]:
        return ['movie_id', 'keyword']


class Company(Serializable, ToDB):
    def __init__(self, id: int, name: str, *args, **kwargs):
        self.id = id
        self.name = name.strip()

    @classmethod
    def export_order(cls) -> List[str]:
        return ['id', 'name']

    @classmethod
    def override_target_names(cls) -> Dict[str, str]:
        return {
            'name': 'production_company'
        }


class MovieProductionCompany(ToDB):
    def __init__(self, movie_id: int, company: str, *args, **kwargs):
        self.movie_id = movie_id
        self.company = company.strip()

    @classmethod
    def export_order(cls) -> List[str]:
        return ["movie_id", "company"]

    @classmethod
    def override_target_names(cls) -> Dict[str, str]:
        return {
            'company': 'production_company'
        }


class Country(Serializable, ToDB):
    def __init__(self, name: str, iso_3166_1: str, *args, **kwargs):
        self.name = name.strip()
        self.iso_3166_1 = iso_3166_1.strip()

    @classmethod
    def export_order(cls) -> List[str]:
        return ["name", "iso_3166_1"]

    @classmethod
    def override_target_names(cls) -> Dict[str, str]:
        return {
            'name': 'country_name',
            'iso_3166_1': 'country_name_iso_3166_1'
        }


class MovieProductionCountry(ToDB):
    def __init__(self, movie_id: int, country: str):
        self.movie_id = movie_id
        self.country = country.strip()

    @classmethod
    def export_order(cls) -> List[str]:
        return ['movie_id', 'country']


class Language(Serializable, ToDB):
    def __init__(self, name: str, iso_639_1: str, *args, **kwargs):
        self.name = name.strip()
        self.iso_639_1 = iso_639_1.strip()

    @classmethod
    def export_order(cls) -> List[str]:
        return ['name', 'iso_639_1']

    @classmethod
    def override_target_names(cls) -> Dict[str, str]:
        return {
            'name': 'language',
            'iso_639_1': 'language_iso_639_1'
        }


class MovieLanguage(ToDB):
    def __init__(self, movie_id: int, language: str):
        self.movie_id = movie_id
        self.language = language.strip()

    @classmethod
    def export_order(cls) -> List[str]:
        return ['movie_id', 'language']


class Genre(Serializable, ToDB):
    def __init__(self, id: int, name: str):
        self.id = id
        self.name = name.strip()

    @classmethod
    def export_order(cls) -> List[str]:
        return ["id", "name"]

    @classmethod
    def override_target_names(cls) -> Dict[str, str]:
        return {
            'name': 'genre'
        }


class MovieGenre(ToDB):
    def __init__(self, movie_id: int, name: str):
        self.movie_id = movie_id
        self.name = name.strip()

    @classmethod
    def export_order(cls) -> List[str]:
        return ['movie_id', 'name']

    @classmethod
    def override_target_names(cls) -> Dict[str, str]:
        return {
            'name': 'genre'
        }


class Movie(ToDB):
    def __init__(self, id: int, adult: bool, budget: int, genres: List[Genre], imdb_id: str, original_language: str,
                 original_title: str, overview: str, popularity: float, production_companies: List[Company],
                 production_countries: List[Country], release_date: str, revenue: int, runtime: int,
                 spoken_languages: List[Language], status: str, tagline: str, title: str, vote_average: float,
                 vote_count: int):
        self.id = id
        self.title = title
        self.adult = adult
        self.budget = budget
        self.genres = genres  # type: List[Genre]
        self.imdb_id = imdb_id
        self.original_language = original_language
        self.original_title = original_title
        self.overview = overview
        self.popularity = popularity
        self.production_companies = production_companies  # type: List[Company]
        self.production_countries = production_countries  # type: List[Country]
        self.release_date = release_date if release_date.strip() != "" else None
        self.revenue = revenue
        self.runtime = runtime
        self.spoken_languages = spoken_languages  # type: List[Language]
        self.status = status.strip() if status is not None else None
        self.tagline = tagline
        self.vote_average = vote_average
        self.vote_count = vote_count

    @classmethod
    def export_order(cls) -> List[str]:
        return ['id', 'title', 'adult', 'budget', 'original_language', 'original_title', 'overview', 'popularity',
                'release_date', 'revenue', 'runtime', 'status', 'tagline', 'vote_average', 'vote_count']

    @classmethod
    def override_target_names(cls) -> Dict[str, str]:
        return {
            'budget': 'budget_usd',
            'adult': 'is_adult',
            'revenue': 'revenue_usd',
            'runtime': 'runtime_minutes',
            'vote_average': 'vote_avg',
            'vote_count': 'vote_cnt'
        }
