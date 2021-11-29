import json
import os
from decimal import Decimal
from typing import List, Dict, Callable, Iterable

import flask
from flasgger import Swagger
from flask import Flask, jsonify, request
from flaskext.mysql import MySQL
from werkzeug.datastructures import MultiDict

from SRC.API_DATA_RETRIEVE import contract
from SRC.API_DATA_RETRIEVE.common import mysql as mysql_common
from SRC.API_DATA_RETRIEVE.contract import DEFAULT_DB
from SRC.SERVER import sql
from SRC.SERVER.lookup import Lookup
from SRC.SERVER.validation import Validator

DEFAULT_LIMIT = 1000


class AppEncoder(flask.json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


app = Flask(__name__)
app.json_encoder = AppEncoder
app.config['SWAGGER'] = {
    'openapi': '3.0.0'
}
swagger = Swagger(app, template_file='docs/movie-db-service.yaml')

app.config['MYSQL_DATABASE_USER'] = os.environ['MYSQL_USER']
app.config['MYSQL_DATABASE_PASSWORD'] = os.environ['MYSQL_PWD']
app.config['MYSQL_DATABASE_DB'] = os.environ.get('MYSQL_DB', DEFAULT_DB)
app.config['MYSQL_DATABASE_HOST'] = os.environ.get('MYSQL_HOST', mysql_common.DEFAULT_HOST)
mysql = MySQL(app)
lookup = Lookup()


def as_dict_of_lists(d: MultiDict) -> Dict[str, List[str]]:
    final_d = {}
    for k in set(d.keys()):
        final_d[k] = [val for val in d.getlist(k)]
    return final_d


def get_data(helper: mysql_common.MySQL, table: str, invalid_cols_func: Callable[[Iterable[str]], List[str]]):
    filters = as_dict_of_lists(request.args)
    limit = filters.pop('limit')[0] if 'limit' in filters else None
    projection = filters.pop('projection') if 'projection' in filters else None
    bad_filters = invalid_cols_func(filters.keys())
    bad_projections = invalid_cols_func(projection)
    if len(bad_filters) > 0 or len(bad_projections) > 0:
        return f"following columns are not supported: {', '.join(set(bad_filters + bad_projections))}", 400
    res = helper.query_limit(table, projection, filters, as_dict=True, limit=limit)
    return jsonify(res)


@app.route("/movies/")
def get_movies():
    """
    Query `movies` table by any of it's columns. Parameters should be passed as <COLUMN_NAME>=<VALUE>.
    You can also pass `projection` parameters to return specific columns. If no projection parameter is passed,
    all columns will be returned.
    If desired, you can limit results with `limit` parameter
    """
    conn = mysql.get_db()
    helper = mysql_common.MySQL(conn=conn)
    validator = Validator.get(helper)
    return get_data(helper, contract.MOVIES_VIEW, validator.find_invalid_movies_columns)


@app.route("/crew/")
def get_crew():
    """
    Query `crew` table by any of it's columns. Parameters should be passed as <COLUMN_NAME>=<VALUE>.
    You can also pass `projection` parameters to return specific columns. If no projection parameter is passed,
    all columns will be returned.
    If desired, you can limit results with `limit` parameter
    """
    conn = mysql.get_db()
    helper = mysql_common.MySQL(conn=conn)
    validator = Validator.get(helper)
    return get_data(helper, contract.CREW_VIEW, validator.find_invalid_crew_columns)


@app.route("/cast")
def get_cast():
    """
    Query `cast` table by any of it's columns. Parameters should be passed as <COLUMN_NAME>=<VALUE>.
    You can also pass `projection` parameters to return specific columns. If no projection parameter is passed,
    all columns will be returned.
    If desired, you can limit results with `limit` parameter
    """
    conn = mysql.get_db()
    helper = mysql_common.MySQL(conn=conn)
    validator = Validator.get(helper)
    return get_data(helper, contract.CAST_VIEW, validator.find_invalid_cast_columns)


@app.route("/movies/lookalike")
def get_movie_lookalike():
    """
    Given a movie ID (passed through `id` parameter), return lookalike movies. Whether a movie is related,
    is based on the genre, the crew members, cast and results are sorted by popularity.
    """
    conn = mysql.get_db()
    helper = mysql_common.MySQL(conn=conn)
    movie_id = request.args.get('id')
    limit = request.args.get('limit', DEFAULT_LIMIT)
    if movie_id is None:
        return "id of movie was not given", 400

    res = helper.fetch_limit(sql.LOOKALIKE_MOVIES, {'movie_id': movie_id}, as_dict=True, limit=limit)
    if len(res) == 0:
        return "No lookalike was found", 204
    requested_movie_name, requested_movie_id = res[0]['requested_movie'], res[0]['requested_movie_id']
    return jsonify({
        'movie_id': requested_movie_id,
        'movie_name': requested_movie_name,
        'lookalikes': [{k: v for (k, v) in movie.items() if k not in ['requested_movie', 'requested_movie_id']} for
                       movie in res]
    })


@app.route('/cast/multiRole')
def get_multi_role_actors():
    """
    Return actors that played multiple roles in a single movie.
    Gender can be given and filtered by, in parameter `gender`, but is optional.
    Limit can be given to limit amount of results, in parameter `limit`, but is optional
    """
    conn = mysql.get_db()
    helper = mysql_common.MySQL(conn=conn)
    validator = Validator.get(helper)

    gender = request.args.get('gender')
    limit = request.args.get('limit', DEFAULT_LIMIT)
    if gender is not None:
        if len(validator.find_invalid_genders([gender]) > 0):
            return f"invalid gender given. must be: {', '.join(validator.genders)}", 400

    gender_id = lookup.lookup_one(helper, contract.GENDERS, 'genre', gender, 'id') if gender is not None else None
    res = helper.fetch_limit(
        sql.get_multi_role_actors_query(gender_id),
        (gender_id,) if gender_id is not None else None,
        as_dict=True, limit=limit
    )
    for item in res:
        item['characters'] = item['characters'].split(',')
    return jsonify(res)


@app.route("/misc/bestProfitPerWorker")
def get_best_profit_per_worker():
    """
    For a given genre, return the movie with the best profit per worker.
    This is calculated by dividing the net revenue (gross - budget) between all workers (cast + crew),
    meaning the result is movies where every worker had in the average, the biggest contribution to the movie's
    financial success.
    genre must be passed through `genre` parameter.
    """
    conn = mysql.get_db()
    helper = mysql_common.MySQL(conn=conn)
    validator = Validator.get(helper)

    genre = request.args.get('genre')
    if genre is None:
        return "you did not given a genre"
    if len(validator.find_invalid_genres([genre])) > 0:
        return "invalid genre given", 400
    limit = request.args.get('limit', DEFAULT_LIMIT)
    genre_id = lookup.lookup_one(helper, contract.GENRES_TABLE, 'genre', genre, 'id')
    res = helper.fetch_limit(sql.BEST_PROFIT_PER_MEMBER, (genre_id,), as_dict=True, limit=limit)
    return jsonify(res)


@app.route("/misc/loyalCrewMembers")
def get_loyal_crew_members():
    """
    Returns crew members that are local to their production company, meaning they work ONLY with a single
    production company.
    API returns data for a specific job (or jobs). If no job is given, all jobs will be searched.
    Pass jobs through the parameter `job`. For multiple jobs, pass the param a few times.
    limit param can also be passed to limit query results.
    """
    conn = mysql.get_db()
    helper = mysql_common.MySQL(conn=conn)
    validator = Validator.get(helper)

    args_lists = as_dict_of_lists(request.args)
    jobs = args_lists['job']
    limit = args_lists.pop('limit') if 'limit' in args_lists else DEFAULT_LIMIT
    bad_jobs = validator.find_invalid_jobs(jobs)
    if len(bad_jobs) > 0:
        return f"invalid job given: {', '.join(bad_jobs)}", 400
    if len(jobs) > 0:
        job_ids = lookup.lookup_many(helper, contract.JOBS_TABLE, 'job', jobs, 'id')
    else:
        job_ids = {}
    res = helper.fetch_limit(
        sql.get_loyal_crew_members_query(len(job_ids)), tuple(job_ids.values()), as_dict=True, limit=limit
    )
    for item in res:
        item['jobs'] = item['jobs'].split(',')
    return jsonify(res)


@app.route("/misc/genreDistribution")
def get_genre_distribution():
    """
    Returns the distribution of genres for a given query.
    If no query is given, all movies will be taken into account.
    Query should be passed through `query` parameter, in BOOLEAN QUERY syntax, which is described here:
    https://dev.mysql.com/doc/refman/8.0/en/fulltext-boolean.html
    """
    conn = mysql.get_db()
    helper = mysql_common.MySQL(conn=conn)

    match_query = request.args.get('query')
    if match_query is None:
        return "no match_query given", 400

    res = helper.fetch_limit(
        sql.get_genres_dist_by_bool_query(match_query is not None), [match_query] if match_query is not None else None,
        as_dict=True
    )

    return jsonify(res)


if __name__ == '__main__':
    app.run(port=5050)
