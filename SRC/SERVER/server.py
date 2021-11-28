import os
from typing import List, Dict, Callable, Iterable

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

app = Flask(__name__)

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
    limit = filters.pop('limit') if 'limit' in filters else DEFAULT_LIMIT
    projection = filters.pop('projection') if 'projection' in filters else None
    bad_filters = invalid_cols_func(filters.keys())
    bad_projections = invalid_cols_func(projection)
    if len(bad_filters) > 0 or len(bad_projections) > 0:
        return f"following columns are not supported: {', '.join(set(bad_filters + bad_projections))}", 400
    res = helper.query_limit(table, projection, filters, as_dict=True, limit=limit)
    return jsonify(res)


@app.route("/_query/movies")
def get_movies():
    conn = mysql.get_db()
    helper = mysql_common.MySQL(conn=conn)
    validator = Validator.get(helper)
    return get_data(helper, contract.MOVIES_VIEW, validator.find_invalid_movies_columns)


@app.route("/_query/crew")
def get_crew():
    conn = mysql.get_db()
    helper = mysql_common.MySQL(conn=conn)
    validator = Validator.get(helper)
    return get_data(helper, contract.CREW_VIEW, validator.find_invalid_crew_columns)


@app.route("/_query/cast")
def get_cast():
    conn = mysql.get_db()
    helper = mysql_common.MySQL(conn=conn)
    validator = Validator.get(helper)
    return get_data(helper, contract.CAST_VIEW, validator.find_invalid_cast_columns)


@app.route("/lookalike/movies")
def get_movie_lookalike():
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


@app.route("/misc/bestProfitPerWorker")
def get_best_profit_per_worker():
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


if __name__ == '__main__':
    app.run(port=5050)
