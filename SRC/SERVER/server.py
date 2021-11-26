import os
from typing import List, Dict, Callable, Iterable

from flask import Flask, jsonify, request
from flaskext.mysql import MySQL
from werkzeug.datastructures import MultiDict

from SRC.API_DATA_RETRIEVE import contract
from SRC.API_DATA_RETRIEVE.common import mysql as mysql_common
from SRC.API_DATA_RETRIEVE.contract import DEFAULT_DB
from SRC.SERVER import sql
from SRC.SERVER.validation import Validator

DEFAULT_LIMIT = 1000

app = Flask(__name__)

app.config['MYSQL_DATABASE_USER'] = os.environ['MYSQL_USER']
app.config['MYSQL_DATABASE_PASSWORD'] = os.environ['MYSQL_PWD']
app.config['MYSQL_DATABASE_DB'] = os.environ.get('MYSQL_DB', DEFAULT_DB)
app.config['MYSQL_DATABASE_HOST'] = os.environ.get('MYSQL_HOST', mysql_common.DEFAULT_HOST)
mysql = MySQL(app)


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


@app.route("/movies")
def get_movies():
    conn = mysql.get_db()
    helper = mysql_common.MySQL(conn=conn)
    validator = Validator.get(helper)
    return get_data(helper, contract.MOVIES_VIEW, validator.find_invalid_movies_columns)


@app.route("/crew")
def get_crew():
    conn = mysql.get_db()
    helper = mysql_common.MySQL(conn=conn)
    validator = Validator.get(helper)
    return get_data(helper, contract.CREW_VIEW, validator.find_invalid_crew_columns)


@app.route("/cast")
def get_cast():
    conn = mysql.get_db()
    helper = mysql_common.MySQL(conn=conn)
    validator = Validator.get(helper)
    return get_data(helper, contract.CAST_VIEW, validator.find_invalid_cast_columns)


@app.route("/lookalike/movie")
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


if __name__ == '__main__':
    app.run()
