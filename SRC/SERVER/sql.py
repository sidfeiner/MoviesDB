from typing import List, Optional

from SRC.API_DATA_RETRIEVE import contract

LOOKALIKE_MOVIES = f"""
with same_genres as
         (
             select mg1.movie_id                                            as movie_id_1,
                    mg2.movie_id                                            as movie_id_2,
                    count(case when mg1.genre_id = mg2.genre_id then 1 end) as cnt
             from {contract.MOVIE_GENRES_TABLE} mg1
                      join movie_genres mg2 on mg1.movie_id <> mg2.movie_id
             where mg1.movie_id = %(movie_id)s
             group by 1, 2
         ),
     same_cast as (
         select c1.movie_id                                         as movie_id_1,
                c2.movie_id                                         as movie_id_2,
                count(case when c1.name_id = c2.name_id then 1 end) as cnt
         from {contract.CAST_TABLE} c1
                  join {contract.CAST_TABLE} c2 on c1.movie_id <> c2.movie_id
         where c1.movie_id = %(movie_id)s
         group by 1, 2
     ),
     same_crew as (
         select c1.movie_id                                         as movie_id_1,
                c2.movie_id                                         as movie_id_2,
                count(case when c1.name_id = c2.name_id then 1 end) as cnt
         from {contract.CREW_TABLE} c1
                  join {contract.CREW_TABLE} c2 on c1.movie_id <> c2.movie_id
         where c1.movie_id = %(movie_id)s
         group by 1, 2
     )
select m1.id    as requested_movie_id,
       t1.title as requested_movie,
       m2.id    as movie_id,
       t2.title as title,
       m2.original_language,
       sg.cnt   as mutual_genres_amt,
       sc.cnt   as mutual_cast_amt,
       scr.cnt  as mutual_crew_amt,
       m2.vote_avg,
       IF(m1.original_language = m2.original_language, 5, 0) + 3 * sg.cnt + 2 * sc.cnt +
       scr.cnt  as lookalike_score
from {contract.MOVIES_TABLE} m1
         join movies m2 on m1.id <> m2.id
         join same_genres sg on sg.movie_id_1 = m1.id and sg.movie_id_2 = m2.id
         join same_cast sc on sc.movie_id_1 = m1.id and sc.movie_id_2 = m2.id
         join same_crew scr on scr.movie_id_1 = m1.id and scr.movie_id_2 = m2.id
         left join titles t1 on m1.title_id = t1.id
         left join titles t2 on m2.title_id = t2.id
where m1.id = %(movie_id)s
order by lookalike_score desc,
         m2.vote_avg desc
"""

BEST_PROFIT_PER_MEMBER = f"""
with movie_members_amt as (
    select movie_id, count(distinct name_id) as members
    from (
             select movie_id, name_id
             from cast
             union all
             select movie_id, name_id
             from crew
         ) a
    group by 1
)
select m.id as movie_id, t.title as movie_name, (m.revenue_usd - m.budget_usd) / mm.members as profit_rate
from movies m
         join movie_members_amt mm on m.id = mm.movie_id
         join movie_genres mg on m.id = mg.movie_id
         join titles t on m.title_id = t.id
where genre_id = %s and m.revenue_usd is not null and m.budget_usd is not null
order by 1, profit_rate desc
"""

_loyal_crew_members_query = """
with crew_company as (
    select crew.name_id, crew.job_id, mpc.production_company_id
    from crew
             join movie_production_companies mpc on crew.movie_id = mpc.movie_id
)
select n.name, pc.production_company, group_concat(distinct jobs.job) as jobs
from crew_company cc1
         join names n on cc1.name_id = n.id
         join production_companies pc on cc1.production_company_id = pc.id
         join jobs on cc1.job_id = jobs.id
where exists(select 1 from crew where cc1.name_id = crew.name_id {jobs_filter})
    and not exists(select 1
                 from crew_company cc2
                 where cc1.name_id = cc2.name_id
                   and cc1.production_company_id <> cc2.production_company_id)
group by 1, 2
"""


def get_loyal_crew_members_query(jobs_amt: int) -> str:
    if jobs_amt == 0:
        jobs_filter = ''
    else:
        jobs_filter = f"and crew.job_id in ({', '.join(['%s'])})"
    return _loyal_crew_members_query.format(jobs_filter=jobs_filter)


_genres_dist_by_bool_query = """
select genres.genre, round(pct, 2) as pct
from (
         select distinct genre_id,
                         100 * count(*) over (partition by genre_id) / count(*) over () as pct
         from movies
                  join movie_genres mg on movies.id = mg.movie_id
         {match_clause}
     ) a
         join genres on a.genre_id = genres.id
order by pct desc
"""


def get_genres_dist_by_bool_query(with_match: bool):
    match_clause = f"where MATCH(overview, tagline) AGAINST(%s IN BOOLEAN MODE)" if with_match else ''
    return _genres_dist_by_bool_query.format(match_clause=match_clause)
