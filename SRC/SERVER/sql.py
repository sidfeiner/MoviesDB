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
