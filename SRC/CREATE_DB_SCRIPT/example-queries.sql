-- These are example for queries generated by our API, querying our DB
-- Full templates for these queries can be seen in the file SERVER/sql.py,
-- and are rendered in SERVER/server.py

-- 1. Movies query (queries view directly to make life easier)
select id, title, tagline, original_language
from v_movies
where release_date = '2021-12-28';

-- 2. Cast query (queries view directly to make life easier)
select character_name, movie_title
from v_cast
where name = 'Brad Pitt';

-- 3. Crew query (queries view directly to make life easier)
select movie_title, name
from v_crew
where job = 'Director';

-- 4. Lookalike movies
with same_genres as
         (
             select mg1.movie_id                                            as movie_id_1,
                    mg2.movie_id                                            as movie_id_2,
                    count(case when mg1.genre_id = mg2.genre_id then 1 end) as cnt
             from movie_genres mg1
                      join movie_genres mg2 on mg1.movie_id <> mg2.movie_id
             where mg1.movie_id = 12180
             group by 1, 2
         ),
     same_cast as (
         select c1.movie_id                                             as movie_id_1,
                c2.movie_id                                             as movie_id_2,
                count(case when c1.person_id = c2.person_id then 1 end) as cnt
         from cast c1
                  join cast c2 on c1.movie_id <> c2.movie_id
         where c1.movie_id = 12180
         group by 1, 2
     ),
     same_crew as (
         select c1.movie_id                                             as movie_id_1,
                c2.movie_id                                             as movie_id_2,
                count(case when c1.person_id = c2.person_id then 1 end) as cnt
         from crew c1
                  join crew c2 on c1.movie_id <> c2.movie_id
         where c1.movie_id = 12180
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
from movies m1
         join movies m2 on m1.id <> m2.id
         join same_genres sg on sg.movie_id_1 = m1.id and sg.movie_id_2 = m2.id
         join same_cast sc on sc.movie_id_1 = m1.id and sc.movie_id_2 = m2.id
         join same_crew scr on scr.movie_id_1 = m1.id and scr.movie_id_2 = m2.id
         left join titles t1 on m1.title_id = t1.id
         left join titles t2 on m2.title_id = t2.id
where m1.id = 12180
order by lookalike_score desc,
         m2.vote_avg desc;

-- 5. Multi Role
-- we use group concat, because this is to be used for the API and `characters` column will be parsed into a list
select n.name, t.title, group_concat(distinct cn.character_name) as characters
from cast
         join names n on cast.person_id = n.id
         join movies m on cast.movie_id = m.id
         join character_names cn on cast.character_name_id = cn.id
         join titles t on m.original_title_id = t.id
group by 1, 2
having count(distinct cn.character_name) >= 2;

-- 6. Best profit per worker
with movie_members_amt as (
    select movie_id, count(distinct person_id) as members
    from (
             select movie_id, person_id
             from cast
             union all
             select movie_id, person_id
             from crew
         ) a
    group by 1
)
select m.id as movie_id, t.title as movie_name, (m.revenue_usd - m.budget_usd) / mm.members as profit_rate
from movies m
         join movie_members_amt mm on m.id = mm.movie_id
         join movie_genres mg on m.id = mg.movie_id
         join titles t on m.title_id = t.id
where genre_id = 18
  and m.revenue_usd is not null
  and m.budget_usd is not null
order by 1, profit_rate desc;

-- 7. Loyal crew members
-- we use group concat, because this is to be used for the API and `jobs` column will be parsed into a list
with crew_company as (
    select crew.person_id, crew.job_id, mpc.production_company_id
    from crew
             join movie_production_companies mpc on crew.movie_id = mpc.movie_id
)
select n.name, pc.production_company, group_concat(distinct jobs.job) as jobs
from crew_company cc1
         join names n on cc1.person_id = n.id
         join production_companies pc on cc1.production_company_id = pc.id
         join jobs on cc1.job_id = jobs.id
where exists(select 1 from crew where cc1.person_id = crew.person_id)
  and not exists(select 1
                 from crew_company cc2
                 where cc1.person_id = cc2.person_id
                   and cc1.production_company_id <> cc2.production_company_id)

group by 1, 2;

-- 8. Genre distribution
select genres.genre, round(pct, 2) as pct
from (
         select distinct genre_id,
                         100 * count(*) over (partition by genre_id) / count(*) over () as pct
         from movies m
                  join movie_genres mg on m.id = mg.movie_id
         where MATCH(overview, tagline) AGAINST('+sleep +nightmare' IN BOOLEAN MODE)
     ) a
         join genres on a.genre_id = genres.id
order by pct desc;

-- 9. Genre keywords stats
select g.genre,
       k.keyword,
       movies_in_genre_pct,
       in_tagline_pct,
       in_overview_pct
from (
         select genre_id,
                keyword_id,
                movies_in_genre_pct,
                in_tagline_pct,
                in_overview_pct,
                dense_rank() over (partition by genre_id order by movies_in_genre_pct desc) as rnk
         from keywords_genre_stats
         where genre_id in (18, 35, 80)
     ) a
         join genres g on g.id = a.genre_id
         join keywords k on k.id = a.keyword_id
where rnk <= 10
order by g.genre, a.rnk asc;