TEMP_MOVIES_TABLES_DLLS = """
create temporary table temp_movie_genres
(
    movie_id int,
    genre    varchar(50),
    constraint unique (movie_id, genre)
);

create temporary table temp_movie_production_companies
(
    movie_id           int,
    production_company varchar(200),
    constraint unique (movie_id, production_company)
);

create temporary table temp_movie_production_countries
(
    movie_id int,
    country  varchar(200),
    constraint unique (movie_id, country)
);

create temporary table temp_movie_spoken_languages
(
    movie_id int,
    language varchar(50),
    constraint unique (movie_id, language)
);

create temporary table temp_movies
(
    id                int,
    is_adult             bool,
    budget_usd        int,
    original_language varchar(2),
    original_title    varchar(200),
    overview          varchar(1000),
    popularity        decimal(9, 3),
    release_date      date,
    revenue_usd   bigint,
    runtime_minutes   int,
    status            varchar(15),
    tagline           varchar(500),
    title             varchar(200),
    vote_avg          decimal(4, 2),
    vote_cnt          int
);

"""

FINALIZE_MOVIES_TEMP_TABLES_QUERIES = """
insert into movies(id, is_adult, budget_usd, original_language, original_title_id, overview, popularity,
                          release_date, revenue_usd, runtime_minutes, status_id, tagline, title_id, vote_avg,
                          vote_cnt)
select m.id,
       m.is_adult,
       m.budget_usd,
       m.original_language,
       ot.id,
       m.overview,
       m.popularity,
       m.release_date,
       m.revenue_usd,
       m.runtime_minutes,
       s.id,
       nullif(m.tagline, ''),
       t.id,
       m.vote_avg,
       m.vote_cnt
from temp_movies m
         left join titles ot on m.original_title = ot.title
         left join titles t on m.title = t.title
         left join statuses s on m.status = s.status
on duplicate key update id=m.id;


insert into movie_spoken_languages(movie_id, language_id)
select movie_id, l.id
from temp_movie_spoken_languages m
         left join languages l on l.language = m.language
on duplicate key update movie_id=m.movie_id;

insert into movie_production_countries(movie_id, country_id)
select movie_id, c.id
from temp_movie_production_countries m
         left join countries c on m.country = c.country_name
on duplicate key update movie_id=m.movie_id;


insert into movie_production_companies(movie_id, production_company_id)
select movie_id, c.id
from temp_movie_production_companies m
         left join production_companies c on m.production_company = c.production_company
on duplicate key update movie_id=m.movie_id;

insert into movie_genres(movie_id, genre_id)
select movie_id, g.id
from temp_movie_genres m
         left join genres g on m.genre = g.genre
on duplicate key update movie_id=m.movie_id;
"""

TEMP_CREDITS_TABLES_DLLS = """
create temporary table temp_cast
(
    `name`           VARCHAR(50),
    character_name   VARCHAR(500),
    gender_id        int,
    `order`          int,
    cast_id          int,
    movie_id         int,
    id_in_cast       int
);

create temporary table temp_crew
(
    name       VARCHAR(50),
    gender_id  int,
    job        VARCHAR(100),
    department VARCHAR(50),
    movie_id   int,
    id_in_crew int
);
"""

FINALIZE_CREDITS_TEMP_TABLES_QUERIES = """
insert into persons(name_id, gender_id) 
select n.id as name_id, max(m.gender_id)
from temp_cast m
        left join names n using (name)
group by 1
on duplicate key update name_id=name_id;

insert into persons(name_id, gender_id) 
select n.id as name_id, max(m.gender_id)
from temp_crew m
        left join names n using (name)
group by 1
on duplicate key update name_id=name_id;

insert into `cast`(person_id, character_name_id, `order`, cast_id, movie_id, id_in_cast)
select p.id, cn.id, m.`order`, m.cast_id, m.movie_id, m.id_in_cast
from temp_cast m
         left join names n using (name)
         left join persons p on n.id = p.name_id
         left join character_names cn using (character_name)
on duplicate key update movie_id=m.movie_id;

insert into crew(person_id, job_id, department_id, movie_id, id_in_crew)
select p.id, j.id, d.id, m.movie_id, m.id_in_crew
from temp_crew m
         left join names n using (name)
         left join persons p on n.id = p.name_id
         left join jobs j using (job)
         left join departments d using (department)
on duplicate key update movie_id=m.movie_id;
"""

TEMP_KEYWORDS_TABLES_DLLS = """
create temporary table temp_movie_keywords
(
    movie_id   int,
    keyword    varchar(50)
);
"""

FINALIZE_KEYWORDS_TEMP_TABLES_QUERIES = """
insert into movie_keywords(movie_id, keyword_id) 
select distinct m.movie_id, k.id
from temp_movie_keywords m
         left join keywords k using (keyword)
on duplicate key update movie_id=m.movie_id;
"""
