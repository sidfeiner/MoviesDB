create schema movies;

create table genders
(
    id     int not null primary key,
    gender varchar(10),
    constraint unique (gender)
);

insert into genders(id, gender)
VALUES (1, 'female'),
       (2, 'male');

create table names
(
    id   int not null auto_increment primary key,
    name varchar(50),
    constraint unique (name)
);
create index names_name_idx on names (name(15));

create table persons
(
    id        int not null auto_increment primary key,
    name_id   int,
    gender_id int,
    foreign key (name_id) references names (id),
    foreign key (gender_id) references genders (id),
    constraint unique (name_id, gender_id)
);

create table keywords
(
    id      int primary key,
    keyword varchar(50),
    constraint unique (keyword)
);
create index keywords_keyword_idx on keywords (keyword(15));


create table character_names
(
    id             int not null auto_increment primary key,
    character_name varchar(500),
    constraint unique (character_name)
);
create index character_names_character_name_idx on character_names (character_name(20));

create table jobs
(
    id  int not null auto_increment primary key,
    job varchar(100),
    constraint unique (job)
);
create index jobs_jobs_idx on jobs (job);


create table departments
(
    id         int not null auto_increment primary key,
    department varchar(50),
    constraint unique (department)
);
create index departments_department_idx on departments (department);

create table genres
(
    id    int primary key,
    genre varchar(20),
    constraint unique (genre)
);
create index genres_genre_idx on genres (genre);

create table titles
(
    id    int not null auto_increment primary key,
    title varchar(200),
    constraint unique (title)
);
create index titles_title_idx on titles (title(30));

create table production_companies
(
    id                 int primary key,
    production_company varchar(200),
    constraint unique (production_company)
);
create index production_companies_production_company_idx on production_companies (production_company(30));

create table countries
(
    id                      int not null auto_increment primary key,
    country_name            varchar(50),
    country_name_iso_3166_1 varchar(2),
    constraint unique (country_name, country_name_iso_3166_1)
);
create index countries_country_name_idx on countries (country_name);
create index countries_country_name_iso_3166_1_name_idx on countries (country_name_iso_3166_1);

create table languages
(
    id                 int not null auto_increment primary key,
    language           varchar(20),
    language_iso_639_1 varchar(2),
    constraint unique (language),
    constraint unique (language_iso_639_1)
);
create index languages_language_name_idx on languages (language);
create index languages_language_iso_639_1_name_idx on languages (language_iso_639_1);


create table statuses
(
    id     int not null auto_increment primary key,
    status varchar(15),
    constraint unique (status)
);
create index statuses_status_idx on statuses (status);

create table movies
(
    id                int primary key,
    title_id          int,
    original_title_id int,
    original_language varchar(2),
    popularity        decimal(9, 3),
    release_date      date,
    tagline           varchar(500),
    is_adult          bool,
    overview          varchar(1500),
    budget_usd        int,
    revenue_usd       bigint,
    runtime_minutes   int,
    status_id         int,
    vote_avg          decimal(4, 2),
    vote_cnt          int,
    foreign key (title_id) references titles (id),
    foreign key (original_title_id) references titles (id),
    foreign key (status_id) references statuses (id),
    fulltext key (tagline),
    fulltext key (overview),
    fulltext key (overview, tagline)
);

create table movie_keywords
(
    id         int not null auto_increment primary key,
    movie_id   int,
    keyword_id int,
    constraint unique (movie_id, keyword_id),
    foreign key (movie_id) references movies (id),
    foreign key (keyword_id) references keywords (id)
);



create table cast
(
    id                int not null auto_increment primary key,
    person_id         int,
    character_name_id int,
    `order`           int,
    cast_id           int,
    movie_id          int,
    id_in_cast        int,
    foreign key (person_id) references persons (id),
    foreign key (character_name_id) references character_names (id),
    foreign key (movie_id) references movies (id)
);

create table crew
(
    id            int not null auto_increment primary key,
    person_id     int,
    gender_id     int,
    job_id        int,
    department_id int,
    movie_id      int,
    id_in_crew    int,
    foreign key (person_id) references persons (id),
    foreign key (job_id) references jobs (id),
    foreign key (department_id) references departments (id)
);

create table movie_genres
(
    id       int not null auto_increment primary key,
    movie_id int,
    genre_id int,
    constraint unique (movie_id, genre_id),
    foreign key (movie_id) references movies (id),
    foreign key (genre_id) references genres (id)
);

create table movie_production_companies
(
    id                    int not null auto_increment primary key,
    movie_id              int,
    production_company_id int,
    constraint unique (movie_id, production_company_id),
    foreign key (movie_id) references movies (id),
    foreign key (production_company_id) references production_companies (id)
);



create table movie_production_countries
(
    id         int not null auto_increment primary key,
    movie_id   int,
    country_id int,
    constraint unique (movie_id, country_id),
    foreign key (movie_id) references movies (id),
    foreign key (country_id) references countries (id)
);

create table movie_spoken_languages
(
    id          int not null auto_increment primary key,
    movie_id    int,
    language_id int,
    constraint unique (movie_id, language_id),
    foreign key (movie_id) references movies (id),
    foreign key (language_id) references languages (id)
);

create table keywords_genre_stats
(
    id                  int not null auto_increment primary key,
    genre_id            int,
    keyword_id          int,
    movies_in_genre_pct numeric(5, 2),
    in_tagline_pct      numeric(5, 2),
    in_overview_pct     numeric(5, 2),
    constraint unique (genre_id, keyword_id),
    foreign key (genre_id) references genres (id),
    foreign key (keyword_id) references keywords (id)
);

create view v_movies as
select m.id,
       t.title  as title,
       ot.title as original_title,
       m.original_language,
       m.popularity,
       m.release_date,
       m.tagline,
       m.overview,
       m.is_adult,
       m.budget_usd,
       m.revenue_usd,
       m.runtime_minutes,
       s.status,
       m.vote_avg,
       m.vote_cnt
from movies m
         left join titles t on m.title_id = t.id
         left join titles ot on m.original_title_id = ot.id
         left join statuses s on m.status_id = s.id;

create view v_persons as
select name_id, g.gender
from persons
         left join names on persons.name_id = names.id
         left join genders g on persons.gender_id = g.id;

create view v_cast as
select cast.id, n.name, cn.character_name, `order`, m.title as movie_title
from cast
         left join persons on cast.person_id = persons.id
         left join names n on persons.name_id = n.id
         left join character_names cn on cast.character_name_id = cn.id
         left join v_movies m on cast.movie_id = m.id;

create view v_crew as
select crew.id, n.name, j.job, d.department, m.title as movie_title
from crew
         left join persons on crew.person_id = persons.id
         left join names n on persons.name_id = n.id
         left join jobs j on crew.job_id = j.id
         left join departments d on crew.department_id = d.id
         left join v_movies m on crew.movie_id = m.id;

-- These are procedures meant for creating a specific table, whose purpose is ordering the strongest keywords per genre,
-- and then checking how common a keyword is in the  movies' descriptions for this genre
CREATE PROCEDURE BuildTempKeywordsRanks()
BEGIN
    -- Build a table where we have a rank for every keyword per genre
    -- We count how many times the keyword is given for a movie in a specific genre
    drop temporary table if exists genre_keywords_pct;
    create temporary table genre_keywords_pct
    (
        genre_id            int,
        keyword_id          int,
        movies_in_genre_pct numeric(5, 2),
        unique (genre_id, keyword_id)
    );
    create index genre_keywords_pct_genre_id on genre_keywords_pct (genre_id);
    create index genre_keywords_pct_keyword_id on genre_keywords_pct (keyword_id);

    insert into genre_keywords_pct(genre_id, keyword_id, movies_in_genre_pct)
    select genre_id,
           keyword_id,
           100 * a.cnt / b.cnt as movies_in_genre_pct
    from (
             select distinct mg.genre_id, mk.keyword_id, count(*) as cnt
             from movie_genres mg
                      join movie_keywords mk on mg.movie_id = mk.movie_id
             group by 1, 2
         ) a
             join (select genre_id, count(*) as cnt from movie_genres group by 1) b
                  using (genre_id);
END;

CREATE PROCEDURE BuildKeywordsMatchingMovies()
BEGIN
    DECLARE bDone INT;
    DECLARE cnt INT;
    DECLARE keyword_id INT;
    DECLARE keyword VARCHAR(150);
    DECLARE quoted_keyword VARCHAR(152);

    DECLARE curs CURSOR FOR SELECT * from keywords;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET bDone = 1;

    -- Create temp table to store the keyword-to-movies fulltext match mapping
    DROP TEMPORARY TABLE IF EXISTS keywords_in_descr;
    create temporary table keywords_in_descr
    (
        keyword_id  int,
        movie_id    int,
        in_tagline  tinyint,
        in_overview tinyint,
        unique (keyword_id, movie_id)
    );

    OPEN curs;

    SET bDone = 0;
    SET cnt = 0;

    -- Query our taglines and overviews for every keyword we have (using FULLTEXT).
    -- Save them in our temp table keywords_in_descr
    REPEAT
        FETCH curs INTO keyword_id, keyword;
        SET quoted_keyword = concat('"', keyword, '"');

        INSERT INTO keywords_in_descr
        select keyword_id,
               m.id,
               MATCH(tagline) AGAINST(keyword_id IN BOOLEAN MODE) > 0,
               MATCH(overview) AGAINST(keyword_id IN BOOLEAN MODE) > 0
        from movies m
        where MATCH(tagline, overview) AGAINST(keyword_id IN BOOLEAN MODE);
        SET cnt = cnt + 1;
    UNTIL bDone
        END REPEAT;
    CLOSE curs;
end;

CREATE PROCEDURE BuildKeywordsGenreStats()
BEGIN
    drop table if exists keywords_genre_stats_temp;
    create table keywords_genre_stats_temp like keywords_genre_stats;

    insert into keywords_genre_stats_temp(genre_id, keyword_id, movies_in_genre_pct, in_tagline_pct, in_overview_pct)
    select kr.genre_id,
           kr.keyword_id,
           kr.movies_in_genre_pct,
           100 * sum(kd.in_tagline) / b.genre_movies_cnt  as in_tagline_pct,
           100 * sum(kd.in_overview) / b.genre_movies_cnt as in_overview_pct
    from genre_keywords_pct kr
             join movie_genres mg
                  on kr.genre_id = mg.genre_id
             join keywords_in_descr kd
                  on kd.movie_id = mg.movie_id and kd.keyword_id = kr.keyword_id
             join (select genre_id, count(*) as genre_movies_cnt from movie_genres group by 1) b
                  on mg.genre_id = b.genre_id
    group by 1, 2, 3;

    insert into keywords_genre_stats_temp(genre_id, keyword_id, movies_in_genre_pct, in_tagline_pct, in_overview_pct)
    select genre_id, keyword_id, movies_in_genre_pct, 0, 0
    from genre_keywords_pct kr
    where not exists(select 1
                     from keywords_genre_stats_temp temp
                     where kr.genre_id = temp.genre_id
                       and kr.keyword_id = temp.keyword_id);


end;

CREATE PROCEDURE PrepareMatchingKeywords()
BEGIN
    -- Find matching movies based on keywords and movie overviews - temp table keywords_in_descr
    call BuildKeywordsMatchingMovies();

    -- Build genre_keywords_pct temp table
    select '2';
    call BuildTempKeywordsRanks();

    -- create new temp table to store the final results of our query in table keywords_genre_stats_temp
    select '3';
    call BuildKeywordsGenreStats();

    -- Atomically swap between production table and our newly calculated table
    RENAME TABLE keywords_genre_stats TO keywords_genre_stats_delete, keywords_genre_stats_temp To keywords_genre_stats;
    drop table keywords_genre_stats_delete;
END;

CREATE EVENT dailyKeywordsGenresStatsRefresh ON SCHEDULE EVERY 1 DAY DO BEGIN
    call PrepareMatchingKeywords();
END;