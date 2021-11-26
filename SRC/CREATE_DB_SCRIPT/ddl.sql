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
    adult             bool,
    overview          varchar(1000),
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
    fulltext key (overview)
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
    name_id           int,
    character_name_id int,
    gender_id         int,
    `order`           int,
    cast_id           int,
    movie_id          int,
    id_in_cast        int,
    foreign key (name_id) references names (id),
    foreign key (character_name_id) references character_names (id),
    foreign key (gender_id) references genders (id),
    foreign key (movie_id) references movies (id)
);

create table crew
(
    id            int not null auto_increment primary key,
    name_id       int,
    gender_id     int,
    job_id        int,
    department_id int,
    movie_id      int,
    id_in_crew    int,
    foreign key (name_id) references names (id),
    foreign key (gender_id) references genders (id),
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

create view v_movies as
select m.id,
       t.title  as title,
       ot.title as original_title,
       m.original_language,
       m.popularity,
       m.release_date,
       m.tagline,
       m.adult,
       m.overview,
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

create view v_cast as
select cast.id, n.name, cn.character_name, gender, `order`, m.title as movie_title
from cast
         left join names n on cast.name_id = n.id
         left join character_names cn on cast.character_name_id = cn.id
         left join genders g on cast.gender_id = g.id
         left join v_movies m on cast.movie_id = m.id;

create view v_crew as
select crew.id, n.name, gender, j.job, d.department, m.title as movie_title
from crew
         left join names n on crew.name_id = n.id
         left join genders g on crew.gender_id = g.id
         left join jobs j on crew.job_id = j.id
         left join departments d on crew.department_id = d.id
         left join v_movies m on crew.movie_id = m.id;