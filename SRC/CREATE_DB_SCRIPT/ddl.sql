create table names
(
    id   int primary key,
    name varchar(50),
    constraint unique (name)
);

create table characters
(
    id             int primary key,
    character_name varchar(50),
    constraint unique (character_name)
);

create table jobs
(
    id  int primary key,
    job varchar(50),
    constraint unique (job)
);

create table departments
(
    id         int primary key,
    department varchar(50),
    constraint unique (department)
);


create table cast
(
    id                int primary key,
    name_id           int references names (id),
    character_name_id int references characters (id),
    gender            int,
    movie_id          int references movies (id)
);

create table crew
(
    id            int primary key,
    name_id       int references names (id),
    gender        int,
    job_id        int references jobs (id),
    department_id int references departments (id)
);

create table genres
(
    id    int primary key,
    genre varchar(20),
    constraint unique (genre)
);

create table movie_genres
(
    id       serial primary key,
    movie_id int references movies (id),
    genre_id int references genres (id),
    constraint unique (movie_id, genre_id)
);

create table titles
(
    id    serial primary key,
    title varchar(50),
    constraint unique (title)
);

create table production_companies
(
    id   int primary key,
    name varchar(50),
    constraint unique (name)
);

create table movie_production_companies
(
    id                    serial primary key,
    movie_id              int references movies (id),
    production_company_id int references production_companies (id),
    constraint unique (movie_id, production_company_id)
);


create table countries
(
    id                      int primary key,
    country_name            varchar(50),
    country_name_iso_3166_1 varchar(2),
    constraint unique (country_name, country_name_iso_3166_1)

);

create table movie_production_countries
(
    id         serial primary key,
    movie_id   int references movies (id),
    country_id int references countries (id),
    constraint unique (movie_id, country_id)
);

create table languages
(
    id                 serial primary key,
    language           varchar(20),
    language_iso_639_1 varchar(2),
    constraint unique (language),
    constraint unique (language_iso_639_1)
);

create table movie_spoken_languages
(
    id          serial primary key,
    movie_id    int references movies (id),
    language_id int references languages (id),
    constraint unique (movie_id, language_id)
);

create table movies
(
    id                int,
    adult             bool,
    budget_usd        int,
    original_language varchar(2),
    original_title_id int references titles (id),
    overview          varchar(500),
    popularity        decimal(9, 3),
    release_date      date,
    revenue_dollars   int,
    runtime_minutes   int,
    status            varchar(15),
    tagline           varchar(500),
    title_id          int references titles (id),
    vote_avg          decimal(4, 2),
    vote_cnt          int
);
