create table movies.names
(
    id   serial primary key,
    name varchar(50),
    constraint unique (name)
);

create table movies.keywords
(
    id      int primary key,
    keyword varchar(50),
    constraint unique (keyword)
);

create table movies.movie_keywords
(
    id         serial primary key,
    movie_id   int references movies (id),
    keyword_id int references keywords (id),
    constraint unique (movie_id, keyword_id)
);

create table movies.character_names
(
    id             serial primary key,
    character_name varchar(50),
    constraint unique (character_name)
);

create table movies.jobs
(
    id  serial primary key,
    job varchar(50),
    constraint unique (job)
);

create table movies.departments
(
    id         serial primary key,
    department varchar(50),
    constraint unique (department)
);


create table movies.cast
(
    id                serial primary key,
    name_id           int references names (id),
    character_name_id int references character_names (id),
    gender            int,
    `order`           int,
    cast_id           int,
    movie_id          int references movies (id),
    id_in_cast        int
);

create table movies.crew
(
    id            serial primary key,
    name_id       int references names (id),
    gender        int,
    job_id        int references jobs (id),
    department_id int references departments (id),
    movie_id      int,
    id_in_crew    int
);

create table movies.genres
(
    id    int primary key,
    genre varchar(20),
    constraint unique (genre)
);

create table movies.movie_genres
(
    id       serial primary key,
    movie_id int references movies (id),
    genre_id int references genres (id),
    constraint unique (movie_id, genre_id)
);

create table movies.titles
(
    id    serial primary key,
    title varchar(50),
    constraint unique (title)
);

create table movies.production_companies
(
    id                 int primary key,
    production_company varchar(50),
    constraint unique (production_company)
);

create table movies.movie_production_companies
(
    id                    serial primary key,
    movie_id              int references movies (id),
    production_company_id int references production_companies (id),
    constraint unique (movie_id, production_company_id)
);


create table movies.countries
(
    id                      int primary key,
    country_name            varchar(50),
    country_name_iso_3166_1 varchar(2),
    constraint unique (country_name, country_name_iso_3166_1)

);

create table movies.statuses
(
    id     serial primary key,
    status varchar(15),
    constraint unique (status)
);

create table movies.movie_production_countries
(
    id         serial primary key,
    movie_id   int references movies (id),
    country_id int references countries (id),
    constraint unique (movie_id, country_id)
);

create table movies.languages
(
    id                 serial primary key,
    language           varchar(20),
    language_iso_639_1 varchar(2),
    constraint unique (language),
    constraint unique (language_iso_639_1)
);

create table movies.movie_spoken_languages
(
    id          serial primary key,
    movie_id    int references movies (id),
    language_id int references languages (id),
    constraint unique (movie_id, language_id)
);

create table movies.movies
(
    id                int primary key,
    title_id          int references titles (id),
    original_title_id int references titles (id),
    original_language varchar(2),
    popularity        decimal(9, 3),
    release_date      date,
    tagline           varchar(500),
    adult             bool,
    overview          varchar(500),
    budget_usd        int,
    revenue_dollars   int,
    runtime_minutes   int,
    status_id         int references statuses (id),
    vote_avg          decimal(4, 2),
    vote_cnt          int
);