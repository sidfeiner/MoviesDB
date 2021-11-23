create schema movies;

create table movies.names
(
    id   int not null auto_increment primary key,
    name varchar(50),
    constraint unique (name)
);

create table movies.keywords
(
    id      int primary key,
    keyword varchar(50),
    constraint unique (keyword)
);

create table movies.character_names
(
    id             int not null auto_increment primary key,
    character_name varchar(150),
    constraint unique (character_name)
);


create table movies.jobs
(
    id  int not null auto_increment primary key,
    job varchar(50),
    constraint unique (job)
);

create table movies.departments
(
    id         int not null auto_increment primary key,
    department varchar(50),
    constraint unique (department)
);


create table movies.genres
(
    id    int primary key,
    genre varchar(20),
    constraint unique (genre)
);


create table movies.titles
(
    id    int not null auto_increment primary key,
    title varchar(200),
    constraint unique (title)
);

create table movies.production_companies
(
    id                 int primary key,
    production_company varchar(200),
    constraint unique (production_company)
);


create table movies.countries
(
    id                      int not null auto_increment primary key,
    country_name            varchar(50),
    country_name_iso_3166_1 varchar(2),
    constraint unique (country_name, country_name_iso_3166_1)
);


create table movies.languages
(
    id                 int not null auto_increment primary key,
    language           varchar(20),
    language_iso_639_1 varchar(2),
    constraint unique (language),
    constraint unique (language_iso_639_1)
);


create table movies.statuses
(
    id     int not null auto_increment primary key,
    status varchar(15),
    constraint unique (status)
);


create table movies.movies
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
    revenue_dollars   bigint,
    runtime_minutes   int,
    status_id         int,
    vote_avg          decimal(4, 2),
    vote_cnt          int,
    foreign key (title_id) references titles (id),
    foreign key (original_title_id) references titles (id),
    foreign key (status_id) references statuses (id)
);

create table movies.movie_keywords
(
    id         int not null auto_increment primary key,
    movie_id   int,
    keyword_id int,
    constraint unique (movie_id, keyword_id),
    foreign key (movie_id) references movies (id),
    foreign key (keyword_id) references keywords (id)
);



create table movies.cast
(
    id                int not null auto_increment primary key,
    name_id           int,
    character_name_id int,
    gender            int,
    `order`           int,
    cast_id           int,
    movie_id          int,
    id_in_cast        int,
    foreign key (name_id) references names (id),
    foreign key (character_name_id) references character_names (id),
    foreign key (movie_id) references movies (id)
);

create table movies.crew
(
    id            int not null auto_increment primary key,
    name_id       int,
    gender        int,
    job_id        int,
    department_id int,
    movie_id      int,
    id_in_crew    int,
    foreign key (name_id) references names (id),
    foreign key (job_id) references jobs (id),
    foreign key (department_id) references departments (id)
);


create table movies.movie_genres
(
    id       int not null auto_increment primary key,
    movie_id int,
    genre_id int,
    constraint unique (movie_id, genre_id),
    foreign key (movie_id) references movies (id),
    foreign key (genre_id) references genres (id)
);


create table movies.movie_production_companies
(
    id                    int not null auto_increment primary key,
    movie_id              int,
    production_company_id int,
    constraint unique (movie_id, production_company_id),
    foreign key (movie_id) references movies (id),
    foreign key (production_company_id) references production_companies (id)
);



create table movies.movie_production_countries
(
    id         int not null auto_increment primary key,
    movie_id   int,
    country_id int,
    constraint unique (movie_id, country_id),
    foreign key (movie_id) references movies (id),
    foreign key (country_id) references countries (id)
);

create table movies.movie_spoken_languages
(
    id          int not null auto_increment primary key,
    movie_id    int,
    language_id int,
    constraint unique (movie_id, language_id),
    foreign key (movie_id) references movies (id),
    foreign key (language_id) references languages (id)
);