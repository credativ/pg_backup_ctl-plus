CREATE TABLE archive(
       id integer primary key,
       directory text UNIQUE,
       compression    integer,
       pghost        text,
       pgport        integer,
       pguser        text,
       pgdatabase    text
);

CREATE TABLE backup(
       id integer primary key,
       archive_id integer,
       history_filename text UNIQUE,
       label text,
       started text,
       stopped text,
       pinned integer
);
