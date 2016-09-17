CREATE TABLE archive(
       id integer primary key,
       directory text UNIQUE,
       compression    integer,
       pghost        text,
       pgport        integer,
       pguser        text
);

CREATE TABLE backup(
       archive_id integer primary key,
       label text,
       started text,
       stopped text,
       pinned integer
);
