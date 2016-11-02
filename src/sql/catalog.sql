CREATE TABLE archive(
       id integer primary key,
       name text not null unique,
       directory text not null unique,
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
       pinned integer,
       FOREIGN KEY(archive_id) REFERENCES archive(id)
);

CREATE INDEX backup_id_idx ON backup(id);
CREATE INDEX backup_archive_id_idx ON backup(archive_id);
