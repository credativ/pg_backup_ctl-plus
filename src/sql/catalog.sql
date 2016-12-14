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
       archive_id integer not null,
       history_filename text UNIQUE,
       label text,
       started text,
       stopped text,
       pinned integer,
       status text,
       FOREIGN KEY(archive_id) REFERENCES archive(id)
);

CREATE TABLE stream(
       id integer primary key not null,
       archive_id integer not null,
       stype integer not null,
       slot_name text,
       systemid text not null,
       timeline integer not null,
       xlogpos text     not null,
       dbname  text     not null,
       status text      not null,
       create_date text not null
);

CREATE INDEX backup_id_idx ON backup(id);
CREATE INDEX backup_archive_id_idx ON backup(archive_id);
CREATE INDEX stream_archive_id_idx ON stream(archive_id);
CREATE UNIQUE INDEX stream_archive_id_stype_idx ON stream(archive_id, stype);
