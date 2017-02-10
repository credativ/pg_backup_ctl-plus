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
       id integer not null primary key,
       archive_id integer not null,
       xlogpos text not null,
       timeline integer not null,
       label text not null,
       fsentry text not null,
       started text,
       stopped text,
       pinned integer default 0,
       status text default 'in progress',
       FOREIGN KEY(archive_id) REFERENCES archive(id) ON DELETE CASCADE
);

CREATE INDEX backup_id_idx ON backup(id);
CREATE INDEX backup_archive_id_idx ON backup(archive_id);

CREATE TABLE backup_tablespaces(
       id integer not null,
       backup_id integer not null,
       spcoid integer null,
       spclocation text null,
       spcsize bigint not null,
       PRIMARY KEY(id, backup_id),
       FOREIGN KEY(backup_id) REFERENCES backup(id) ON DELETE CASCADE
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
       create_date text not null,
       FOREIGN KEY(archive_id) REFERENCES archive(id) ON DELETE CASCADE
);

CREATE INDEX stream_archive_id_idx ON stream(archive_id);
CREATE UNIQUE INDEX stream_archive_id_stype_idx ON stream(archive_id, stype);

CREATE TABLE version(
       number integer not null,
       create_date text not null);

/* NOTE: version number must match CATALOG_MAGIC from include/catalog/catalog.hxx */
INSERT INTO version VALUES(0x1000, datetime('now'));

CREATE TABLE backup_profiles(
       id integer not null,
       name text not null,
       compress_type int not null,
       max_rate integer not null CHECK((max_rate BETWEEN 32 AND 1048576) OR (max_rate = 0)),
       label text,
       fast_checkpoint integer not null default false,
       include_wal integer not null default false,
       wait_for_wal integer not null default true,
       PRIMARY KEY(id)
);

CREATE UNIQUE INDEX backup_profiles_name_idx ON backup_profiles(name);

/* Default backup profile */
INSERT INTO backup_profiles
       (name,
        compress_type,
        max_rate,
        label,
        fast_checkpoint,
        include_wal,
        wait_for_wal)
VALUES
        ('default',
         0,
         0,
         'PG_BACKUP_CTL BASEBACKUP',
         0,
         0,
         1);

