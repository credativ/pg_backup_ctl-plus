CREATE TABLE archive(
       id integer primary key,
       name text not null unique,
       directory text not null unique,
       compression    integer
);

CREATE TABLE connections(
       archive_id integer NOT NULL,
       type       text NOT NULL,
       dsn        text,
       pghost        text,
       pgport        integer,
       pguser        text,
       pgdatabase    text,
       PRIMARY KEY(archive_id, type),
       FOREIGN KEY(archive_id) REFERENCES archive(id) ON DELETE CASCADE
);

CREATE TABLE backup(
       id integer not null primary key,
       archive_id integer not null,
       xlogpos text not null,
       xlogposend text null,
       timeline integer not null,
       label text not null,
       fsentry text not null,
       started text,
       stopped text,
       pinned integer default 0,
       status text default 'in progress',
       systemid text not null,
       wal_segment_size int not null,
       used_profile int not null,
       pg_version_num int not null,
       FOREIGN KEY(archive_id) REFERENCES archive(id) ON DELETE CASCADE,
       FOREIGN KEY(used_profile) REFERENCES backup_profiles(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE INDEX backup_id_idx ON backup(id);
CREATE INDEX backup_archive_id_idx ON backup(archive_id);

CREATE TABLE backup_tablespaces(
       backup_id integer not null,
       spcoid integer null,
       spclocation text null,
       spcsize bigint not null,
       PRIMARY KEY(backup_id, spcoid),
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
INSERT INTO version VALUES(108, datetime('now'));

CREATE TABLE backup_profiles(
       id integer not null,
       name text not null,
       compress_type int not null,
       max_rate integer not null CHECK((max_rate BETWEEN 32 AND 1048576) OR (max_rate = 0)),
       label text,
       fast_checkpoint integer not null default false,
       include_wal integer not null default false,
       wait_for_wal integer not null default true,
       noverify_checksums integer not null default false,
       manifest boolean not null default false,
       manifest_checksums text not null default 'CRC32C',
       PRIMARY KEY(id)
);

CREATE UNIQUE INDEX backup_profiles_name_idx ON backup_profiles(name);

CREATE TABLE procs(
       pid integer not null PRIMARY KEY,
       archive_id integer not null default -1,
       type text not null CHECK(type IN ('launcher', 'streamer')),
       started text not null,
       state text not null default 'running',
       shm_key integer default NULL,
       shm_id  integer default NULL
);

CREATE UNIQUE INDEX procs_archive_id_type_idx ON procs(archive_id, type);

/* Default backup profile */
INSERT INTO backup_profiles
       (name,
        compress_type,
        max_rate,
        label,
        fast_checkpoint,
        include_wal,
        wait_for_wal,
        noverify_checksums)
VALUES
        ('default',
         0,
         0,
         'PG_BACKUP_CTL BASEBACKUP',
         0,
         0,
         1,
         0);

CREATE TABLE retention(
       id integer not null primary key,
       name text not null,
       created text not null
);

CREATE UNIQUE INDEX retention_name_uniq_idx ON retention(name);

CREATE TABLE retention_rules(
       id integer not null,
       type integer not null,
       value text not null,
       FOREIGN KEY(id) REFERENCES retention(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX retention_rules_id_type_uniq_idx ON retention_rules(id, type);
