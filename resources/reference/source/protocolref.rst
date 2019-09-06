pg_backup_ctl++ extensions to the PostgreSQL streaming API
**********************************************************

A ``pg_backup_ctl++`` recovery instance can be started for a specific
archive, see the ``START RECOVERY STREAM`` command syntax for details.

Recovery instances are either bound to an archive or even a basebackup.
Since a recovery instance understands the PostgreSQL streaming protocol,
any compatible tool can be used to e.g. stream a basebackup from it
(``pg_basebackup``, ...).

The following will discuss additional commands understood by
``pg_backup_ctl++`` own tools, which are added to the PostgreSQL
streaming API.

LIST_BASEBACKUPS
================

The ``LIST_BASEBACKUPS`` streaming API command returns a list of
currently materialized basebackups in the connected archive. You
can retrieve the list easily with ``psql`` for example:

Example::

  bernd@siptah {bernd} (0)/0  HEAD% PGSSLMODE=disable  psql "replication=database user=bernd port=5667 dbname=catalog"
  NOTICE:  streaming API commands disabled
  Timing is on.
  Pager is used for long output.
  Null display is "__NULL__".
  Line style is unicode.
  psql (13devel, server pg_backup_ctl++, version 0.1)
  Type "help" for help.
  
  bernd@localhost:catalog :1 >= LIST_BASEBACKUPS;
   id │                            fsentry                            
  ────┼───────────────────────────────────────────────────────────────
   22 │ /home/bernd/tmp/pg-backup/11/base/streambackup-20190815213500
   21 │ /home/bernd/tmp/pg-backup/11/base/streambackup-20190711152752
  (2 rows)

