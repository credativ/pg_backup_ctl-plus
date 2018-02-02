pg_backup_ctl++ command reference
*********************************

ALTER ARCHIVE
=============

Syntax::

  ALTER ARCHIVE <identifier> SET
     { DSN="<database connection parameters>"
       | PGHOST=<hostname> [PGDATABASE=<database name>] [PGUSER=<username>] [PGPORT=<port number>] }

``ALTER ARCHIVE`` can be used to change the parameters of an existing
archive. Either ``DSN`` or the special ``PGHOST`` syntax can be used.


.. note::
   When the ``DSN`` syntax is used, you need to specify all required database
   connection parameters, since ``ALTER ARCHIVE`` replaces the whole connection
   string with the new one. There is no merge with the existing DSN parameters.

Example::

  ALTER ARCHIVE pg10 SET DSN="dbname=foo hostname=localhost user=postgres";

CREATE ARCHIVE
==============

Syntax::

   CREATE ARCHIVE <identifier> PARAMS DIRECTORY="<path>"
     { DSN="<database connection parameters>"
       | PGHOST=<hostname> [PGDATABASE=<database>] [PGUSER=<username>] [PGPORT=<port>] }

The ``CREATE ARCHIVE`` command creates a PostgreSQL archive
and initializes the specified ``DIRECTORY`` for storing basebackups
and transaction logs.

Either ``DSN`` syntax or ``PGHOST`` with its following
keywords ``PGDATABASE``, ``PGUSER`` and ``PGPORT`` can be used. The latter
is usable if you don't have to specify any fancy connection parameters, but
for common usage the ``DSN`` syntax is encouraged.

.. note::

   ``CREATE ARCHIVE`` creates a database connection entry of type ``basebackup``
   implicitely. This can be viewed by using the ``LIST CONNECTION FOR ARCHIVE`` command.

Example::

  CREATE ARCHIVE pg10 PARAMS DIRECTORY="/srv/pgarchive/10"
    DSN="hostname=localhost port=5455 user=bernd";

CREATE RETENTION POLICY
=======================

.. note::

   Not yet implemented

Syntax::

  CREATE RETENTION POLICY <identifier>
     { [KEEP <number of basebackups>]
       | [DROP] }

CREATE BACKUP PROFILE
=====================

Syntax::

  CREATE BACKUP PROFILE <identifier>
    [CHECKPOINT { DELAYED|FAST }]
    [COMPRESSION { GZIP|NONE|ZSTD }]
    [LABEL "<label string>"]
    [MAX_RATE <bytes per second>]
    [WAIT_FOR_WAL { TRUE|FALSE }]
    [WAL { EXCLUDED|INCLUDED }]

A backup profile is basically as set of configuration options on how
to perform basebackups. The PostgreSQL streaming protocol for basebackups
allows several settings to configure a specific profile for streamed
backups, which can be created with the ``CREATE BACKUP PROFILE`` command.
The specific options are:

+------------+----------+------------------------------------------------------------+----------+
|Parameter   | Value    | Description                                                | Default  |
+============+==========+============================================================+==========+
|CHECKPOINT  | DELAYED  | Uses delayed checkpoint, backup start might be delayed     |          |
|            +----------+------------------------------------------------------------+ DELAYED  |
|            | FAST     | Use immediate checkpoint, can cause I/O during backup start|          |
+------------+----------+------------------------------------------------------------+----------+
|COMPRESSION | GZIP     | Basebackups are compressed with gzip                       |          |
|            +----------+------------------------------------------------------------+          |
|            | ZSTD     | Basebackups are compressed with zstd                       | NONE     |
|            +----------+------------------------------------------------------------+          |
|            | NONE     | No compression used (TAR)                                  |          |
+------------+----------+------------------------------------------------------------+----------+
|WAIT_FOR_WAL| TRUE     | Wait until all required WAL files are archived             |          |
|            +----------+------------------------------------------------------------+ TRUE     |
|            | FALSE    | Don't wait for required WAL files to be archived           |          |
+------------+----------+------------------------------------------------------------+----------+
|WAL         | INCLUDED | Include WAL in basebackup                                  |          |
|            +----------+------------------------------------------------------------+ EXCLUDED |
|            | EXCLUDED | No WALs in basebackup included                             |          |
+------------+----------+------------------------------------------------------------+----------+
| MAX_RATE   | xx Bytes | If set, number of bytes for requested throughput           | 0 (off)  |
+------------+----------+------------------------------------------------------------+----------+
| LABEL      | String   | Backup label string, default is PG_BCK_CTL BASEBACKUP      |          |
+------------+----------+------------------------------------------------------------+----------+

LIST ARCHIVE
============

Syntax::

  LIST ARCHIVE [<identifier>]

The ``LIST ARCHIVE`` command lists the archives created in the
current backup catalog. If an archive identifier is specified, the
details of this specific archive are displayed only.

Examples::

  LIST ARCHIVE;

  LIST ARCHIVE pg10;

LIST BACKUP PROFILE
===================

Syntax::

  LIST BACKUP PROFILE [<identifier>]

Lists all created backup profile or the details of the specified backup profile
if ``<identifier>`` was given.

Examples::

  LIST BACKUP PROFILE;

  LIST BACKUP PROFILE my_profile;

DROP ARCHIVE
============

Syntax::

  DROP ARCHIVE <identifier>

Drops the specified archive from the current catalog.

.. note::
  This does not delete the physical files and directories from the storage. All entries
  from the catalog are purged, but the backup itself won't be destroyed. You'll need to
  cleanup the directory yourself.

.. warning::
  There is currently no code to ensure that there is no background workers (e.g. streaming)
  running for an archive, which is about being dropped.

START BASEBACKUP FOR ARCHIVE
============================

Syntax::

  START BASEBACKUP FOR ARCHIVE <identifier> [PROFILE <identifier>]

Starts a basebackup in the archive recognized by ``<identifier>``, using
the backup profile ``<identifier>``. If ``PROFILE`` is omitted, the
``default`` backup profile will be used.

Example::

  START BASEBACKUP FOR ARCHIVE pg10;

START STREAMING FOR ARCHIVE
===========================

Syntax::

  START STREAMING FOR ARCHIVE <identifier> [RESTART] [NODETACH]

Starts a streaming process to stream all WAL files with the specified
archive recognized by ``<identifier>``. Per default, this will start the streaming
process in detached mode by using a background worker process. If ``RESTART``
was specified, the streaming process will start at the WAL location reported
by the PostgreSQL instance defined in the archive. If ``NODETACH`` is used, the
streaming process won't detach from the interactive shell and block as long
as the command is interrupted (e.g. Strg+C).

Examples::

  START STREAMING FOR ARCHIVE pg10;

  START STREAMING FOR ARCHIVE pg10 RESTART;

  START STREAMING FOR ARCHIVE pg10 NODETACH;

  START STREAMING FOR ARCHIVE pg10 RESTART NODETACH;
