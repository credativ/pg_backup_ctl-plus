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

APPLY RETENTION POLICY
======================

Syntax::

  APPLY RETENTION POLICY <identifier> TO ARCHIVE <identifier>

``APPLY RETENTION POLICY`` applies and executes the specified rules
contained in the retention policy to the specified archive.

Example::

  APPLY RETENTION POLICY dropwithlabel TO ARCHIVE pg10;

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
     {
       { KEEP | DROP }
           { +<number of basebackups>
             | WITH LABEL <regular expression> }
       | { KEEP NEWER THAN
           | DROP OLDER THAN } [ <nn> YEARS ] [ <nn> MONTHS ] [ <nn> DAYS ] [ <nn> HOURS ] [ <nn> MINUTES ]
       | CLEANUP
     }

The ``CREATE RETENTION POLICY`` command creates a retention policy
rule which can be applied to clean up backup archives. A retention policy
consists of a retention action ``KEEP`` or ``DROP`` and a rule specification which
describes what exactly the action should be doing. The following rule types are currently
implemented:

- ``<number of basebackups>``

  Defines the number of basebackups to drop or keep. A retention action
  with <number of basebackups> works as follows:

  * ``KEEP``

    If ``KEEP`` is specified the retention rules scans for the specified
    number of basebackups starting from the newest. If it reaches the number of basebackups
    to keep, all remaining (older) basebackups are selected for deletion. If the number
    if basebackups to keep aren't guaranteed, the retention rule will error out.

    The following example will always keep two current and valid basebackups in the archive and drop all
    other remaining:

    Example::

      CREATE RETENTION POLICY keeptwo KEEP +2;

  * ``DROP``

    If ``DROP`` is the retention action, the retention rule will scan the list
    of current basebackups from the oldest to newest until it reaches the number
    of basebackups to drop. If there is at least no current basebackup remaining, the
    retention policy will do nothing.

    Following example will drop the two oldest basebackup from the list, but only if
    at least one current basebackup is available

    Example::

      CREATE RETENTION POLICY droptwo DROP +2;

- ``CLEANUP``

  A ``CLEANUP`` rule can only specified with a ``DROP`` action and will delete
  all broken and invalid basebackup from the archive. A basebackup is considered invalid if
  the following conditions is met:

  * The basebackup has state ``aborted``-

    This usually means the basebackup was terminated without being finished successfully, either
    through connection problems, errors on the upstream server at al.

  There might be cases where a basebackup is stuck within state ``in progress``, either due to
  a crash of the backup process or other conditions. The ``CLEANUP`` rule currently doesn't do
  anything with those basebackups with that kind of state, but will print a hint, e.g.

  Example::

    "abort cleanup retention, since a basebackup is still in progress
     if this basebackup is broken somehow, you'll need to cleanup it manually"

  In this case you should investigate the current basebackup status and do a ``DROP BASEBACKUP``
  manually.

  There might also be problems accessing the filesystem structure when applying the retention policy.
  This could happen because an NFS share is currently not properly responding due to network problems
  or other issues. A missing physical structure on-disk will cause an error during a ``CLEANUP`` run, the
  basebackup will not be considered invalid, though ``LIST BASEBACKUPS`` will print a corresponding
  on-disk state. If the physical representation of the basebackups is permanently gone, you should
  drop the basebackup from the archive manually, again with ``DROP BASEBACKUP``.

- ``DROP OLDER THAN`` or ``DROP NEWER THAN``

- ``KEEP OLDER THAN`` or ``KEEP NEWER THAN``


CREATE STREAMING CONNECTION
===========================

Syntax::

  CREATE STREAMING CONNECTION FOR ARCHIVE <identifier>
     { DSN "<database connection string>"
       | PGHOST=<hostname>
         [PGDATABASE=<database name>]
         [PGUSER=<username>]
         [PGPORT=<port number>] }

The ``CREATE STREAMING CONNECTION`` command creates a dedicated
streaming connection for the specified archive. This connection is
used by a streaming worker exclusively. That way it is possible to define
connections which doesn't influence basebackups during high peaks
of WAL traffic.

.. note::

   The ``CREATE ARCHIVE`` command creates a connection which
   is used by basebackups and streaming workers. These connnections
   are of type ``basebackup`` and are managed via the
   various ``ARCHIVE`` commands. The ``LIST CONNECTIONS`` command
   will also display these connection types, but they cannot
   be dropped specifically.

Examples::

  CREATE STREAMING CONNECTION FOR ARCHIVE pg10
     DSN "host=localhost port=5433 dbname=bar user=foo";

CREATE BACKUP PROFILE
=====================

Syntax::

  CREATE BACKUP PROFILE <identifier>
    [CHECKPOINT { DELAYED|FAST }]
    [COMPRESSION { GZIP|NONE|ZSTD }]
    [LABEL "<label string>"]
    [MAX_RATE <KBytes per second>]
    [WAIT_FOR_WAL { TRUE|FALSE }]
    [WAL { EXCLUDED|INCLUDED }]
    [NOVERIFY { TRUE|FALSE }]
    [MANIFEST { INCLUDED [ WITH CHECKSUMS {NONE|CRC32C|SHA224|SHA256|SHA384|SHA512 } ]
                | EXCLUDED } ]

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
| MAX_RATE   | xx KBytes| If set, number of KBytes for requested throughput          | 0 (off)  |
+------------+----------+------------------------------------------------------------+----------+
| LABEL      | String   | Backup label string, default is PG_BCK_CTL BASEBACKUP      |          |
+------------+----------+------------------------------------------------------------+----------+
| NOVERIFY   | TRUE     | Do no check page checksums during backup                   | FALSE    |
|            +----------+------------------------------------------------------------+          |
|            | FALSE    | Leaves checksum checking during backup on                  |          |
+------------+----------+------------------------------------------------------------+----------+
| MANIFEST   | INCLUDED | Include backup manifest file                               |          |
|            +----------+------------------------------------------------------------+ EXCLUDED |
|            |            WITH CHECKSUMS {NONE|CRC32C|SHA224|SHA256|SHA384|SHA512}   |          |
|            +----------+------------------------------------------------------------+          |
|            | EXCLUDED | Omit backup manifest file                                  |          |
+------------+----------+------------------------------------------------------------+----------+
| MANIFEST_CHECKSUMS    | Specifies a string identifying the method to be used       | CRC32    |
|                       | to create file checksums used in the manifest file         |          |
+------------+----------+------------------------------------------------------------+----------+

.. note::

   Specific options aren't available in all supported PostgreSQL versions. If a backup profile
   is used (see the ``START BASEBACKUP`` command reference for details) with options not supported
   by the PostgreSQL version, the option will be ignored. This behavior should avoid having
   special profiles for all kind of different PostgreSQL versions.

.. note::

   The `MANIFEST` option can optionally specify the checksums used to validate
   the contents of a basebackup. The default (if `INCLUDED` is specified) is `CRC32C`, `NONE`
   turns checksums off. Per default, `MANIFEST` is `EXCLUDED`.

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

LIST BASEBACKUPS IN ARCHIVE
===========================

Syntax::

  LIST BASEBACKUPS IN ARCHIVE <identifier>

The ``LIST BASEBACKUPS`` command gives a list of
basebackups and its status in the specified archive.
Basebackups are always listed in descending order, sorted
by their creation date. Thus, the newest basebackup is the
first in the list.

Examples::

  LIST BASEBACKUPS IN ARCHIVE pg10;

  Basebackups in archive pg10
  --------------------------------------------------------------------------------
  Property       	Value                                                       
  --------------------------------------------------------------------------------
  ID             5                                                           
  Backup         	/srv/test/pgarchive/10/base/streambackup-20180306184203     
  Status         	ready                                                       
  Label          	PG_BCK_CTL BASEBACKUP                                       
  Started        	2018-03-06 18:42:03                                         
  tablespaces
  --------------------------------------------------------------------------------
  tablespace property 	value                                                       
  --------------------------------------------------------------------------------
  - oid                 16407                                                       
  - upstream location   /srv/test/pgdata/tablespaces/10.0                           
  - upstream size       9                                                           
  - oid                 32730                                                       
  - upstream location                                                               
  - upstream size       15407193                                                    
  Summary:
  Total size upstream:     	15046 MB                                
  Total local backup size: 	15045 MB


LIST CONNECTION FOR ARCHIVE
===========================

Syntax::

  LIST CONNECTION FOR ARCHIVE <identifier>

The ``LIST CONNECTION`` commands lists the connection
defined for the given archive. Currently there are two types
of connections: ``basebackup`` and ``streaming``. The latter
is created by the ``CREATE STREAMING CONNECTION`` command and
represents a database streaming connection dedicated to WAL streaming
connections. The ``basebackup`` connections are created via
``CREATE ARCHIVE`` and cannot be dropped without dropping
the archive itself. Basebackups always use the ``basebackup``
connection types, whereas streaming workers are using either
``basebackup`` connections or, if existing, dedicated
``streaming`` connections.

Examples::

  LIST CONNECTION FOR ARCHIVE pg10;

  List of connections for archive "pg10"
  connection type basebackup
  --------------------------------------------------------------------------------
  Attribute      	Setting
  --------------------------------------------------------------------------------
  DSN            	host=db_basebackup port=5455 user=bernd dbname=bernd
  PGHOST         	                                                            
  PGDATABASE     	                                                            
  PGUSER         	                                                            
  PGPORT         	0                                                           
  connection type streamer
  --------------------------------------------------------------------------------
  Attribute      	Setting                                                     
  --------------------------------------------------------------------------------
  DSN            	host=db_streamer port=5455 dbname=bernd user=bernd
  PGHOST         	                                                            
  PGDATABASE     	                                                            
  PGUSER         	                                                            
  PGPORT         	0                                                           
  LIST CONNECTION

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

DROP STREAMING CONNECTION
=========================

Syntax::

  DROP STREAMING CONNECTION FROM ARCHIVE <identifier>

Drops a ``streaming`` connection from the specified archive. If
a streaming worker is still running for the specified archive, it
won't be notified or interrupted, but a restart of the worker will
cause it to fall back to the ``basebackup`` connection.

PIN
===

Syntax::

  PIN { <basebackup ID> | OLDEST | NEWEST | +<COUNT> }
     IN ARCHIVE <identifier>

The ``PIN`` command creates a pin on the specified basebackups. To pin
a basebackup, ``PIN`` supports the following action modes:

* basebackup ID

  If specified a number to ``PIN``, this will be treated as the
  ID of a basebackup currently stored in the specified archive.
  If the ID isn't found, an error will raised. If the ID is already
  pinned, nothing will happen.

* OLDEST

  Pin the oldest basebackup in the specified archive.

* NEWEST

  Pin the newest basebackup in the specified archive.

* +COUNT

  If the specified number is prefixed with a ``+`` literal,
  the number is treated as the number of basebackups to pin. The basebackups
  are pinned in asccending order, where the basebackups are sorted by
  their creation date, starting with the newest. Thus, a argument of
  ``+2`` pins the two newest basebackups in the archive. ``PIN`` stops
  as soon as the end of the list is reached. If there aren't any basebackups
  to pin, this command is effectively a noop. If a basebackup was already pinned,
  it is treated as where it wasn't previously pinned, so it counts to the
  number of basebackup to be pinned.

.. note::

   If a basebackup was marked aborted or is still in progress, it won't
   be recognized for a pin action. If ``+COUNT`` was specified for example,
   such basebackups won't be part of ``COUNT``. The same applies to ``NEWEST``
   or ``OLDEST``, if an aborted or in-progress basebackup is either the newest
   or oldest basebackup, it will be ignored. Instead the next valid basebackup
   meeting the criteria is choosen. If there aren't any, an error will be
   raised.

RESTORE
=======

Syntax::

  RESTORE [FROM ARCHIVE] <identifier>
  BASEBACKUP { { CURRENT|NEWEST|LATEST|OLDEST } | <ID> }
  TO DIRECTORY="<directory>"
  TABLESPACE MAP { ALL="<directory>"
                   | <OID>="<directory>" [ .... ] }

To restore a basebackup locally to a directory, use the `RESTORE FROM ARCHIVE`
command. Currently the reserved keywords `CURRENT`, `LATEST` or `NEWEST` can be used
to specify the most recent basebackup to restore from the archive. To select
a specific basebackup to restore, specify it by its `ID`. The `TO DIRECTORY` defines
the target directory to restore the basebackup to. Please note that no whitespaces
between the `TO DIRECTORY`, the following `=` and `<directory>` string are allowed.
The `"` quotes are mandatory, too. The same rule applies to the `ALL` and `<OID`
properties of the `TABLESPACE MAP` directive.

The `TABLESPACE MAP` directory can be used to override the target directories
for specific tablespaces (specified by their OIDs which can be found in the output
of `LIST BASEBACKUPS ... VERBOSE`) or for every tablespaces contained in the backup.
The latter redirects all tablespaces into `<directory>`, created as subdirectories
there.

The default tablespace (aka as `PGDATA` or `pg_default`) can't be redirected,
multiple colliding specifications of tablespace redirections throw an error.

Example::

  RESTORE FROM ARCHIVE pg13 BASEBACKUP LATEST
     TO DIRECTORY="/srv/restore";

  RESTORE pg13 BASEBACKUP 3
     TO DIRECTORY="/srv/restore/pgdata-13"
     TABLESPACE MAP ALL="/srv/restore/tablespaces-13";

  RESTORE FROM ARCHIVE pg13 BASEBACKUP OLDEST
     TO DIRECTORY="/srv/restore/pgdata-13"
     TABLESPACE MAP 16788="/srv/restore/tablespaces-13/tblspc1"
                    18655="/srv/restore/tablespaces-13/tblspc2";

START BASEBACKUP FOR ARCHIVE
============================

Syntax::

  START BASEBACKUP FOR ARCHIVE <identifier> [PROFILE <identifier>] [FORCE_SYSTEMID_UPDATE]

Starts a basebackup in the archive recognized by ``<identifier>``, using
the backup profile ``<identifier>``. If ``PROFILE`` is omitted, the
``default`` backup profile will be used.

.. note::

   The ``FORCE_SYSTEMID_UPDATE`` option allows to stream a basebackup into
   a backup archive, which catalog already contains former basebackups with
   a different SYSTEMID. This usually means that the source database instance
   was freshly initialized and contains a new database cluster directory. pg_backup_ctl++
   usually refuses to stream basebackups with a new systemid if there are already existing
   basebackups with a mismatching SYSTEMID, but specifying the ``FORCE_SYSTEMID_UPDATE`` option
   allows to override this protection. Use with care!

Example::

  START BASEBACKUP FOR ARCHIVE pg10;

START RECOVERY STREAM
=======================

Syntax::

  START RECOVERY STREAM FOR ARCHIVE <identifier>
  [ PORT <port number> ] [ { LISTEN_ON <ip address } ]

This command starts a recovery background process attached to the
specified archive identified by ``<identifier>``. The recovery background
process can be used to recover a specified basebackup over the PostgreSQL
Streaming Replication protocol.

A recovery instance must be started for each archive to be able to stream
a basebackup. Currently only a single ip address can be used to bind
the instance to. The default is localhost, the default port the recovery instance
uses is 7432.

A recovery instance is a worker process of ``pg_backup_ctl++`` which can be
used to stream a basebackup over the PostgreSQL streaming protocol. Thus, it
is possible to just use the ``pg_basebackup`` tool to stream a basebackup
from the archive to any host where ``pg_basebackup`` is available.

Start a recovery instance listening on localhost ::1, port 7734

Example::

  START RECOVERY STREAM FOR ARCHIVE pg10 PORT 7734 LISTEN_ON(:::1);

Start a recovery instance listening on 192.168.122.34, default port 7432

Example::

  START RECOVERY STREAM FOR ARCHIVE pg10 LISTEN_ON(192.168.122.34);

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

STAT ARCHIVE
============

Syntax::

  STAT ARCHIVE <identifier> BASEBACKUP <ID>

The `STAT ARCHIVE` command allows to view the filesystem contents
of the basebackup as they are stored in the archive.

Examples::

  STAT ARCHIVE pg13 BASEBACKUP 3;

This shows the filesystem contents of the basebackup with
ID `3` as it is stored in the archive.

UNPIN
=====

Syntax::

  UNPIN { <basebackup ID> | OLDEST | NEWEST | PINNED | +<COUNT> }
     IN ARCHIVE <identifier>

The ``UNPIN`` command removes any pins on basebackup specified
by one of the following actions:

* basebackup ID

  When specified a basebackup ID, the specified ID is unpinned.
  The ``UNPIN`` command does not check, if the basebackup ID was
  pinned before. Though, if the ID cannot be found, an error
  occurs.

* OLDEST

  The ``OLDEST`` keyword references the oldest basebackup
  in the specified archive. If there is one, it will be unpinned.
  This action has no effect, if no basebackup is currently
  present, or if the the oldest basebackup is not pinned.

* NEWEST

  The ``NEWEST`` keyword lets the ``UNPIN`` command
  to unpin the newest basebackup in the specified archive. If no
  basebackup exists or the newest basebackup wasn't pinned, this
  is effectively a no op.

* +COUNT

  If the argument to UNPIN is a number, prefix by the ``+`` literal,
  then UNPIN treats this number as the number of basebackups to
  unpin. It will travers the list of basebackups down in descending
  order, whereas the list is sorted by creation date, newest first.
  It will stop, if ``COUNT`` number of basebackups are unpinned.
  ``UNPIN`` will stop, as soon as the end of list is reached.

* PINNED

  If ``PINNED`` is specified to the ``UNPIN`` command, all
  currently existing pins on all basebackups in the specified
  archive will be removed.

In general, if any basebackups referenced by one of the
specified actions is not yet pinned, ``UNPIN`` won't complain.

If the specified archive doesn't exist, ``UNPIN`` will throw
an error.

.. note::

  Aborted basebackups cannot be pinned, and ``UNPIN`` will
  ignore basebackups in such a state, too.

VERIFY ARCHIVE
==============

Syntax::

  VERIFY ARCHIVE <identifier> [CONNECTION]

Verify the archive structure. ``VERIFY ARCHIVE`` currently
checks wether the archive directory exists and is writable. To
perform this check, ``VERIFY ARCHIVE`` creates and writes a
file PG_BACKUP_CTL_MAGIC into the archive directory. If the
optional ``CONNECTION`` keyword is specified, the verification
includes wether any specified database server used by the
archive via ``basebackup`` or ``streaming`` connection types are
reachable.

Examples::

  VERIFY ARCHIVE pg10 CONNECTION;

