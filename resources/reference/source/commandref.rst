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
           { <number of basebackups>
             | WITH LABEL <regular expression> }
       | { KEEP NEWER THAN
           | DROP OLDER THAN } [ <nn> YEARS ] [ <nn> MONTHS ] [ <nn> DAYS ] [ <nn> HOURS ]
     }

The ``CREATE RETENTION POLICY`` command creates a retention policy
rule which can be applied to clean up backup archives.

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

