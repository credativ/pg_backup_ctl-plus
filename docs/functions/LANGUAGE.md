ALTER ARCHIVE <name> (opts)
	opts are:
		directory
		hostname
		database
		username
		portnumber

CREATE [ ARCHIVE | BACKUP PROFILE ] <name> command

CREATE ARCHIVE <name> (options)
	Creates an archive named 'name'.

	Options are:
		TODO - mandatory
		TODO

CREATE BACKUP PROFILE <name>

DROP [ ARCHIVE | BACKUP PROFILE ] <name>

DROP ARCHIVE
	TODO

DROP BACKUP PROFILE
	TODO

LIST [ ARCHIVE | BACKUP PROFILE ]

LIST ARCHIVE
	TODO

LIST BACKUP PROFILE
	TODO

START BASEBACKUP FOR ARCHIVE <name> [ USING PROFILE <profile> ] [ BACKGROUND ]
	Main Backuproutine.

VERIFY ARCHIVE <name> command
	Performs a number of sanity- and accesschecks on an archive.

