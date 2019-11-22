# db-journal
General purpose Database Journaling Tool based on doctrine/dbal

`! This document is under construction !`

### Version
This is an Alpha version and shouldn't be used in any kind of production environment.

### Configuration
You can set your configuration on an `.env` file. These are DbJournal's configurations (docs coming soon):
```
DB_HOST=
DB_DATABASE=
DB_USERNAME=
DB_PASSWORD=
DB_DRIVER=
DB_JOURNAL_TABLE=
DB_JOURNAL_CREATED_AT_NAME=
DB_JOURNAL_UPDATED_AT_NAME=
DB_JOURNAL_TABLES_FILTER=
```

### Commands

In the project's root directory you can run console like this:

```
php bin/console db-journal {action}

```

Where `{action}` can be any of these commands:

```
   setup                   Create the internal table required to run the journal
   init                    Create the initial records on the main journal table
   update                  Ensure that every table journal will be updated to the current database timestamp
   dump                    Output the journal queries for the given filters
   run                     [WARNING] Apply the given Journal to the current database
   list                    List the available DbJournal commands
   schema                  Dump the database structure: ['Table' => ['column1' => $column1Object, 'column2' => $column2Object, ...], ...]
   time                    Show the current database time
   clean                   Clean the existing journal records and files (warning, you won't be able to run pre-existing journals after this)
   uninstall               Remove DbJournal table and files
```

### Setup

1) `setup`
2) `ini`
3) `update`

