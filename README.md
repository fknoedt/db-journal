# DbJournal

**General purpose Database Journaling Tool based on doctrine/dbal**

### Introduction

How much user data would you lose if any of your production databases crashes? Most likely everything since the last backup?

What if you need to synchronize user data created and updated, on specific tables during a given period of time, between an UAT and a production server? 

**DbJournal** allows you to have a journal - SQL dump file - with all these _INSERT_ and _UPDATE_ (_DELETE_ coming soon) transactions ready to run to quickly shift your databases to the state you need.   

### How it works

Instead of working in an event level, which requires complex operations handling and would have limited DBMS' portability, this tool relies on `created_at` and `updated_at` columns to determine each INSERT and UPDATE command necessary to achieve that result.

This means that it doesn't matter how many times a row has been updated since the last Journal. When you run `update` it will generate the necessary UPDATE (SQL) command to set the row (matched by it's Primary Key) with the exact same values it had at that moment.

INSERT (SQL) queries will be generated for rows that were created since the last `update` command.     

### Version

This is an Alpha version and shouldn't be used in any kind of production environment.

Beta version to be released in December 2019 while the release `1.0` version is scheduled to January 2020.   

### Configuration

DbJournal requires a standard database connection which should be set on an `.env` file (using basic ) with these **required** entries related to the database being journaled:

`DB_HOST`: The database host (IP or hostname)

`DB_DATABASE`: Database name

`DB_USERNAME`: Username to access the database

`DB_PASSWORD`: Password to access the database

`DB_DRIVER`: DB Driver name (see dbal connection below)

And these are non-mandatory options to customize your DbJournal:

`DB_JOURNAL_TABLE`: How do you want the main internal Journal table to be named (you should be ok to use prefixes/schema here)

`DB_JOURNAL_CREATED_AT_NAME`: The name of your DB's `created_at` columns 

`DB_JOURNAL_UPDATED_AT_NAME`: The name of your DB's `updated_at` columns

`DB_JOURNAL_TABLES_FILTER`: To restrict the Journal operations to one or more tables, set them separated by comma (`,`) 

`DB_JOURNAL_FILE`: How the main Journal dump file should be named (defaults to `journal.dump`)  

`DB_JOURNAL_DIR`: Directory where the main Journal dump file should be named (defaults to `/var/journal`)

See [.env syntax](https://symfony.com/doc/current/components/dotenv.html) and [doctrine/dbal connection](https://www.doctrine-project.org/projects/doctrine-dbal/en/2.9/reference/configuration.html)


### Commands

In the project's root directory you can run console like this:

```
php bin/console db-journal {action}
```

Where `{action}` can be any of these commands:

 * `setup`: Create the internal table required to run the journal
 * `init`: Create the initial records on the main journal table (accepts `time`)
 * `update`: Ensure that every table journal will be updated to the current database timestamp (this should be the command called through a _cron job_)
 * `dump`: Output the journal queries for the given filters
 * `run`: [WARNING] Apply the given Journal to the current database
 * `list`: List the available DbJournal commands
 * `schema`: Dump the database structure like `['Table' => ['column1' => $column1Object, 'column2' => $column2Object, ...], ...]`
 * `time`: Show the current database time
 * `clean`: Clean the existing journal records and files (warning, you won't be able to run pre-existing journals after this)
 * `uninstall`: Remove DbJournal table and files

You can also use the following options (always preceded by `--`) combined with the above commands:

* `v`, `vv` or `vvv`: Verbose level (`vvv` being the most verbose) for any command
* `time`: set the current time for the `init` or `update` command (so they'll run with the inputted time)
* `table`: use it with the `dump` command to restrict the dump to a single table
* `mintime` | `gt`: use it with the `dump` command to restrict the dump to operations that happened after that timestamp (format YYYY-mm-dd)
* `maxtime` | `lt`: use it with the `dump` command to restrict the dump to operations that happened before that timestamp (format YYYY-mm-dd)

### Setup

To have DbJournal up & running all you need to do is to create the main journal table, insert the records per-table and run the update command (ideally with a _cron job_) whenever you want it to be updated until the current time.

Here's how to do it:

1) ```php bin/console db-journal setup```: will create the main Journal table  (see conf `DB_JOURNAL_TABLE`)

2) ```php bin/console db-journal init```: will create one record per table to be journaled with a timestamp to control the last time the journal was updated for that table

3) ```php bin/console db-journal update```: will run the journal for every table (record) on the main Journal table, meaning each of those tables will have the SQL queries created for operations ran between the `last_journal` and the current time, so this is the command to be called through a cron job. 




`! This document is under construction !`
