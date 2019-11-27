Setup
=====

To have DbJournal up & running all you need to do is to create the main journal table, insert the records per-table and run the update command (ideally with a _cron job_) whenever you want it to be updated until the current time.

Here's how to do it:

#### 1) Create DB structure
```
php bin/console db-journal setup
```
Will create the main Journal table  (see conf `DB_JOURNAL_TABLE`)

#### 2) Create the records (per table being _journaled_)
```
php bin/console db-journal init
```
Will create one record per table to be journaled with a timestamp to control the last time the journal was updated for that table

#### 3) Run the Journal  
```
php bin/console db-journal update
```
Will run the journal for every table (record) on the main Journal table, meaning each of those tables will have the SQL queries created for operations ran between the `last_journal` and the current time, so this is the command to be called regularly through a cron job. 
