How it works
============

Instead of working in an event level, which requires complex operations handling and would have limited DBMS' portability, this tool relies on `created_at` and `updated_at` columns to determine the INSERT or UPDATE query necessary to achieve that result.

This method will cover an overwhelming majority of the cases as having `created/updated_at` like columns on every table that has dynamic content is an universal standard and can easily be implemented when needed. There's also an [option](configuration.md) to use your own naming convention to specify these columns.

Each time the `update` command runs, the journal will be updated to include the necessary queries to reflect each row's (that were inserted or updated since the last time the journal was updated) values. 

To achieve that, DbJournal will simply look for rows that have the `created/updated_at` timestamps after the last time it ran and create INSERT queries for rows that were created and UPDATE queries for rows that were updated since the last time, ensuring no operation will be left behind.
