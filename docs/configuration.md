Configuration
=============

DbJournal requires a standard database connection which should be set on an `.env` file (see .env syntax below) with these **required** entries related to the database being journaled:

| Configuration   | Description                |
| :-------------- | -------------------------: |
| [DB_HOST](#DB_HOST) | The database host (IP or hostname) |
| [DB_DATABASE](#DB_DATABASE) | Database name |
| [DB_USERNAME](#DB_USERNAME) | Username to access the database |
| [DB_PASSWORD](#DB_PASSWORD) | Password to access the database |
| [DB_DRIVER](#DB_DRIVER) | DB Driver name (see dbal connection below) |

And these are non-mandatory options to customize your DbJournal:

| Configuration   | Description                |
| :-------------- | -------------------------: |
| [DB_JOURNAL_TABLE](#DB_JOURNAL_TABLE) | How do you want the main internal Journal table to be named (you should be ok to use prefixes/schema here) |
| [DB_JOURNAL_CREATED_AT_COLUMN_NAME](#DB_JOURNAL_CREATED_AT_COLUMN_NAME) | The name of your DB's `created_at` columns  |
| [DB_JOURNAL_UPDATED_AT_COLUMN_NAME](#DB_JOURNAL_UPDATED_AT_COLUMN_NAME) | The name of your DB's `updated_at` columns |
| [DB_JOURNAL_TABLES_FILTER](#DB_JOURNAL_TABLES_FILTER) | To restrict the Journal operations to one or more tables, set them separated by comma (`,`) |
| [DB_JOURNAL_FILE](#DB_JOURNAL_FILE) | How the main Journal dump file should be named (defaults to `journal.dump`) |
| [DB_JOURNAL_DIR](#DB_JOURNAL_DIR) | Directory where the main Journal dump file should be named (defaults to `/var/journal`) |


See [.env syntax](https://symfony.com/doc/current/components/dotenv.html) and [doctrine/dbal connection](https://www.doctrine-project.org/projects/doctrine-dbal/en/2.9/reference/configuration.html)
