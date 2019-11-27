Commands
========

In the project's root directory you can run console commands like this:

```bash
php bin/console db-journal {action} [--option1=value] [--option2=value]
```

This will run a PHP script where `{action}` can be any of these commands:

| Action   | Description                | Options         |
| --------- |:-------------| -----:|
| [setup](#setup) | Create the internal table required to run the journal | - |
| [init](#init) | Output the journal queries for the given filters      |   `time` |
| [update](#update) | Ensure that every table journal will be updated to the current database timestamp (this should be the command called through a _cron job_) |    `time` |
| [dump](#dump) | Output the journal queries filtering by the given options | `table` `mintime` `maxtime` |
| [run](#run) | [WARNING] Apply the given Journal to the current database  | `table` `mintime` `maxtime` |
| [list](#list) | List the available DbJournal commands |    - |
| [schema](#schema) | Dump the database structure like<br/>`['Table' => ['column1' => $column1Object, 'column2' => $column2Object, ...], ...]` | - |
| [time](#time) | Show the current database time | - |
| [clean](#clean) | Clean the existing journal records and files (warning, you won't be able to run pre-existing journals after this) | - |
| [uninstall](#uninstall) | Remove DbJournal table and files | - |


You can also use the following options (always preceded by `--`) combined with the above commands:

* `v`, `vv` or `vvv`: Verbose level (`vvv` being the most verbose) for any command
* `time`: set the current time for the `init` or `update` command (so they'll run with the inputted time)
* `table`: restrict the `dump` or `run` command to a single table
* `mintime` | `gt`: use it with the `dump` command to restrict the dump to operations that happened after that timestamp (format YYYY-mm-dd)
* `maxtime` | `lt`: use it with the `dump` command to restrict the dump to operations that happened before that timestamp (format YYYY-mm-dd)
