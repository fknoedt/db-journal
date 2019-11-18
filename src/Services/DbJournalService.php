<?php

namespace DbJournal\Services;

use DbJournal\Exceptions\DbJournalConfigException;
use DbJournal\Exceptions\DbJournalOutputException;
use DbJournal\Exceptions\DbJournalRuntimeException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\Schema;
use PHPUnit\Framework\MockObject\Exception;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Output\OutputInterface;

class DbJournalService
{
    /**
     * Human-readable Project Name
     */
    const PROJECT_NAME = 'DbJournal';

    /**
     * Repository Manager URL
     */
    const REPO_URL = 'https://github.com/fknoedt/db-journal';

    /**
     * Composer user/package name
     */
    const COMPOSER_PACKAGE = 'https://github.com/fknoedt/db-journal';

    /**
     * @var \Doctrine\DBAL\Connection
     */
    protected $conn;

    /**
     * @var \Symfony\Component\Console\Output\OutputInterface
     */
    protected $outputInterface;

    /**
     * single instance of a ProgressBar
     * @var \Symfony\Component\Console\Helper\ProgressBar
     */
    protected $progressBar;

    /**
     * Main journal table
     * @var string
     */
    protected $internalTable = 'db_journal';

    /**
     * Tables to journal
     * @var array
     */
    protected $tables;

    /**
     * A datetime column with this name on any table will allow journaling
     * @var
     */
    protected $createdAtColumnName = 'created_at';

    /**
     * A datetime column with this name on any table will allow journaling
     * @var
     */
    protected $updatedAtColumnName = 'updated_at';

    /**
     * Datetime format for input and stored values
     * @var string
     */
    CONST DB_DATETIME_FORMAT = 'Y-m-d H:i:s';

    /**
     * Date format for input and stored values
     * @var string
     */
    CONST DB_DATE_FORMAT = 'Y-m-d';

    /**
     * Execution time (start) with milliseconds
     * @var mixed
     */
    protected $appStartTime;

    /**
     * DbJournalService constructor.
     * This class is tight coupled with doctrine/dbal so no DI for the DB Connection
     * For testing, the database can be mocked with a .env.test file
     * @param $output -- OutputInterface
     * @param $ignoreTable -- do not check if the main db table exists
     * @throws DbJournalConfigException
     */
    public function __construct(OutputInterface $output=null, $ignoreTable = false)
    {
        // performance purpose
        $this->appStartTime = microtime();

        // let's start the output to be verbose
        $this->outputInterface = $output;

        // \Doctrine\DBAL\Connection
        $this->conn = DbalService::getConnection();

        $this->output('DB Connection OK', OutputInterface::VERBOSITY_VERBOSE);

        // load and set properties for .env confs
        $this->loadConfigs();

        $this->output('Configs loaded', OutputInterface::VERBOSITY_VERBOSE);

        // internalTable is defined within loadConfigs()
        if (! $this->internalTable) {
            throw new DbJournalConfigException("Table name not defined (DB_JOURNAL_TABLE) on .env");
        }

        // ensure the internal db-journal table is created (if not running setup)
        if (! DbalService::tableExists($this->internalTable) && ! $ignoreTable) {
            throw new DbJournalConfigException("Table `{$this->internalTable}` doesn't exist. Run `setup`.");
        }

        $this->output("{$this->internalTable} table " . ($ignoreTable ? 'ignored' : 'created'), OutputInterface::VERBOSITY_VERBOSE);
    }

    /**
     * Load and set properties for .env confs
     * @throws DbJournalConfigException
     */
    public function loadConfigs(): void
    {
        // main journal table name can be defined on the .env file to allow prefixes/schemas
        $this->internalTable = $_ENV['DB_JOURNAL_TABLE'] ?? $this->internalTable;

        // custom or default `created_at` column name
        $this->createdAtColumnName = $_ENV['DB_JOURNAL_CREATED_AT_NAME'] ?? $this->createdAtColumnName;

        // custom or default `updated_at` column name
        $this->updatedAtColumnName = $_ENV['DB_JOURNAL_UPDATED_AT_NAME'] ?? $this->updatedAtColumnName;

        if (empty($this->createdAtColumnName) || empty($this->updatedAtColumnName)) {
            throw new DbJournalConfigException("You cannot have an empty entry for DB_JOURNAL_CREATED_AT_NAME or DB_JOURNAL_UPDATED_AT_NAME on .env");
        }

        // custom tables to journal
        if ($conf = $_ENV['DB_JOURNAL_TABLES_FILTER'] ?? null) {

            $this->tables = explode([',',', ',';','; ',PHP_EOL], $conf);

            if (empty($tables)) {
                throw new DbJournalConfigException("Invalid .env DB_JOURNAL_TABLES_FILTER configuration. Enter valid table names separated by comma `,`");
            }

        }
        else {
            $this->tables = [];
        }
    }

    /**
     * Return the current DB Platform which extends AbstractPlatform
     * @return AbstractPlatform
     * @throws \Doctrine\DBAL\DBALException
     */
    public function getPlatform(): AbstractPlatform
    {
        return $this->conn->getDatabasePlatform();
    }

    /**
     * Return the current datetime (CURRENT_TIMESTAMP)
     * @throws \Doctrine\DBAL\DBALException
     * @return mixed
     */
    public function getDbCurrentTimestampSQL()
    {
        return $this->getPlatform()->getCurrentTimestampSQL();
    }

    /**
     * Return an array of table_name|last_journal|last_execution_time_miliseconds
     * @return array
     */
    public function getLastJournals(): array
    {
        return $this->conn->fetchAll("SELECT * FROM {$this->internalTable};");
    }

    /**
     * Return an array of table_name
     * @return array
     */
    public function getTablesLastJournal(): array
    {
        $tables = [];
        foreach ($this->getLastJournals() as $journal) {
            $tables[] = $journal['table_name'];
        }
        return $tables;
    }

    /**
     * Return the current database time
     * @throws \Doctrine\DBAL\DBALException
     * @return mixed
     */
    public function time()
    {
        // CURRENT_TIMESTAMP
        $time = $this->getDbCurrentTimestampSQL();
        return $this->conn->fetchColumn("SELECT {$time};");
    }

    /**
     * Truncate the main journal table
     * @return int
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Doctrine\DBAL\Exception\InvalidArgumentException
     */
    public function truncateJournalTable()
    {
        return $this->conn->delete($this->internalTable, ['1' => '1']);
    }

    /**
     * List and return the tables that have one or both created/updated_at fields
     * @return array
     */
    public function retrieveAbleTables(): array
    {
        $tables = [];

        // iterate on every table
        foreach (DbalService::getTablesColumnsMap() as $table => $columns) {

            foreach ($columns as $columnName => $column) {
                // if the table has one or both columns,
                if (in_array($columnName, [$this->createdAtColumnName, $this->updatedAtColumnName])) {
                    $tables[] = $table;
                    break;
                }
            }

        }

        return $tables;
    }

    /**
     * Create the internal table required to run the journal
     * @throws DbJournalConfigException
     * @throws \Doctrine\DBAL\DBALException
     */
    public function setup(): void
    {
        if (DbalService::tableExists($this->internalTable)) {
            throw new DbJournalConfigException("Internal table was already created (setup)");
        }

        $schema = new Schema();

        $table = $schema->createTable($this->internalTable);
        $table->setComment('Each table being monitored will have an entry on this table');

        $table->addColumn("table_name", "string", ["length" => 256]);
        $table->setPrimaryKey(array("table_name"));

        $table->addColumn("last_journal", "datetime", ["default" => 'CURRENT_TIMESTAMP']);

        $table->addColumn("last_execution_time_miliseconds", "float", ["notnull" => false]);

        // generate DDL for the table
        $queries = $schema->toSql($this->getPlatform());

        // and run it
        foreach ($queries as $query) {
            $this->output($query, OutputInterface::VERBOSITY_VERBOSE);
            $this->conn->exec($query);
        }

        $this->output("<success>Table {$this->internalTable} created</success>");
    }

    /**
     * Create the initial records on the main journal table
     * @param null $options
     * @throws \Doctrine\DBAL\DBALException
     * @throws DbJournalConfigException
     */
    public function init($options): void
    {
        // no tables defined on .env: scan tables
        if (empty($this->tables)) {

            $this->tables = $this->retrieveAbleTables();

            if (empty($this->tables)) {
                throw new DbJournalConfigException("No table with either `{$this->createdAtColumnName}` or `{$this->updatedAtColumnName}` column name found. Your tables need one of these columns to enable journaling.");
            }

            $this->output(count($this->tables) . " Able Table(s) retrieved", OutputInterface::VERBOSITY_VERBOSE);

        }

        $startTime = $options['time'] ?? $this->time();

        $this->output("<info>Journal time: {$startTime} (next `update` will generate journals for operations between this datetime and the execution time)</info>");

        // populate the journal with each able table starting from the timestamp
        $count = $this->populateDatabase($startTime);

        $this->output("<success>{$count} table(s) populated" . ($count > 0 ? ". Here we go." : "") . "</success>");
    }

    /**
     * Ensure that every table journal will be updated to the current database timestamp
     * @throws \Doctrine\DBAL\DBALException
     * @param array $options
     * @throws DbJournalConfigException
     */
    public function update(array $options): void
    {
        $journals = $this->getLastJournals();

        if (empty($journals)) {
            throw new DbJournalConfigException("DbJournal table ({$this->internalTable}) is empty. Run `init`.");
        }

        $startTime = $this->time();

        // @TODO if $options['time']: prompt WILL DELETE THE CURRENT JOURNAL!

        $currentTimeOption = $options['time'] ?? null;

        if ($currentTimeOption) {
            $this->output("Current Datetime Option: {$currentTimeOption} | journals will be generated if the operation was before this datetime (instead of current_timestamp)");
        }
        else {
            $this->output("Start (database) time: {$startTime}");
        }

        // @TODO: table filtering
        // $separatedFiles = $options['separate-files'] ?? false;
        // $filterTable should work better

        foreach ($journals as $journal) {
            $this->journalTable($journal, $currentTimeOption);
        }

        $this->output("<success>Update finished</success>");
    }

    /**
     * Update the Journal (file) for the given table / time
     * @param $journal -- main journal table's record
     * @param $currentTimeOption
     * @throws \Doctrine\DBAL\DBALException
     */
    public function journalTable($journal, $currentTimeOption=false)
    {
        $appStartTime = microtime(true);

        // table's journal
        $tableSql = [];

        $table = $journal['table_name'];
        $lastJournal = $journal['last_journal'];

        // optional time will overwrite the default (database now())
        $currentTime = $currentTimeOption ? $currentTimeOption : $this->time();

        $this->output("<info>Journaling table `{$table}` between {$lastJournal} (last journal) and {$currentTime} (currrent time)</info>");

        // created_at: generate insert queries
        if (DbalService::tableHasColumn($table, $this->createdAtColumnName)) {
            // nice to have: Iterator
            $inserts = $this->conn->fetchAll(
                "SELECT * FROM {$table}
                WHERE {$this->createdAtColumnName} > ?
                &&
                {$this->createdAtColumnName} <= ?",
                [$lastJournal, $currentTime]
            );

            // which SQL Statement will be generated
            $operation = 'insert';

            $totalInserts = count($inserts);

            $this->output("{$totalInserts} insert statements to run", OutputInterface::VERBOSITY_VERBOSE);
            $this->newProgressBar($totalInserts);

            foreach ($inserts as $insertRow) {
                $rowSql = $this->rowToJournal($table, $insertRow, $operation, $lastJournal, $currentTime);
                $tableSql[] = $rowSql;
                $this->advanceProgressBar();
                $this->output(PHP_EOL . $rowSql, OutputInterface::VERBOSITY_VERY_VERBOSE);
            }

            $this->finishProgressBar();
        }

        // updated_at: generate update queries
        if (DbalService::tableHasColumn($table, $this->updatedAtColumnName)) {
            // retrieve all the updates avoiding rows that generated inserts
            $updates = $this->conn->fetchAll(
                "SELECT * FROM {$table}
                WHERE {$this->updatedAtColumnName} > ?
                &&
                {$this->updatedAtColumnName} <= ?
                AND
                (
                    -- if both columns are equal (probably updated_at was set upon insert) it was already handled in the inserts section
                    {$this->updatedAtColumnName} <> {$this->createdAtColumnName}
                    OR
                    -- let's get this guy if created_at was not set
                    {$this->createdAtColumnName} IS NULL
                );",
                [$lastJournal, $currentTime]
            );

            // which SQL Statement will be generated
            $operation = 'update';

            $totalUpdates = count($updates);

            $this->output("{$totalUpdates} update statements to run", OutputInterface::VERBOSITY_VERBOSE);
            $this->newProgressBar($totalUpdates);

            foreach ($updates as $updateRow) {
                $rowSql = $this->rowToJournal($table, $updateRow, $operation, $lastJournal, $currentTime);
                $tableSql[] = $rowSql;
                $this->advanceProgressBar();
                $this->output(PHP_EOL . $rowSql, OutputInterface::VERBOSITY_VERY_VERBOSE);
            }

            $this->finishProgressBar();

        }

        $executionTime = microtime(true) - $appStartTime;

        // @TODO: BEGIN TRANSACTION / UPDATE / APPEND TO JOURNAL FILE / COMMIT (per table)

        $this->conn->update($this->internalTable, ['last_journal' => $lastJournal, 'last_execution_time_miliseconds' => $executionTime], ['table_name' => $table]);

        $this->output("<info>Journal updated for table `{$table}`</info>");
        $this->output("<info>Execution time: {$executionTime}</info>", OutputInterface::VERBOSITY_VERBOSE);

    }

    /**
     * @param $table
     * @param $row
     * @param $operation
     * @param $minTimestamp
     * @param $maxTimestamp
     * @return string -- INSERT or UPDATE SQL
     * @throws DbJournalRuntimeException
     */
    public function rowToJournal($table, $row, $operation, $minTimestamp, $maxTimestamp): string
    {
        // get the operation's column name
        $baseColumn = $operation == 'update' ? $this->updatedAtColumnName : $this->createdAtColumnName;

        // this method should only be called to rows pre-filtered (baseColumn between min and max timestamp)
        if ($row[$baseColumn] <= $minTimestamp || $row[$baseColumn] > $maxTimestamp) {
            throw new DbJournalRuntimeException("Invalid row for table {$table}: " . json_encode($row));
        }

        $queryBuilder = DbalService::getQueryBuilder();
        $values = [];

        if ($operation == 'insert') {
            $queryBuilder->insert($table);
            foreach ($row as $column => $value) {
                $queryBuilder->setValue($column, ":{$column}");

                /**
                 * TODO:
                 * - find PK
                 * - get column type
                 * - detect if type requires $this->conn->quotes();
                 * - see the full query being generated, escaped and quoted
                 * - if necessary, use https://github.com/twister-php/sql (watchout)
                 * - add the query meta tags
                 * - implement update
                 * - implement file saving
                 * - implement transaction to have ATOM file + db
                 * - implement load journal
                 */

                $values[] = $value;
            }
            die($queryBuilder->getSQL());
            // $sql = "INSERT INTO {$table} (`" . implode('`,`', array_keys($row)) . "`) VALUES ('" . implode("', '", $row) . ");";
        }
        else {
            $sql = "UPDATE {$table} set col = value WHERE PK = X;";
        }

        return $sql;
    }

    public function updateJournalFile()
    {

    }

    /**
     * Create a record on the journal for each $this->tables
     * @throws \Doctrine\DBAL\DBALException
     * @param $startTime
     * @return int
     */
    public function populateDatabase($startTime): int
    {
        $count = 0;

        // retrieve the existing records
        $journalTables = $this->getTablesLastJournal();

        // iterate and create a record on the journal for each table
        foreach ($this->tables as $table) {

            if (in_array($table, $journalTables)) {
                $this->output("<info>Table {$table} was already in the Journal</info>", OutputInterface::VERBOSITY_VERBOSE);
            }
            else {
                $this->conn->insert($this->internalTable, ['table_name' => $table, 'last_journal' => $startTime]);
                $count++;
            }
        }

        return $count;
    }

    /**
     * Truncate the Journal table (restart journal) and archive the old journal file
     */
    public function clean(): void
    {
        $this->truncateJournalTable();
        $this->output("<success>Table {$this->internalTable} truncated</success>");
    }

    /**
     * Drop the journal table and
     */
    public function uninstall(): void
    {
        $this->output("Table {$this->internalTable} deleted");

        $this->output("Journal files deleted");
    }

    public function dump()
    {
        return __METHOD__;
    }

    public function apply()
    {
        return __METHOD__;
    }

    /**
     * Write a $msg to the OutputInterface
     * @param $msg
     * @param $verboseLevel -- OutputInterface::VERBOSITY_*
     * @param bool $linebreak -- break lines?
     */
    public function output($msg, $verboseLevel=OutputInterface::VERBOSITY_NORMAL, $linebreak=true): void
    {
        if ($this->outputInterface) {
            $this->outputInterface->write($msg, $linebreak, $verboseLevel);
        }
    }

    /**
     * Start a new Progress Bar instance
     * @param $totalUnits
     * @throws DbJournalOutputException
     */
    public function newProgressBar($totalUnits): void
    {
        if ($this->outputInterface) {

            if ($this->progressBar) {
                throw new DbJournalOutputException("Cannot start a Progress Bar when one is still running");
            }

            // creates a new progress bar
            $this->progressBar = new ProgressBar($this->outputInterface, $totalUnits);

            // starts and displays the progress bar
            $this->progressBar->start();

        }
    }

    /**
     * Call ProgressBar->advance()
     * @param $units
     * @throws DbJournalOutputException
     */
    protected function advanceProgressBar($units=1): void
    {
        // when there's no OutputInterface, everything is ignored
        if ($this->outputInterface) {

            if (!$this->progressBar) {
                throw new DbJournalOutputException("Cannot advance when there's no Progress Bar started");
            }

            $this->progressBar->advance($units);

        }
    }

    /**
     * Call ProgressBar->finish()
     * @throws DbJournalOutputException
     */
    protected function finishProgressBar(): void
    {
        // when there's no OutputInterface, everything is ignored
        if ($this->outputInterface) {

            if (!$this->progressBar) {
                throw new DbJournalOutputException("Cannot finish when there's no Progress Bar started");
            }

            $this->progressBar->finish();

            $this->progressBar = null;

            $this->outputInterface->writeln('');
        }
    }
}
