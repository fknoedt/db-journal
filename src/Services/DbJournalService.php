<?php

namespace DbJournal\Services;

use DbJournal\Exceptions\DbJournalConfigException;
use DbJournal\Exceptions\DbJournalOutputException;
use DbJournal\Exceptions\DbJournalRuntimeException;
use DbJournal\Exceptions\DbJournalUserException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Types\Type;
use PHPUnit\Framework\MockObject\Exception;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Class DbJournalService
 * DbJournal Operation Manager
 *
 * @TODO:
 * - queries pattern
 * - check tivel's
 * - large database test
 * - write unit tests
 * - db_journal_sessions containing the time, log and execution time
 *
 * @package DbJournal\Services
 */
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
    const COMPOSER_PACKAGE = 'fknoedt/db-journal';

    /**
     * Default journal file dir
     */
    const DEFAULT_FILE_DIR = __DIR__ . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR . 'var' . DIRECTORY_SEPARATOR . 'journal';

    /**
     * Defaul journal file name
     */
    const DEFAULT_FILE_NAME = 'journal.dump';

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
     * @param OutputInterface|null $output
     * @param bool $ignoreTable
     * @throws DbJournalConfigException
     * @throws \Doctrine\DBAL\DBALException
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

        $this->output("{$this->internalTable} table " . ($ignoreTable ? 'ignored' : 'ok'), OutputInterface::VERBOSITY_VERBOSE);
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
     * Return an array of table_name|last_journal|last_execution_time_milliseconds
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
     * @throws DbJournalConfigException
     * @throws \Doctrine\DBAL\DBALException
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
     * Return the journal file name from the .env conf or the default one
     * @return string
     */
    public function getJournalFilename(): string
    {
        $filename = $_ENV['DB_JOURNAL_FILE'] ?? self::DEFAULT_FILE_NAME;
        return $this->getJournalFileDir() . DIRECTORY_SEPARATOR . $filename;
    }

    /**
     * Return the journal file directory from the .env conf or the default one
     * @return string
     */
    public function getJournalFileDir(): string
    {
        $dir = $_ENV['DB_JOURNAL_DIR'] ?? self::DEFAULT_FILE_DIR;
        return $dir;
    }

    /**
     * Ensure that the journal directory exists
     * @throws DbJournalConfigException
     */
    public function checkJournalFileDir()
    {
        $dir = $this->getJournalFileDir();

        if (! is_dir($dir)) {
            try {
                mkdir($dir, 0777, true);
                $this->output("Directory {$dir} created", OutputInterface::VERBOSITY_VERBOSE);
            }
            catch(\Exception $e) {
                throw new DbJournalConfigException("Could not create directory {$dir}");
            }

        }
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

        $table->addColumn("last_execution_time_milliseconds", "float", ["notnull" => false]);

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

        // --time set: create records ensuring the existing records will be updated to the inputted time
        if (isset($options['time']) && $options['time']) {
            $startTime = $options['time'];
            $forceUpdate = true;

            // TODO: backup journal dump file

        }
        else {
            $startTime = $this->time();
            $forceUpdate = true;
        }

        $this->output("<info>Journal time: {$startTime} (next `update` will generate journals for operations between this datetime and the execution time)</info>");

        // populate the journal with each able table starting from the timestamp
        $count = $this->populateDatabase($startTime, $forceUpdate);

        $this->output("<success>{$count} table(s) populated/updated" . ($count > 0 ? ". Here we go." : "") . "</success>");
    }

    /**
     * Ensure that every table journal will be updated to the current database timestamp
     * @param array $options
     * @throws DbJournalConfigException
     * @throws DbJournalOutputException
     * @throws DbJournalRuntimeException
     * @throws DbJournalUserException
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Doctrine\DBAL\Types\ConversionException
     */
    public function update(array $options): void
    {
        $journals = $this->getLastJournals();

        if (empty($journals)) {
            throw new DbJournalConfigException("DbJournal table ({$this->internalTable}) is empty. Run `init`.");
        }

        $this->checkJournalFileDir();

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

        $this->output("<success>Update finished. SQL queries appended to {$this->getJournalFilename()}</success>");
    }

    /**
     * Update the Journal (file) for the given table / time
     * @param $journal
     * @param bool $currentTimeOption
     * @throws DbJournalConfigException
     * @throws DbJournalOutputException
     * @throws DbJournalRuntimeException
     * @throws DbJournalUserException
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Doctrine\DBAL\Types\ConversionException
     */
    public function journalTable($journal, $currentTimeOption=false)
    {
        $journalStartTime = microtime(true);

        // table's journal
        $tableSQLs = [];

        $table = $journal['table_name'];
        $lastJournal = $journal['last_journal'];

        // optional time will overwrite the default (database now())
        $currentTime = $currentTimeOption ? $currentTimeOption : $this->time();

        $this->output("<info>Journaling table `{$table}` between {$lastJournal} (last journal) and {$currentTime} (currrent time)</info>");

        // let's use a transaction per table to ensure ATOMicity between the database and file
        $this->conn->beginTransaction();

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

            $rowsToJournal['insert'] = $inserts;
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

            $rowsToJournal['update'] = $updates;

        }

        // inserts and then updates
        foreach ($rowsToJournal as $operation => $rows) {

            $totalRows = count($rows);

            $this->output("{$totalRows} {$operation} statements to journal", OutputInterface::VERBOSITY_VERBOSE);

            if ($totalRows > 0) {

                $this->newProgressBar($totalRows);

                // one query per iteration
                foreach ($rows as $row) {
                    $rowSql = $this->rowToSQL($table, $row, $operation, $lastJournal, $currentTime);
                    $tableSQLs[$operation][] = $rowSql;
                    $this->advanceProgressBar();
                    $this->output(PHP_EOL . "<info>{$rowSql}</info>", OutputInterface::VERBOSITY_DEBUG);
                }

                $this->finishProgressBar();

            }

        }

        $executionTime = microtime(true) - $journalStartTime;

        // first we update the journal table (we'll commit if the file is written successfully)
        $this->conn->update($this->internalTable, ['table_name' => $table, 'last_journal' => $currentTime, 'last_execution_time_milliseconds' => $executionTime], ['table_name' => $table]);

        // then we write (append) the SQL queries to the journal file
        // TODO: one file per table option
        foreach ($tableSQLs as $operation => $queries) {
            file_put_contents($this->getJournalFilename(), implode(PHP_EOL, $queries), FILE_APPEND);
        }

        // now we can commit the journal table update
        $this->conn->commit();

        $this->output("<info>Journal updated for table `{$table}`</info>");
        $this->output("<info>Execution time: {$executionTime}</info>", OutputInterface::VERBOSITY_VERBOSE);

    }

    /**
     * Convert a fetchAll() row to it's INSERT or UPDATE operation (between min and max timestamps) SQL string
     * @param $table
     * @param $row
     * @param $operation
     * @param $minTimestamp
     * @param $maxTimestamp
     * @return string
     * @throws DbJournalConfigException
     * @throws DbJournalRuntimeException
     * @throws DbJournalUserException
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Doctrine\DBAL\Types\ConversionException
     */
    public function rowToSQL($table, $row, $operation, $minTimestamp, $maxTimestamp): string
    {
        // get the operation's column name
        $baseColumn = $operation == 'update' ? $this->updatedAtColumnName : $this->createdAtColumnName;

        // this method should only be called to rows pre-filtered (baseColumn between min and max timestamp)
        if ($row[$baseColumn] <= $minTimestamp || $row[$baseColumn] > $maxTimestamp) {
            throw new DbJournalRuntimeException("Invalid row for table {$table}: " . json_encode($row));
        }

        $columnValues = [];

        foreach ($row as $column => $value) {
            // get SQL syntax string for the $value according to it's Type
            $dbValue = DbalService::getDatabaseValue($value, $table, $column);
            $columnValues[$column] = $dbValue;
        }

        if ($operation == 'insert') {

            $sql = "INSERT INTO {$table} (`" . implode('`,`', array_keys($row)) . "`) VALUES (" . implode(", ", $columnValues) .   ");";

        }
        // update
        else {

            $set = [];

            foreach ($columnValues as $column => $value) {
                $set[] = "`$column` = {$value}";
            }

            $tablePKs = DbalService::getTablePrimaryKeys($table);

            // @TODO: ignore / ignore_all
            if (empty($tablePKs)) {
                throw new DbJournalUserException("Table {$table} has a `{$this->updatedAtColumnName}` column but doesn't have a Primary Key");
            }

            $wherePk = [];

            foreach ($tablePKs as $pkColumnName) {
                // get the PK's value
                $value = $columnValues[$pkColumnName];
                $wherePk[] = "`{$pkColumnName}` = " . $value;
            }

            $sql = "UPDATE {$table} set " . implode(', ', $set) . " WHERE " . implode('AND ', $wherePk) . ";";

        }

        return $sql;
    }

    /**
     * Create a record on the journal for each $this->tables
     * @throws \Doctrine\DBAL\DBALException
     * @param $startTime
     * @param $forceUpdate
     * @return int
     */
    public function populateDatabase($startTime, $forceUpdate): int
    {
        $count = 0;

        // retrieve the existing records
        $journalTables = $this->getTablesLastJournal();

        // iterate and create a record on the journal for each table
        foreach ($this->tables as $table) {

            if (in_array($table, $journalTables)) {

                if ($forceUpdate) {
                    $this->conn->update($this->internalTable, ['last_journal' => $startTime, 'last_execution_time_milliseconds' => null], ['table_name' => $table]);
                    $operation = 'updated';
                    $count++;
                }
                else {
                    $this->output("<info>Table {$table} was already in the Journal</info>", OutputInterface::VERBOSITY_VERBOSE);
                }
            }
            else {
                $this->conn->insert($this->internalTable, ['table_name' => $table, 'last_journal' => $startTime]);
                $operation = 'created';
                $count++;
            }

            $this->output("Journal: table {$table} entry {$operation}", OutputInterface::VERBOSITY_VERBOSE);
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

    /**
     * Dump the journal file filtering by time
     * @TODO: filter by min & max timestamps
     * @return string
     */
    public function dump()
    {
        return file_get_contents($this->getJournalFilename());
    }

    /**
     * Run the journal queries from the /var/import/* dir to the current database
     * @return string
     */
    public function run()
    {
        return "TODO: run the journal queries from a /var/import dir to the current database";
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
