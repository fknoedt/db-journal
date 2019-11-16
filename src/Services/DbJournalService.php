<?php

namespace DbJournal\Services;

use DbJournal\Exceptions\DbJournalConfigException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\Schema;
use PHPUnit\Framework\MockObject\Exception;
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

        $forcedLastJournal = $options['time'] ?? null;

        $this->output("Start time: {$startTime}", OutputInterface::VERBOSITY_VERBOSE);

        // option to create files separately
        // $separatedFiles = $options['separate-files'] ?? false;
        // $filterTable should work better

        foreach ($journals as $journal) {
            $this->generateTableJournal($journal['table_name'], ($forcedLastJournal ? $forcedLastJournal : $journal['last_journal']));
        }

        $this->output("<success>Update finished</success>");
    }

    /**
     * Update the Journal (file) with the
     * @param $table
     * @param $lastJournal
     * @throws \Doctrine\DBAL\DBALException
     */
    public function generateTableJournal($table, $lastJournal)
    {
        $startTime = microtime(true);

        $this->output("<info>Journaling table `{$table}` - last journal: {$lastJournal}</info>");

        if (DbalService::tableHasColumn($table, $this->createdAtColumnName)) {
            // nice to have: Iterator
            $inserts = $this->conn->fetchAll("SELECT * FROM {$table} WHERE {$this->createdAtColumnName} > ?", [$lastJournal]);

            $totalInserts = count($inserts);

            $this->output("{$totalInserts} insert statements to run", OutputInterface::VERBOSITY_VERBOSE);
        }

        if (DbalService::tableHasColumn($table, $this->updatedAtColumnName)) {

            $updates = $this->conn->fetchAll("SELECT * FROM {$table} WHERE {$this->updatedAtColumnName} > ?", [$lastJournal]);

            $totalUpdates = count($updates);

            $this->output("{$totalUpdates} update statements to run", OutputInterface::VERBOSITY_VERBOSE);

        }

        $executionTime = microtime(true) - $startTime;

        $this->conn->update($this->internalTable, ['last_journal' => $lastJournal, 'last_execution_time_miliseconds' => $executionTime], ['table_name' => $table]);

        $this->output("<info>Journal updated for table `{$table}`</info>");

    }

    public function updateJournalFile()
    {

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

        if (!$startTime) {
            $startTime = $this->time();
            $this->output("Start time: {$startTime}", OutputInterface::VERBOSITY_VERBOSE);
        }

        // populate the journal with each able table starting from the timestamp
        $count = $this->populateDatabase($startTime);

        $this->output("<success>{$count} table(s) populated. Here we go.</success>");
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
     * Truncate the main journal table
     * @return int
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Doctrine\DBAL\Exception\InvalidArgumentException
     */
    public function truncateJournalTable()
    {
        return $this->conn->delete($this->internalTable, ['1' => '1']);
    }

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

}
