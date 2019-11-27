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
 * DbJournal Main Methods, Logic and Operations
 *
 * == Roadmap ==
 *
 * @TODO v0.1.0
 * - basic functionality working ✓
 *
 * @TODO v0.2.0
 * - dump() with filters ✓
 *
 * @TODO v0.3.0
 * - ReadTheDocs Documentation
 *
 * @TODO v0.4.0
 * - run() to import and run journals in the current database (with checks to avoid row conflict)
 * - write unit and integration tests
 *
 * @TODO v0.5.0
 * - namespace: working with parallel databases
 *
 * @TODO v0.6.0
 * - new internal table `db_journal_sessions` containing the time, log and execution time of each command call
 *
 * ====
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
     * Default journal backup directory
     */
    const DEFAULT_BKP_DIR = __DIR__ . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR . 'var' . DIRECTORY_SEPARATOR . 'bkp';

    /**
     * Defaul journal file name
     */
    const DEFAULT_FILE_NAME = 'journal.dump';

    /**
     * The /* header * / before each query in the journal file contains the PK
     */
    const DEFAULT_PK_SEPARATOR_QUERY_HEADER = '%';

    /**
     * Datetime format for input and stored values
     * @var string
     */
    const DB_DATETIME_FORMAT = 'Y-m-d H:i:s';

    /**
     * Date format for input and stored values
     * @var string
     */
    const DB_DATE_FORMAT = 'Y-m-d';

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
     * Hash map for the INSERTs created (so we can avoid UPDATEs for the same record)
     * @var
     */
    protected $insertQueriesCreated = [];

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

        // set custom Types handlers
        $this->registerCustomTypes();

        $this->output('Custom Types registered', OutputInterface::VERBOSITY_VERBOSE);

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
        $this->createdAtColumnName = $_ENV['DB_JOURNAL_CREATED_AT_COLUMN_NAME'] ?? $this->createdAtColumnName;

        // custom or default `updated_at` column name
        $this->updatedAtColumnName = $_ENV['DB_JOURNAL_UPDATED_AT_COLUMN_NAME'] ?? $this->updatedAtColumnName;

        if (empty($this->createdAtColumnName) || empty($this->updatedAtColumnName)) {
            throw new DbJournalConfigException("You cannot have an empty entry for DB_JOURNAL_CREATED_AT_COLUMN_NAME or DB_JOURNAL_UPDATED_AT_COLUMN_NAME on .env");
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
     * Custom Types need to be configured so DBAL knows how to handle them
     * @throws DbJournalConfigException
     * @throws \Doctrine\DBAL\DBALException
     */
    public function registerCustomTypes()
    {
        // enum
        DbalService::getSchemaManager()->getDatabasePlatform()->registerDoctrineTypeMapping('enum', 'string');
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

            if ($table == $this->internalTable) {
                continue;
            }

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
        return $_ENV['DB_JOURNAL_FILE'] ?? self::DEFAULT_FILE_NAME;
    }

    /**
     * Return the journal file directory from the .env conf or the default one
     * @return string
     */
    public function getJournalDir(): string
    {
        return $_ENV['DB_JOURNAL_DIR'] ?? self::DEFAULT_FILE_DIR;
    }

    /**
     * Return the journal file name from the .env conf or the default one
     * @return string
     */
    public function getJournalFilepath(): string
    {
        return $this->getJournalDir() . DIRECTORY_SEPARATOR . $this->getJournalFilename();
    }

    /**
     * Ensure that the journal directory (current or backup) exists
     * @param $dir
     * @throws DbJournalConfigException
     */
    public function checkJournalDir($dir)
    {
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
     * @param $table
     * @param $pkValuesString
     * @throws DbJournalRuntimeException
     */
    public function addInsertQuery($table, $pkValuesString): void
    {
        if (isset($this->insertQueriesCreated[$table][$pkValuesString])) {
            throw new DbJournalRuntimeException("Cannot generate the same INSERT more than once");
        }
        $this->insertQueriesCreated[$table][$pkValuesString] = true;
    }

    /**
     * Check if an INSERT query was generated for this $table / $pk
     * @param $table
     * @param $pkValuesString
     * @return bool
     */
    public function hasInsertedQuery($table, $pkValuesString): bool
    {
        return isset($this->insertQueriesCreated[$table][$pkValuesString]);
    }

    /**
     * Move the current journal dump file to the backup dir
     * @param null $newFilename -- backup file path (dir + filename)
     * @throws \Doctrine\DBAL\DBALException
     * @throws DbJournalConfigException
     */
    public function backupJournalFile($newFilename=null): void
    {
        if (! file_exists($this->getJournalFilepath())) {
            $this->output("No Journal file to backup", OutputInterface::VERBOSITY_VERBOSE);
            return;
        }

        // bkp file name will be YYYY-mm-dd_H_i_s_journal.dump
        if (! $newFilename) {
            $newFilename = str_replace([' ', ':'], '_', $this->time()) . '_' . $this->getJournalFilename();
        }

        // ensure the main journal directory exists
        $this->checkJournalDir(self::DEFAULT_BKP_DIR);

        // move file
        $bkpPath = self:: DEFAULT_BKP_DIR . DIRECTORY_SEPARATOR . $newFilename;

        rename($this->getJournalFilepath(), $bkpPath);

        $this->output("Journal dump file backed up on {$bkpPath}", OutputInterface::VERBOSITY_VERBOSE);
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
        }
        else {
            $startTime = $this->time();
            $forceUpdate = false;
        }

        $this->output("<info>Journal time: {$startTime} (next `update` will generate journals for operations between this datetime and the execution time)</info>");

        // populate the journal with each able table starting from the timestamp
        $count = $this->populateDatabase($startTime, $forceUpdate);

        $this->output("<success>{$count} table(s) populated/updated" . ($count > 0 ? ". Here we go." : "") . "</success>");

        // one or more
        if ($count > 0) {
            $this->backupJournalFile();
        }
    }

    /**
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

        // ensure the main journal directory exists
        $this->checkJournalDir($this->getJournalDir());

        $startTime = $this->time();

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

        $this->output("<success>Update finished. SQL queries appended to {$this->getJournalFilepath()}</success>");
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

        $this->output("<info>Journaling table `{$table}` between {$lastJournal} (last journal) and {$currentTime} (currrent time)</info>", OutputInterface::VERBOSITY_VERBOSE);

        // let's use a transaction per table to ensure ATOMicity between the database and file
        $this->conn->beginTransaction();

        // created_at: generate insert queries
        if (DbalService::tableHasColumn($table, $this->createdAtColumnName)) {

            // nice to have: Iterator
            $inserts = $this->conn->fetchAll(
                "SELECT * FROM {$table}
                WHERE {$this->createdAtColumnName} > ?
                &&
                {$this->createdAtColumnName} <= ?
                ORDER BY {$this->createdAtColumnName} ASC",
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
                )
                ORDER BY {$this->updatedAtColumnName} ASC
                ;",
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

                    // SQL can be empty if it's an UPDATE for a record that already had an INSERT
                    if (empty($rowSql)) {
                        $this->output(PHP_EOL . "<info>UPDATE query skipped</info>", OutputInterface::VERBOSITY_DEBUG);
                        continue;
                    }

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
        // TODO?: one file per table option
        foreach ($tableSQLs as $operation => $queries) {
            file_put_contents($this->getJournalFilepath(), PHP_EOL . implode(PHP_EOL, $queries), FILE_APPEND);
        }

        // now we can commit the journal table update
        $this->conn->commit();

        $this->output("<info>Journal updated for table `{$table}`</info>", OutputInterface::VERBOSITY_VERBOSE);
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

        // handle PKs

        // update query where clauses
        $wherePk = [];

        // [$pkColumnName => $pkValue]
        $pkValue = [];

        $tablePKs = DbalService::getTablePrimaryKeys($table);

        // every journaled table has to have a PK (even for INSERT as we wanna avoid an UPDATE later for the same record
        // and the only way to ensure that is by the row's PK)
        if (empty($tablePKs)) {

            // TODO?: ignore / ignore_all
            throw new DbJournalUserException("Table {$table} has a `{$this->updatedAtColumnName}` column but doesn't have a Primary Key");

        } else {

            foreach ($tablePKs as $pkColumnName) {

                // get the PK's value
                $value = $columnValues[$pkColumnName];

                // no PK column name containing the separator is allowed
                if ($pkColumnName == self::DEFAULT_PK_SEPARATOR_QUERY_HEADER) {
                    // TODO: .env config
                    throw new DbJournalUserException("PK Column {$pkColumnName} on table {$table} contains the illegal character " . self::DEFAULT_PK_SEPARATOR_QUERY_HEADER);
                }

                $pkValue[$pkColumnName] = $value;

                $wherePk[] = "`{$pkColumnName}` = " . $value;

            }

        }

        // primary key(s) name / value are used to ensure the same row won't be UPDATED after an INSERT (as that would be redundant)
        $pkColumnsString = implode(self::DEFAULT_PK_SEPARATOR_QUERY_HEADER, array_keys($pkValue));
        $pkValuesString = implode(self::DEFAULT_PK_SEPARATOR_QUERY_HEADER, $pkValue);

        if ($operation == 'insert') {

            $sql = "INSERT INTO {$table} (`" . implode('`,`', array_keys($row)) . "`) VALUES (" . implode(", ", $columnValues) .   ");";
            $columnUsed = $this->createdAtColumnName;

            //
            $this->addInsertQuery($table, $pkValuesString);

        }
        // update
        else {

            // we don't wanna create an UPDATE if we just created an INSERT that will get the same result
            if ($this->hasInsertedQuery($table, $pkValuesString)) {
                return '';
            }

            $set = [];

            foreach ($columnValues as $column => $value) {
                $set[] = "`$column` = {$value}";
            }

            $sql = "UPDATE {$table} set " . implode(', ', $set) . " WHERE " . implode('AND ', $wherePk) . ";";

            $columnUsed = $this->updatedAtColumnName;

        }

        // created_at or updated_at
        $operationTime = str_replace("'", "", $columnValues[$columnUsed]);

        // $pkColumns and $pkValues can be empty strings
        return "/*{$operationTime}|{$table}|{$columnUsed}|{$pkColumnsString}|{$pkValuesString}*/ " . $sql;
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

            if (isset($operation)) {
                $this->output("Journal: table {$table} entry {$operation}", OutputInterface::VERBOSITY_VERBOSE);
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
     * Drop the journal table, remove the files and show the composer command to remove the package
     */
    public function uninstall(): void
    {
        $this->output("TODO: Drop the journal table, remove the files and show the composer command to remove the package");
        exit;
    }

    /**
     * Dump the journal file (filter by time and table)
     * @param null $table
     * @param $minTimestamp
     * @param $maxTimestamp
     * @return false|string
     * @throws DbJournalConfigException
     * @throws DbJournalRuntimeException
     */
    public function dump($table=null, $minTimestamp=null, $maxTimestamp=null): array
    {
        $filePath = $this->getJournalFilepath();

        $queries = $this->getJournalFileContents($filePath, $table, $minTimestamp, $maxTimestamp);

        return $queries;
    }

    /**
     * Run the journal queries from the /var/import/* dir to the current database
     * @throws DbJournalUserException
     */
    public function run()
    {
        throw new DbJournalUserException("To be implemented on v0.3.0: run the journal queries from a /var/import dir to the current database");
    }

    /**
     * Return a filtered content of a journal file
     * @param $filePath
     * @param null $table
     * @param null $minTimestamp
     * @param null $maxTimestamp
     * @return array
     * @throws DbJournalConfigException
     * @throws DbJournalRuntimeException
     */
    public function getJournalFileContents($filePath, $table=null, $minTimestamp=null, $maxTimestamp=null): array
    {
        $fileContent = file($filePath, FILE_IGNORE_NEW_LINES);

        if (! $fileContent) {
            throw new DbJournalConfigException("The file {$filePath} doesn't exist or is empty. Make sure it has a valid journal file (have you ever ran `update`?).");
        }

        $content = [];

        $lineNumber = 0;

        // let's [filter and] remove the comments for each query
        foreach ($fileContent as $line) {

            $matches = [];
            $lineNumber++;

            //Sempre deve começar com um comentário que terá o nome da tabela, a data do registro e o(s) campo(s) de chave primária
            if (preg_match('=^/\*(.*)?\*/=', $line, $matches))  {

                $queryInfo = explode('|', $matches[1]);

                $queryTime = $queryInfo[0];
                $queryTable = $queryInfo[1];

                // table filter
                if ($table && $table != $queryTable) {
                    $this->output("Table `{$queryTable}` doesn't match `{$table}`. Skipping", OutputInterface::VERBOSITY_DEBUG);
                    continue;
                }

                if ($maxTimestamp && $queryTime > $maxTimestamp) {
                    $this->output("Query timestamp `{$queryTime}` exceeds `{$maxTimestamp}`. Skipping", OutputInterface::VERBOSITY_DEBUG);
                    continue;
                }

                if ($minTimestamp && $queryTime < $minTimestamp) {
                    $this->output("Query timestamp `{$queryTime}` exceeds `{$minTimestamp}`. Skipping", OutputInterface::VERBOSITY_DEBUG);
                    continue;
                }

                $query = str_replace($matches[0] . ' ', '', $line);

                $content[] = $query;

            }
            else {

                // empty line: ignore
                if (empty($line)) {
                    continue;
                }

                throw new DbJournalRuntimeException("Journal: Invalid line ({$lineNumber}): {$line}");

            }

        }

        return $content;


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
