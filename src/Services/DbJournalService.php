<?php

namespace DbJournal\Services;

use DbJournal\Exceptions\DbJournalConfigException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\Schema;
use PHPUnit\Framework\MockObject\Exception;

class DbJournalService
{
    /**
     * @var \Doctrine\DBAL\Connection
     */
    protected $conn;

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
    protected $dateFormat = 'Y-m-d H:i:s';

    /**
     * The methods can output buffer to be used externally (e.g. from the Console)
     * @var
     */
    public $outputBuffer;

    /**
     * DbJournalService constructor.
     * This class is tight coupled with doctrine/dbal so no DI here =)
     * For testing, the database can be mocked with a .env.test file
     *
     * @param $ignoreTable
     * @throws DbJournalConfigException
     */
    public function __construct($ignoreTable = false)
    {
        $this->conn = DbalService::getConnection();

        // @TODO: a logger with verbose levels would be nice
        $this->outputBuffer = [];

        // main journal table name can be defined on the .env file to allow prefixes/schemas
        $this->internalTable = $_ENV['DB_JOURNAL_TABLE'] ?? $this->internalTable;

        if (! $this->internalTable) {
            throw new DbJournalConfigException("Table name not defined (DB_JOURNAL_TABLE) on .env");
        }

        // ensure the internal db-journal table is created (if not running setup)
        if (! DbalService::tableExists($this->internalTable) && ! $ignoreTable) {
            throw new DbJournalConfigException("Table `{$this->internalTable}` doesn't exist. Run `setup`.");
        }

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

        $table->addColumn("table", "string", ["length" => 256]);
        $table->setPrimaryKey(array("table"));

        $table->addColumn("last_journal", "datetime", ["default" => 'CURRENT_TIMESTAMP']);

        $platform = $this->conn->getDatabasePlatform();

        // get SQL for the table above
        $queries = $schema->toSql($platform);

        // and run it
        foreach ($queries as $query) {
            $this->conn->exec($query);
        }

        $this->appendBuffer("Table {$this->internalTable} created");
    }

    /**
     * Return an array of table|last_journal
     * @return array
     */
    public function getTablesLastJournal(): array
    {
        return $this->conn->fetchAll("SELECT * FROM {$this->internalTable};");
    }

    /**
     * Ensure that every table journal will be updated to the current database timestamp
     * @throws DbJournalConfigException
     */
    public function update()
    {
        $tables = $this->getTablesLastJournal();

        if (empty($tables)) {
            throw new DbJournalConfigException("DbJournal table ({$this->internalTable}) is empty. Run `init`.");
        }

        dd($tables);
    }

    /**
     * Create the initial records on the journal table
     * @param null $startTime
     * @throws DbJournalConfigException
     */
    public function init($startTime=null): void
    {
        // no tables defined on .env: scan tables
        if (empty($this->tables)) {

            $this->tables = $this->retrieveAbleTables();

            if (empty($this->tables)) {
                throw new DbJournalConfigException("No table with either `{$this->createdAtColumnName}` or `{$this->updatedAtColumnName}` column name found. Your tables need one of these columns to enable journaling.");
            }

            $this->appendBuffer(count($this->tables) . " Able Table(s) retrieved");

        }

        if (!$startTime) {
            $startTime = date($this->dateFormat);
            $this->appendBuffer("Start time: {$startTime}");
        }

        // populate the journal with each able table starting from the timestamp
        $this->populateDatabase($startTime);

        $this->appendBuffer("Tables populated. Here we go.");

    }

    /**
     * List and return the tables that have one or both created/updated_at fields
     * @return array
     */
    public function retrieveAbleTables(): array
    {
        $tables = [];
        // iterate on every table
        foreach (DbalService::getSchemaManager()->listTables() as $table) {

            foreach ($table->getColumns() as $column) {
                // if the table has one or both columns,
                if (in_array($column->getName(), [$this->createdAtColumnName, $this->updatedAtColumnName])) {
                    $tables[] = $table->getName();
                    break;
                }
            }

        }
        return $tables;
    }

    public function populateDatabase($startTime): void
    {
        // iterate on the tables to journal
        foreach ($this->tables as $table) {

            // TODO: create record for each table

        }

        dd('DO IT: ' . implode(', ', $this->tables));
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
     * SRP: these are auxiliary methods; the class manages the journal
     * Append a string to the (array) output buffer
     * @param $output
     */
    public function appendBuffer($output): void
    {
        $this->outputBuffer[] = $output;
    }

    /**
     * Return the output buffer
     * @return string
     */
    public function getBuffer($glue=PHP_EOL): string
    {
        return implode($glue, $this->outputBuffer);
    }

    /**
     * Return the buffer and clean it
     * @return string
     */
    public function getBufferClean(): string
    {
        $buffer = $this->getBuffer();
        $this->outputBuffer = [];
        return $buffer;
    }

}
