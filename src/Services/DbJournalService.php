<?php

namespace DbJournal\Services;

use DbJournal\Exceptions\DbJournalConfigException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\Schema;

class DbJournalService
{
    /**
     * @var \Doctrine\DBAL\Connection
     */
    protected $conn;

    /**
     * @var string
     */
    protected $internalTable;

    /**
     * DbJournalService constructor.
     * This class is tight coupled with doctrine/dbal so there's no need to use DI =)
     * For testing, the database can be mocked with a .env.test file
     * @param $ignoreTable
     * @throws DbJournalConfigException
     */
    public function __construct($ignoreTable = false)
    {
        $this->conn = DbalService::getConnection();

        // main table name can be defined on the .env to allow prefixes/schemas
        $this->internalTable = $_ENV['DB_JOURNAL_TABLE'] ?? null;

        if (! $this->internalTable) {
            throw new DbJournalConfigException("Table name not defined (DB_JOURNAL_TABLE) on .env");
        }

        // ensure the internal db-journal table is created (if not running setup)
        if (! DbalService::tableExists($this->internalTable) && ! $ignoreTable) {
            throw new DbJournalConfigException("Table `{$this->internalTable}` doesn't exist. Run `setup`.");
        }
    }

    /**
     * Create the internal table required to run the journal
     * @throws DbJournalConfigException
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
    }

    public function updateJournal()
    {

    }

    public function dumpJournal()
    {
        return __METHOD__;
    }

    public function applyJournal()
    {
        return __METHOD__;
    }


}