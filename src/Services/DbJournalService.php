<?php

namespace DbJournal\Services;

class DbJournalService
{
    /**
     * \Doctrine\DBAL\Connection
     * @var
     */
    protected $conn;

    /**
     * DbJournalService constructor.
     * This class implements doctrine/dbal so there's no need - for now - to use DI
     * To test, the database can be mocked by a simple .env.test file
     */
    public function __construct()
    {
        $this->conn = DbalService::getConnection();
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