<?php

namespace DbJournal\Services;

use DbJournal\Exceptions\DbJournalConfigException;
use \Doctrine\DBAL\Configuration;
use \Doctrine\DBAL\DriverManager;
use \Doctrine\DBAL\Schema\AbstractSchemaManager;
use \Doctrine\DBAL\Connection;

class DbalService
{
    /**
     * DBAL Connection Singleton
     * @var \Doctrine\DBAL\Connection
     */
    protected static $conn;

    /**
     * Check if every required Database Connection Env Var is set
     * @throws \DbJournalConfigException
     */
    public static function validateEnv(): void
    {
        $requiredVars = [
            'DB_HOST'       => 'Host Name',
            'DB_DATABASE'   => 'Database Name',
            'DB_USERNAME'   => 'Username',
            'DB_PASSWORD'   => 'Password',
            'DB_DRIVER'     => 'Database Driver'
            // port and charset are not mandatory
            // 'DB_PORT'       => 'Database Port',
            // 'DB_CHARSET'    => 'Database Charset',
        ];

        $missingVars = [];

        // check if every required environment variable is set
        foreach ($requiredVars as $var => $name) {
            if (! isset($_ENV[$var])) {
                $missingVars[$var] = $name;
            }
        }

        if (! empty($missingVars)) {
            throw new DbJournalConfigException("DB Configuration not found. Make sure you have a .env file with these entries: " . implode(', ', array_keys($missingVars)));
        }
    }

    /**
     * Wrapper for self::getSchemaManager()->tablesExist()
     * @param string $table
     * @return bool;
     */
    public static function tableExists(string $table): bool
    {
        return self::getSchemaManager()->tablesExist($table);
    }

    /**
     * Wrapper for getConnection()->getSchemaManager() *IDE purposes
     * @return AbstractSchemaManager
     */
    public static function getSchemaManager(): AbstractSchemaManager
    {
        return self::getConnection()->getSchemaManager();
    }

    /**
     * Initialize and return a singleton DBAL Connection
     * @return \Doctrine\DBAL\Connection
     */
    public static function getConnection(): Connection
    {
        if (! self::$conn) {

            // ensure the configs are set on $_ENV
            self::validateEnv();

            $config = new Configuration();

            $connectionParams = array(
                'dbname' =>     $_ENV['DB_DATABASE'],
                'user' =>       $_ENV['DB_USERNAME'],
                'password' =>   $_ENV['DB_PASSWORD'],
                'host' =>       $_ENV['DB_HOST'],
                'driver' =>     $_ENV['DB_DRIVER']
            );

            self::$conn = DriverManager::getConnection($connectionParams, $config);

        }

        return self::$conn;
    }
}