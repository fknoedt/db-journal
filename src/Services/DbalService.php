<?php

namespace DbJournal\Services;

use DbJournal\Exceptions\DbJournalConfigException;
use \Doctrine\DBAL\Configuration;
use \Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Query\QueryBuilder;
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
     * Multidimensional array: table => [[column1], [column2]...]
     * @var
     */
    protected static $tablesColumnsMap;

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
     * Wrapper for getConnection()->getDatabasePlatform()
     * @return AbstractPlatform
     */
    public function getPlatform(): AbstractPlatform
    {
        return $this->getConnection()->getDatabasePlatform();
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

    /**
     * Return a new \Doctrine\DBAL\Query\QueryBuilder instance
     * @return QueryBuilder
     */
    public static function getQueryBuilder(): QueryBuilder
    {
        return self::getConnection()->createQueryBuilder();
    }

    /**
     * Query and retrieve all tables from the database
     * @return array
     */
    public static function retrieveTables(): array
    {
        return self::getSchemaManager()->listTables();
    }

    /**
     * [Create a singleton and] return a 'Table' => ['column1' => $column1Object, 'column2' => $column2Object, ...] multidimensional array
     * @return array
     */
    public static function getTablesColumnsMap(): array
    {
        // initialize table/column map
        if (! isset(self::$tablesColumnsMap)) {

            foreach (self::retrieveTables() as $table) {
                foreach ($table->getColumns() as $column) {
                    self::$tablesColumnsMap[$table->getName()][$column->getName()] = $column;
                }
            }

        }
        return self::$tablesColumnsMap;
    }

    /**
     * Same as self::getTablesColumnsMap but only the Doctrine Type for each column
     * @return array
     */
    public static function getTablesColumnsType(): array
    {
        $map = self::getTablesColumnsMap();
        foreach ($map as $table => $columns) {
            foreach ($columns as $columnName => $columnObject) {
                $map[$table][$columnName] = $columnObject->getType()->getName();
            }
        }
        return $map;
    }

    /**
     * Check in the TablesColumnsMap if the given table has the given column
     * @param $tableName
     * @param $columnName
     * @return bool
     */
    public static function tableHasColumn($tableName, $columnName): bool
    {
        return isset(self::getTablesColumnsMap()[$tableName][$columnName]);
    }
}