<?php

namespace DbJournal\Services;

use DbJournal\Exceptions\DbJournalConfigException;
use DbJournal\Exceptions\DbJournalRuntimeException;
use \Doctrine\DBAL\Configuration;
use \Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Query\QueryBuilder;
use \Doctrine\DBAL\Schema\AbstractSchemaManager;
use \Doctrine\DBAL\Connection;
use Doctrine\DBAL\Types\DateTimeType;
use Doctrine\DBAL\Types\IntegerType;
use Doctrine\DBAL\Types\SmallIntType;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Schema\Table;

/**
 * Class DbalService
 * DBAL Wrapper for generic database operations
 * @package DbJournal\Services
 */
class DbalService
{
    /**
     * DBAL Connection Singleton
     * @var \Doctrine\DBAL\Connection
     */
    protected static $conn;

    /**
     * Cache for all SchemaManager Tables
     * @var array Table
     */
    protected static $schemaManagerTables;

    /**
     * Multidimensional array: table => [[column1], [column2]...]
     * @var
     */
    protected static $tablesColumnsMap;

    /**
     * Check if every required Database Connection Env Var is set
     * @throws DbJournalConfigException
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
     * @return bool
     * @throws DbJournalConfigException
     * @throws \Doctrine\DBAL\DBALException
     */
    public static function tableExists(string $table): bool
    {
        return self::getSchemaManager()->tablesExist($table);
    }

    /**
     * Wrapper for getConnection()->getSchemaManager() *IDE purposes
     * @return AbstractSchemaManager
     * @throws DbJournalConfigException
     * @throws \Doctrine\DBAL\DBALException
     */
    public static function getSchemaManager(): AbstractSchemaManager
    {
        return self::getConnection()->getSchemaManager();
    }

    /**
     * Wrapper for getConnection()->getDatabasePlatform()
     * @return AbstractPlatform
     * @throws \Doctrine\DBAL\DBALException
     * @throws DbJournalConfigException
     */
    public static function getPlatform(): AbstractPlatform
    {
        return self::getConnection()->getDatabasePlatform();
    }

    /**
     * Initialize and return a singleton DBAL Connection
     * @return Connection
     * @throws DbJournalConfigException
     * @throws \Doctrine\DBAL\DBALException
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
     * @throws DbJournalConfigException
     * @throws \Doctrine\DBAL\DBALException
     */
    public static function getQueryBuilder(): QueryBuilder
    {
        return self::getConnection()->createQueryBuilder();
    }

    /**
     * Query and retrieve all tables from the database (with cache/singleton)
     * @return array
     * @throws DbJournalConfigException
     * @throws \Doctrine\DBAL\DBALException
     */
    public static function retrieveTables(): array
    {
        // initialize singleton
        if (! isset(self::$schemaManagerTables)) {
            self::$schemaManagerTables = self::getSchemaManager()->listTables();
        }

        return self::$schemaManagerTables;
    }

    /**
     * [Create a singleton and] return a 'Table' => ['column1' => $column1Object, 'column2' => $column2Object, ...] multidimensional array
     * Columns will be handled as Doctrine\DBAL\Schema\Column
     * @see https://www.doctrine-project.org/api/dbal/2.9/Doctrine/DBAL/Schema/Column.html
     * @return array
     * @throws DbJournalConfigException
     * @throws \Doctrine\DBAL\DBALException
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
     * Return the full self::getTablesColumnsMap but replacing the column's value with the datatype (txt)
     * @return array
     * @throws DbJournalConfigException
     * @throws \Doctrine\DBAL\DBALException
     */
    public static function getTablesColumnsOutput(): array
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
     * @throws DbJournalConfigException
     * @throws \Doctrine\DBAL\DBALException
     */
    public static function tableHasColumn($tableName, $columnName): bool
    {
        return isset(self::getTablesColumnsMap()[$tableName][$columnName]);
    }

    /**
     * Get the SchemaManager Table object for the given table
     * @TODO: Table hashmap to improve performace
     * @param $tableName
     * @return Table
     * @throws DbJournalConfigException
     * @throws DbJournalRuntimeException
     * @throws \Doctrine\DBAL\DBALException
     */
    public static function getTable($tableName): Table
    {
        // retrieveTables() is cached
        foreach (self::retrieveTables() as $table) {

            if ($table->getName() == $tableName) {
                return $table;
            }

        }

        throw new DbJournalRuntimeException("Table {$tableName} not found");
    }

    /**
     * Return the Doctrine\DBAL\Types\Type of the given $table . $column
     * @param string $table
     * @param string $column
     * @return Type
     * @throws DbJournalConfigException
     * @throws DbJournalRuntimeException
     * @throws \Doctrine\DBAL\DBALException
     */
    public static function getColumnType(string $table, string $column): Type
    {
        $tablesMap = self::getTablesColumnsMap();

        if (! isset($tablesMap[$table][$column])) {
            throw new DbJournalRuntimeException("No entry on TablesColumnsMap | {$table} . {$column}  ");
        }

        $column = $tablesMap[$table][$column];

        return $column->getType();
    }

    /**
     * @param $tableName
     * @return array
     * @throws DbJournalConfigException
     * @throws DbJournalRuntimeException
     * @throws \Doctrine\DBAL\DBALException
     */
    public static function getTablePrimaryKeys($tableName): array
    {
        $table = self::getTable($tableName);
        return $table->getPrimaryKey()->getUnquotedColumns();
    }

    /**
     * Is the given table . column a primary key?
     * @param string $table
     * @param string $column
     * @return bool
     * @throws DbJournalConfigException
     * @throws DbJournalRuntimeException
     * @throws \Doctrine\DBAL\DBALException
     */
    public static function isColumnPk(string $table, string $column): bool
    {
        return in_array($column, self::getTablePrimaryKeys($table));
    }

    /**
     * Return Type->convertToDatabaseValue() (Converts a value from its PHP representation to its database representation of this type)
     * @param $value
     * @param string $table
     * @param string $column
     * @return string
     * @throws DbJournalConfigException
     * @throws \Doctrine\DBAL\DBALException
     * @throws DbJournalRuntimeException
     * @throws \Doctrine\DBAL\Types\ConversionException
     */
    public static function getDatabaseValue($value, string $table, string $column): string
    {
        if (is_null($value)) {
            return 'NULL';
        }

        // get the value's Type object
        $type = DbalService::getColumnType($table, $column);

        // cast to the PHP value according to the Type
        // TODO: this should be unnecessary since we already have the values in strings
        $value = $type->convertToPHPValue($value, self::getPlatform());

        if (self::shouldQuote($type->getBindingType())) {
            $value = self::getConnection()->quote($value, $type);
        }

        // $value = $type->convertToDatabaseValue($value, self::getPlatform());

        return $value;
    }

    /**
     * Does the given binding type require quotes or not
     * @param $bindingType
     * @return bool
     * @throws \Exception
     */
    public static function shouldQuote($bindingType): bool
    {
        switch ($bindingType) {

            case ParameterType::INTEGER:
            case ParameterType::BOOLEAN:
            case ParameterType::NULL:
                return false;

            case ParameterType::STRING:
                return true;

            case ParameterType::BINARY:
            case ParameterType::LARGE_OBJECT:
                throw new \Exception("Binary/LOB types are not implemented");

        }
    }
}
