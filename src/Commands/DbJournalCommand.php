<?php

namespace DbJournal\Commands;

use DbJournal\Exceptions\DbJournalConfigException;
use DbJournal\Services\DbalService;
use DbJournal\Services\DbJournalService;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Exception\CommandNotFoundException;
use Symfony\Component\Console\Exception\InvalidOptionException;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Respect\Validation\Validator as v;

/**
 * Main DbJournal's Command Class
 * Class DbJournalCommand
 * @package Commands
 */
class DbJournalCommand extends Command
{
    /**
     * Command name
     * @var string
     */
    protected static $defaultName = 'db-journal';

    /**
     * Execution time
     * @var
     */
    protected static $startTime;

    /**
     * @var
     */
    public static $commands = [
        'setup' => 'Create the internal table required to run the journal',
        'init' => 'Create the initial records on the main journal table',
        'update' => 'Ensure that every table journal will be updated to the current database timestamp',
        'dump' => '',
        'apply' => '[WARNING] Apply the given Journal to the current database',
        'list' => 'List the available DbJournal commands',
        'time' => 'Show the current database time'
    ];

    /**
     * Any option has to be declared here to be sanitized and used
     * @var array
     */
    public static $options = [
        'time' => 'timestamp'
    ];

    /**
     * DbJournalCommand constructor.
     */
    public function __construct()
    {
        $this::$startTime = microtime(true);
        parent::__construct();
    }

    /**
     * Standard Command method
     */
    public function configure()
    {
        $this
            // the short description shown while running "php bin/console list"
            ->setDescription('DbJournal commands')

            // the full command description shown when running the command with
            // the "--help" option
            ->setHelp('Commands to run the database journaling tool')

            ->addArgument('action', InputArgument::REQUIRED, 'DbJournal Action')

            ->addOption('time', 't', InputOption::VALUE_OPTIONAL, 'Time used to manipulate (init, update, dump,...) the Journal')
        ;
    }

    /**
     * @param string $tab
     * @return array
     */
    public function getCommandsList($tab="\t"): array
    {
        $list = [];

        foreach (self::$commands as $command => $description) {
            $list[] = "{$command}{$tab}{$tab}{$tab}{$description}";
        }

        return $list;
    }

    /**
     * Run a Command
     * @TODO have a Command class per command instead of everything under one (db-journal) command
     * @param InputInterface $input
     * @param OutputInterface $output
     * @throws \Symfony\Component\Console\Exception\ExceptionInterface
     * @return int
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $returnCode = 1;

        // main console action
        $action = $input->getArgument('action');

        $output->writeln("Action: " . $input->getArgument('action'), OutputInterface::VERBOSITY_VERBOSE);

        try {

            // `setup` will create the table, so let's not check for it
            $ignoreTable = (in_array($action, ['setup','list']));

            // the constructor will load configs and run checks
            $service = new DbJournalService($output, $ignoreTable);

            if (! isset(self::$commands[$action])) {
                throw new CommandNotFoundException("Invalid action: {$action}");
            }

            // sanitize options
            $options = $this->sanitizeOptions($input, $output);

            switch ($action) {

                case 'list':
                    foreach (self::$commands as $command => $description) {
                        $output->writeln("{$command}\t\t\t{$description}");
                    }
                    $repoUrl = DbJournalService::REPO_URL;
                    $output->writeln(PHP_EOL . "<href={$repoUrl}>Github\t\t\t{$repoUrl}</>");
                    break;

                case 'time':
                    $output->writeln($service->time());
                    break;

                default:
                    $service->$action($options);
                    break;

            }

        }
        catch (DbJournalConfigException $e) {
            // @TODO: differentiate these functional error messages
            $output->writeln('<error>' . $e->getMessage() . '</error>');
        }
        catch (\Exception $e) {
            // @TODO: differentiate these functional error messages
            $output->writeln('<error>ERROR: ' . $e->getMessage() . '</error>');
        }

        $executionTime = microtime(true) - $this::$startTime;

        $output->writeln("Execution time: {$executionTime}", OutputInterface::VERBOSITY_VERBOSE);

        return $returnCode;
    }

    /**
     * Return only valid -- @see self::$options -- and sanitized options
     * @param InputInterface $input
     * @param OutputInterface $output
     * @return array
     */
    public function sanitizeOptions(InputInterface $input, OutputInterface $output): array
    {
        $options = [];

        foreach ($input->getOptions() as $option => $value) {

            // db-journal options
            $optionType = self::$options[$option] ?? null;

            // valid option was set
            if ($optionType && $value) {

                // validate
                switch ($optionType) {

                    case 'timestamp':

                        // no datetime
                        if (! v::date(DbJournalService::DB_DATETIME_FORMAT)->validate($value)) {
                            // date?
                            if (v::date(DbJournalService::DB_DATE_FORMAT)->validate($value)) {
                                $value .= ' 00:00:00'; // @todo: create conf?
                            } else {
                                throw new InvalidOptionException("Option `{$option}` ({$value}) is invalid. Use " . DbJournalService::DB_DATETIME_FORMAT . ' or ' . DbJournalService::DB_DATE_FORMAT);
                            }
                        }
                        break;

                    default:

                        throw new InvalidOptionException("I don't know how to sanitize the option `{$option}` ({$optionType})`. Shame on me =/ But just give me a shout");

                }

                // allow option to be passed
                $options[$option] = $value;

                $output->writeln("{$option} => {$value}", OutputInterface::VERBOSITY_VERBOSE);

            }

        }

        return $options;
    }
}