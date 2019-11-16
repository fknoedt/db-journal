<?php

namespace DbJournal\Commands;

use DbJournal\Exceptions\DbJournalConfigException;
use DbJournal\Services\DbalService;
use DbJournal\Services\DbJournalService;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Exception\CommandNotFoundException;
use Symfony\Component\Console\Exception\InvalidOptionException;
use Symfony\Component\Console\Formatter\OutputFormatterStyle;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Respect\Validation\Validator as v;
use Symfony\Component\Console\Question\ConfirmationQuestion;

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
        'dump' => 'Output the journal queries for the given filters',
        'apply' => '[WARNING] Apply the given Journal to the current database',
        'list' => 'List the available DbJournal commands',
        'time' => 'Show the current database time',
        'clean' => "Clean the existing journal records and files (warning, you won't be able to run pre-existing journals after this)",
        'uninstall' => 'Remove DbJournal table and files'
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
        $outputStyle = new OutputFormatterStyle('white', 'green', ['bold', 'blink']);
        $output->getFormatter()->setStyle('success', $outputStyle);

        // 1 means the command has ran
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

            // should call the Service's action
            $callAction = true;

            switch ($action) {

                case 'list':

                    foreach (self::$commands as $command => $description) {
                        $output->writeln("{$command}\t\t\t{$description}");
                    }
                    $repoUrl = DbJournalService::REPO_URL;
                    $output->writeln(PHP_EOL . "<href={$repoUrl}>Github\t\t\t{$repoUrl}</>");

                    $callAction = false;

                    break;

                case 'time':

                    $output->writeln($service->time());

                    $callAction = false;

                    break;

                case 'clean':

                    $helper = $this->getHelper('question');
                    $question = new ConfirmationQuestion('Are you sure you want to truncate the journal table? ', false);
                    if (!$helper->ask($input, $output, $question)) {
                        return;
                    }

                    break;

                case 'update':

                    // update with a fixed time can easily mess up your journal - watch out
                    if (isset($options['time']) && $options['time']) {
                        $helper = $this->getHelper('question');
                        $question = new ConfirmationQuestion("WARNING: This will generate journals for operations between the table's last_journal and the optional datetime." . PHP_EOL .
                            "Are you sure you want to run `update` for the optional ({$options['time']}) time?", false);
                        if (!$helper->ask($input, $output, $question)) {
                            return;
                        }
                        else {
                            $output->writeln('DO IT');
                            return;
                        }
                    }

                    break;

                case 'init':

                    // update with a fixed time can easily mess up your journal - watch out
                    if (isset($options['time']) && $options['time']) {
                        $helper = $this->getHelper('question');
                        $question = new ConfirmationQuestion("This will start your journals for every (able) table starting on the optional date." . PHP_EOL .
                            "Are you sure you want to initialize the Journal starting on {$options['time']}?", false);
                        if (!$helper->ask($input, $output, $question)) {
                            return;
                        }
                        else {
                            $output->writeln('DO IT');
                            return;
                        }
                    }

                    break;

                default:

                    break;

            }

            // some actions don't call a Service method
            if ($callAction) {
                $service->$action($options);
            }

        }
        catch (DbJournalConfigException $e) {
            $output->writeln('<error>' . $e->getMessage() . '</error>');
        }
        catch (\Exception $e) {
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
