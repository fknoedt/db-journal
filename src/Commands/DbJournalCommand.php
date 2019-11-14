<?php

namespace DbJournal\Commands;

use DbJournal\Exceptions\DbJournalConfigException;
use DbJournal\Services\DbalService;
use DbJournal\Services\DbJournalService;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Exception\CommandNotFoundException;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Main DbJournal's Command Class
 * Class DbJournalCommand
 * @package Commands
 */
class DbJournalCommand extends Command
{
    /**
     * the name of the command (the part after "bin/console")
     * @var string
     */
    protected static $defaultName = 'db-journal:command';

    /**
     * Execution time
     * @var
     */
    protected static $startTime;

    /**
     * @todo it
     * @var
     */
    var $attribute;

    /**
     * DbJournalCommand constructor.
     */
    public function __construct()
    {
        $this::$startTime = microtime();
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
        ;
    }

    /**
     * Run a Command
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

        $output->writeln("Action: " . $input->getArgument('action'));

        // `setup` will create the table, so let's not check for it
        $ignoreTable = ($action == 'setup');

        // the constructor will load configs and run checks
        $service = new DbJournalService($ignoreTable);

        try {

            switch ($action) {

                case 'setup':
                    $service->setup();
                    break;

                case 'init':
                    // @TODO: datetime as param
                    $service->init();
                    break;

                case 'update':
                    $service->update();
                    break;

                case 'dump':
                    $service->dump();
                    break;

                case 'apply':
                    $service->apply();
                    break;

                default:
                    throw new CommandNotFoundException("Invalid action: {$action}");

            }

        }
        catch (DbJournalConfigException $e) {

            // @TODO: differentiate these functional error messages
            $output->writeln($e->getMessage());

        }

        if($buffer = $service->getBufferClean()) {
            $output->writeln($buffer);
        }

        $output->writeln("Execution time: {@TODO}");

        return $returnCode;
    }


}