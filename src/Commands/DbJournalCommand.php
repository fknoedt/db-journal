<?php

namespace DbJournal\Commands;

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
     * @todo it
     * @var
     */
    var $attribute;

    public function __construct()
    {
        $this->attribute = 'value set';

        parent::__construct();
    }

    /**
     *
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
     *
     * @param InputInterface $input
     * @param OutputInterface $output
     * @throws \Symfony\Component\Console\Exception\ExceptionInterface
     * @return int
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        // main console action
        $action = $input->getArgument('action');

        $output->writeln("Action: " . $input->getArgument('action'));

        // `setup` will create the table, so let's not check for it
        $ignoreTable = ($action == 'setup');

        // checks on the constructor
        $service = new DbJournalService($ignoreTable);

        switch ($action) {

            case 'setup':
                $service->setup();
                $output->writeln("Table created - setup complete");
                break;

            case 'update-journal':
                $service->updateJournal();
                break;

            case 'dump-journal':
                $service->dumpJournal();
                break;

            case 'apply-journal':
                $service->applyJournal();
                break;

            default:
                throw new CommandNotFoundException("Invalid action: {$action}");

        }

        $output->writeln("Execution time: {@TODO}");
    }


}