<?php

declare(strict_types=1);

use \PHPUnit\Framework\TestCase;
use \Symfony\Component\Console\Application;
use \Symfony\Component\Console\Tester\CommandTester;
use \DbJournal\Commands\DbJournalCommand;

class ConsoleTest extends TestCase
{
    public function testCanRunConsole(): void
    {
        $this->assertEquals(true,true);
    }

    /**
     * @see https://symfony.com/doc/current/console.html#creating-a-command
     */
    public function testExecute()
    {
        $app = new Application();
        $app->add(new DbJournalCommand());

        $command = $app->find('db-journal:command');
        $commandTester = new CommandTester($command);
        $commandTester->execute([
            'command'  => $command->getName(),

            // pass arguments to the helper
            'action' => 'dump',

            // prefix the key with two dashes when passing options,
            // e.g: '--some-option' => 'option_value',
        ]);

        // the output of the command in the console
        $output = $commandTester->getDisplay();
        $this->assertEquals('Action: dump' . PHP_EOL, $output);

        // ...
    }
}