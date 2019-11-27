Introduction
============

How much user data would you lose if any of your production databases crashes? Do you even know the last time your database backup ran successfully?

What if you need to synchronize user data created and updated, on specific tables during a given period of time, between an UAT and a production server? 

DbJournal allows you to have a _journal_ - think of it as a SQL ledger - anywhere you want with all the _INSERT_ and _UPDATE_ (_DELETE_ coming soon) queries ready to run and quickly shift your databases to a more up to date state.   

Useful Cases
============

#### Real-time data backup

DbJournal can be used as a data backup resource to allow time-framed data restorations when your main database dump backup can't help. 

For example, if you need to restore your database 5 hours after the last backup dump was generated, that would mean 5 hours of user or application generated data lost. 

That's when you can use DbJournal by _running the journal_ to recover every INSERT and UPDATE transactions that were ran on the database from the moment of the last backup until the last journal update (which can happen much more frequently as it consists in a light command that won't compromise your database performance or availability).

#### Database Synchronization

If you have different database instances on different environments and need to write

For example, if you use a CMS on a multi environment setup and all the yet-to-be-released changes made through the CMS are done in a centralized internal database, how do you publish those changes to the production database upon release?

DbJournal allows exporting (see the [dump](dump.md) command) all the queries that the CMS ran on that database for a given period of time / specific tables so you can run it on the production database making it up to date with the internal changes without losing any production data. 
