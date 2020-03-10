# ansible-module-mysql-migration

Ansible module for mysql migration, Runs database migration on MySQL server. It reads a mysql SQL scripts from sources (folder) and applies them in correct order to a database.

This is preety useful module as we need to use thrid party database migration tools.
This module stores data in mysql table itslef to have what migration scripts already runned or on which stage our database migration is, it will then only apply new sql scripts.

## Funtions Supported

Before discussing on feature support we can share some terminology of database migration. Lets say we have 5 different script (sql scripts on version) for our complete database. So `up` means we are going forward to database version; `down` means we are moving backward in database version

1. up: number of Version up 
2. down: number of Version down
3. goto: Want to goto specific version
4. drop: Drop a database
5. all -> want to go to latest version (means apply all migration version)

Detailed information can be found in code docs itself [library/mysql_database_migration.py](./library/mysql_database_migration.py)

**Ansible version checked**
1. Ansible 2.7

## Author

Varun Palekar 
github: varunpalekar
