#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function
__metaclass__ = type

DOCUMENTATION = r'''
---
module: mysql_database_migration
short_description: Runs a database migration on Mysql database
description:
   - Runs database migration on MySQL server. It reads a mysql SQL scripts from sources (folder) and applies them in correct order to a database
version_added: "0.0.1"
options:
  source:
    description:
      - migration folder location
    type: path
    default: ""
    required: true

  database_name:
    description:
      - database name to which migration needs to be done
    type: str
    default: ""
    required: true

  up:
    description:
      - Run migration upward
    type: int
    default: 0
    required: false

  down: 
    description:
      - Run migration rollback
    type: int
    default: 0
    required: false

  goto:
    description:
      - Stuck to specific version to the migration, it takes care if version is above then run down and if version below then run up
    type: int
    default: 0
    required: false

  drop:
    description:
      - Drop all things inside a database
    type: bool
    default: false
    required: false

  migration_table:
    description:
      - migration table to store migration information
    type: string
    default: migrate
    required: false


notes:
   - "MySQL server installs with default login_user of 'root' and no password. To secure this user
     as part of an idempotent playbook, you must create at least two tasks: the first must change the root user's password,
     without providing any login_user/login_password details. The second must drop a ~/.my.cnf file containing
     the new root credentials. Subsequent runs of the playbook will then succeed by reading the new credentials from
     the file."
   - Folder structure of source folder need to follow below tree structure

      ├── 1_user_entry.down.sql
      ├── 1_user_entry.up.sql
      ├── 2_location_update.down.sql
      └── 2_location_update.up.sql

author:
- Varun Palekar (@varunpalekar)
extends_documentation_fragment: mysql
'''

EXAMPLES = r'''
- name: Run database migration stored in folder /tmp/database on server
  mysql_database_migration:
    source: /tmp/database
    database_name: test
    up: 0

- name: Run database migration stored in folder /tmp/database on server to increment by two only
  mysql_database_migration:
    source: /tmp/database
    database_name: test
    up: 2

- name: Drop database
  mysql_database_migration:
    database_name: test
    drop: true

- name: Run rollback migration in down scripts stored in folder /tmp/database on server to decrease by two only
  mysql_database_migration:
    source: /tmp/database
    database_name: test
    down: 2

- name: Run migration to reach to specific version maintains in file
  mysql_database_migration:
    source: /tmp/database
    database_name: test
    goto: 8

- name: Run migration to reach to specific version maintains in file and use migrate_pre as migration table
  mysql_database_migration:
    source: /tmp/database
    migration_table: migrate_pre
    database_name: test
    goto: 8

  mysql_database_migration:
    source: /tmp/database
    migration_table: migrate_post
    database_name: test
    goto: 8

'''

RETURN = r'''
current_version:
  description: Previous current database migration version
  returned: always
  type: int
  sample: 5
updated_version:
  description: Migration version after running migration
  returned: always
  type: int
  sample: 10
'''

TESTING = r'''
- up
    - Simple up call 2
    - up call with 0
    - wrong SQL in migration script and check what will happen
- down
    - down 2
    - down with 0
    - wrong SQL in migration script and check what will happen
- drop
    - check drop database
- goto:
    - goto 2 versions up
    - goto 2 versions down
    - goto to 0 version
{
  "ANSIBLE_MODULE_ARGS" : {
    "login_user": "test",
    "login_password" : "password" ,
    "login_host": "192.168.94.100",

    "source" : "library/test",
    "database_name": "test",
    "goto" : 10
  }
}
'''

from ansible.module_utils.basic import AnsibleModule

import os, re, pymysql
from ansible.module_utils._text import to_native

try:
    import pymysql as mysql_driver
    _mysql_cursor_param = 'cursor'
except ImportError:
    try:
        import MySQLdb as mysql_driver
        import MySQLdb.cursors
        _mysql_cursor_param = 'cursorclass'
    except ImportError:
        mysql_driver = None

from ansible.module_utils._text import to_native

mysql_driver_fail_msg = 'The PyMySQL (Python 2.7 and Python 3.X) or MySQL-python (Python 2.X) module is required.'

# ===========================================
# Helping Functions.
#

class database:
    def __init__(self, module, migration_table, source_folder):
        self.module = module
        self.migration_table = migration_table
        self.source_folder = source_folder
        self.changed = False
        # self.migrations = [] -> init
        # self.updated_version = None
        # self.current_version = None -> init

    def init(self):
        self.migrations = self.load_metadata_migration_files(self.source_folder)
        self.create_migration_table()
        self.validate_new_with_older_run()
        self.current_version = self.get_current_version()
        self.updated_version = self.current_version
        

    def mysql_connect(self, login_user=None, login_password=None, config_file='', ssl_cert=None, ssl_key=None, ssl_ca=None, db=None,
                    connect_timeout=30):
        config = {}

        if ssl_ca is not None or ssl_key is not None or ssl_cert is not None:
            config['ssl'] = {}

        if self.module.params['login_unix_socket']:
            config['unix_socket'] = self.module.params['login_unix_socket']
        else:
            config['host'] = self.module.params['login_host']
            config['port'] = self.module.params['login_port']

        if os.path.exists(config_file):
            config['read_default_file'] = config_file

        # If login_user or login_password are given, they should override the
        # config file
        if login_user is not None:
            config['user'] = login_user
        if login_password is not None:
            config['passwd'] = login_password
        if ssl_cert is not None:
            config['ssl']['cert'] = ssl_cert
        if ssl_key is not None:
            config['ssl']['key'] = ssl_key
        if ssl_ca is not None:
            config['ssl']['ca'] = ssl_ca
        if db is not None:
            config['db'] = db
        if connect_timeout is not None:
            config['connect_timeout'] = connect_timeout

        try:
            self.db_connection = mysql_driver.connect(**config)

        except Exception as e:
            self.module.fail_json(msg="unable to connect to database: %s" % to_native(e))

        self.cursor = self.db_connection.cursor(**{_mysql_cursor_param: mysql_driver.cursors.DictCursor})
        self.db_connection.autocommit = False


    def create_migration_table(self):
        query = "CREATE TABLE IF NOT EXISTS `%s` (`version` bigint(20) NOT NULL, `dirty` BOOLEAN DEFAULT 0, `name` varchar(255), PRIMARY KEY (`version`) );" % self.migration_table
        try:
            self.cursor.execute(query)
            self.db_connection.commit()
        except (mysql_driver.Error) as e:
            self.db_connection.rollback()
            self.module.fail_json(msg="Mysql Error: "+str(e))

    def get_current_version(self):
        query = "Select MAX(version) as current_version from `%s` where dirty IS FALSE;" % self.migration_table
        try:
            self.cursor.execute(query)
            return self.cursor.fetchone()['current_version']
        except (mysql_driver.Error) as e:
            self.module.fail_json(msg="Mysql Error: "+str(e))

    def validate_new_with_older_run(self):
        query = "Select * from `%s` order by version;" % self.migration_table
        try:
            self.cursor.execute(query)
            row = self.cursor.fetchone()
            pos = 0

            while row is not None:
                #1. validate version exists
                if self.migrations[pos][0] != row['version']:
                    self.module.fail_json(msg="version conflict version:%s not found having name %s" % (self.migrations[pos][0], self.migrations[pos][1]['name']))

                #2. validate name exists in that version
                if self.migrations[pos][1]['name'] != row['name']:
                    self.module.fail_json(msg="version conflict version:%s name:%s not matching with older name: %s" % (self.migrations[pos][0] , self.migrations[pos][1]['name'], row['name']))
                
                pos +=1
                row = self.cursor.fetchone()

        except (mysql_driver.Error) as e:
            self.module.fail_json(msg="Mysql Error: "+str(e))

    def load_metadata_migration_files(self, source):
        migrations = {}
        regex = '^([0-9]+)_(.*)\.(' + "down" + '|' + "up" + ')\.(.*)$'
        for f in os.listdir(source):
            if os.path.isfile(os.path.join(source, f)):
                reg_out = re.findall(regex, f)
                version = int(reg_out[0][0])
                name = reg_out[0][1]
                direction = reg_out[0][2]
                if version in migrations.keys():
                    migrations[version].update({
                        "name": name,
                        direction: f
                    })
                else: 
                    migrations[version] = {
                        "name": name,
                        direction: f
                    }
        return sorted(migrations.items(), key = lambda i: i[0])

    def migrate_up(self, up=None):
        iteration = 0
        for migrate in self.migrations:
            self.updated_version = migrate[0]
            if self.current_version >= migrate[0]:
                continue
            if (iteration >= up) and (up is not 0):
                break
            try:
                if 'up' not in migrate[1]:
                    self.module.fail_json(msg="UP script not found for version: %s" % migrate[0] )
                
                #1. Run migration script
                sql_file = os.path.join(self.source_folder, migrate[1]['up'])
                with open(sql_file) as f:
                    self.cursor.execute( f.read().decode('utf-8') , None)
                
                #2. Add entry in version table
                query = "Insert INTO `%s`( version, name ) VALUES (%s, '%s') ON DUPLICATE KEY UPDATE dirty=FALSE, name = '%s';" % (self.migration_table, migrate[0], migrate[1]['name'], migrate[1]['name'] )
                self.cursor.execute(query)
                self.db_connection.commit()
                iteration += 1
                self.changed = True
            except (mysql_driver.Error) as e:
                self.db_connection.rollback()
                try:
                    if 'down' in migrate[1]:
                        sql_file = os.path.join(self.source_folder, migrate[1]['down'])
                        with open(sql_file) as f:
                            self.cursor.execute( f.read().decode('utf-8') , None)
                            self.db_connection.commit()
                except Exception as e:
                    self.db_connection.rollback()
                    query = "Insert INTO `%s`( version, name, dirty ) VALUES (%s, '%s', TRUE );" % (self.migration_table, migrate[0], migrate[1]['name'] )
                    self.module.fail_json(msg="Mysql Error: Need manual intervention, version: %s got dirty   %s" % ( migrate[0], str(e)) )
                    self.cursor.execute(query)
                    self.db_connection.commit()
                self.module.fail_json(msg="Mysql Error: version: %s -- %s" % ( migrate[0], str(e)) )


    def drop(self, db):
        query = "DROP DATABASE %s;" % db
        try:
            self.cursor.execute(query)
            self.db_connection.commit()
        except (mysql_driver.Error) as e:
            self.db_connection.rollback()
            self.module.fail_json(msg="Mysql Error: %s" % e)


    def migrate_down(self, down):
        iteration = 0
        for migrate in reversed(self.migrations):
            if self.current_version < migrate[0]:
                continue
            if (iteration >= down) and (down is not 0):
                break
            try:
                if 'down' not in migrate[1]:
                    self.module.fail_json(msg="Down script not found for version: %s" % migrate[0] )
                
                #1. Run migration script
                sql_file = os.path.join(self.source_folder, migrate[1]['down'])
                with open(sql_file) as f:
                    self.cursor.execute( f.read().decode('utf-8') , None)
                
                #2. Delete entry in version table
                query = "DELETE FROM `%s` where version = %s and name = '%s'" % (self.migration_table, migrate[0], migrate[1]['name'] )
                self.cursor.execute(query)
                self.db_connection.commit()
                iteration += 1
                self.updated_version = migrate[0]
                self.changed = True
            except (mysql_driver.Error) as e:
                self.db_connection.rollback()
                try:
                    if 'up' in migrate[1]:
                        sql_file = os.path.join(self.source_folder, migrate[1]['up'])
                        with open(sql_file) as f:
                            self.cursor.execute( f.read().decode('utf-8') , None)
                            self.db_connection.commit()
                except Exception as e:
                    self.db_connection.rollback()
                    query = "Insert INTO `%s`( version, name, dirty ) VALUES (%s, '%s', TRUE) ON DUPLICATE KEY UPDATE dirty=TRUE, name = '%s';" % (self.migration_table, migrate[0], migrate[1]['name'], migrate[1]['name'] )
                    self.module.fail_json(msg="Mysql Error: Need manual intervention, version: %s got dirty   %s" % ( migrate[0], str(e)) )
                    self.cursor.execute(query)
                    self.db_connection.commit()
                self.module.fail_json(msg="Mysql Error: version: %s -- %s" % ( migrate[0], str(e)) )


    def migrate_goto(self, goto):
        # iterate from self.cureent_vertion to goto
        if self.current_version < goto:
            up = [migrate[0] for migrate in self.migrations if migrate[0] > self.current_version and migrate[0] <= goto ]
            if goto not in up:
                self.module.fail_json(msg="Goto version: %s not found" % goto)
            self.migrate_up(len(up))
        elif self.current_version > goto:
            down = [ migrate[0] for migrate in reversed(self.migrations) if self.current_version > migrate[0] and migrate[0] >= goto ]
            if goto not in down:
                self.module.fail_json(msg="Goto version: %s not found" % goto)
            self.migrate_down(len(down))
        else:
            self.changed = False


# ===========================================
# Module execution.
#

def main():
    module = AnsibleModule(
        argument_spec=dict(
            login_user=dict(type='str'),
            login_password=dict(type='str', no_log=True),
            login_host=dict(type='str', default='localhost'),
            login_port=dict(type='int', default=3306),
            login_unix_socket=dict(type='str'),
            client_cert=dict(type='path', aliases=['ssl_cert']),
            client_key=dict(type='path', aliases=['ssl_key']),
            ca_cert=dict(type='path', aliases=['ssl_ca']),
            connect_timeout=dict(type='int', default=30),
            config_file=dict(type='path', default='~/.my.cnf'),
            # single_transaction=dict(type='bool', default=False),

            source=dict(type='path', required=True),
            database_name=dict(type='str', required=True),
            migration_table=dict(type='str', default='migration'),
            up=dict(type='int'),
            down=dict(type='int'),
            goto=dict(type='int'),
            drop=dict(type='bool'),
        )
    )

    if mysql_driver is None:
        module.fail_json(msg=mysql_driver_fail_msg)

    login_user = module.params["login_user"]
    login_password = module.params["login_password"]
    login_host = module.params["login_host"]
    login_port = module.params["login_port"]
    if login_port < 0 or login_port > 65535:
        module.fail_json(msg="login_port must be a valid unix port number (0-65535)")
    socket = module.params["login_unix_socket"]
    ssl_cert = module.params["client_cert"]
    ssl_key = module.params["client_key"]
    ssl_ca = module.params["ca_cert"]
    connect_timeout = module.params['connect_timeout']
    config_file = module.params['config_file']
    
    source = module.params["source"]
    database_name = module.params["database_name"]
    migration_table = module.params["migration_table"]
    up = module.params["up"]
    down = module.params["down"]
    goto = module.params["goto"]
    drop = module.params["drop"]

    changed = False

    db = database(module=module, migration_table=migration_table, source_folder=source)

    try:
        db.mysql_connect(login_user, login_password, config_file, ssl_cert, ssl_key, ssl_ca,
                               connect_timeout=connect_timeout, db=database_name)
    except Exception as e:
        if os.path.exists(config_file):
            module.fail_json(msg="unable to connect to database, check login_user and login_password are correct or %s has the credentials. "
                                 "Exception message: %s" % (config_file, to_native(e)))
        else:
            module.fail_json(msg="unable to find %s. Exception message: %s" % (config_file, to_native(e)))

    if drop is True:
        db.drop(db=database_name)

    if not os.path.isdir(source):
        module.fail_json(msg="%s must be valid folder" % source )
    db.init()

    if up is not None:
        db.migrate_up(up=up)
    elif down is not None:
        db.migrate_down(down=down)
    elif goto is not None:
        db.migrate_goto(goto=goto)
    
    module.exit_json(changed=db.changed, msg={
        'current_version': db.current_version,
        'updated_version': db.updated_version
    })

if __name__ == '__main__':
    main()
