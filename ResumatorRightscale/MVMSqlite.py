from ResumatorRightscale.LogWrapper import LogWrapper
import sys, time, datetime
import sqlite3

class DB(object):
    def __init__():
        pass

    def fetch_data(self, table_name, data={}):
        ## right now we're always selecting everything from the table. These table(s) will never
        ## grow larger than a handful of rows.
        sql = "SELECT %s FROM %s" % (', '.join(self._get_struct(table_name).keys()), table_name)
        
        if data:
            sql = sql + " WHERE %s" % ' AND '.join("%s = ?" % key for key in data.keys())
            self.cur.execute(sql, data.values())
        else:
            self.cur.execute(sql)

        return self.cur.fetchall()

    def query(self, sql, values=()):
        if values:
            res = self._exec_sql(sql, values)
        else:
            res = self._exec_sql(sql)
        return self.cur.fetchall()

    """ Insert data into the specified table. Return the last insert id """
    def insert(self, table_name, data):
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (table_name, ', '.join(data.keys()), ', '.join(['?'] * len(data.keys())))
        if self._exec_sql(sql, data.values()):
            return self.cur.lastrowid
        return 0
    
    """ 
        Update data in the specified table. provide an optional 'where' dictionary if need be.
        Return the affected rowcount
    """
    def update(self, table_name, update, where={}):
        sql_where = ''
        if where:
            sql_where = " WHERE " + ' AND '.join("%s = '%s'" % (key, val) for key, val in where.iteritems())
        sql = "UPDATE %s SET %s %s" % (table_name, ', '.join("%s = ?" % key for key in update.keys()), sql_where)
        if self._exec_sql(sql, update.values()):
            return self.cur.rowcount
        return 0

    """
        Delete a row from the specified table. the where clause is required at this time.
        Return the affected rowcount
    """
    def delete(self, table_name, data):
        sql = "DELETE FROM %s WHERE %s" % (table_name, ' AND '.join("%s = ?" % key for key in data.keys()))
        if self._exec_sql(sql, data.values()):
            return self.cur.rowcount
        return 0

"""
    class MVMSqlite
    This is just meant to make things easier on the front-end.
    Everything should be using parameter substitution (bound parameters).
    Right now we only need one table to hold our information but this was written
    to handle more tables as requirements dictate.
"""

class MVMSqlite(DB):
    def __init__(self, db_file='/mnt/data/mvm.db'):
        self.conn   = sqlite3.connect(db_file)
        self.conn.row_factory = sqlite3.Row
        self.cur    = self.conn.cursor()

        self.structures     = {}
        self.structures['deployment']   = {
            'id':                   'integer primary key',
            'key':                  'text',
            'name':                 'text',
            'domain':               'text',
            'user_email':           'text',
            'timestamp_created':    'real',
            'timestamp_updated':    'real',
            'requested_time_hours': 'integer',
            'auto_tear_down':       'integer'
        }
        self.structures['server']       = {
            'id':                   'integer primary key',
            'deployment_id':        'integer',
            'server_nickname':      'text',
            'server_type':          'text',
            'state':                'text',
            'timestamp_created':    'real',
            'timestamp_updated':    'real'
        }

        logger                  = LogWrapper('/var/log/rightscale_api', self.__class__.__name__)
        self.logger             = logger.get_logger()

    """ Get a table's structure. Log an error if the table doesn't exist """
    def _get_struct(self, table_name):
        if table_name in self.structures:
            return self.structures[table_name]
        self.logger.error("%s not in self.structures" % table_name)
        return {}

    """ execute sql that adds/changes data """
    def _exec_sql(self, sql, data={}):
        try:
            if data:
                self.cur.execute(sql, data)
            else:
                self.cur.execute(sql)
        except sqlite3.Error, e:
            self.logger.error(e)
            if self.conn:
                self.conn.rollback()
            return False
        finally:
            if self.conn:
                self.logger.debug("Committing: %s, data: %s" % (sql, data))
                self.conn.commit()
                return True

    """ Create a non-existent table """
    def _create_table(self, table_name):
        if not table_name in self.structures:
            raise KeyError("%s not in self.structures" % table_name)
        sql = "CREATE TABLE %s(%s)" % (table_name, ', '.join("%s %s" % (key, val) for key, val in self._get_struct(table_name).iteritems()))
        if self._exec_sql(sql):
            self.logger.info("Created %s table" % table_name)
        else:
            self.logger.error("Unable to create table: %s" % table_name)

    """ Does a table exist ? """
    def table_exists(self, table_name):
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        self.cur.execute(sql, (table_name,))
        res = self.cur.fetchone()
        ## redundant, but paranoid...
        if res and res['name'] == table_name:
            return True
        return False

    """ bootstrap a table - create it if it doesn't exist yet... """
    def bootstrap(self, table_name):
        if not self.table_exists(table_name):
            self._create_table(table_name)

    """
        auto_tear_down will default to 'on'. This function exists to switch this on and off depending
        on the needs of the user for the deployment.
    """
    def set_auto_tear_down(self, on_off, where={}):
        ## basically... set this to 'off' if we send anything other than 'on' or 1
        update = {'auto_tear_down': (1 if on_off == 'on' or on_off == 1 else 0)}
        return self.update('deployment', update, where)

    """ Simple function to convert the timestamp to a datetime """
    #def timestamp_to_datetime(self, timestamp, format="%Y-%m-%d %I:%M:%S %p"):
    def timestamp_to_datetime(self, timestamp):
        if timestamp:
            #return datetime.datetime.fromtimestamp(int(timestamp)).strftime(format)
            return datetime.datetime.fromtimestamp(int(timestamp))
        return ''

    """ Override parent to update the 'updated' timestamp... call parent method """
    def update(self, table_name, update, where={}):
        update['timestamp_updated'] = time.time()
        return super(MVMSqlite, self).update(table_name, update, where)






