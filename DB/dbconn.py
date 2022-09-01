from config import Config
import pymysql
 
class mysqlDB:
    def __init__(self, dbname):
        if dbname.lower() == 'ts':
            self._conn=pymysql.Connect(
                user=Config.DATABASE_CONFIG_TS['user'],
                password=Config.DATABASE_CONFIG_TS['password'],
                host=Config.DATABASE_CONFIG_TS['server'],
                port=3306,
                database=Config.DATABASE_CONFIG_TS['dbname']
            )
        self._cursor = self._conn.cursor()
 
    def __enter__(self):
        return self
 
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
 
    @property
    def connection(self):
        return self._conn
 
    @property
    def cursor(self):
        return self._cursor
 
    def commit(self):
        self.connection.commit()
 
    def close(self, commit=True):
        if commit:
            self.commit()
        self.connection.close()
 
    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())
 
    def fetchall(self):
        return self.cursor.fetchall()
 
    def fetchone(self):
        return self.cursor.fetchone()
 
    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()
 
    def rows(self):
        return self.cursor.rowcount