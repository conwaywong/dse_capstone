'''
Created on Dec 5, 2015

@author: dyerke

dependent on pg8000 sql api

pip install pg8000

'''

import pg8000
import traceback
import numpy as np
import pandas as pd

class StatementExecutorTemplateCallback:
    def __init__(self):
        self._mQuery = self._get_query()
        self.m_results= []
    
    def _get_query(self):
        raise NotImplementedError
    
    def _map_row(self, row):
        raise NotImplementedError
    
    def do_in_cursor(self, cur):
        cur.execute(self._mQuery)
        rows = cur.fetchmany(500)
        while len(rows) > 0:
            self.process_rows(rows)
            rows = cur.fetchmany(500)
        self.post_intercept(self.m_results)
        return self.m_results
    
    def process_rows(self, rows):
        for row in rows:
            self.m_results.append(self._map_row(row))
    
    def post_intercept(self, results):
        pass

class StatementExecutorTemplate:
    
    def __init__(self, db_name, username, password, hostname, port):
        self._mDbName = db_name
        self._mUsername = username
        self._mPassword = password
        self._mHostname = hostname
        self._mPort = port
    
    def execute(self, callback):
        conn = None
        cur = None
        try:
            conn = pg8000.connect(database=self._mDbName, user=self._mUsername, password=self._mPassword, host=self._mHostname, port=self._mPort)
            cur = conn.cursor()
            return callback.do_in_cursor(cur)
        except RuntimeError:
            traceback.print_exc()
            raise
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

def to_data_frame(results, column_names):
    if len(results) == 0:
        return None
    m_arr= np.array(results)
    m_df= pd.DataFrame.from_records(m_arr, columns=column_names)
    return m_df
        
