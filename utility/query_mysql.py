"""
Query MySQL datasource
"""

import mysql.connector

from log import log


CONNECTIONS = {}
CURSORS = {}

# Maximum reties, handles connection failures
MAX_RETRIES = 3


def MaxRetryFailure(Exception):
  """Failed to do the query requested."""


class MySQLCursorDict(mysql.connector.cursor.MySQLCursor):
    def _row_to_python(self, rowdata, desc=None):
        row = super(MySQLCursorDict, self)._row_to_python(rowdata, desc)
        if row:
            return dict(zip(self.column_names, row))
        return None


def GetConectionAndCursor(datasource):
  global CONNECTIONS
  global CURSORS
  
  key = '%s.%s.%s' % (datasource['host'], datasource['user'], datasource['database'])
  
  if key not in CONNECTIONS or CONNECTIONS[key] == None:
    CONNECTIONS[key] = mysql.connector.connect(user=datasource['user'], password=datasource['password'], host=datasource['host'], database=datasource['database'])
    CURSORS[key] = CONNECTIONS[key].cursor(cursor_class=MySQLCursorDict)
  
  return (CONNECTIONS[key], CURSORS[key])


def CloseConnection(datasource):
  global CONNECTIONS
  global CURSORS
  
  key = '%s.%s.%s' % (datasource['host'], datasource['user'], datasource['database'])

  # Close cursor and connection, if they exist
  if key in CONNECTIONS and CONNECTIONS[key] != None:
    CURSORS[key].close()
    CONNECTIONS[key].close()
  
  # Set keys to None.  We created them, and closed them.
  CONNECTIONS[key] = None
  CURSORS[key] = None


def Query(sql, datasource):
  attempt = 0
  
  failures = []
  
  while attempt < MAX_RETRIES:
    # Keep track of how many times we attempt to perform this query
    attempt +=1
    
    try:
      (conn, cursor) = GetConectionAndCursor(datasource)
      
      cursor.execute(sql)
      
      if sql.upper().startswith('INSERT'):
        result = cursor.lastrowid
        conn.commit()
      elif sql.upper().startswith('UPDATE') or sql.upper().startswith('DELETE'):
        conn.commit()
        result = None
      elif sql.upper().startswith('SELECT'):
        result = cursor.fetchall()
      else:
        result = None
      
      break
    
    except mysql.connector.errors.Error, e:
      msg = 'MySQL error: %s: %s' % (e, sql)
      failures.append(msg)
      log(msg)
      
      #TODO(g): Only do this with connection related issues.  Need to work those out, because the docs suck.
      CloseConnection(datasource)
  
  if attempt >= MAX_RETRIES:
    raise Exception('Failed: %s -- Messages: %s ---- %s' % (attempt, sql, failures))
  
  return result

