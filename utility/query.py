"""
Query Datasources

Only MySQL is implemented at the present time.
"""


import threading

import query_mysql


# Only allow 1 Query at a time, to defeat problems with threading
#TODO(g): Make a pooling system that allows more activity while protecting against thread collisions
QUERY_LOCK = threading.Lock()


def Query(sql, config):
  """Query the datasources specified in the config"""
  global QUERY_LOCK
  
  try:
    # Lock
    QUERY_LOCK.acquire()
    
    if config['datasource']['type'] == 'mysql':
      result = query_mysql.Query(sql, config['datasource'])
    
    else:
      raise Exception('Unknown datasource type: %s' % config['type'])
  
  finally:
    # Unlock, always
    QUERY_LOCK.release()
  
  return result
  

def SanitizeSQL(text):
  if text == None:
    text = 'NULL'
  else:
    text = str(text)
  
  return text.replace("'", "''").replace('\\', '\\\\')

