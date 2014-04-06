"""
Query Datasources

Only MySQL is implemented at the present time.
"""


import query_mysql


def Query(sql, config):
  """Query the datasources specified in the config"""
  if config['datasource']['type'] == 'mysql':
    result = query_mysql.Query(sql, config['datasource'])
    return result

  else:
    raise Exception('Unknown datasource type: %s' % config['type'])
  

def SanitizeSQL(text):
  if text == None:
    text = 'NULL'
  else:
    text = str(text)
  
  return text.replace("'", "''").replace('\\', '\\\\')

