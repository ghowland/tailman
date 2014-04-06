"""
Processing

Notes:
  - time.strptime(time_format, '%Y-%m-%d %H:%M:%S.%f')
"""


import re
import time

from log import log
from path import *
from query import *


def ProcessTextRule(line, line_data, process_rule):
  """Updates line_data based on the rules."""
  # Split
  if process_rule['type'] == 'split':
    split_data = process_rule['split']
    
    #print split_data
    #print
    
    parts = line_data[process_rule['key']].split(split_data['separator'], split_data.get('max split', -1))
    
    #print parts
    
    for (key, part_indexes) in split_data['values'].items():
      key_parts = []
      
      try:
        for part_index in part_indexes:
          key_parts.append(parts[part_index])
      except IndexError, e:
        print 'WARNING: Part not found: %s: %s: %s' % (part_index, parts, line_data[process_rule['key']])
      
      line_data[key] = ' '.join(key_parts)
      
  
  # Replace
  elif process_rule['type'] == 'replace':
    # Perform replacement on each term we match
    for match in process_rule['match']:
      # Match -> Replaced (usually deleting things out)
      line_data[process_rule['key']] = line_data[process_rule['key']].replace(match, process_rule['replace'])
  
  # Delete
  elif process_rule['type'] == 'delete':
    if process_rule['key'] in line_data:
      del line_data[process_rule['key']]
  
  # Match
  elif process_rule['type'] == 'match':
    database = LoadYaml(process_rule['database'])
    
    match_found = False
    
    for item in database:
      terms = re.findall('%\((.*?)\)s', item['regex'])
      #print item['regex']
      #print terms
      
      regex = item['regex']
      
      # Pre-processing step, to remove any conflicting characters with the rest of the regex which need to be escaped/sanitized
      for term in terms:
        regex = regex.replace('%%(%s)s' % term, 'MATCHMATCHMATCH')
        
      regex = SanitizeRegex(regex)
      regex = regex.replace('MATCHMATCHMATCH', '(.*?)')
      
      regex_result = re.findall(regex, line_data[process_rule['key']])
      if regex_result:
        
        # Python does something stupid with multiple variables, so pull them out of the embedded tuple it adds to the list
        if type(regex_result[0]) == tuple:
          regex_result = regex_result[0]
        
        for count in range(0, len(terms)):
          #print '%s: %s' % (count, regex_result)
          line_data[terms[count]] = regex_result[count]
        
        #print regex
        #print 'MATCHED! %s' % regex
        #print regex_result
        
        match_found = True
        break
    
    if not match_found:
      #print 'MISSING: %s' % line_data[process_rule['key']]
      pass
      
  
  # Convert
  elif process_rule['type'] == 'convert':
    if process_rule['target'] == 'integer':
      try:
        line_data[process_rule['key']] = int(line_data[process_rule['key']])
      
      except ValueError, e:
        #print 'WARNING: Bad formatting: %s: %s' % (process_rule['key'], line_data)
        pass
      
    else:
      raise Exception('Unknown covnert target type: %s: %s' % (line_data['spec_path'], process_rule['rule']))
  
  # Error - Misconfiguration
  else:
    raise Exception('Unknown process type: %s: %s' % (line_data['spec_path'], process_rule['rule']))


def SanitizeRegex(text):
  characters = '()[].*?'
  
  for character in characters:
    text = text.replace(character, '\\' + character)
  
  return text


def ProcessLine(line, processing, previous_line_data):
  """Process this life, based on it's current position and spec."""
  line_data = {'line':line}
  # Update with always-included data, like glob keys, and the component
  line_data.update(processing['data'])
  
  # Test if this line is multi-line
  is_multi_line = False
  for multi_line_test_regex in processing['spec_data']['multi line regex test']:
    if re.match(multi_line_test_regex, line):
      #print 'Multiline: %s' % line
      is_multi_line = True
      
      # If we have a real previous line to embed this data in
      if previous_line_data != None:
        if 'multiline' not in previous_line_data:
          previous_line_data['multiline'] = []
        
        previous_line_data['multiline'].append(line)
      break

  # Only process rules on first lines (not multi lines), and return the line_data to be the next line's previous_line_data
  if not is_multi_line:
    for process_rule in processing['spec_data']['process']:
      ProcessTextRule(line, line_data, process_rule)
    
    return line_data
  
  # Else, this is multi-line, so return it to continue to be the next line's previous_line_data
  else:
    #TODO(g): Save this multi-line data every time?  Otherwise when does it get saved out?
    pass
    
    return previous_line_data


def GetLatestLogFileInfo(processing):
  """Returns the latest log file on the server to write data into."""
  # Get the latest log file from the DB
  sql = "SELECT * FROM log_file WHERE host = '%s' AND host_path = '%s' ORDER BY updated DESC LIMIT 1" % \
        (SanitizeSQL(processing['host']), SanitizeSQL(processing['path']))
  latest_log_file = Query(sql, LoadYaml(processing['spec_data']['datasource config']))
  
  # If we did not receive a log file, create it
  if not latest_log_file:
    latest_log_file = CreateNewLogFile(processing)
  
  # Else, we got it, so extract from the single list
  else:
    latest_log_file = latest_log_file[0]
  
  return latest_log_file


def CreateNewLogFile(processing):
  """Create a new log file."""
  # Get the component ID
  component_id = GetComponentId(processing)
  
  sql = "INSERT INTO log_file (host, host_path, updated, component) VALUES ('%s', '%s', NOW(), %s)" % \
        (SanitizeSQL(processing['host']), SanitizeSQL(processing['path']), component_id)
  new_id = Query(sql, LoadYaml(processing['spec_data']['datasource config']))
  
  sql = "SELECT * FROM log_file WHERE id = %s" % new_id
  latest_log_file = Query(sql, LoadYaml(processing['spec_data']['datasource config']))[0]
  
  return latest_log_file


def SaveMultiLine(multi_line_data, processing):
  """Save multi-line entry.
  
  TODO(g): The SQL needs to be generalized, right now it is for a specific schema that is non-universal.
  """
  # Get the component ID
  component_id = GetComponentId(processing)
  
  if type(multi_line_data['service_id']) != int:
    service_id_value = 'NULL'
  else:
    service_id_value = multi_line_data['service_id']
  
  sql = "INSERT INTO log_exception (component, occurred, subcomponent, service_id, stack_trace, log_id, log_offset) VALUES (%s, '%s', '%s', %s, '%s', %s, %s)" % (\
        component_id, multi_line_data['occurred'], SanitizeSQL(multi_line_data['subcomponent']), service_id_value,
        SanitizeSQL(multi_line_data['multiline']), processing['latest_log_file']['id'], processing['offset_processed'])
  exception_id = Query(sql, LoadYaml(processing['spec_data']['datasource config']))
  
  return exception_id
  

def GetComponentId(processing):
  """Returns a matching component id from 'component' field in processing dict, or -1 if none were found.
  Cannot be NULL because component is a primary key part and MySQL doesnt like NULLs in primary keys
  
  TODO(g): The SQL needs to be generalized, right now it is for a specific schema that is non-universal.
  """
  sql = "SELECT * FROM component WHERE name = '%s'" % SanitizeSQL(processing['spec_data']['component'])
  component = Query(sql, LoadYaml(processing['spec_data']['datasource config']))
  
  # Get component_id (NULL or id)
  if not component:
    component_id = -1
  else:
    component_id = component[0]['id']
  
  return component_id


def SaveLineKeys(line_data, processing):
  """Save this line's key value pair data into the DB, along with log file ID and offset to be able to look at the log data directly."""
  for (key, value) in line_data.items():
    sql = "INSERT INTO log_key (`key`, occurred, value, log_id, log_offset) VALUES ('%s', '%s', '%s', %s, %s)" % \
          (SanitizeSQL(key), line_data['occurred'], SanitizeSQL(value), processing['latest_log_file']['id'], processing['offset_processed'])
    log_key_id = Query(sql, LoadYaml(processing['spec_data']['datasource config']))

