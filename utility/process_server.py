"""
Processing

Notes:
  - time.strptime(time_format, '%Y-%m-%d %H:%M:%S.%f')
"""


import re
import time
import json
from decimal import Decimal
import stat
import os

from log import log
from path import *
from query import *


def ProcessTextRule(line_data, process_rule):
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
        log('WARNING: Part not found: %s: %s: %s' % (part_index, parts, line_data[process_rule['key']]))
      
      line_data[key] = ' '.join(key_parts)
      
  
  # Replace
  elif process_rule['type'] == 'replace':
    # Perform replacement on each term we match
    for match in process_rule['match']:
      # Match -> Replaced (usually deleting things out)
      #print 'Replacing: "%s" with "%s"' % (match, process_rule['replace'])
      #print line_data[process_rule['key']]
      line_data[process_rule['key']] = line_data[process_rule['key']].replace(match, process_rule['replace'])
      #print line_data[process_rule['key']]
  
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
        
        # Save the line match ID, so we can reference it for markup/state information
        line_data[process_rule['match key']] = item['id']
        
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
  line_data = {'line':line, 'line_offset':processing['offset_processed']}
  
  # Update with always-included data, like glob keys, and the component
  line_data.update(processing['data'])
  
  # Test if this line is multi-line (positive test)
  is_multi_line = False
  for multi_line_test_regex in processing['spec_data'].get('multi line regex test', []):
    if re.match(multi_line_test_regex, line):
      is_multi_line = True
      break
  # Negative regex test
  for multi_line_test_regex in processing['spec_data'].get('multi line regex not', []):
    if not re.match(multi_line_test_regex, line):
      is_multi_line = True
      break
  
  # If this is multi_line and we have a real previous line to embed this data in
  if is_multi_line and previous_line_data != None:
    #print 'Multiline: %s' % line
    if 'multiline' not in previous_line_data:
      previous_line_data['multiline'] = []
    
    previous_line_data['multiline'].append(line)


  # Only process rules on first lines (not multi lines), and return the line_data to be the next line's previous_line_data
  if not is_multi_line:
    for process_rule in processing['spec_data']['process']:
      ProcessTextRule(line_data, process_rule)
    
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
  #print sql
  latest_log_file = Query(sql, LoadYaml(processing['spec_data']['datasource config']))

  # Get the log size, if it exists.
  if latest_log_file and os.path.isfile(latest_log_file[0]['path']):
    log_size = os.stat(latest_log_file[0]['path'])[stat.ST_SIZE]
  else:
    log_size = 0

  # If we did not receive a log file, create it
  #TODO(g): Add test for the occurred time or checksum of the first 1024 bytes of the file.  Then we known its working.
  if not latest_log_file or log_size > processing['size']:
    latest_log_file = CreateNewLogFile(processing)
  
  # Else, we got it, so extract from the single list
  else:
    latest_log_file = latest_log_file[0]
  
  return latest_log_file


def UpdateLogFileInfo(processing):
  """Update the log file we are working on, we want to know the offset."""
  # Get current file position
  offset = processing['storage_path_fp'].seek(0, 1)
  
  # Update the log file entry
  sql = "UPDATE log_file SET updated = NOW(), remote_offset = %s, size = %s WHERE id = %s" % \
        (processing['offset_processed'], processing['size'], processing['latest_log_file']['id'])
  Query(sql, LoadYaml(processing['spec_data']['datasource config']))


def CreateNewLogFile(processing):
  """Create a new log file."""
  # Get the component ID
  component_id = GetComponentId(processing)
  
  sql = "INSERT INTO log_file (host, host_path, created, updated, component) VALUES ('%s', '%s', NOW(), NOW(), %s)" % \
        (SanitizeSQL(processing['host']), SanitizeSQL(processing['path']), component_id)
  new_id = Query(sql, LoadYaml(processing['spec_data']['datasource config']))
  
  # Update our path, which needs our ID
  storage_path = '%s/%s' % (processing['spec_data']['storage directory'] % processing, new_id)
  sql = "UPDATE log_file SET path = '%s' WHERE id = %s" % (SanitizeSQL(storage_path), new_id)
  Query(sql, LoadYaml(processing['spec_data']['datasource config']))
  
  # Get the log file data, we just created (includes defaults)
  sql = "SELECT * FROM log_file WHERE id = %s" % new_id
  latest_log_file = Query(sql, LoadYaml(processing['spec_data']['datasource config']))[0]
  
  # If the directory doesnt exist, create it
  if not os.path.isdir(os.path.dirname(latest_log_file['path'])):
    os.makedirs(os.path.dirname(latest_log_file['path']))
  
  return latest_log_file


def SaveMultiLine(multi_line_data, processing):
  """Save multi-line entry.
  
  TODO(g): The SQL needs to be generalized, right now it is for a specific schema that is non-universal.
  """
  # Prepend the first line to the multiline list
  multi_line_data['multiline'] = [multi_line_data['line']] + multi_line_data['multiline']
  
  # Get the component ID
  component_id = GetComponentId(processing)
  
  if type(multi_line_data['service_id']) != int:
    service_id_value = 'NULL'
  else:
    service_id_value = multi_line_data['service_id']


  # Convert occurred to decimal, so milliseconds are retained and we can sort and do range operations
  #NOTE(g): MySQL will chop milliseconds from datetimes, which is why this is required
  occurred = ConvertDateToDecimal(multi_line_data['occurred'])

  sql = "INSERT INTO log_multiline (component, occurred, subcomponent, service_id, `lines`, log_id, log_offset) VALUES  " + \
        "(%s, %s, '%s', %s, '%s', %s, %s)"
  
  sql = sql %(component_id, occurred, SanitizeSQL(multi_line_data['subcomponent']), service_id_value,
              SanitizeSQL(multi_line_data['multiline']), processing['latest_log_file']['id'],
              multi_line_data['line_offset'])
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


def SaveLine(line_data, processing):
  """Save this line's key value pair data into the DB, along with log file ID and offset to be able to look at the log data directly."""
  #for (key, value) in line_data.items():
  #  sql = "INSERT INTO log_key (`key`, occurred, value, log_id, log_offset) VALUES ('%s', '%s', '%s', %s, %s)" % \
  #        (SanitizeSQL(key), line_data['occurred'], SanitizeSQL(value), processing['latest_log_file']['id'], processing['offset_processed'])
  #  log_key_id = Query(sql, LoadYaml(processing['spec_data']['datasource config']))
  
  # Massage line data
  data = dict(line_data)
  
  #print line_data

  # Extract the message match ID, or use -1 for Unknown
  if 'message_match_id' in line_data:
    match_id = line_data['message_match_id']
    del line_data['message_match_id']
  else:
    match_id = -1
 
  occurred = line_data['occurred']
  del data['occurred']
  component = line_data['component']
  del data['component']
  subcomponent = line_data['subcomponent']
  del data['subcomponent']
  line_offset = line_data['line_offset']
  del data['line_offset']

  # Remove the full line, if we know we have a match (and have extracted data)
  if match_id != -1:
    del data['line']
  

  # Encode data in JSON for field storage
  data_json = json.dumps(data, sort_keys=True)
  
  # Convert occurred to decimal, so milliseconds are retained and we can sort and do range operations
  #NOTE(g): MySQL will chop milliseconds from datetimes, which is why this is required
  occurred = ConvertDateToDecimal(occurred)
  
  sql = "INSERT INTO log_line (occurred, log_id, match_id, log_offset, component, subcomponent, data_json) VALUES " + \
        "(%s, %s, %s, %s, %s, '%s', '%s')"
  sql = sql % (occurred, processing['latest_log_file']['id'], match_id, line_offset, component, SanitizeSQL(subcomponent),
               SanitizeSQL(data_json))
  new_log_id = Query(sql, LoadYaml(processing['spec_data']['datasource config']))


def ConvertDateToDecimal(datetime):
  """Acceptions datetime string, returns Decimal version with milliseconds retained"""
  time_pieces = re.findall('(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d) (\\d\\d):(\\d\\d):(\\d\\d.\\d+)', datetime)
  
  occurred = Decimal(time_pieces[0][0])  * Decimal(10000000000)      # Year
  occurred += Decimal(time_pieces[0][1]) * Decimal(100000000)        # Month
  occurred += Decimal(time_pieces[0][2]) * Decimal(1000000)          # Day
  occurred += Decimal(time_pieces[0][3]) * Decimal(10000)            # Hour
  occurred += Decimal(time_pieces[0][4]) * Decimal(100)              # Minute
  occurred += Decimal(time_pieces[0][5]) * Decimal(1)                # Seconds
  
  return occurred

