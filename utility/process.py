"""
Processing
"""


import re

from log import log
from path import *


def ProcessFile(file_path, file_data):
  """Process this file, based on it's current position and spec."""
  log('Processing: %s' % file_path)
  
  count = 0
  
  previous_line_data = None
  
  for line in file_data['fp']:
    line_data = {'line':line}
    
    #print line
    
    # Test if this line is multi-line
    is_multi_line = False
    for multi_line_test_regex in file_data['spec_data']['multi line regex test']:
      if re.match(multi_line_test_regex, line):
        is_multi_line = True
        #print 'Multiline: %s' % line
        # If we have a real previous line to embed this data in
        if previous_line_data != None:
          if 'multiline' not in previous_line_data:
            previous_line_data['multiline'] = []
          
          previous_line_data['multiline'].append(line)
        break

    # Only process rules on first lines (not multi lines)    
    if not is_multi_line:
      for process_rule in file_data['spec_data']['process']:
        ProcessTextRule(line, line_data, process_rule)
      
      #DEBUG
      if previous_line_data != None and 'multiline' in previous_line_data:
        #print 'Multi: %s' % previous_line_data
        pass
      
      # Move forward in our multi-line handling variable
      previous_line_data = line_data
    
    #print 'Result: %s' % line_data
    #print
    
    #TESTING - Limit runs
    count += 1
    #if count > 10000:
    #  break
    
    if count % 50 == 0:
      print '...%s...' % count
  
    #sys.exit(1)
    

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