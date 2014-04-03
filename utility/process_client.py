"""
Processing
"""


import re

from log import log
from path import *

from process_server import *


def TailLogsFromSpecs(options, spec_paths):
  """Begin to tail files based on their specs."""
  # Load the specs
  specs = {}
  
  # Load our specs
  for spec_path in spec_paths:
    specs[spec_path] = LoadYaml(spec_path)
  
  # Load any previously stored state
  pass

  # Gather all our file handles and create our state
  input_files = {}
  for (spec_path, spec_data) in specs.items():
    #TODO(g): Handle other types of input besides glob?  If no reason, flatten it?  Input still has nice container for mark up data
    files = glob.glob(spec_data['input']['glob'])
    
    for file_path in files:
      # Open the file
      fp = open(file_path)
      
      #TODO(g): Seek to previously saved position
      position = 0
      
      # Store everything we know about this file, so we can work with it elsewhere
      input_files[file_path] = {'fp':fp, 'position': position, 'spec_path':spec_path, 'spec_data':spec_data}
  
  # Process all our files
  for (file_path, file_data) in input_files.items():
    ProcessFile(file_path, file_data)


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
    
