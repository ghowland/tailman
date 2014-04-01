#!/usr/bin/env python2
"""
TailMan - Manage tailing multiple files, parsing and relaying raw and/or summary data to remote systems
"""


import sys
import os
import getopt
import yaml
import glob
import re

import utility
from utility.log import log
from utility.error import Error


def TailLogsFromSpecs(options, spec_paths):
  """Begin to tail files based on their specs."""
  # Load the specs
  specs = {}
  
  # Load our specs
  for spec_path in spec_paths:
    specs[spec_path] = yaml.load(open(spec_path))
  
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
  
  for line in file_data['fp']:
    line_data = {'line':line}
    
    #print line
    
    for process_rule in file_data['spec_data']['process']:
      ProcessTextRule(line, line_data, process_rule)
      
    #print 'Line Result: %s' % line_data
    #print
    
    #TESTING - Limit runs
    count += 1
    if count > 1000:
      break
    
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
      
      for part_index in part_indexes:
        key_parts.append(parts[part_index])
      
      line_data[key] = ' '.join(key_parts)
      
  
  # Replace
  elif process_rule['type'] == 'replace':
    # Perform replacement on each term we match
    for match in process_rule['match']:
      # Match -> Replaced (usually deleting things out)
      line_data[process_rule['key']] = line_data[process_rule['key']].replace(match, process_rule['replace'])
  
  # Match
  elif process_rule['type'] == 'match':
    database = yaml.load(open(process_rule['database']))
    
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
      print 'MISSING: %s' % line_data[process_rule['key']]
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

def Usage(error=None):
  """Print usage information, any errors, and exit.  

  If errors, exit code = 1, otherwise 0.
  """
  if error:
    print '\nerror: %s' % error
    exit_code = 1
  else:
    exit_code = 0
  
  print
  print 'usage: %s <--server/--client> [options] <spec_file_1> [spec_file_2] [spec_file_3] ...' % os.path.basename(sys.argv[0])
  print
  print 'Options:'
  print
  print '  -h, -?, --help          This usage information'
  print '  -c, --client            Client: Collect logs and relay them'
  print '  -s, --server            Server: Collect logs and relay them'
  print '      --no-relay          Dont relay any logs to another host.  Process them here.'
  print
  print '  -v, --verbose           Verbose output'
  print
  
  sys.exit(exit_code)


def Main(args=None):
  if not args:
    args = []

  
  long_options = ['help', 'client', 'server', 'no-relay']
  
  try:
    (options, args) = getopt.getopt(args, '?hvcs', long_options)
  except getopt.GetoptError, e:
    Usage(e)
  
  # Dictionary of command options, with defaults
  command_options = {}
  command_options['no-relay'] = False
  command_options['client'] = False
  command_options['server'] = False
  
  
  # Process out CLI options
  for (option, value) in options:
    # Help
    if option in ('-h', '-?', '--help'):
      Usage()
    
    # Verbose output information
    elif option in ('-v', '--verbose'):
      command_options['verbose'] = True
    
    # Client?
    elif option in ('-c', '--client'):
      command_options['client'] = True
    
    # Server?
    elif option in ('-s', '--server'):
      command_options['client'] = True
    
    # Invalid option
    else:
      Usage('Unknown option: %s' % option)


  if not command_options['client'] and not command_options['server']:
    Usage('Must specify Client (-c/--client) or Server (-s/--server), or --no-relay')
  
  # Store the command options for our logging
  utility.log.RUN_OPTIONS = command_options
  
  
  # Ensure we at least have one spec file
  if len(args) < 1:
    Usage('No spec files specified')
  
  # If there are any command args, get them
  command_args = args
  
  # Process the command
  if 1:
  #try:
    # Process the command and retrieve a result
    TailLogsFromSpecs(command_options, command_args)
  
  #NOTE(g): Catch all exceptions, and return in properly formatted output
  #TODO(g): Implement stack trace in Exception handling so we dont lose where this
  #   exception came from, and can then wrap all runs and still get useful
  #   debugging information
  #except Exception, e:
  else:
    Error({'exception':str(e)}, command_options)


if __name__ == '__main__':
  #NOTE(g): Fixing the path here.  If you're calling this as a module, you have to 
  #   fix the utility/handlers module import problem yourself.
  sys.path.append(os.path.dirname(sys.argv[0]))
  
  Main(sys.argv[1:])
