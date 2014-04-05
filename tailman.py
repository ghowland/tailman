#!/usr/bin/env python2
"""
TailMan - Manage tailing multiple files, parsing and relaying raw and/or summary data to remote systems
"""


import sys
import os
import getopt

import utility
from utility.log import log
from utility.error import Error
from utility.path import *
from utility.process_server import *
from utility.process_client import *
from utility.network_client import *
from utility.network_server import *


def StartServers(options, spec_paths):
  specs = {}
  
  # Load all the specs
  for spec_path in spec_paths:
    spec_data = LoadYaml(spec_path)
    
    specs[spec_path] = spec_data
  
  #TODO(g): Start up as many servers as is required for the number of spec files we are handling that have different ports
  ServerManager(specs, options)


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
  print '  -s port, --server=port  Server: Store logs we receive, based on specs'
  print '      --no-relay          Dont relay any logs to another host.  Process them here.'
  print '  -1, --once              Run once and exit.  To the end of each file from saved position.'
  print
  print '  -v, --verbose           Verbose output'
  print
  
  sys.exit(exit_code)


def Main(args=None):
  if not args:
    args = []

  
  long_options = ['help', 'client', 'server', 'no-relay', 'once']
  
  try:
    (options, args) = getopt.getopt(args, '?hvcs1', long_options)
  except getopt.GetoptError, e:
    Usage(e)
  
  # Dictionary of command options, with defaults
  command_options = {}
  command_options['no-relay'] = False
  command_options['client'] = False
  command_options['server'] = False
  command_options['run_once'] = False
  
  
  # Process out CLI options
  for (option, value) in options:
    # Help
    if option in ('-h', '-?', '--help'):
      Usage()
    
    # Verbose output information
    elif option in ('-v', '--verbose'):
      command_options['verbose'] = True
    
    # Run once?
    elif option in ('-1', '--once'):
      command_options['run_once'] = True
    
    # Client?
    elif option in ('-c', '--client'):
      command_options['client'] = True
    
    # Server?
    elif option in ('-s', '--server'):
      command_options['server'] = True
    
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
  

  # Server
  if command_options['server']:
    StartServers(command_options, args)
  
  # Else, Client
  elif command_options['client']:
  #try:
    # Process the command and retrieve a result
    TailLogsFromSpecs(command_options, args)
  
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
