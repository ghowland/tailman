"""
TailMan - Manage tailing multiple files, parsing and relaying raw and/or summary data to remote systems
"""


import sys
import os
import getopt
import yaml


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
  print 'usage: %s [options] <spec_file_1> [spec_file_2] [spec_file_3] ...' % os.path.basename(sys.argv[0])
  print
  print
  print 'Options:'
  print
  print '  -h, -?, --help          This usage information'
  print '  -C, --commit            Commit changes.  No changes will be made, unless set.'
  print '      --hostgroups[=path] Path to host groups (directory)'
  print '      --deploy[=path]     Path to deployment files (directory)'
  print '      --packages[=path]   Path to package files (directory)'
  print '      --handlers[=path]   Path to handler default yaml data (directory)'
  print '      --buildas[=group]   Manually specify Host Group, cannot be in one already'
  print
  print '  -v, --verbose           Verbose output'
  print
  
  sys.exit(exit_code)


def Main(args=None):
  if not args:
    args = []

  
  long_options = ['help', 'output=', 'format=', 'verbose', 'hostgroups=', 
      'deploy=', 'packages=', 'bootstrap', 'commit', 'handlers=',
      'buildas=']
  
  try:
    (options, args) = getopt.getopt(args, '?hvo:f:bC', long_options)
  except getopt.GetoptError, e:
    Usage(e)
  
  # Dictionary of command options, with defaults
  command_options = {}
  command_options['commit'] = False
  command_options['bootstrap'] = False
  command_options['hostgroup_path'] = DEFAULT_HOST_GROUP_PATH
  command_options['deploy_path'] = DEFAULT_DEPLOY_PATH
  command_options['deploy_temp_path'] = DEFAULT_DEPLOY_TEMP_PATH
  command_options['package_path'] = DEFAULT_PACKAGE_PATH
  command_options['handler_data_path'] = DEFAULT_HANDLER_DEFAULT_PATH
  command_options['verbose'] = False
  command_options['build_as'] = None
  command_options['format'] = 'pprint'
  
  
  # Process out CLI options
  for (option, value) in options:
    # Help
    if option in ('-h', '-?', '--help'):
      Usage()
    
    # Verbose output information
    elif option in ('-v', '--verbose'):
      command_options['verbose'] = True
    
    # Commit changes for Install or Package
    #NOTE(g): If not set (False), no installation will be done, no packages
    #   will be created.  This will be a dry-run to test what would be 
    #   performed if commit=True
    elif option in ('-C', '--commit'):
      command_options['commit'] = True
    
    # Bootstrap this host?  Install "bootstrap packages" before "packages"
    elif option in ('-b', '--bootstrap'):
      command_options['bootstrap'] = True
    
    # Host Groups Path
    elif option in ('--hostgroups'):
      if os.path.isdir(value):
        command_options['hostgroup_path'] = value
      else:
        Error('Host Groups path specified is not a directory: %s' % value)
    
    # Deployment Path
    elif option in ('--deploy'):
      if os.path.isdir(value):
        command_options['deploy_path'] = value
      else:
        Error('Deployment path specified is not a directory: %s' % value)
    
    # Package Path
    elif option in ('--packages'):
      if os.path.isdir(value):
        command_options['package_path'] = value
      else:
        Error('Package path specified is not a directory: %s' % value)
    
    
    # Invalid option
    else:
      Usage('Unknown option: %s' % option)
  
  
  # Store the command options for our logging
  utility.log.RUN_OPTIONS = command_options
  
  
  # Ensure we at least have a command, it's required
  if len(args) < 1:
    Usage('No command sepcified')
  
  # Get the command
  command = args[0]
  
  # If this is an unknown command, say so
  if command not in COMMANDS:
    Usage('Command "%s" unknown.  Commands: %s' % (command, ', '.join(COMMANDS)))
  
  # If there are any command args, get them
  command_args = args[1:]
  
  # Process the command
  if 1:
  #try:
    # Process the command and retrieve a result
    result = ProcessCommand(command, command_options, command_args)
    
    # Format and output the result (pprint/json/yaml to stdout/file)
    FormatAndOuput(result, command_options)
  
  #NOTE(g): Catch all exceptions, and return in properly formatted output
  #TODO(g): Implement stack trace in Exception handling so we dont lose where this
  #   exception came from, and can then wrap all runs and still get useful
  #   debugging information
  #except Exception, e:
  else:
    Error({'exception':str(e)}, command_options)



  (options, args) = getopt.getopt(args, )

if __name__ == '__main__':
  #NOTE(g): Fixing the path here.  If you're calling this as a module, you have to 
  #   fix the utility/handlers module import problem yourself.
  sys.path.append(os.path.dirname(sys.argv[0]))

	Main(sys.argv[1:])
