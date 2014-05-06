"""
Processing
"""


import glob
import re
import stat
import os
import time
import socket

from log import log
from path import *

from process_server import *
from network_client import *

# Seconds to delay between runs
RUN_DELAY_SECONDS = 5


def TailLogsFromSpecs(options, spec_paths):
  """Wrapper for TailLogsFromSpecs_Once() to control if we keep running."""
  RUNNING = True
  
  while RUNNING:
    TailLogsFromSpecs_Once(options, spec_paths)
    
    # If we are only supposed to run once, quit
    if options['run_once']:
      log('Run once completed.  Quitting.')
      break
    else:
      log('Run completed.  Sleeping: %s seconds' % RUN_DELAY_SECONDS)
      time.sleep(RUN_DELAY_SECONDS)
  
  # Close all our connections
  CloseAllConnections()


def TailLogsFromSpecs_Once(options, spec_paths):
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
      #TODO(g): Seek to previously saved position
      position = 0
      
      # Store everything we know about this file, so we can work with it elsewhere
      input_files[file_path] = {'fp':None, 'position': position, 'spec_path':spec_path, 'spec_data':spec_data}
  
  
  # Process all our files
  for (file_path, file_data) in input_files.items():
    # Relay
    try:
      # Open the file
      fp = open(file_path)
      file_data['fp'] = fp
      
      RelayFile(file_path, file_data)
    
    except socket.error, e:
      log('Network Error: %s' % e)
    
    finally:
      # Close the file so we stay clean on file handles
      fp.close()


def RelayFile(file_path, file_data):
  """Relay this file to the server"""
  path_mtime = os.stat(file_path)[stat.ST_MTIME]
  path_size = os.stat(file_path)[stat.ST_SIZE]
  
  target_host = file_data['spec_data']['relay host']
  target_port = file_data['spec_data']['relay port']
  
  # Get the offset from the server, the last place we read from
  offset_data = ClientGetOffset(file_path, path_mtime, path_size, target_host, target_port)
  #log('Offset data: %s' % offset_data)
  
  #TODO(g): Use the queried data for this, not just starting at the beginning
  if offset_data['offset'] in (None, 'None'):
    path_offset = 0
  else:
    #NOTE(g):remote_offset is always +1 from contents we have checked.  Doing the stat test means subtracting 1 (-1) from remote_offset
    #   to compare against local file, because a newline is always assumed at the end, and if the file is 0 bytes, the offset will
    #   be 1.
    path_offset = int(offset_data['offset']) - 1
  
  # Set the position to start reading from, based on offset
  file_data['fp'].seek(path_offset, 0)
  
  # Read to the end of the file
  text = file_data['fp'].read()
  
  # Split off the last unfinished sentence, and re-seek() to set offset back to correct position
  pass
  
  # If the file has reached its end, stat the file to determine if its been rotated (size is less than current offset)
  pass
  
  # Send this text to the server
  ClientSend(file_path, path_mtime, path_offset, path_size, text, target_host, target_port)

