"""
Processing
"""


import glob
import re
import stat
import os

from log import log
from path import *

from process_server import *
from network_client import *


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
    RelayFile(file_path, file_data)


def RelayFile(file_path, file_data):
  """Relay this file to the server"""
  path_mtime = os.stat(file_path)[stat.ST_MTIME]
  path_size = os.stat(file_path)[stat.ST_SIZE]
  
  #TODO(g): Use the queried data for this, not just starting at the beginning
  path_offset = 0
  
  text = file_data['fp'].read()
  
  target_host = file_data['spec_data']['relay host']
  target_port = file_data['spec_data']['relay port']
  
  # Send this text to the server
  ClientSend(file_path, path_mtime, path_offset, path_size, text, target_host, target_port)

