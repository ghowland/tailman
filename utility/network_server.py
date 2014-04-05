"""
Network: Server for relaying and processing logs
"""


import SocketServer
import select
import time
import re
import os

from process_server import *
from log import log


# Connection singleton
CONNECTION = None

# Global running info
RUNNING = True


class NetworkRetryFailure(Exception):
  """If we get a network failure, and retrying doesnt work."""


class TailTCPServer(SocketServer.ThreadingTCPServer):
  allow_reuse_address = True


class TailTCPServerHandler(SocketServer.BaseRequestHandler):
  def handle(self):
    global RUNNING
    
    try:
      # Get our file descriptor
      fd = self.request.fileno()
      
      # Dont wait once we try to read, take whats there and return
      self.request.setblocking(0)
      
      # Received data goes here, and we break off lines as we get them
      buffer = ''
      
      # Process data (host, path, etc)
      processing = None
      
      # Run forever
      while RUNNING:
        (wait_in, wait_out, wait_err) = select.select([fd], [fd], [], 0)
        
        # Handle HTTP request
        if fd in wait_in:
          buffer += self.request.recv(1024)
        
        
        # Process any complete lines
        while '\n' in buffer:
          (line, buffer) = buffer.split('\n', 1)
          
          processed_command = False

          # Received request to find out where we last had information about this log
          file_header = re.findall('------QUERYHOST:(.*?):PATH:(.*?):------', line)
          if file_header:
            # Extract file data
            query_request = {'host':file_header[0][0], 'path':file_header[0][1]}
            log('Query data log: Host: %(host)s  Path: %(path)s' % query_request)
            processed_command = True
            
            # Query our data source and find out when we last got logs from that system
            log('Server: %s' % self.server.server_data)
            pass
          
          file_header = re.findall('------FINISHED:HOST:(.*?):PATH:(.*?):------', line)
          if file_header:
            finish_request = {'host':file_header[0][0], 'path':file_header[0][1]}
            processed_command = True
            log('Finished processing: %(host)s: %(path)s' % finish_request)
            log('   Processing Data: %s' % processing)
            # Remove the last newline, we always add one too many
            processing['storage_path_fp'].seek(-1, os.SEEK_END)
            processing['storage_path_fp'].truncate()
            # Close the storage page
            processing['storage_path_fp'].close()
          
          # Received new file relay header.  Telling us host/path and log file state
          file_header = re.findall('------HOST:(.*?):PATH:(.*?):MTIME:(.*?):OFFSET:(.*?):SIZE:(.*?):------', line)
          if file_header:
            # If we were processing a different file, save it's updated offset and any other data
            if processing:
              #TODO(g): Save offset_processed...
              pass
            
            # Extract file data
            processing = {'host':file_header[0][0], 'path':file_header[0][1], 'mtime':file_header[0][2], 'offset':file_header[0][3],
                          'size':file_header[0][4], 'offset_processed':0, 'data':{}}
            processed_command = True
            
            # Add in the spec file information to this processing data
            for (spec_path, spec_data) in self.server.server_data['specs'].items():
              path_regex = spec_data['input']['glob'].replace('*', '(.*?)')
              path_regex_result = re.findall(path_regex, processing['path'])
              if path_regex_result:
                processing['spec_path'] = spec_path
                processing['spec_data'] = spec_data
                
                # Get all the glob key data out of our path
                for count in range(0, len(spec_data['input']['glob keys'])):
                  glob_key = spec_data['input']['glob keys'][count]
                  # If we arent getting tuple wrapped regex data (list with a tuple in it, with our actual data), extract directly
                  if type(path_regex_result[0]) != tuple:
                    processing['data'][glob_key] = path_regex_result[count]
                  # Else, extract the glob key from inside the tuple in the list
                  else:
                    processing['data'][glob_key] = path_regex_result[0][count]
                    
                
                # We found our spec, no need to look for more
                break
            
            log('Receiving log data: Host: %(host)s  Path: %(path)s  mtime: %(mtime)s  offset: %(offset)s  size: %(size)s  data: %(data)s' % \
                processing)
            
            # Match path against Spec File input glob
            pass
          
          #TODO(g): Everything...
          if processing == None:
            print 'NULL: %s' % line
          
          else:
            # If this is a normal line, and not a command
            if not processed_command:
              print '%s: %s: %s' % (processing['host'], processing['path'], line)
              
              # Store this line
              path_id = 0
              storage_path = '%s/%s' % (processing['spec_data']['storage directory'] % processing, path_id)
              
              # Open the path for storing data, seek to the offset
              if 'storage_path' not in processing:
                processing['path_id'] = path_id
                processing['storage_path'] = storage_path
                
                # Ensure the directory exists
                if not os.path.isdir(os.path.dirname(storage_path)):
                  os.makedirs(os.path.dirname(storage_path))
                
                processing['storage_path_fp'] = open(storage_path, 'w+')
                processing['storage_path_fp'].seek(int(processing['offset']), 0)
              
              # Write the line
              processing['storage_path_fp'].write(line + '\n')
              
              # Update our state
              processing['offset_processed'] += len(line)
    
    except Exception, e:
      print "Error processing connection: ", e


def ServerManager(specs, options):
  """Set up the server, and serve it forever."""
  global RUNNING
  
  ports = []
  servers = []
  server_fds = []
  server_fd_specs = {}
  
  # Start servers for all our spec ports
  for (spec_path, spec_data) in specs.items():
    # If we arent listening on this port yet, start a server on it
    if spec_data['relay port'] not in ports:
      ports.append(spec_data['relay port'])
      
      log('Starting server on port %s' % spec_data['relay port'])
      
      # Create the server
      server = TailTCPServer(('0.0.0.0', spec_data['relay port']), TailTCPServerHandler)
      
      # Get the socket file descriptor
      fd = server.fileno()
      server_fds.append(fd)
      
      # Add to our servers dict
      server_data = {'server':server, 'port':spec_data['relay port'], 'fd':fd, 'specs':{spec_path: spec_data}}
      server_fd_specs[fd] = server_data
      servers.append(server_data)
      
      # Add our server_data to the server, so it is accessable (all specs) when handling requests
      server.server_data = server_data
    
    # Else, add this spec to an existing server
    else:
      # Look through all the existing servers
      for count in range(0, len(servers)):
        # If this is a port match, add it to the server
        if servers[count]['port'] == spec_data['relay port']:
          servers[count]['specs'][spec_path] = spec_data
          break
  
  
  # Handle requests
  while RUNNING:
    try:
      (wait_in, wait_out, wait_err) = select.select(server_fds, server_fds, [], 0)

      # Look through each server socket file descriptor
      for fd in server_fds:
        # Handle incoming TCP request
        if fd in wait_in:
          for count in range(0, len(servers)):
            if servers[count]['fd'] == fd:
              servers[count]['server'].handle_request()
      
      # Give back to the system as we spin loop
      time.sleep(0.001)
    
    except KeyboardInterrupt, e:
      RUNNING = False
    except Exception, e:
      log('Tail Server Error: %s' % e)


#------HOST:somehost:PATH:/tmp/blah.log:MTIME:1234567:OFFSET:0:SIZE:1024:------
#------QUERYHOST:somehost:PATH:/tmp/blah.log:------

