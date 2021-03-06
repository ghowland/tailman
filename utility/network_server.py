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
    
    #try:
    if 1:
      # Get our file descriptor
      fd = self.request.fileno()
      
      # Dont wait once we try to read, take whats there and return
      self.request.setblocking(0)
      
      # Received data goes here, and we break off lines as we get them
      buffer = ''
      
      # Process data (host, path, etc)
      processing = None
      
      # Keep track of our total work
      counter = 0
      
      # Run forever, or until break
      while RUNNING:
        (wait_in, wait_out, wait_err) = select.select([fd], [fd], [], 0)
        
        # Handle HTTP request
        if fd in wait_in:
          # Get more data
          buffer_chunk = self.request.recv(1024)
          
          # If we received the end, break out of our forever-while 
          if not buffer_chunk:
            break
          
          # Add it to our buffer
          buffer += buffer_chunk
        
        # Process any complete lines
        while '\n' in buffer:
          (line, buffer) = buffer.split('\n', 1)
          
          #print line #DEBUG: Watch everything
          counter += 1
          if counter % 250 == 0:
            print 'Counter: %s' % counter
          
          processed_command = False

          # Received request to find out where we last had information about this log
          file_header = re.findall('------QUERYHOST:(.*?):PATH:(.*?):SIZE:(.*?):CHECKSUM:(.*?):------', line)
          if file_header:
            # Extract file data
            #print file_header
            query_request = {'host':file_header[0][0], 'path':file_header[0][1], 'size':int(file_header[0][2]),
                             'checksum':file_header[0][3]}
            #log('Query data log: Host: %(host)s  Path: %(path)s' % query_request)
            processed_command = True
            
            #print line
            
            # Respond to query request with current information
            GetProcessingSpecData(query_request, self.server)
            #print 'Getting latest log file...'
            latest_log_file = GetLatestLogFileInfo(query_request)
            response = '------FILERESPONSE:PATH:%s:SIZE:%s:OFFSET:%s:------\n' % (query_request['path'], latest_log_file['size'],
                                                                                  latest_log_file['remote_offset'])
            #log(response)
            self.request.send(response)
            
            # Query our data source and find out when we last got logs from that system
            #log('Server: %s' % self.server.server_data)
            pass
          
          # Received close-log relay header.  We're done with relaying/parsing this host/path at the moment.
          file_header = re.findall('------FINISHED:HOST:(.*?):PATH:(.*?):------', line)
          if file_header:
            finish_request = {'host':file_header[0][0], 'path':file_header[0][1], 'offset':processing['offset_processed']}
            processed_command = True
            log('Finished processing: %(host)s: %(path)s: %(offset)s' % finish_request)
            #log('   Processing Data: %s' % processing)
            # Remove the last newline, we always add one too many
            processing['storage_path_fp'].seek(-1, os.SEEK_END)
            processing['storage_path_fp'].truncate()
            # Close the storage page
            processing['storage_path_fp'].close()
            processing['storage_path_fp'] = None
          
          # Received new file relay header.  Telling us host/path and log file state
          file_header = re.findall('------HOST:(.*?):PATH:(.*?):MTIME:(.*?):OFFSET:(.*?):SIZE:(.*?):------', line)
          if file_header:
            # If we were processing a different file, save it's updated offset and any other data
            if processing:
              #TODO(g): Save offset_processed...
              pass
            
            # Extract file data
            processing = {'host':file_header[0][0], 'path':file_header[0][1], 'mtime':int(file_header[0][2]), 'offset':int(file_header[0][3]),
                          'size':int(file_header[0][4]), 'offset_processed':int(file_header[0][3]), 'data':{}}
            processed_command = True
            previous_line_data = None
            
            # Using what we know, populate spec file information and default data (created 'data' key dict if missing)
            GetProcessingSpecData(processing, self.server)
            
            # Store component ID in the data, we get that from our spec_data, so every line gets it
            processing['data']['component'] =  GetComponentId(processing)
            
            # Get the latest log file, for this host/path and occurred time
            latest_log_file = GetLatestLogFileInfo(processing)
            processing['latest_log_file'] = latest_log_file
            
            log('Receiving log data: Host: %(host)s  Path: %(path)s  mtime: %(mtime)s  offset: %(offset)s  size: %(size)s  data: %(data)s' % \
                processing)
            
            # Match path against Spec File input glob
            pass
          
          #TODO(g): Everything...
          if processing == None:
            # We do not know where this line came from, and so are doing nothing with it.  This should not appear.
            print 'NULL: %s' % line
          
          else:
            # If this is a normal line, and not a command
            if not processed_command:
              #print '%s: %s: %s' % (processing['host'], processing['path'], line)
              
              # Store this line
              storage_path = '%s/%s' % (processing['spec_data']['storage directory'] % processing, processing['latest_log_file']['id'])
              
              # Open the path for storing data, seek to the offset
              if 'storage_path' not in processing:
                processing['storage_path'] = storage_path
                
                # Ensure the directory exists
                if not os.path.isdir(os.path.dirname(storage_path)):
                  os.makedirs(os.path.dirname(storage_path))
                
                # Open the file, and move to the current offset
                if os.path.isfile(storage_path):
                  processing['storage_path_fp'] = open(storage_path, 'r+')
                else:
                  processing['storage_path_fp'] = open(storage_path, 'w')
                processing['storage_path_fp'].seek(int(processing['offset']), 0)
              
              
              # Process this line
              saved_previous_line_data = previous_line_data
              previous_line_data = ProcessLine(line, processing, previous_line_data)
              
              
              # If we went from a previous multiline, to a non-multiline, we now how to store the multiline
              if saved_previous_line_data and 'multiline' in saved_previous_line_data and 'multiline' not in previous_line_data:
                SaveMultiLine(saved_previous_line_data, processing)
              
              # Else, if this is a normal line, save all the key value pairs
              elif saved_previous_line_data and 'multiline' not in saved_previous_line_data:
                #try:
                if 1:
                  SaveLine(saved_previous_line_data, processing)
                #except Exception, e:
                #  log('Failed to save line keys, line: %s --- error: %s' % (line, e))
              
              # Write the line
              if processing['storage_path_fp'] != None:
                processing['storage_path_fp'].write(line + '\n')
                
                # Update our state
                processing['offset_processed'] += len(line) + 1
                
                # Bound the processed on the size
                if processing['offset_processed'] > processing['size']:
                  processing['offset_processed'] = processing['size']
                
                # Update the log file
                UpdateLogFileInfo(processing)
    
    #except Exception, e:
    #  print "Error processing connection: ", e
    
    log('Closing connection (fd=%s)' % fd)


def GetProcessingSpecData(processing, server):
  """Using what we know, populate spec file information and default data (created 'data' key dict if missing)"""
  if 'data' not in processing:
    processing['data'] = {}
  
  # Add in the spec file information to this processing data
  for (spec_path, spec_data) in server.server_data['specs'].items():
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

