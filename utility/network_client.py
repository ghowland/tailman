"""
Network: Client and Server for relaying logs
"""


import SocketServer
import commands
import select
import time
import re

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
          
          file_header = re.findall('------HOST:(.*?):PATH:(.*?):MTIME:(.*?):OFFSET:(.*?):SIZE:(.*?):------', line)
          if file_header:
            # Extract file data
            processing = {'host':file_header[0][0], 'path':file_header[0][1], 'mtime':file_header[0][2], 'size':file_header[0][3]}
            processed_command = True
            
            # Match path against Spec File input glob
            pass
          
          #TODO(g): Everything...
          if processing == None:
            print 'NULL: %s' % line
          else:
            # If this is a normal line, and not a command
            if not processed_command:
              print '%s: %s: %s' % (processing['host'], processing['path'], line)
    
    except Exception, e:
      print "Error processing connection: ", e


def Server(port):
  """Set up the server, and serve it forever."""
  global RUNNING
  
  log('Starting server on port %s' % port)
  
  server = TailTCPServer(('0.0.0.0', port), TailTCPServerHandler)

  fd = server.fileno()
  
  while RUNNING:
    try:
      (wait_in, wait_out, wait_err) = select.select([fd], [fd], [], 0)
      
      # Handle incoming TCP request
      if fd in wait_in:
        server.handle_request()
      
      # Give back to the system as we spin loop
      time.sleep(0.001)
    
    except KeyboardInterrupt, e:
      RUNNING = False
    except Exception, e:
      log('Tail Server Error: %s' % e)


def ClientSend(path_host, path, path_mtime, path_offset, path_size, text, target_host, target_port, retry=0):
  global CONNECTION
  
  if retry >= 3:
    raise NetworkRetryFailure('Failed to connect and send data %s times: %s (%s)' % (retry, target_host, target_port))
  
  if CONNECTION == None:
    CONNECTION = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    CONNECTION.connect((target_host, target_port))
  
  try:
    CONNECTION.send('\n------HOST:%s:PATH:%s:MTIME:%s:OFFSET:%s:SIZE:%s:------\n' % (path_host, path, path_mtime, path_offset,
                                                                                     path_size))
    CONNECTION.send(text)
  
  except Exception, e:
    log('Network failure: %s: %s: %s' % (target_host, target_port, e))


def GetHostname():
  """Returns a string, the fully qualified domain name (FQDN) of this local host."""
  (status, output) = commands.getstatusoutput('/bin/hostname')
  return output.split('.')[0]


#------HOST:somehost:PATH:/tmp/blah.log:MTIME:1234567:OFFSET:0:SIZE:1024:------