"""
Network: Client for relaying logs
"""


import commands
import time
import socket
import re

from log import log
from network_server import NetworkRetryFailure


# Caching client connections, for handling multiple target host/ports
CLIENT_CONNECTIONS = {}

# Max retries for network
MAX_RETRIES = 3


def GetConnection(target_host, target_port):
  global CLIENT_CONNECTIONS
  
  key = '%s.%s' % (target_host, target_port)

  # If we dont have this connection, or it is None, create it
  if key not in CLIENT_CONNECTIONS or CLIENT_CONNECTIONS[key] == None:
    CLIENT_CONNECTIONS[key] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    CLIENT_CONNECTIONS[key].connect((target_host, target_port))
  
  return CLIENT_CONNECTIONS[key]


def ResetConnection(target_host, target_port):
  global CLIENT_CONNECTIONS
  
  key = '%s.%s' % (target_host, target_port)
  
  # Clear the connection
  if key in CLIENT_CONNECTIONS and CLIENT_CONNECTIONS[key]:
    CLIENT_CONNECTIONS[key].close()
    
  CLIENT_CONNECTIONS[key] = None


def CloseAllConnections():
  global CLIENT_CONNECTIONS
  
  for key in CLIENT_CONNECTIONS:
    log('Closing connection: %s: (fd=%s)' % (key, CLIENT_CONNECTIONS[key].fileno()))
    CLIENT_CONNECTIONS[key].close()


def ClientSend(path, path_mtime, path_offset, path_size, text, target_host, target_port, retry=0):
  log('ClientSend: %s: %s: %s: %s: %s: %s' % (path, path_mtime, path_offset, path_size, target_host, target_port))
  
  # Get our local hostname
  path_host = GetHostname()
  
  if retry >= MAX_RETRIES:
    raise NetworkRetryFailure('Failed to connect and send data %s times: %s (%s)' % (retry, target_host, target_port))
  
  # Get the connection (cached or connect now)
  conn = GetConnection(target_host, target_port)
  
  # Send the data
  try:
    conn.send('\n------HOST:%s:PATH:%s:MTIME:%s:OFFSET:%s:SIZE:%s:------\n' % (path_host, path, path_mtime,
                                                                                                  path_offset, path_size))
    conn.send(text)
    conn.send('\n------FINISHED:HOST:%s:PATH:%s:------\n' % (path_host, path))
    
    log('Client Send complete: %s: %s' % (path_host, path))
    
    #TODO(g): Server needs to keep track of the offset of the actual lines it processed, because we dont know if there are partial
    #   lines in this method
    pass

  # Handle errors
  except Exception, e:
    log('Network failure: %s: %s: %s' % (target_host, target_port, e))
    
    # Reset the connection
    ResetConnection(target_host, target_port)


def ClientGetOffset(path, path_mtime, path_size, target_host, target_port, retry=0):
  """Get the last offset for this host/path from the server."""
  log('ClientGetOffset: %s: %s: %s: %s: %s' % (path, path_mtime, path_size, target_host, target_port))
  
  # Get the connection (cached or connect now)
  conn = GetConnection(target_host, target_port)
  
  # Get our hostname
  hostname = GetHostname()
  
  # Send the data
  #try:
  if 1:
    #TODO(g): Perform checksum of first 1024 bytes of the file, so we know its the same file
    conn.send('\n------QUERYHOST:%s:PATH:%s:SIZE:%s:CHECKSUM:0:------\n' % (hostname, path, path_size))
    
    # Get the response
    fp = conn.makefile()
    response = fp.readline()
    
    #print response
    
    # Extract the data with a regex
    regex = '------FILERESPONSE:PATH:(.*?):SIZE:(.*?):OFFSET:(.*?):------'
    regex_result = re.findall(regex, response)
    
    #print regex_result
    
    # Get the fields from the regex
    known_size = regex_result[0][1]
    known_offset = regex_result[0][2]
    
    # Return result data
    data = {'size':known_size, 'offset':known_offset}
    
    log('Received Path Offset: %s: %s: %s: %s' % (hostname, path, known_size, known_offset))
    
    #TODO(g): Server needs to keep track of the offset of the actual lines it processed, because we dont know if there are partial
    #   lines in this method
    pass

  ## Handle errors
  #except Exception, e:
  #  #log('Network failure: %s: %s: %s' % (target_host, target_port, e))
  #  #
  #  ## Reset the connection
  #  #ResetConnection(target_host, target_port)
  #  
  #  raise Exception('Failed to connect and send data %s times: %s (%s): %s' % (retry, target_host, target_port, e))
  
  return data
  

def GetHostname():
  """Returns a string, the fully qualified domain name (FQDN) of this local host."""
  (status, output) = commands.getstatusoutput('/bin/hostname')
  return output.split('.')[0]


#------HOST:somehost:PATH:/tmp/blah.log:MTIME:1234567:OFFSET:0:SIZE:1024:------
#------QUERYHOST:somehost:PATH:/tmp/blah.log:------

