"""
Network: Client for relaying logs
"""


import commands
import time
import socket

from log import log


# Caching client connections, for handling multiple target host/ports
CLIENT_CONNECTIONS = {}


def ClientSend(path, path_mtime, path_offset, path_size, text, target_host, target_port, retry=0):  
  global CLIENT_CONNECTIONS
  
  log('ClientSend: %s: %s: %s: %s: %s: %s' % (path, path_mtime, path_offset, path_size, target_host, target_port))
  
  key = '%s.%s' % (target_host, target_port)
  
  # Get our local hostname
  path_host = GetHostname()
  
  if retry >= 3:
    raise NetworkRetryFailure('Failed to connect and send data %s times: %s (%s)' % (retry, target_host, target_port))
  
  # If we dont have this connection, or it is None, create it
  if key not in CLIENT_CONNECTIONS or CLIENT_CONNECTIONS[key] == None:
    CLIENT_CONNECTIONS[key] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    CLIENT_CONNECTIONS[key].connect((target_host, target_port))
  
  # Send the data
  try:
    CLIENT_CONNECTIONS[key].send('\n------HOST:%s:PATH:%s:MTIME:%s:OFFSET:%s:SIZE:%s:------\n' % (path_host, path, path_mtime,
                                                                                                  path_offset, path_size))
    CLIENT_CONNECTIONS[key].send(text)
    CLIENT_CONNECTIONS[key].send('\n------FINISHED:HOST:%s:PATH:%s:------\n' % (path_host, path))
    
    log('Client Send complete: %s: %s' % (path_host, path))
    
    #TODO(g): Server needs to keep track of the offset of the actual lines it processed, because we dont know if there are partial
    #   lines in this method
    pass

  # Handle errors
  except Exception, e:
    log('Network failure: %s: %s: %s' % (target_host, target_port, e))
    
    # Clear the connection
    if key in CLIENT_CONNECTIONS and CLIENT_CONNECTIONS[key]:
      CLIENT_CONNECTIONS[key].close()
    CLIENT_CONNECTIONS[key] = None


def GetHostname():
  """Returns a string, the fully qualified domain name (FQDN) of this local host."""
  (status, output) = commands.getstatusoutput('/bin/hostname')
  return output.split('.')[0]


#------HOST:somehost:PATH:/tmp/blah.log:MTIME:1234567:OFFSET:0:SIZE:1024:------
#------QUERYHOST:somehost:PATH:/tmp/blah.log:------

