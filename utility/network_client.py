"""
Network: Client for relaying logs
"""


import commands
import select
import time
import re

from log import log


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