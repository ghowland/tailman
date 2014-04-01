"""
Network: Client and Server for relaying logs
"""


import SocketServer


from log import log


# Connection singleton
CONNECTION = None


class NetworkRetryFailure(Exception):
  """If we get a network failure, and retrying doesnt work."""
  

class TailTCPServer(SocketServer.ThreadingTCPServer):
    allow_reuse_address = True


class TailTCPServerHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        try:
            data = json.loads(self.request.recv(1024).strip())
            # process the data, i.e. print it:
            print data
            # send some 'ok' back
            self.request.sendall(json.dumps({'return':'ok'}))
        except Exception, e:
            print "Exception wile receiving message: ", e


def Server(port):
  """Set up the server, and serve it forever."""
  server = TailTCPServer(('0.0.0.0', port), TailTCPServerHandler)
  #TODO(g): Add tests for individual socket requests, so can loop and abort on quit prompt (keyboard, signal)...
  server.serve_forever()


def ClientSend(path_host, path, text, target_host, target_port, retry=0):
  global CONNECTION
  
  if retry >= 3:
    raise NetworkRetryFailure('Failed to connect and send data %s times: %s (%s)' % (retry, target_host, target_port))
  
  if CONNECTION == None:
    CONNECTION = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    CONNECTION.connect((target_host, target_port))
  
  try:
    CONNECTION.send(text)
  
  except Exception, e:
    log('Network failure: %s: %s: %s' % (target_host, target_port, e))
