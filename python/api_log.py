from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
import threading
import json
from urllib.parse import urlparse, parse_qs
import socket
import api_log_func

class Handler(BaseHTTPRequestHandler):

    def do_GET(self):

        path = urlparse(self.path).path
        qs = urlparse(self.path).query
        qs = parse_qs(qs)

        res =  api_log_func.callback(path, qs, self)

        resp = json.dumps(res, default=api_log_func.json_serial).encode()

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Expose-Headers", "Access-Control-Allow-Origin")
        self.send_header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
        self.end_headers()
        self.wfile.write(resp)
        
        return

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""

if __name__ == '__main__':
    server = ThreadedHTTPServer(('0.0.0.0', 8010), Handler)
    print('Starting server, use <Ctrl-C> to stop')
    server.serve_forever()