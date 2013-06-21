#import basics
import sqlite3
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
#get the settings
from settings import *

"""
Handler -

This class handles the requests to check the status of jobs
"""
class Handler(BaseHTTPRequestHandler):

    #delegate to GET
    def do_POST(self):
        return self.do_GET()

    def do_GET(self):
        html="%s%s"%(HEADER,HTML_START)
        if self.path.endswith(RUNNING_PATH):
            conn=sqlite3.connect(DB_PATH)
            res=conn.cursor().execute(RUNNING_JOBS)
            for r in res:                
                t=TABLE_ROW%r
                html="%s %s"%(html,t)
            conn.close()
        elif self.path.endswith(WAITING_PATH):
            conn=sqlite3.connect(DB_PATH)
            res=conn.cursor().execute(WAITING_JOBS)
            for r in res:                
                t=TABLE_ROW%r
                html="%s %s"%(html,t)
            conn.close()
        elif self.path.endswith(COMPLETED_PATH):
            conn=sqlite3.connect(DB_PATH)
            res=conn.cursor().execute(COMPLETED_JOBS)
            for r in res:                
                t=TABLE_ROW%r
                html="%s %s"%(html,t)
            conn.close()
        else:
            self.send_response(200)
            self.send_header('Content-type','text/html')
            self.end_headers()
            self.wfile.write("%s%s"%(HEADER,INDEX_PAGE))
            return
        
        html="%s %s"%(html,HTML_END)
        self.send_response(200)
        self.send_header('Content-type','text/html')
        self.end_headers()
        self.wfile.write(html)
        return

class WebServer(object):
    def start(self,port):
        try:
            #start the HTTP server and listen for requests
            logger.info(('Starting webserver on %s'%port))
            server=HTTPServer((HOST,port),Handler)
            server.serve_forever()
        except:
            #catch any exception and close
            server.socket.close()
