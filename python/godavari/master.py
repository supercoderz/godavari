#import basics
import xmlrpclib,threading,time,sqlite3
from datetime import datetime
from collections import deque
#import internal stuff
from common import Serve,HeartBeatManager,JobRunner
from settings import *
from database import *

class Master(object):
    def __init__(self):
        #This will be used to keep track of all the available nodes
        #This list will contain ServerProxy objects
        self.nodes=deque([])
        
        #This will be used to control how many threads can be started
        #at once to handle requests
        self.tokens=deque(range(MAX_THREADS))
        
        #This dict is used to keep track of all the heartbeats received
        #from the nodes - if the value in this is greater than the max
        #delay then the proxy is removed from the nodes list
        self.heartbeat={}
        
        #heartbeat thread
        self.hb_thread=None
        
        #db cleaner
        self.db_cleaner=None
    
    
    """
    register -
    
    This method will be exposed over the XML RPC interface to allow nodes to
    register with this master server.
    
    This takes 2 parameters - the fully qualified domain name (FQDN) of the
    node and the port where the node is running.
    
    """
    def register(self,ip,port):
        #create the URL
        url="http://%s:%s"%(ip,port)
        #append this to the list
        self.nodes.append(url)
    
    
    """
    execute_on_grid -
    
    This method will be exposed over the XML RPC interface to allow the clients
    to submit jobs to be executed on the grid. This takes the job ID, serialized
    code and the arguments and send it to an available node in a separate thread.
    
    """
    def execute_on_grid(self,job_id,code,args):
        #check and init the state DB
        db_mgr=DbManager()
        db_mgr.init_db()
        #getthe proxy - this will also block
        url=self.__getProxy()
        #get the token - the method will block till we get a token
        token=self.__getToken()
        #create an inline method decorated with the JobRunner and call it
        #it is a dummy method
        @JobRunner(job_id,code,args,db_mgr,token,url)
        def __wrap():
            pass
        #release the proxy so that new jobs can be scheduled on this
        self.nodes.append(url)
        #return the result
        res=__wrap
        #release the token
        self.tokens.append(token)
        return res
    
    
    """
    send_heartbeat -
    
    This records the heartbeat from a node
    """
    def send_heartbeat(self,ip,port):
        #create URL and record time
        url="http://%s:%s"%(ip,port)
        self.heartbeat[url]=time.time()
        
    
    
    """
    start-
    This starts the master using the given port.
    
    """
    def start(self,port,host=None):
        #create heartbeat thread
        self.hb_thread=HeartBeatManager(self)
        self.db_cleaner=DatabaseCleaner()
        
        try:
            @Serve(port,host,[self.register,self.execute_on_grid,self.send_heartbeat])    
            def __start():
                logger.info('Starting server on %s'%(port))
                #start the heart beat manager            
                self.hb_thread.start()
                self.db_cleaner.start()
    
            __start()
    
        except KeyboardInterrupt,SystemExit:
            #stop heartbeat manager
            self.hb_thread.stop.set()
            self.db_cleaner.stop.set()
            #hb_thread.join()
            #db_cleaner.join()
            logger.info('Shutting down')
            raise
        

    def __getToken(self):
        #delegate to get helper
        return self.__get(self.tokens)

    def __getProxy(self):
        #delegate to get helper
        return self.__get(self.nodes)

    def __get(self,l):
        res=None
        #try once
        try:
            res=l.popleft()
        except:
            pass
        #loop till we get it
        while res is None:
            try:
                res=l.popleft()
            except:
                pass
        #finally got a value - return it
        return res
