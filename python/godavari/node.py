#get required imports
import xmlrpclib,socket,threading,time
from multiprocessing import Process,Queue
#get the required decorators
from common import Deserialize,Serve,HeartBeatThread
#get the settings
from settings import *

class Node(object):
    def __init__(self):
        #This is the thread that sends heartbeat to master
        self.hb_thread=None
        
    """
    execute -
    This is the method which gets exposed on the node server. This
    receives the serialized code from the master, converts it to a
    function object and executes it using the given arguments. The
    result or the error is returned over the XML RPC interface.
    
    """
    def execute(self,code_string,params):
        #deserialize the code string that was sent
        #using a wrapper function
        @Deserialize('func')
        def extract():
            return code_string
        #execute the de-serialized function with
        #the arguments that were passed in to us
        q=Queue()
        def task(extract,params,q):
            q.put(extract(params))
        p=Process(target=task,args=(extract,params,q))
        p.start()
        p.join()
        return q.get()
    
    """
    start-
    This starts the node server using the given port and the
    master url.
    
    """
    def start(self,master_url,port,host=None):
        self.master_url=master_url
        self.port=port
        self.host=host
        #create the proxy and the heartbeat thread first
        proxy=xmlrpclib.ServerProxy(master_url,allow_none=True)
        self.hb_thread=HeartBeatThread(port,proxy)            
        try:            
            #This decorator calls the start() method which registers
            #with master node before the node server is started in the
            #decorator
            @Serve(self.port,self.host,[self.execute])    
            def __start():
                logger.info('Starting server on %s'%(port))
                #register this node with the master     
                proxy.register(host,port)
                #start heart beat thred
                self.hb_thread.start()
            #start the server with the execute function
            __start()
        except KeyboardInterrupt,SystemExit:
            self.hb_thread.stop.set()
            self.hb_thread.join()
            logger.info('Shutting down')