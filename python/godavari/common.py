#import necessary libraries
import marshal,xmlrpclib,types,base64,time,SocketServer,threading,socket,time
from datetime import datetime
from SimpleXMLRPCServer import SimpleXMLRPCServer
#import the settings
from settings import *

#A threaded XML RPC server - simple one; might not scale
class AsyncXMLRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCServer): pass

"""
Serialize -
This decorator should be used to marshal the function and encode
the string using BASE64 encoding. This string can then be sent
over the grid to be executed.


Serialize is a Python decorator without parameters. So the behavior
of this is slightly different from Deserialize which is a decorator
with parameters. The decorated function gets passed to the __init__
of Serialize and __call__ has no parameters. For Deserialize, the
parameter is passed to __init__ and the decorated function is passed
to __call__.

Example:

    @Serialize
    def hello():
        print 'hello'
        
"""
class Serialize:
    def __init__(self,f):
        self.f=f
    
    def __call__(self):
        #marshal the function to string and base64 encode it
        return base64.b64encode(marshal.dumps(self.f.func_code))


"""
Deserialize -
This decorator should be used to convert a serialized method back
to a function object.

This decorator takes a paramter - the name that should be given to
the function after it is deserialized. When the decorator is called,
it takes the name and returns a wrapper method. When you call the
decorated method, it calls this wrapper method with the function
object.

The function that is decorated with this decorator should return a
string which represents the function.

See the documentation on Serialize for the difference in the way
Serialize and Deserialize are implemented.

Example:
Lets say you want to retrieve the serialized function from a file.
Then write a function to read the file and return the text, and decorate
it with this. So, when you call the function, the effect is that it will
call the serialized function.

    @Deserialize('test_func')
    def read():
        f=open('test.dat','r')
        s=f.read()
        f.close()
        return s

"""
class Deserialize:
    def __init__(self,name):
        self.name=name

    def __call__(self,f):
        #get the function code
        code=marshal.loads(base64.b64decode(f()))
        #create and return the function type
        return types.FunctionType(code,globals(),self.name)

"""
Serve -

This decorator should be used to start an XML RPC server on the given
port using the list of functions. The actual function that is decorated
with this does not need to do anything related to starting the server.
The function is executed when this decorator is called - so it can be
used to perform logic like validating user ID etc and throwing error so
that the server does not start.

This takes two parameters - the port to start the server and a list of
functions that need to be registered on the XML RPC interface.

Example:

    def hello():
        print 'hello'
        
    @Serve(1111,[hello])
    def main():
        pass

"""
class Serve:
    def __init__(self,port,host,functions):
        self.port=port
	self.host=host
        self.functions=functions

    def __call__(self,f):
        #invoke the decorated function - if this throws an error
        #then we dont proceed to start the server.
        f()
        #start the server
        return self.__serve

    #This will be used to remote terminate the server
    def terminate(self):
        self.server.shutdown()

    """
    __serve -
    This is an internal method that is used to start an XML RPC server
    which exposes the given methods over the interface.

    The port parameter specifies the port to use.
    The functions parameter specifies a list of functions that will
    be registered with this XML RPC server.

    """
    def __serve(self):
	if self.host is None:
		self.host=HOST
        #create the server
        self.server=AsyncXMLRPCServer((self.host,self.port))
        #for now this is fixed in settings
        self.server.allow_none=True
        #register terminate function
        self.server.register_function(self.terminate)
        #iterate over the functions
        for f in self.functions:
            #and register the function on the interface
            self.server.register_function(f)
        #now start the server and wait
        try:
            self.server.serve_forever()
        except:
            self.server.shutdown()
            raise


"""
OnGrid -

This decorator is used by the client to submit the code for execution
on the grid. This should be used together with the serialize decorator.

This takes 5 parameters

name - the name of the task, this will be used to generate the job id
host - the host where the master server is running
port - the master server port
data_provider - a function that can provide the data for this method.
                The data returned by this is passed on to the grid as
                arguments to this method call
exec_local_on_error - if True, then the code is executed locally when
                    an error occurs in grid execution

Example:

    def data():
    return 1

    @OnGrid('hello','localhost',2207,data,True)
    @Serialize
    def hello(number):
        return number

    print hello

The run local in case of error option is true by default.

    @OnGrid('hello','localhost',2207,data)
    @Serialize
    def hello(number):
        return number

"""
class OnGrid:
    def __init__(self,name,host,port,data_provider,exec_local_on_error=True):
        #create the proxy which will be used to run on grid
        url="http://%s:%s"%(host,port)
        self.proxy=xmlrpclib.ServerProxy(url,allow_none=True)
        #create a job ID - same as the name that was passed in
        self.job_id="%s_%s"%(name,time.time()*1000)
        self.data_provider=data_provider
        self.name=name
        self.host=host
        self.port=port
        self.exec_local_on_error=exec_local_on_error

    def __call__(self,f):
        try:
            #try to run the code on the grid - execute f to get the serialized code
            return self.proxy.execute_on_grid(self.job_id,f(),self.data_provider())
        except:
            #if we can execute locally on error
            if self.exec_local_on_error:
                #deserailize the code using wrapper
                @deserialize('t')
                def __t():
                    return f
                #execute using data from provider and return result
                #no bucket size applicable for local
                return __t(self.data_provider())
            else:
                #else throw exception back up
                raise

"""
HeartBeatThread -

This class sends heartbeat to the master.

"""
class HeartBeatThread(threading.Thread):
    def __init__(self,port,proxy):
        super(HeartBeatThread, self).__init__()
        self.stop = threading.Event()
        self.port=port
        self.proxy=proxy

    def run(self):
        try:
            #loop around
            while not self.stop.isSet():
                logger.info('Sending heartbeat')
                self.proxy.send_heartbeat(\
                    socket.gethostbyname(socket.gethostname()),self.port)
                #now wait for a few times
                time.sleep(NODE_HEART_BEAT_INTERVAL)
            logger.info('Stopping heartbeat thread')
        except KeyboardInterrupt,SystemExit:
            logger.info('Stopping heartbeat thread')

"""
HeartBeatManager -

This class manages the heartbeat from the nodes. If the node
has not sent a heartbeat in a defined time then it is booted
out and has to be restarted to reconnect.

"""
class HeartBeatManager(threading.Thread):
    def __init__(self,master):
        super(HeartBeatManager, self).__init__()
        self.stop = threading.Event()
        self.master=master

    def run(self):
        try:
            #loop around
            while not self.stop.isSet():
                logger.info('Checking heartbeats')
                #for each key in heartbeat
                for proxy in self.master.heartbeat.keys():
                    #check if last heartbeat was more than expiry sec away
                    if time.time()-self.master.heartbeat[proxy]>HEART_BEAT_EXPIRY:
                        logger.info('Removing proxy %s'%(proxy))
                        try:
                            #and remove the node
                            self.master.nodes.remove(proxy)
                            #remove entry from heartbeat
                            self.master.heartbeat.__delitem__(proxy)
                        except:
                            pass
                #now wait for a few times
                time.sleep(HEART_BEAT_CHECK_INTERVAL)
            logger.info('Stopping heartbeat manager')
        except KeyboardInterrupt,SystemExit:
            logger.info('Stopping heartbeat manager')
            raise

"""
JobRunner -
This decorator will be used to perform the code execution on an
available node in the grid in a thread and then return the result.

The thread is created by the AsyncXMLRPCServer.

"""
class JobRunner:
    def __init__(self,job_id,code,args,db_mgr,token,url):
        #assign parameters 
        self.job_id=job_id
        self.result=None
        self.code=code
        self.args=args
        self.db_mgr=db_mgr
        #and create the initial record
        self.db_mgr.create(self.job_id)
        #the token and the url
        self.token=token
        self.url=url

    def __call__(self,f):
        #execute and return the result
        self.run()
        return self.result

    def run(self):
        #initial delay to avoid locking on the nodes
        time.sleep(1)
        logger.info('Using token %s'%(self.token))
        self.db_mgr.set_started(self.job_id,self.url)
        logger.info('Using node URL %s'%(self.url))
        self.proxy=xmlrpclib.ServerProxy(self.url)
        logger.info('Submitting to %s'%(self.proxy))
        #now run the code and set the result
        self.result=self.proxy.execute(self.code,self.args)
        self.db_mgr.job_complete(self.job_id)
        self.db_mgr.close()
