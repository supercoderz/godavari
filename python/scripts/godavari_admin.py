#!/usr/bin/python

#get the basics
import sys,re
#get godavari packages
from godavari import master,node,settings,webserver

class Admin(object):
    def __init__(self):
        #get the commandline args so that we determine
        #what the command is and take action
        self.command=None
        self.port=None
        self.master_url=None
        self.host=None
    
    
    #The usage of this script
    def usage(self):
        print 'godavari_admin.py - Admin script for Godavari'
        print 'Usage:'
        print """ To start the master server type \n godavari_admin.py start_master [optional port, default 2207] [optional hostname or ip]"""
        print """ To start the node type \n godavari_admin.py start_node [optional port, default 1111] [optional host] [optional master url, default localhost:111]"""
        print """ To start the web server type \n godavari_admin.py start_webserver [optional port, default 1110]"""
        print """ Note: You can specify no parameters or all parameters. You cannot specify only some of the parameters"""
    
    def parse(self):
        self.command=sys.argv[1]
    
        #sink any exception in parsing port and master url
        try:
            self.port=int(sys.argv[2])
            self.host=sys.argv[3]
            self.master_url=sys.argv[4]
        except:
            pass
    
        #validate port and the master url
        if self.port is None:
            if self.command=='start_master':
                self.port=settings.MASTER_PORT
            elif self.command=='start_webserver':
                self.port=settings.WEB_PORT
            else:
                self.port=settings.PORT
    
        if int(self.port) < 1024 or int(self.port)>65536:
            print 'Invalid port value ',self.port
            sys.exit(1)
            
    
    def execute(self):
        #now check the command and figure out what to do
        if self.command=='start_master':
            self.m=master.Master()
            self.m.start(self.port,self.host)
        elif self.command=='start_node':
            if self.master_url is None:
                self.master_url="http://%s:%s"%(settings.HOST,settings.MASTER_PORT)
    
            #the URL pattern
            self.r=re.compile('http://.*:\d*')
    
            if self.r.match(self.master_url) is None:
                print 'Invalid master URL ',self.master_url
                sys.exit(1)
            self.n=node.Node()
            self.n.start(self.master_url,self.port,self.host)
        elif self.command=='start_webserver':
            w=webserver.WebServer()
            w.start(self.port)
        else:
            self.usage()

if __name__=='__main__':
    started=False
    try:
        a=Admin()
        a.parse()
        a.execute()
        started=True
    except:
        a.usage()
        print """---------------"""
        raise 
