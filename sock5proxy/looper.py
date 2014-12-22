# -*- coding: utf-8 -*-
'''
Created on 2014-7-1

@author: Owner
'''
import threading
import select
import socket
import os
import logging
import errno
from collections import defaultdict
import tcpRelay

POLL_NULL = 0x00
POLL_IN = 0x01
POLL_OUT = 0x04
POLL_ERR = 0x08
POLL_HUP = 0x10
POLL_NVAL = 0x20

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.ERROR)

class Looper(object):
    _instance_lock = threading.Lock()
    def __init__(self):
        self._rlist = set()
        self._wlist = set()
        self._elist = set()
        self._handlers = []
        self.isStop = True
    
    def start_loop(self):
        self.isStop = False
        print('start running...')
        while not self.isStop:
            try:
                events = self.poll(1)
            except (OSError, IOError) as e:
                if errno_from_exception(e) == errno.EPIPE:
                    # Happens when the client closes the connection
                    logging.error('poll:%s', e)
                    continue
                else:
                    logging.error('poll:%s', e)
                    import traceback
                    traceback.print_exc()
                    continue
            for handler in self._handlers:
                # TODO when there are a lot of handlers
                try:
                    handler(events)
                except (OSError, IOError) as e:
                    logging.error(e)
                    import traceback
                    traceback.print_exc()
    
    def stop_loop(self):
        self.isStop = True
    
    @staticmethod
    def instance():
        if not hasattr(Looper, "_instance"):
            with Looper._instance_lock:
                if not hasattr(Looper, "_instance"):
                    Looper._instance = Looper()
        return Looper._instance
    
    def poll(self,timeout):
        r,w,e = select.select(self._rlist, self._wlist, self._elist,timeout)
        #print r,w,e
        results = defaultdict(lambda: POLL_NULL)
        for p in [(r, POLL_IN), (w, POLL_OUT), (e, POLL_ERR)]:
            for fd in p[0]:
                results[fd] |= p[1]
        return results.items()
    
    def add(self,fd,mode):
        if mode & POLL_IN:
            self._rlist.add(fd)
        if mode & POLL_OUT:
            self._wlist.add(fd)
        if mode & POLL_ERR:
            self._elist.add(fd)
    
    def remove(self,fd):
        if fd in self._rlist:
            self._rlist.remove(fd)
        if fd in self._wlist:
            self._wlist.remove(fd)
        if fd in self._elist:
            self._elist.remove(fd)
            
    def update(self,fd,mode):
        self.remove(fd)
        self.add(fd, mode)
            
    def add_handler(self,handler):
        self._handlers.append(handler)
    
    def remove_handler(self,handler): 
        self._handlers.remove(handler)   
        
        
# from tornado


def errno_from_exception(e):
    """Provides the errno from an Exception object.

    There are cases that the errno attribute was not set so we pull
    the errno out of the args but if someone instatiates an Exception
    without any args you will get a tuple error. So this function
    abstracts all that behavior to give you a safe way to get the
    errno.
    """

    if hasattr(e, 'errno'):
        return e.errno
    elif e.args:
        return e.args[0]
    else:
        return None


# from tornado
def get_sock_error(sock):
    error_number = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
    return socket.error(error_number, os.strerror(error_number))
            
if __name__ == '__main__':
    print("start")
    config = {
        'addr':'0.0.0.0',
        'port':1080,
        }
    tcp = tcpRelay.TcpRelay(config)
    tcp.add_to_loop(Looper.instance())
    Looper.instance().start_loop()    
    
    
    
    