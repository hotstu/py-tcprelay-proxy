# -*- coding: utf-8 -*-
'''
Created on 2014-7-1

@author: Owner
'''
import socket,struct
import looper
import logging
import errno,traceback
import time

# config = {
#         addr:x.x.x.x,
#         port:xxxx,
#         }
BUFF = 32 * 1024

STATE_INIT = 0
STATE_HELLOSENT = 1
STATE_REPLYSENT = 2
STATE_STREAMING = 3
STATE_DESTROYED = 4
STATE_UDP_ASSOC = 5

CMD_CONNECT = 1
CMD_BIND = 2
CMD_UDP_ASSOCIATE = 3

# stream direction
STREAM_UP = 0
STREAM_DOWN = 1

# stream wait status
WAIT_STATUS_INIT = 0
WAIT_STATUS_READING = 1
WAIT_STATUS_WRITING = 2
WAIT_STATUS_READWRITING = WAIT_STATUS_READING | WAIT_STATUS_WRITING

TIMEOUT_CHECK_PERIOD = 4
TIMEOUT_SET = 60
TIMEOUTS_CLEAN_SIZE = 512

class TcpRelay(object):
    def __init__(self,config):
        listen_addr = config['addr']
        listen_port = config['port']
        addrs = socket.getaddrinfo(listen_addr, listen_port, 0,
                                   socket.SOCK_STREAM, socket.SOL_TCP)
        if len(addrs) == 0:
            raise Exception("can't get addrinfo for %s:%d" %
                            (listen_addr, listen_port))
        af, socktype, proto, canonname, sa = addrs[0]
        server_socket = socket.socket(af, socktype, proto)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(sa)
        server_socket.setblocking(False)
        server_socket.listen(1024)
        self._server_socket = server_socket
        self._eventloop = None
        self._closed = False
        self.handlerdispather = {}
        self._timeouts = []  # a list for all the handlers
        self._timeout_offset = 0  # last checked position for timeout,we trim the timeouts once a while
        self._handler_to_timeouts = {}  # key: handler value: index in timeouts
        self._last_timeout_check = time.time()
                                         
    def add_to_loop(self, loop):
        if self._eventloop:
            raise Exception('already add to loop')
        if self._closed:
            raise Exception('already closed')
        self._eventloop = loop
        self._eventloop.add_handler(self._handle_events)
        self._eventloop.add(self._server_socket.fileno(),
                            looper.POLL_IN | looper.POLL_ERR)  
          
    def _handle_events(self,events):
        for fd,event in events:
            if fd == self._server_socket.fileno():
                #handle server event
                #time.sleep(3)
                self._handle_server(event)
            else:
                #handle client event
                clent_handler = self.handlerdispather.get(fd,None)
                if clent_handler:
                    clent_handler.handle_event(fd,event) 
                else:
                    logging.warn("fd not have handler,maybe removed?")
        #end of for fd,event in events: 
        now = time.time() 
        if now - self._last_timeout_check > TIMEOUT_CHECK_PERIOD:
            self._sweep_timeout()
            self._last_timeout_check = now            
        #end of timeout checking             
    
    def _handle_server(self,event):
        if event & looper.POLL_ERR:
            # TODO
            raise Exception('server_socket error')
        try:
            logging.debug("_handle_server")
            conn,addr = self._server_socket.accept()
            print('accept connction from ' + addr[0])
            ClientHandler(conn, self._eventloop, self)
        except (OSError, IOError) as e:
            error_no = looper.errno_from_exception(e)
            if error_no in (errno.EAGAIN, errno.EINPROGRESS):
                return
            else:
                logging.error(e)
                traceback.print_exc()
                
    def _sweep_timeout(self):
        # tornado's timeout memory management is more flexible than we need
        # we just need a sorted last_activity queue and it's faster than heapq
        # in fact we can do O(1) insertion/remove so we invent our own
        if self._timeouts:
            now = time.time()
            length = len(self._timeouts)
            pos = self._timeout_offset
            logging.debug("sweeping timeout....%d,%d" % (pos,length))
            while pos < length:
                handler = self._timeouts[pos]
                if handler:
                    if now - handler.last_activity < TIMEOUT_SET:
                        break
                    else:
                        if handler._remote_address:
                            logging.warn('timed out: %s:%d' %
                                         handler._remote_address)
                        else:
                            logging.warn('timed out')
                        handler.destroy()
                        self._timeouts[pos] = None  # free memory
                        pos += 1
                else:
                    pos += 1
            if pos > TIMEOUTS_CLEAN_SIZE and pos > length >> 1:
                # clean up the timeout queue when it gets larger than half
                # of the queue
                self._timeouts = self._timeouts[pos:]
                for key in self._handler_to_timeouts:
                    self._handler_to_timeouts[key] -= pos
                pos = 0
            self._timeout_offset = pos
            #print self._timeout_offset,self._timeouts

    def update_activity(self, handler):
        """ set handler to active """
        now = int(time.time())
        if now - handler.last_activity < TIMEOUT_CHECK_PERIOD:
            # thus we can lower timeout modification frequency
            return
        handler.last_activity = now
        index = self._handler_to_timeouts.get(hash(handler), -1)
        if index >= 0:
            # delete is O(n), so we just set it to None
            self._timeouts[index] = None
        length = len(self._timeouts)
        self._timeouts.append(handler)
        self._handler_to_timeouts[hash(handler)] = length
    
    def remove_handler(self, handler):
        index = self._handler_to_timeouts.get(hash(handler), -1)
        if index >= 0:
            # delete is O(n), so we just set it to None
            self._timeouts[index] = None
            del self._handler_to_timeouts[hash(handler)]
            
            
class ClientHandler(object):
    def __init__(self,local_sock,loop,server_object):
        self._local = local_sock
        self._loop = loop
        self._server = server_object
        self._local.setblocking(False)
        self._local.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self._loop.add(local_sock.fileno(), looper.POLL_IN | looper.POLL_ERR)
        self._server.handlerdispather[self._local.fileno()] = self
        self._remote = None
        self._remote_address = None
        self._data_to_write_to_local = []
        self._data_to_write_to_remote = []
        self._state = STATE_INIT
        self._downstream_status = WAIT_STATUS_READING
        self._upstream_status = WAIT_STATUS_INIT
        self.last_activity = int(time.time()) 
        self.last_activity = 0
        self._server.update_activity(self)       
        
    def handle_event(self,fd,event):
        if self._state == STATE_DESTROYED:
            logging.debug('ignore handle_event: destroyed')
            return
        # order is important
        
        if fd == self._local.fileno():
            if event & looper.POLL_ERR:
                self._on_local_error()
                if self._state == STATE_DESTROYED:
                    return
            if event & (looper.POLL_IN | looper.POLL_HUP):
                self._on_local_read()
                if self._state == STATE_DESTROYED:
                    return
            if event & looper.POLL_OUT:
                self._on_local_write()
        elif fd == self._remote.fileno():
            if event & looper.POLL_ERR:
                self._on_remote_error()
                if self._state == STATE_DESTROYED:
                    return
            if event & (looper.POLL_IN | looper.POLL_HUP):
                self._on_remote_read()
                if self._state == STATE_DESTROYED:
                    return
            if event & looper.POLL_OUT:
                self._on_remote_write()
        else:
            logging.warn('unknown socket')
            
        
    def _on_local_read(self):
        logging.debug('on_local_read()')
        self._server.update_activity(self)
        data = None
        try:
            data = self._local.recv(BUFF)
        except (OSError, IOError) as e:
            if looper.errno_from_exception(e) in \
                    (errno.ETIMEDOUT, errno.EAGAIN):
                return
        if not data:
            self.destroy()
            return
        #print(''.join(hex(ord(n))[2:].zfill(2) for n in data))
        print(data)
        if STATE_STREAMING == self._state:
            logging.debug("STATE_STREAMING")
            self._write_to_sock(data, self._remote)
            return
        elif STATE_INIT == self._state:
            #select auth methd
            logging.debug("STATE_INIT")
            self._write_to_sock('\x05\00', self._local)
            self._state = STATE_HELLOSENT
            return
        elif STATE_HELLOSENT == self._state:
            #receive cmd type ,remote addr
            logging.debug("STATE_HELLOSENT")
            self._handle_cmd(data)
            return
                              
    def _handle_cmd(self, data):    
        try:
            cmd = ord(data[1])
            if cmd == CMD_UDP_ASSOCIATE:
                logging.debug('UDP associate')
                #do not support ipv6
                header = '\x05\x00\x00\x01'
                addr, port = self._local.getsockname()
                addr_to_send = socket.inet_aton(addr)
                port_to_send = struct.pack('>H', port)
                self._write_to_sock(header + addr_to_send + port_to_send,
                                    self._local)
                self._state = STATE_UDP_ASSOC
                # just wait for the client to disconnect
                return
            elif cmd == CMD_CONNECT:
                # just trim VER CMD RSV
                data = data[3:]
            else:
                logging.error('unknown command %d', cmd)
                self.destroy()
                return
            header_result = parse_header(data)
            if header_result is None:
                raise Exception('can not parse header')
            addrtype, remote_addr, remote_port, header_length = header_result
            logging.info('connecting %s:%d' % (remote_addr, remote_port))
            self._remote_address = (remote_addr, remote_port)
            # pause reading
            # here we will create a remote socket ,it will return with writable state
            # so before on_remote_write ,we pause reading from local
            self._update_stream(STREAM_UP, WAIT_STATUS_WRITING)
            self._write_to_sock('\x05\x00\x00\x01\x00\x00\x00\x00\x10\x10',
                                    self._local)
            #self._state = STATE_REPLYSENT
            addrs = socket.getaddrinfo(remote_addr, remote_port, 0, socket.SOCK_STREAM,
                                   socket.SOL_TCP)
            if len(addrs) == 0:
                raise Exception("getaddrinfo failed for %s:%d" % (remote_addr,  remote_port))
            af, socktype, proto, canonname, sa = addrs[0]
            self._remote = socket.socket(af, socktype, proto)
            self._server.handlerdispather[self._remote.fileno()] = self
            self._remote.setblocking(False)
            self._remote.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            try:
                self._remote.connect((remote_addr, remote_port))
            except (OSError, IOError) as e:
                if looper.errno_from_exception(e) == \
                        errno.EINPROGRESS:
                    pass
            self._loop.add(self._remote.fileno(),
                           looper.POLL_ERR | looper.POLL_OUT)            
            
        except Exception as e:
            logging.error(e)
            traceback.print_exc()
            # TODO use logging when debug completed
            self.destroy()
                         
    def _update_stream(self, stream, status):
        dirty = False
        if stream == STREAM_DOWN:
            if self._downstream_status != status:
                self._downstream_status = status
                dirty = True
        elif stream == STREAM_UP:
            if self._upstream_status != status:
                self._upstream_status = status
                dirty = True
        if dirty:
            if self._local:
                event = looper.POLL_ERR
                if self._downstream_status & WAIT_STATUS_WRITING:
                    event |= looper.POLL_OUT
                if self._upstream_status & WAIT_STATUS_READING:
                    event |= looper.POLL_IN
                self._loop.update(self._local.fileno(), event)
            if self._remote:
                event = looper.POLL_ERR
                if self._downstream_status & WAIT_STATUS_READING:
                    event |= looper.POLL_IN
                if self._upstream_status & WAIT_STATUS_WRITING:
                    event |= looper.POLL_OUT
                self._loop.update(self._remote.fileno(), event)
            
    def _write_to_sock(self,data,sock):
        logging.debug('writting to: %s:%d' % (sock.getpeername()))
        if not data or not sock:
            return False
        uncomplete = False
        try:
            length = len(data)
            sendedlen = sock.send(data)
            if sendedlen < length:
                data = data[sendedlen:]
                uncomplete = True
        except (OSError, IOError) as e:
            error_no = looper.errno_from_exception(e)
            #在VxWorks和 Windows上，EAGAIN的名字叫做EWOULDBLOCK。
            if error_no in (errno.EAGAIN, errno.EINPROGRESS, errno.EWOULDBLOCK):
                uncomplete = True
            else:
                logging.error(e)
                traceback.print_exc()
                self.destroy()
                return False
        if uncomplete:
            if sock == self._local:
                self._data_to_write_to_local.append(data)
                self._update_stream(STREAM_DOWN, WAIT_STATUS_WRITING)
            elif sock == self._remote:
                self._data_to_write_to_remote.append(data)
                self._update_stream(STREAM_UP, WAIT_STATUS_WRITING)
            else:
                logging.error('write_all_to_sock:unknown socket')
        else:
            if sock == self._local:
                self._update_stream(STREAM_DOWN, WAIT_STATUS_READING)
            elif sock == self._remote:
                self._update_stream(STREAM_UP, WAIT_STATUS_READING)
            else:
                logging.error('write_all_to_sock:unknown socket')
        return True
            
    def _on_local_write(self):
        logging.debug('on_local_write')
        if self._data_to_write_to_local:
            data = ''.join(self._data_to_write_to_local)
            self._data_to_write_to_local = []
            self._write_to_sock(data, self._local)
        else:
            self._update_stream(STREAM_DOWN, WAIT_STATUS_READING)
    
    def _on_local_error(self):
        logging.debug('got local error')
        if self._local:
            logging.error(looper.get_sock_error(self._local))
        self.destroy()
    
    def _on_remote_read(self):
        logging.debug('on_remote_read')
        self._server.update_activity(self)
        data = None
        try:
            data = self._remote.recv(BUFF)
        except (OSError, IOError) as e:
            if looper.errno_from_exception(e) in \
                    (errno.ETIMEDOUT, errno.EAGAIN):
                return
        if not data:
            self.destroy()
            return
        try:
            self._write_to_sock(data, self._local)
        except Exception as e:
            logging.error(e)
            traceback.print_exc()
            # TODO use logging when debug completed
            self.destroy()
    
    def _on_remote_write(self):
        logging.debug('on_remote_write')
        self._state = STATE_STREAMING
        if self._data_to_write_to_remote:
            data = ''.join(self._data_to_write_to_remote)
            self._data_to_write_to_remote = []
            self._write_to_sock(data, self._remote)
        else:
            self._update_stream(STREAM_UP, WAIT_STATUS_READING)  
    
    def _on_remote_error(self):
        logging.debug('got remote error')
        if self._remote:
            logging.error(looper.get_sock_error(self._remote))
        self.destroy()
    
        
    
    def destroy(self):
        if self._state == STATE_DESTROYED:
            logging.debug('already destroyed')
            return
        self._state = STATE_DESTROYED
        if self._remote_address:
            logging.debug('destroy: %s:%d' %
                          self._remote_address)
        else:
            logging.debug('destroy')
        if self._remote:
            logging.debug('destroying remote ')
            self._loop.remove(self._remote.fileno())
            del self._server.handlerdispather[self._remote.fileno()]
            self._remote.close()
            self._remote = None
        if self._local:
            logging.debug('destroying local ')
            self._loop.remove(self._local.fileno())
            del self._server.handlerdispather[self._local.fileno()]
            self._local.close()
            self._local = None
        self._server.remove_handler(self)
        
ADDRTYPE_IPV4 = 1
ADDRTYPE_IPV6 = 4
ADDRTYPE_HOST = 3


def parse_header(data):
    addrtype = ord(data[0])
    dest_addr = None
    dest_port = None
    header_length = 0
    if addrtype == ADDRTYPE_IPV4:
        if len(data) >= 7:
            dest_addr = socket.inet_ntoa(data[1:5])
            dest_port = struct.unpack('>H', data[5:7])[0]
            header_length = 7
        else:
            logging.warn('header is too short')
    elif addrtype == ADDRTYPE_HOST:
        if len(data) > 2:
            addrlen = ord(data[1])
            if len(data) >= 2 + addrlen:
                dest_addr = data[2:2 + addrlen]
                dest_port = struct.unpack('>H', data[2 + addrlen:4 +
                                          addrlen])[0]
                header_length = 4 + addrlen
            else:
                logging.warn('header is too short')
        else:
            logging.warn('header is too short')
    else:
        logging.warn('unsupported addrtype %d, maybe wrong password' % addrtype)
    if dest_addr is None:
        return None
    return addrtype, dest_addr, dest_port, header_length  
        
        
        
        
        
        
        
        
        
        
        
        
        
            