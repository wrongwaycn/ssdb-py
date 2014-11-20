#coding=utf-8
from __future__ import with_statement
from itertools import chain
from select import select
import os
import socket
import sys
import threading
from ssdb._compat import (b, xrange, imap, byte_to_chr, unicode, bytes, long,
                           BytesIO, nativestr, basestring,
                           LifoQueue, Empty, Full)
from ssdb.utils import get_integer
from ssdb.exceptions import (
    RES_STATUS_MSG,
    RES_STATUS,    
    SSDBError,
    TimeoutError,
    ConnectionError,
    BusyLoadingError,
    ResponseError,
    InvalidResponse,
    AuthenticationError,
    NoScriptError,
    ExecAbortError,
    )
        
SYM_LF = b('\n')
SYM_EMPTY = b('')

SERVER_CLOSED_CONNECTION_ERROR = "Connection closed by server."

class Token(object):
    """
    Literal strings in SSDB commands, such as the command names and any
    hard-coded arguments are wrapped in this class so we know not to
    apply and encoding rules on them.
    """

    def __init__(self, value):
        if isinstance(value, Token):
            value = value.value
        self.value = value

    def __repr__(self):
        return self.value

    def __str__(self):
        return self.value


class BaseParser(object):
    EXCEPTION_CLASSES = {
        'ERR': ResponseError,
        'EXECABORT': ExecAbortError,
        'LOADING': BusyLoadingError,
        'NOSCRIPT': NoScriptError,
    }

    def parse_error(self, response):
        "Parse an error response"
        error_code = response.split(' ')[0]
        if error_code in self.EXCEPTION_CLASSES:
            response = response[len(error_code) + 1:]
            return self.EXCEPTION_CLASSES[error_code](response)
        return ResponseError(response)


class SocketBuffer(object):
    def __init__(self, socket, socket_read_size):
        self._sock = socket
        self.socket_read_size = socket_read_size
        self._buffer = BytesIO()
        # number of bytes written to the buffer from the socket
        self.bytes_written = 0
        # number of bytes read from the buffer
        self.bytes_read = 0

    @property
    def length(self):
        return self.bytes_written - self.bytes_read

    def read(self, length):
        # make sure to read the \n terminator
        length = length + 1
        # make sure we've read enough data from the socket
        if length > self.length:
            self._read_from_socket(length - self.length)

        self._buffer.seek(self.bytes_read)
        data = self._buffer.read(length)
        self.bytes_read += len(data)

        # purge the buffer when we've consumed it all so it doesn't
        # grow forever
        if self.bytes_read == self.bytes_written:
            self.purge()
        return data[:-1]

    def _read_from_socket(self, length=None):
        socket_read_size = self.socket_read_size
        buf = self._buffer
        buf.seek(self.bytes_written)
        marker = 0
        try:
            while True:
                data = self._sock.recv(socket_read_size)
                # an empty string indicates the server shutdown the socket
                if isinstance(data, bytes) and len(data) == 0:
                    raise socket.error(SERVER_CLOSED_CONNECTION_ERROR)
                buf.write(data)
                data_length = len(data)
                self.bytes_written += data_length
                marker += data_length
                
                if length is not None and length > marker:
                    continue                
                break
        except socket.timeout:
            raise TimeoutError("Timeout reading from socket")
        except socket.error:
            e = sys.exc_info()[1]
            raise ConnectionError("Error while reading from socket:%s"
                                  %(e.args,))
        

    def readline(self):
        buf = self._buffer
        buf.seek(self.bytes_read)
        data = buf.readline()
        while not data.endswith(SYM_LF):
            # there's more data in the socket that we need
            self._read_from_socket()
            buf.seek(self.bytes_read)
            data = buf.readline()            
        self.bytes_read += len(data)
        # purge the buffer when we've consumed it all so it doesn't
        # grow forever
        if self.bytes_read == self.bytes_written:
            self.purge()
        return data[:-1]

    def purge(self):
        self._buffer.seek(0)
        self._buffer.truncate()
        self.bytes_written = 0
        self.bytes_read = 0

    def close(self):
        self.purge()
        self._buffer.close()
        self._buffer = None
        self._sock = None
    
class PythonParser(BaseParser):
    """
    Plain Python parsing class
    """
    encoding = None

    def __init__(self, socket_read_size):
        self.socket_read_size = socket_read_size
        self._sock = None
        self._buffer = None    

    def __del__(self):
        try:
            self.on_disconnect()
        except Exception:
            pass

    def on_connect(self, connection):
        """
        Called when the socket connects
        """
        self._sock = connection._sock
        self._buffer = SocketBuffer(self._sock, self.socket_read_size)        
        if connection.decode_responses:
            self.encoding = connection.encoding
            
    def on_disconnect(self):
        "Called when the socket disconnects"
        if self._sock is not None:
            self._sock.close()
            self._sock = None
        if self._buffer is not None:
            self._buffer.close()
            self._buffer = None
        self.encoding = None

    def can_read(self):
        return self._buffer and bool(self._buffer.length)            

    def read_response(self):
        try:
            lgt = int(self._buffer.readline())
        except ValueError:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        status = self._buffer.readline()
        if status not in RES_STATUS or lgt!=len(status):
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        result = [status]
        while True:
            lgt = self._buffer.readline()
            if lgt == '':
                break
            try:
                value = self._buffer.read(int(lgt))
            except ValueError:
                raise ConnectionError(RES_STATUS_MSG.ERROR)
            if isinstance(value, bytes) and self.encoding:
                value = value.decode(self.encoding)
            result.append(value)

        return result


DefaultParser = PythonParser


class Connection(object):
    """
    Manages TCP communication to and from a SSDB server

        >>> from ssdb.connection import Connection
        >>> conn = Connection(host='localhost', port=8888)
    """

    description_format = "Connection<host=%(host)s,port=%(port)s>"
    
    def __init__(self, host="127.0.0.1",port=8888,socket_timeout=None,
                 socket_connect_timeout=None,socket_keepalive=False,
                 socket_keepalive_options=None,retry_on_timeout=False, 
                 encoding='utf-8', encoding_errors='strict',
                 decode_responses=False, parser_class=DefaultParser,
                 socket_read_size=65536):
        self.pid = os.getpid()        
        self.host = host
        self.port = port
        self._sock = None
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout or socket_timeout
        self.socket_keepalive = socket_keepalive
        self.socket_keepalive_options = socket_keepalive_options or {}
        self.retry_on_timeout = retry_on_timeout        
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.decode_responses = decode_responses
        self._parser = parser_class(socket_read_size=socket_read_size)        
        self._description_args = {
            'host': self.host,
            'port': self.port,
        }
        self._connect_callbacks = []

    @property
    def kwargs(self):
        return self._description_args

    def __repr__(self):
        return self.description_format % self._description_args

    def __del__(self):
        try:
            self.disconnect()
        except Exception:
            pass

    def register_connect_callback(self, callback):
        self._connect_callbacks.append(callback)

    def clear_connect_callbacks(self):
        self._connect_callbacks = []

    def connect(self):
        """
        Connects to the SSDB server if not already connected
        """
        if self._sock:
            return
        try:
            sock = self._connect()
        except socket.error:
            e = sys.exc_info()[1]
            raise ConnectionError(self._error_message(e))

        self._sock = sock
        try:
            self.on_connect()
        except SSDBError:
            # clean up after any error in on_connect
            self.disconnect()
            raise

        # run any user callbacks. right now the only internal callback
        # is for pubsub channel/pattern resubscription
        for callback in self._connect_callbacks:
            callback(self)        

    def _connect(self):
        """
        Create a TCP socket connection
        """
        # we want to mimic what socket.create_connection does to support
        # ipv4/ipv6, but we want to set options prior to calling
        # socket.connect()
        err = None
        for res in socket.getaddrinfo(self.host, self.port, 0,
                                      socket.SOCK_STREAM):
            family, socktype, proto, canonname, socket_address = res
            sock = None
            try:
                sock = socket.socket(family, socktype, proto)
                # TCP_NODELAY
                sock.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY, 1)

                # TCP_KEEPALIVE
                if self.socket_keepalive:
                    sock.setsockopt(socket.SOL_SOCKET,socket.SO_KEEPALIVE, 1)
                    for k, v in iteritems(self.socket_keepalive_options):
                        sock.setsockopt(socket.SOL_TCP, k, v)                

                # set the socket_connect_timeout before we connect
                sock.settimeout(self.socket_connect_timeout)
                # connect
                sock.connect(socket_address)
                # set the socket_timeout now that we're connected
                sock.settimeout(self.socket_timeout)
                return sock                

            except socket.error as _:
                err = _
                if sock is not None:
                    sock.close()
        if err is not None:
            raise err
        raise socket.error("socket.getaddrinfo returned an empty list")
        #sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sock.settimeout(self.socket_timeout)
        #sock.connect((self.host, self.port))
        #return sock

    def _error_message(self, exception):
        """
        args for socket.error can either be (errno, "message") or just "message"
        """
        if len(exception.args) == 1:
            return "Error connecting to %s:%s. %s." % \
                (self.host, self.port, exception.args[0])
        else:
            return "Error %s connecting %s:%s. %s." % \
                (exception.args[0], self.host, self.port, exception.args[1])
        
    def on_connect(self):
        """
        Initialize the connection
        """
        self._parser.on_connect(self)

    def disconnect(self):
        """
        Disconnects from the SSDB server
        """
        self._parser.on_disconnect()
        if self._sock is None:
            return
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
            self._sock.close()
        except socket.error:
            pass
        self._sock = None

    def send_packed_command(self, command):
        """
        Send an already packed command to the SSDB server        
        """
        if not self._sock:
            self.connect()
        try:
            if isinstance(command, str):
                command = [command]
            for item in command:
                self._sock.sendall(item)            
        except socket.timeout:
            self.disconnect()
            raise TimeoutError("Timeout writing to socket")            
        except socket.error:
            e = sys.exc_info()[1]
            self.disconnect()
            if len(e.args) == 1:
                _errno, errmsg = 'UNKNOWN', e.args[0]
            else:
                _errno, errmsg = e.args
            raise ConnectionError("Error %s while writing to socket. %s." %
                (_errno, errmsg))
        except:
            self.disconnect()
            raise

    def send_command(self, *args):
        """
        Pack and send a command to the SSDB server
        """
        self.send_packed_command(self.pack_command(*args))

    def can_read(self, timeout=0):
        "Poll the socket to see if there's data that can be read."
        sock = self._sock
        if not sock:
            self.connect()
            sock = self._sock
        return self._parser.can_read() or \
            bool(select([sock], [], [], timeout)[0])        

    def read_response(self):
        """
        Read the response from a previously sent command
        """
        try:
            response = self._parser.read_response()
        except:
            self.disconnect()
            raise
        if isinstance(response, ResponseError):
            raise response
        #print(response)
        return response

    def encode(self, value):
        """
        Return a bytestring representation of the value
        """
        if isinstance(value, Token):
            return b(value.value)        
        if isinstance(value, bytes):
            return value
        elif isinstance(value, (int, long)):
            value = b(str(value))        
        elif isinstance(value, float):
            value = repr(value)
        elif not isinstance(value, basestring):
            value = str(value)
        if isinstance(value, unicode):
            value = value.encode(self.encoding, self.encoding_errors)
        return value

    def pack_command(self, *args):
        """
        Pack a series of arguments into a value SSDB command
        """
        # the client might have included 1 or more literal arguments in
        # the command name, e.g., 'CONFIG GET'. The SSDB server expects
        # these arguments to be sent separately, so split the first
        # argument manually. All of these arguements get wrapped
        # in the Token class to prevent them from being encoded.
        command = args[0]
        if ' ' in command:
            args = tuple([Token(s) for s in command.split(' ')]) + args[1:]
        else:
            args = (Token(command),) + args[1:]
        args_output = SYM_EMPTY.join([
            SYM_EMPTY.join((
                b(str(len(k))),
                SYM_LF,
                k,
                SYM_LF
            )) for k in imap(self.encode, args)
        ])
        output = "%s%s" % (args_output,SYM_LF)
        return output

    def pack_commands(self, commands):
        "Pack multiple commands into the SSDB protocol"
        output = []
        pieces = []
        buffer_length = 0

        for cmd in commands:
            for chunk in self.pack_command(*cmd):
                pieces.append(chunk)
                buffer_length += len(chunk)
            if buffer_length > 6000:
                output.append(SYM_EMPTY.join(pieces))
                buffer_length = 0
                pieces = []

        if pieces:
            output.append(SYM_EMPTY.join(pieces))
        return output        
    
class ConnectionPool(object):
    """
    Generic connection pool.

        >>> from ssdb.client import SSDB
        >>> client = SSDB(connection_pool=ConnectionPool())
        
    If max_connections is set, then this objectraises ssdb.ConnectionError when
    the pool's limit is reached. By default, TCP connections are created
    connection_class is specified. Any additionan keyword arguments are passed
    to the constructor of connection_class.
    """
    def __init__(self, connection_class=Connection, max_connections=None,
                 **connection_kwargs):
        """
        Create a connection pool. If max_connections is set, then this object
        raises ssdb.ConnectionError when the pool's limit is reached. By
        default, TCP connections are created connection_class is specified. Any
        additionan keyword arguments are passed to the constructor of
        connection_class.
        """
        max_connections = max_connections or 2 ** 31
        if not isinstance(max_connections, (int, long)) or max_connections < 0:
            raise ValueError('"max_connections" must be a positive integer')        
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.max_connections = max_connections
        self.reset()
        
    def __repr__(self):
        return "%s<%s>" % (
            type(self).__name__,
            self.connection_class.description_format % self.connection_kwargs,
        )

    def reset(self):
        self.pid = os.getpid()
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()
        self._check_lock = threading.Lock()    

    def _checkpid(self):
        if self.pid != os.getpid():
            with self._check_lock:
                if self.pid == os.getpid():
                    # another thread already did the work while we waited
                    # on the lock.
                    return
                self.disconnect()
                self.reset()

    def get_connection(self, command_name, *keys, **options):
        """
        Get a connection from pool.
        """
        self._checkpid()
        try:
            connection = self._available_connections.pop()
        except IndexError:
            connection = self.make_connection()
        self._in_use_connections.add(connection)
        return connection

    def make_connection(self):
        """
        Create a new connection
        """
        if self._created_connections >= self.max_connections:
            raise ConnectionError("Too many connections")
        self._created_connections += 1
        return self.connection_class(**self.connection_kwargs)

    def release(self, connection):
        """
        Release the connection back to the pool.
        """
        self._checkpid()
        if connection.pid != self.pid:
            return        
        self._in_use_connections.remove(connection)
        self._available_connections.append(connection)
            
    def disconnect(self):
        """
        Disconnects all connections in the pool.
        """
        all_conns = chain(self._available_connections,
                          self._in_use_connections)
        for connection in all_conns:
            connection.disconnect()

            
class BlockingConnectionPool(ConnectionPool):
    """
    Thread-safe blocking connection pool::

        >>> from ssdb.client import SSDB
        >>> client = SSDB(connection_pool=BlockingConnectionPool())

    It performs the same function as default
    ``:py:class: ~ssdb.connection.ConnectionPool`` implementation, in that, it
    maintains a pool of reusable connections that can be shared by multiple ssdb
    clients (safely across threads if required).

    The difference is that, in the event that a client tries to get a connection
    from the pool when all of connections are in use, rather than raising a
    ``:py:class: ~ssdb.exceptions.ConnectionError`` (as the default ``:py:class:
    ~ssdb.connection.ConnectionPool`` implementation does), it makes the client
    wait ("blocks") for a specified number of seconds until a connection becomes
    available/].

    Use ``max_connections`` to increase / decrease the pool size::

        >>> pool = BlockingConnectionPool(max_connections=10)

    Use ``timeout`` to tell it either how many seconds to wait for a connection
    to become available, or to block forever:

        >>> #Block forever.
        >>> pool = BlockingConnectionPool(timeout=None)

        >>> #Raise a ``ConnectionError`` after five seconds if a connection is not
        >>> #available
        >>> pool = BlockingConnectionPool(timeout=5)
    """
    def __init__(self, max_connections=50, timeout=20,
                 connection_class=Connection,queue_class=LifoQueue,
                 **connection_kwargs):

        self.queue_class = queue_class
        self.timeout = timeout
        super(BlockingConnectionPool, self).__init__(
            connection_class=connection_class, max_connections=max_connections,
            **connection_kwargs)

    def reset(self):
        self.pid = os.getpid()
        self._check_lock = threading.Lock()

        # Create and fill up a thread safe queue with ``None`` values.
        self.pool = self.queue_class(self.max_connections)
        while True:
            try:
                self.pool.put_nowait(None)
            except Full:
                break            

        # Keep a list of actual connection instances so that we can
        # disconnect them later.
        self._connections = []

    def make_connection(self):
        "Make a fresh connection."
        connection = self.connection_class(**self.connection_kwargs)
        self._connections.append(connection)
        return connection


    def get_connection(self, command_name, *keys, **options):
        """
        Get a connection, blocking for ``self.timeout`` until a connection
        is available from the pool.
        
        If the connection returned is ``None`` then creates a new connection.
        Because we use a last-in first-out queue, the existing connections
        (having been returned to the pool after the initial ``None`` values
        were added) will be returned before ``None`` values. This means we only
        create new connections when we need to, i.e.: the actual number of
        connections will only increase in response to demand.
        """

        # Make sure we haven't changed process.
        self._checkpid()

        # Try and get a connection from the pool. If one isn't available within
        # self.timeout then raise a ``ConnectionError``.
        connection = None
        try:
            connection = self.pool.get(block=True,timeout=self.timeout)
        except Empty:
            # Note that this is not caught by the redis client and will be
            # raised unless handled by application code. If you want never to            
            raise ConnectionError("No connection available.")

        # If the ``connection`` is actually ``None`` then that's a cue to make
        # a new connection to add to the pool.
        if connection is None:
            connection = self.make_connection()

        return connection

    def release(self, connection):
        "Releases the connection back to the pool."
        # Make sure we haven't changed process.
        self._checkpid()
        if connection.pid != self.pid:
            return
        # Put the connection back into the pool.
        try:
            self.pool.put_nowait(connection)
        except Full:
            # perhaps the pool has been reset() after a fork? regardless,
            # we don't want this connection
            pass

    def disconnect(self):
        "Disconnects all connections in the pool."
        for connection in self._connections:
            connection.disconnect()
