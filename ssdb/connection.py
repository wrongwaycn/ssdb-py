#coding=utf-8
from itertools import chain
import os
import socket
import sys


from ssdb._compat import (b, xrange, imap, byte_to_chr, unicode, bytes, long,
                           BytesIO, nativestr, basestring,
                           LifoQueue, Empty, Full)

from ssdb.exceptions import (
    SSDBError,
    ConnectionError,
    BusyLoadingError,
    ResponseError,
    InvalidResponse,
    AuthenticationError,
    NoScriptError,
    ExecAbortError,
    )
        
#SYM_STAR = b('*')
#SYM_DOLLAR = b('$')
SYM_CRLF = b('\r\n')
SYM_LF = b('\n')
SYM_LFLF = b('\n\n')
SYM_EMPTY = b('')


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

    
class PythonParser(BaseParser):
    """
    Plain Python parsing class
    """
    MAX_READ_LENGTH = 1000000
    encoding = None

    def __init__(self):
        self._fp = None

    def __del__(self):
        try:
            self.on_disconnect()
        except Exception:
            pass

    def on_connect(self, connection):
        """
        Called when the socket connects
        """
        self._fp = connection._sock.makefile('rb')
        if connection.decode_responses:
            self.encoding = connection.encoding
            
    def on_disconnect(self):
        """
        Called when the socket disconnects
        """
        if self._fp is not None:
            self._fp.close()
            self._fp = None

    def read(self, length=None):
        """
        Read a line from the socket if no length is specified,
        otherwise read ``length`` bytes. Always strip away the
        newlines.
        """
        try:
            if length is not None:
                bytes_left = length + 1  # read the line ending
                if length > self.MAX_READ_LENGTH:
                    # apparently reading more than 1MB or so from a windows
                    # socket can cause MemoryErrors. 
                    # read smaller chunks at a time to work around this
                    try:
                        buf = BytesIO()
                        while bytes_left > 0:
                            read_len = min(bytes_left, self.MAX_READ_LENGTH)
                            buf.write(self._fp.read(read_len))
                            bytes_left -= read_len
                        buf.seek(0)
                        return buf.read(length)
                    finally:
                        buf.close()
                return self._fp.read(bytes_left)[:-1]
            
            # no length, read a full line
            return self._fp.readline()
        except (socket.error, socket.timeout):
            e = sys.exc_info()[1]
            raise ConnectionError("Error while reading from socket: %s" %
                                  (e.args,))

    def read_response(self):
        result = []
        while True:
            response = self.read()
            if not response:
                raise ConnectionError("Socket closed on remote end")
            if response == '\n':
                break
            try:
                length = long(response[:-1])
            except ValueError:
                raise InvalidResponse(
                    "Protocol Error: cannot get the command code")                
            value = self.read(length)
            if isinstance(value, bytes) and self.encoding:
                value = value.decode(self.encoding)
            result.append(value)
        return result


DefaultParser = PythonParser


class Connection(object):
    """
    Manages TCP communication to and from a SSDB server

    parameters:
        host:host to connect
        port:port to connect
    """

    description_format = "Connection<host=%(host)s,port=%(port)s>"
    
    def __init__(self, host="127.0.0.1", port=8888, socket_timeout=None,
                 encoding='utf-8', encoding_errors='strict',
                 decode_responses=False, parser_class=DefaultParser):
        self.pid = os.getpid()        
        self.host = host
        self.port = port
        self._sock = None
        self.socket_timeout = socket_timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.decode_responses = decode_responses
        self._parser = parser_class()        
        self._description_args = {
            'host': self.host,
            'port': self.port,
        }

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

    def _connect(self):
        """
        Create a TCP socket connection
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.socket_timeout)
        sock.connect((self.host, self.port))
        return sock

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
            self._sock.sendall(command)
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
        if isinstance(value, bytes):
            return value
        if isinstance(value, float):
            value = repr(value)
        if not isinstance(value, basestring):
            value = str(value)
        if isinstance(value, unicode):
            value = value.encode(self.encoding, self.encoding_errors)
        return value

    def pack_command(self, *args):
        """
        Pack a series of arguments into a value SSDB command
        """
        args_output = SYM_EMPTY.join([
            SYM_EMPTY.join((
                b(str(len(k))),
                SYM_LF,
                k,
                SYM_LF
            )) for k in imap(self.encode, args)
        ])
        output = "%s%s" % (args_output,SYM_LF)
        #print(output)
        return output

    
class ConnectionPool(object):
    """
    Generic connection pool
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
        self.pid = os.getpid()
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.max_connections = max_connections or 2 ** 31
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()        
        
    def __repr__(self):
        return "%s<%s>" % (
            type(self).__name__,
            self.connection_class.description_format % self.connection_kwargs,
        )

    def _checkpid(self):
        if self.pid != os.getpid():
            self.disconnect()
            self.__init__(self.connection_class, self.max_connections,
                          **self.connection_kwargs)

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
        if connection.pid == self.pid:
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

            
class BlockingConnectionPool(object):
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

        #Block forever.
        >>> pool = BlockingConnectionPool(timeout=None)

        #Raise a ``ConnectionError`` after five seconds if a connection is not
        #available
        >>> pool = BlockingConnectionPool(timeout=5)
    """
    def __init__(self, max_connections=50, timeout=20, connection_class=None,
                 queue_class=None, **connection_kwargs):
        """
        Compose and assign value.
        """
        # Compose.
        if connection_class is None:
            connection_class = Connection
        if queue_class is None:
            queue_class = LifoQueue

        # Assign.
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.queue_class = queue_class
        self.max_connections = max_connections
        self.timeout = timeout

        # Validate the ``max_connections``. WIth the ``fill up the queue``
        # algorithm we use, it must be a positive integer.
        is_valid = isinstance(max_connections, int) and max_connections > 0
        if not is_valid:
            raise ValueError('``max_connections`` must be a positive integer')

        # Get the current process id, so we can disconnect and reinstantiate if
        # it changes.
        self.pid = os.getpid()

        # Create and fill up a thread safe queue with ``None`` values.
        self.pool = self.queue_class(max_connections)
        while True:
            try:
                self.pool.put_nowait(None)
            except Full:
                break

        # Keep a list of actual connection instances so that we can disconnect
        # them later.
        self._connections = []

    def __repr__(self):
        return "%s<%s>" % (
            type(self).__name__,
            self.connection_class.description_format % self.connection_kwargs,
        )

    def _checkpid(self):
        """
        Check the current process id. If it has changed, disconnect and
        re-instantiate this connection pool instance.
        """
        #Get the current process id.
        pid = os.getpid()

        # If it hasn't changed since we were instantiated, then we're fine, so
        # just exit, remaining connected.
        if self.pid == pid:
            return

        # If it has changed, then disconnect and re-instantiate.
        self.disconnect()
        self.reinstantiate()
        
    def make_connection(self):
        """
        Make a fresh connection.
        """
        connection = self.connection_class(**self.connection_kwargs)
        self._connections.append(connection)
        return connection

    def get_connection(self, command_name, *keys, **options):
        # Make sure we haven't changed process
        self._checkpid()

        # Try and get a connection from the pool. If one isn't available within
        # self.timeout then raise a ``ConnectionError``.
        connection = None
        try:
            connection = self.pool.get(block=True, timeout=self.timeout)
        except Empty:
            # Note that this is not caught by the redis client and will be
            # raised unless handled by application code. If you want never to
            raise ConnectionError("No connection available")

        # If the ``connection`` is actually ``None`` then that's a cue to make a
        # new connection to add to the pool.
        if connection is None:
            connection = self.make_connection()

        return connection

    def release(self, connection):
        """
        Release the connection back to the pool.
        """
        # Make sure we haven't changed process.
        self._checkpid()

        # Put the connection back into the pool.
        try:
            self.pool.put_nowait(connection)
        except Full:
            # This shouldn't normally happen but might perhaps happen after a
            # reinstantiation. So, we can handle the exception by not putting
            # the connection back on the pool, because we definitely do not want
            # to reuse it.
            pass

    def disconnect(self):
        """
        Disconnects all connections in the pool.
        """
        for connection in self._connections:
            connection.disconnect()

    def reinstantiate(self):
        """
        Reinstantiate this instance within a new process with a new connection
        pool set.
        """
        self.__init__(max_connections=self.max_connections,
                      timeout=self.timeout,
                      connection_class=self.connection_class,
                      queue_class=self.queue_class,
                      **self.connection_kwargs
                      )
