#coding=utf-8
from itertools import starmap
from ssdb._compat import b
from ssdb.exceptions import (
    ConnectionError,
    DataError,
    SSDBError,
    ResponseError,
    WatchError,
    NoScriptError,
    ExecAbortError,
    )

SYM_EMPTY = b('')

class BaseBatch(object):

    def __init__(self, connection_pool, response_callbacks):
        self.connection_pool = connection_pool
        self.connection = None
        self.response_callbacks = response_callbacks
        self.reset()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def __del__(self):
        try:
            self.reset()
        except Exception:
            pass

    def __len__(self):
        return len(self.command_stack)

    def reset(self):
        self.command_stack = []
        if self.connection:
            self.connection_pool.release(self.connection)
            self.connection = None

    def execute_command(self, *args, **kwargs):
        return self.pipeline_execute_command(*args, **kwargs)

    def pipeline_execute_command(self, *args, **options):
        self.command_stack.append((args, options))
        return self

    def _execute_pipeline(self, connection, commands, raise_on_error):
        # build up all commands into a single request to increase network perf
        all_cmds = SYM_EMPTY.join(
            starmap(connection.pack_command,
                    [args for args, options in commands]))
        connection.send_packed_command(all_cmds)

        response = []
        for args, options in commands:
            try:
                response.append(
                    self.parse_response(connection, args[0], **options))
            except ResponseError:
                response.append(sys.exc_info()[1])

        if raise_on_error:
            self.raise_first_error(commands, response)
        return response

    def raise_first_error(self, commands, response):
        for i, r in enumerate(response):
            if isinstance(r, ResponseError):
                self.annotate_exception(r, i + 1, commands[i][0])
                raise r

    def annotate_exception(self, exception, number, command):
        cmd = unicode(' ').join(imap(unicode, command))
        msg = unicode('Command # %d (%s) of pipeline caused error: %s') % (
            number, cmd, unicode(exception.args[0]))
        exception.args = (msg,) + exception.args[1:]

    def execute(self, raise_on_error=True):
        "Execute all the commands in the current pipeline"
        stack = self.command_stack
        if not stack:
            return []
        execute = self._execute_pipeline

        conn = self.connection
        if not conn:
            conn = self.connection_pool.get_connection('batch')
            # assign to self.connection so reset() releases the connection
            # back to the pool after we're done
            self.connection = conn

        try:
            return execute(conn, stack, raise_on_error)
        except ConnectionError:
            conn.disconnect()
            return execute(conn, stack, raise_on_error)
        finally:
            self.reset()


