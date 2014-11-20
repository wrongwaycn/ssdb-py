#coding=utf-8

"Core exceptions raised by the SSDB client"
from ssdb._compat import unicode
from collections import namedtuple

response_status = ["ok", "not_found", "error", "fail", "client_error"]
RES_STATUS_MSG = {
    "ok": "Opreation successfully.",
    "not_found": "Not found.",
    "error": "SSDB error.",
    "fail": "Operation failed.",    
    "client_error": "Client or command error."
    }
RES_STATUS = namedtuple('response_status', ','.join(response_status).upper())._make(response_status)

class SSDBError(Exception):
        pass

# python 2.5 doesn't implement Exception.__unicode__. Add it here to all
# our exception types
if not hasattr(SSDBError, '__unicode__'):
    def __unicode__(self):
        if isinstance(self.args[0], unicode):
            return self.args[0]
        return unicode(self.args[0])
    SSDBError.__unicode__ = __unicode__

    
class AuthenticationError(SSDBError):
    pass

class ServerError(SSDBError):
    pass

class TimeoutError(SSDBError):
    pass

class ConnectionError(SSDBError):
    pass

class BusyLoadingError(ConnectionError):
    pass

class InvalidResponse(ServerError):
    pass

class ResponseError(SSDBError):
    pass

class DataError(SSDBError):
    pass

class PubSubError(SSDBError):
    pass

class WatchError(SSDBError):
    pass

class NoScriptError(ResponseError):
    pass

class ExecAbortError(ResponseError):
    pass

class LockError(SSDBError, ValueError):
    "Errors acquiring or releasing a lock"
    # NOTE: For backwards compatability, this class derives from ValueError.
    # This was originally chosen to behave like threading.Lock.
    pass
