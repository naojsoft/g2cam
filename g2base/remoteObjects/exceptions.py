
#
# Exceptions raised by implementations of the RPC transport
#
class RemoteObjectsError(Exception):
    pass

class AuthenticationError(RemoteObjectsError):
    pass

class TimeoutError(RemoteObjectsError):
    pass

class ServiceUnavailableError(RemoteObjectsError):
    pass
