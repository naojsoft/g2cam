#
# test_server_methods.py -- what a remote object server does and does not expose
#
"""Which methods a service publishes.

The introspection scan takes every public callable on the served object, so a
service written the usual way -- by subclassing ``remoteObjectServer`` --
used to publish the server's own lifecycle methods along with its own.  Any
client that could reach the service could then stop it.  Those are withheld
now, which is a deliberate change from the old behaviour.
"""

import xmlrpc.client

import pytest

from g2base.remoteObjects import remoteObjects as ro

HOST = '127.0.0.1'


class SubclassedService(ro.remoteObjectServer):
    """A service written the way the documentation suggests."""

    def search(self, ra, dec):
        return [ra, dec]

    def _private(self):
        return 'no'


@pytest.fixture
def service():
    """A running service, stopped again afterwards."""
    started = []

    def _make(cls=SubclassedService, **kwargs):
        kwargs.setdefault('svcname', None)
        kwargs.setdefault('name', 'methods')
        kwargs.setdefault('host', HOST)
        kwargs.setdefault('logger', ro.nullLogger())
        kwargs.setdefault('usethread', True)
        kwargs.setdefault('ns', False)
        kwargs.setdefault('default_auth', False)
        svc = cls(**kwargs)
        svc.ro_start(wait=True, timeout=10.0)
        started.append(svc)
        return svc

    yield _make

    for svc in started:
        svc.ro_stop(wait=True, timeout=10.0)


def call(svc, method, args=()):
    from tinyrpc import RPCClient
    from tinyrpc.protocols.xmlrpc import XMLRPCProtocol
    from tinyrpc.transports.http import HttpPostClientTransport

    client = RPCClient(
        XMLRPCProtocol(allow_none=True, allow_large_ints=True),
        HttpPostClientTransport('http://%s:%d/' % (HOST, svc.port),
                                timeout=10.0))
    return client.call(method, args, None)


LIFECYCLE = ['ro_start', 'ro_stop', 'ro_wait_start', 'ro_wait_stop']


@pytest.mark.parametrize('method', LIFECYCLE)
def test_lifecycle_methods_are_not_published(service, method):
    svc = service()
    assert method not in svc.ro_list()


@pytest.mark.parametrize('method', LIFECYCLE)
def test_lifecycle_methods_cannot_be_called_remotely(service, method):
    svc = service()
    with pytest.raises(xmlrpc.client.Fault) as excinfo:
        call(svc, method)
    assert excinfo.value.faultCode == -32601


@pytest.mark.parametrize('method', LIFECYCLE)
def test_an_explicit_method_list_cannot_publish_them_either(service, method):
    """Withholding these is absolute; asking for one by name does not
    override it, because there is no version of stopping a service from
    outside that is safe to publish by accident."""
    svc = service(method_list=['search', method])
    assert method not in svc.ro_list()
    assert 'search' in svc.ro_list()


@pytest.mark.parametrize('method', LIFECYCLE)
def test_the_hosting_process_can_still_call_them(method):
    """They are withheld from the wire, not removed: this is how a service
    is started and stopped by the process that hosts it."""
    svc = SubclassedService(svcname=None, name='local', host=HOST,
                            logger=ro.nullLogger(), usethread=True, ns=False,
                            default_auth=False)
    assert callable(getattr(svc, method))
    svc.ro_start(wait=True, timeout=10.0)
    try:
        svc.ro_wait_start(timeout=5.0)
    finally:
        svc.ro_stop(wait=True, timeout=10.0)
    svc.ro_wait_stop(timeout=5.0)


def test_the_service_own_methods_are_published(service):
    svc = service()
    assert 'search' in svc.ro_list()
    assert call(svc, 'search', (1.0, 2.0)) == [1.0, 2.0]


def test_private_methods_are_not_published(service):
    svc = service()
    assert '_private' not in svc.ro_list()


def test_the_introspection_methods_are_still_published(service):
    svc = service()
    published = svc.ro_list()
    for method in ['ro_echo', 'ro_list', 'ro_help', 'ro_get_pid',
                   'ro_stacktraces', 'ro_setLogLevel']:
        assert method in published, method
    assert call(svc, 'ro_echo', ('hi',)) == 'hi'


def test_a_served_object_is_unaffected(service):
    """When obj= is given, the server's own methods were never in the scan
    to begin with; only the object's are published."""
    class Plain:
        def only_this(self):
            return 1

    svc = service(cls=ro.remoteObjectServer, obj=Plain())
    assert 'only_this' in svc.ro_list()
    for method in LIFECYCLE:
        assert method not in svc.ro_list()
