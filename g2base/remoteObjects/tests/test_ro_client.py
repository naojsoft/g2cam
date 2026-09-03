#
# test_ro_client.py -- the client side: clients, endpoints and call strategies
#
"""What replaced ``servicePack`` and the ``remoteObjectSP*`` hierarchy.

Two things are tested here that used to be tangled together: where a
service's providers come from (a list, or the name service) and what is done
with them (call one with failover, or call them all).  They are separate now
and compose, so the combination the manager service needs -- call all the
providers the name service knows about -- exists for the first time.
"""

import pytest

from g2base.remoteObjects import remoteObjects as ro
from g2base.remoteObjects import ro_endpoints

HOST = '127.0.0.1'

#: A port nothing is listening on, to provoke a connection failure.
DEAD_PORT = 9


class ServiceObject:
    def echo(self, value):
        return value

    def add(self, a, b):
        return a + b

    def boom(self):
        raise ValueError('boom')

    def whoami(self):
        return 'service'


@pytest.fixture
def service():
    """One or more running services, stopped again afterwards."""
    started = []

    def _make(**kwargs):
        kwargs.setdefault('svcname', None)
        kwargs.setdefault('name', 'testsvc')
        kwargs.setdefault('obj', ServiceObject())
        kwargs.setdefault('host', HOST)
        kwargs.setdefault('logger', ro.nullLogger())
        kwargs.setdefault('usethread', True)
        kwargs.setdefault('ns', False)
        kwargs.setdefault('default_auth', False)
        kwargs.setdefault('method_list', ['echo', 'add', 'boom', 'whoami'])
        svc = ro.remoteObjectServer(**kwargs)
        svc.ro_start(wait=True, timeout=10.0)
        started.append(svc)
        return svc

    yield _make

    for svc in started:
        svc.ro_stop(wait=True, timeout=10.0)


class FakeNameService:
    """Just the getInfo() that NameSvcEndpoints depends on."""

    def __init__(self, records=()):
        self.records = list(records)
        self.lookups = 0

    def getInfo(self, name):
        self.lookups += 1
        return list(self.records)


def record(port, host=HOST):
    return dict(name='testsvc', host=host, port=port, secure=False,
                transport='xmlrpc', encoding='xml', keep=False,
                pingtime=0, registrar=host)


# ------------------------------------------------------------------ auth --

def test_normalize_auth_accepts_the_shapes_callers_actually_pass():
    """Regression: the old version accepted None and a string and raised
    ValueError on everything else -- including the (user, passwd) tuple the
    rest of the module passes around.  scripts/ro_test.py --auth=bob:pw
    splits into a *list* before calling, so it died before its first call.
    """
    assert ro.normalize_auth(None, name='svc') == ('svc', 'svc')
    assert ro.normalize_auth(None, name='svc', default_auth=False) is None
    assert ro.normalize_auth('bob:pw') == ('bob', 'pw')
    assert ro.normalize_auth(['bob', 'pw']) == ('bob', 'pw')
    assert ro.normalize_auth(('bob', 'pw')) == ('bob', 'pw')


def test_normalize_auth_rejects_what_it_cannot_read():
    with pytest.raises(ValueError):
        ro.normalize_auth('no-colon-here')
    with pytest.raises(ValueError):
        ro.normalize_auth(['too', 'many', 'parts'])


# ---------------------------------------------------------------- client --

def test_client_calls_a_service(service):
    svc = service()
    client = ro.remoteObjectClient(HOST, svc.port, name='testsvc',
                                   default_auth=False, timeout=10.0)
    assert client.echo('hi') == 'hi'
    assert client.add(3, 4) == 7
    assert client.echo(2 ** 40) == 2 ** 40
    assert client.echo(None) is None


def test_client_raises_remoteObjectError_when_the_method_raises(service):
    svc = service()
    client = ro.remoteObjectClient(HOST, svc.port, name='testsvc',
                                   default_auth=False, timeout=10.0)
    with pytest.raises(ro.remoteObjectError) as excinfo:
        client.boom()
    assert 'boom' in str(excinfo.value)


def test_client_raises_when_nothing_is_listening():
    client = ro.remoteObjectClient(HOST, DEAD_PORT, name='testsvc',
                                   default_auth=False, timeout=5.0)
    with pytest.raises(ro.remoteObjectError):
        client.echo('hi')


# ----------------------------------------------------------- call_remote --

def test_call_remote_classifies_success(service):
    svc = service()
    client = ro.remoteObjectClient(HOST, svc.port, name='testsvc',
                                   default_auth=False, timeout=10.0)
    assert ro.call_remote(client, 'echo', ('hi',), {}) == (ro.OK, 'hi')


def test_call_remote_classifies_an_unreachable_service_as_failover():
    """This classification *is* the fault-tolerance policy: only errors
    marked FAILOVER cause another provider to be tried."""
    client = ro.remoteObjectClient(HOST, DEAD_PORT, name='testsvc',
                                   default_auth=False, timeout=5.0)
    flag, _ = ro.call_remote(client, 'echo', ('hi',), {})
    assert flag == ro.ERROR_FAILOVER


def test_call_remote_classifies_a_raising_method_as_fatal(service):
    """The service answered; it just said no.  Asking somewhere else would
    get the same answer, so this must not fail over."""
    svc = service()
    client = ro.remoteObjectClient(HOST, svc.port, name='testsvc',
                                   default_auth=False, timeout=10.0)
    flag, res = ro.call_remote(client, 'boom', (), {})
    assert flag == ro.ERROR_FATAL
    assert 'boom' in res


def test_call_remote_classifies_an_unknown_method_as_fatal(service):
    svc = service()
    client = ro.remoteObjectClient(HOST, svc.port, name='testsvc',
                                   default_auth=False, timeout=10.0)
    flag, _ = ro.call_remote(client, 'no_such_method', (), {})
    assert flag == ro.ERROR_FATAL


# ------------------------------------------------------------- endpoints --

def test_endpoints_resolve_lazily_and_once(service):
    svc = service()
    ns = FakeNameService([record(svc.port)])
    endpoints = ro_endpoints.NameSvcEndpoints(
        'testsvc', ns, lambda rec: rec['port'])

    assert ns.lookups == 0, "must not look up before it is needed"
    assert endpoints.clients() == [svc.port]
    assert endpoints.clients() == [svc.port]
    assert ns.lookups == 1, "and must not look up again once resolved"


def test_refresh_asks_again(service):
    svc = service()
    ns = FakeNameService([record(svc.port)])
    endpoints = ro_endpoints.NameSvcEndpoints(
        'testsvc', ns, lambda rec: rec['port'])

    endpoints.clients()
    ns.records = [record(1234)]
    assert endpoints.refresh() == [1234]
    assert ns.lookups == 2


def test_endpoints_without_a_name_service_say_so():
    endpoints = ro_endpoints.NameSvcEndpoints('testsvc', None, lambda r: r)
    with pytest.raises(LookupError):
        endpoints.clients()


def test_endpoints_for_an_unregistered_name_say_so():
    endpoints = ro_endpoints.NameSvcEndpoints('nope', FakeNameService([]),
                                              lambda r: r)
    with pytest.raises(LookupError):
        endpoints.clients()


# ---------------------------------------------------------------- proxies --

def test_proxy_over_a_static_list(service):
    svc = service()
    proxy = ro.remoteObjectProxy('testsvc', hostports=[(HOST, svc.port)],
                                 default_auth=False, timeout=10.0)
    assert proxy.echo('hi') == 'hi'


def test_proxy_over_the_name_service(service):
    svc = service()
    ns = FakeNameService([record(svc.port)])
    proxy = ro.remoteObjectProxy('testsvc', ns=ns, default_auth=False,
                                 timeout=10.0)
    assert proxy.echo('hi') == 'hi'
    assert ns.lookups == 1


def test_proxy_fails_over_to_a_live_provider(service):
    """A dead provider first, a live one second: the call must still land."""
    svc = service()
    ns = FakeNameService([record(DEAD_PORT), record(svc.port)])
    proxy = ro.remoteObjectProxy('testsvc', ns=ns, default_auth=False,
                                 timeout=5.0)
    assert proxy.echo('hi') == 'hi'
    assert ns.lookups >= 2, "a failure should re-resolve from the name service"


def test_proxy_re_resolves_when_the_provider_moves(service):
    """The usual case: one provider, which is restarted somewhere else.  The
    name service is the authority on where it went."""
    old = service()
    ns = FakeNameService([record(old.port)])
    proxy = ro.remoteObjectProxy('testsvc', ns=ns, default_auth=False,
                                 timeout=5.0)
    assert proxy.echo('hi') == 'hi'

    new = service()
    old.ro_stop(wait=True, timeout=10.0)
    ns.records = [record(new.port)]

    assert proxy.echo('again') == 'again'


def test_proxy_raises_when_no_provider_answers():
    ns = FakeNameService([record(DEAD_PORT)])
    proxy = ro.remoteObjectProxy('testsvc', ns=ns, default_auth=False,
                                 timeout=5.0)
    with pytest.raises(ro.remoteObjectError):
        proxy.echo('hi')


def test_a_fatal_error_does_not_fail_over(service):
    """The provider answered.  Re-resolving and asking again would only
    produce the same error, and would hide it behind extra calls."""
    svc = service()
    ns = FakeNameService([record(svc.port)])
    proxy = ro.remoteObjectProxy('testsvc', ns=ns, default_auth=False,
                                 timeout=10.0)
    with pytest.raises(ro.remoteObjectError):
        proxy.boom()
    assert ns.lookups == 1, "a fatal error must not re-resolve"


# --------------------------------------------------------------- call all --

def test_proxy_all_over_a_static_list(service):
    a, b = service(), service()
    proxy = ro.remoteObjectProxyAll(
        'testsvc', hostports=[(HOST, a.port), (HOST, b.port)],
        default_auth=False, timeout=10.0)

    results = proxy.echo('hi')
    assert set(results) == {(HOST, a.port), (HOST, b.port)}
    assert all(r == (ro.OK, 'hi') for r in results.values())


def test_proxy_all_reports_failures_rather_than_raising(service):
    svc = service()
    proxy = ro.remoteObjectProxyAll(
        'testsvc', hostports=[(HOST, svc.port), (HOST, DEAD_PORT)],
        default_auth=False, timeout=5.0)

    results = proxy.echo('hi')
    assert results[(HOST, svc.port)] == (ro.OK, 'hi')
    assert results[(HOST, DEAD_PORT)][0] == ro.ERROR_FAILOVER


def test_call_all_over_the_name_service(service):
    """The combination the old hierarchy could not express: the manager
    service had to build its host list by hand because call-all and
    name-service lookup lived in different, unrelated classes."""
    a, b = service(), service()
    ns = FakeNameService([record(a.port), record(b.port)])
    proxy = ro.remoteObjectProxyAll('testsvc', ns=ns, default_auth=False,
                                    timeout=10.0)

    results = proxy.whoami()
    assert len(results) == 2
    assert all(r == (ro.OK, 'service') for r in results.values())


def test_the_old_call_all_name_still_resolves():
    assert ro.remoteObjectSPAll is ro.remoteObjectProxyAll


# --------------------------------------------------------------- bunches --

def test_make_robunch(service):
    a, b = service(), service()
    bunch = ro.make_robunch('testsvc', hostports=[(HOST, a.port),
                                                  (HOST, b.port)])

    import socket as _socket
    fqdn = _socket.getfqdn(HOST)
    assert bunch['%s:%d' % (fqdn, a.port)].echo('hi') == 'hi'
    assert len(bunch['all'].echo('hi')) == 2


def test_proxies_do_not_share_credentials():
    """Regression guard for the per-host auth map: it must be per instance,
    or one service's credentials leak into another's calls."""
    one = ro.remoteObjectProxy('a', hostports=[(HOST, 1, ('u1', 'p1'))],
                               default_auth=False)
    two = ro.remoteObjectProxy('b', hostports=[(HOST, 2, ('u2', 'p2'))],
                               default_auth=False)
    assert one._auth_overrides == {(HOST, 1): ('u1', 'p1')}
    assert two._auth_overrides == {(HOST, 2): ('u2', 'p2')}
