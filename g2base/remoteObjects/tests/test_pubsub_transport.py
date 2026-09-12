#
# test_pubsub_transport.py -- how a pubsub is reached, and by what
#
"""PubSub delivers updates by calling remote_update() on each subscriber over
remoteObjects, so which protocol that call uses is a pubsub question as much
as an RPC one.

A subscriber that offers more than one way in is called back over the fastest
the publisher can speak, and one that has not been upgraded still gets
XML-RPC.  Nothing is negotiated to arrange that: the publisher looks the
subscriber up by name, and the registration says what it answers to.
"""

import logging
import socket
import threading
import time

import pytest

from g2base.remoteObjects import Monitor, PubSub
from g2base.remoteObjects import remoteObjectNameSvc as ns_mod
from g2base.remoteObjects import remoteObjects as ro
from g2base.remoteObjects import ro_transport

HOST = '127.0.0.1'

#: Tell the fixture to pass no transport at all, and take the default.
_UNSPECIFIED = object()


class FakePubSub:
    def subscribe(self, channel):
        pass

    def add_callback(self, channel, fn):
        pass

    def publish(self, channel, envelope, pack_info):
        pass


@pytest.fixture
def nameservice():
    service = ns_mod.remoteObjectNameService('names', FakePubSub(),
                                             ro.nullLogger(), HOST)
    previous, ro.default_ns = ro.default_ns, service
    yield service
    ro.default_ns = previous


@pytest.fixture
def recording():
    """A logger that keeps what it is told, and takes what a logger takes."""

    class Recording:
        def __init__(self):
            self.warnings = []

        def warning(self, msg, *args, **kwargs):
            self.warnings.append(msg % args if args else msg)

        warn = warning

        def __getattr__(self, name):
            return lambda *args, **kwargs: None

    return Recording()


@pytest.fixture
def monitors(nameservice):
    started = []

    def _make(name, transport='xmlrpc'):
        monitor = Monitor.Monitor(name, ro.nullLogger(), numthreads=12)
        monitor.start()
        extra = ({} if transport is _UNSPECIFIED
                 else {'transport': transport})
        monitor.start_server(svcname=name, host=HOST, ns=nameservice,
                             default_auth=False, usethread=True, wait=True,
                             **extra)
        started.append(monitor)
        return monitor

    yield _make
    for monitor in reversed(started):
        try:
            monitor.stop_server()
        except Exception:
            pass
        monitor.stop()
    time.sleep(0.2)


def deliver(publisher, subscriber, options=None, path='TSCS', timeout=15.0):
    """Publish one update and wait for it to land on the subscriber."""
    arrived = threading.Event()
    original = subscriber.do_update

    def counting(where, value):
        result = original(where, value)
        arrived.set()
        return result

    subscriber.do_update = counting
    publisher.subscribe(subscriber.name, [publisher.name],
                        dict(options or {}, unsub=False))
    time.sleep(0.3)
    publisher.update(path, {'a': 1}, [publisher.name])
    return arrived.wait(timeout=timeout)


def chosen_transport(publisher, subscriber):
    proxy = publisher._partner[subscriber.name].proxy
    return proxy.endpoints.clients()[0].transport


# ----------------------------------------------- offering several ways --

def test_an_un_upgraded_subscriber_still_gets_xmlrpc(monitors):
    """The default, and what every service does today."""
    publisher = monitors('pub-a')
    subscriber = monitors('sub-a')

    assert deliver(publisher, subscriber)
    assert chosen_transport(publisher, subscriber) == 'xmlrpc'


@pytest.mark.parametrize('faster', ['g2rpc-tcp', 'g2rpc-zmq'])
def test_a_subscriber_offering_more_is_called_back_the_faster_way(monitors,
                                                                  faster):
    """And no option says so: the registration does."""
    publisher = monitors('pub-b')
    subscriber = monitors('sub-b', transport=['xmlrpc', faster])

    assert deliver(publisher, subscriber)
    assert chosen_transport(publisher, subscriber) == faster


def test_a_pubsub_listens_for_the_default_protocol(monitors, nameservice):
    """A pubsub is not special: told nothing, it serves what any other
    service serves.  One still spoken to by an un-upgraded peer says so the
    same way anything else does."""
    monitors('sub-d2', transport=_UNSPECIFIED)

    offered = [protocol for protocol, _port, _encoding
               in ro.endpoints_in(nameservice.getInfo('sub-d2')[0])]
    assert offered == [ro.default_transport]


def test_the_default_reaches_a_subscriber_the_faster_way(monitors):
    publisher = monitors('pub-s')
    subscriber = monitors('sub-s', transport=_UNSPECIFIED)

    assert deliver(publisher, subscriber)
    assert chosen_transport(publisher, subscriber) == 'g2rpc-tcp'


def test_a_pubsub_that_says_so_still_registers_xmlrpc_as_the_primary(
        monitors, nameservice):
    """The escape hatch, for a pubsub shared with un-upgraded peers -- the
    standalone one that ro_ps_svc runs, above all.  XML-RPC first is what
    makes it the primary such a caller reads."""
    monitors('sub-t', transport=['xmlrpc', 'g2rpc-tcp'])

    assert nameservice.getInfo('sub-t')[0]['protocol'] == 'xmlrpc'


# ------------------------------------------------ a pool that expands --

def test_a_pubsub_can_be_started_with_a_small_pool_that_grows():
    """A pubsub permanently holds a worker per delivery daemon, one for the
    subscription loop, one for the server's start task and one per threaded
    listener.  Everything above that is only wanted while calls are in
    flight, so the pool need not be held open at full width."""
    pubsub = PubSub.PubSub('elastic', ro.nullLogger(), numthreads=30,
                           minthreads=2)

    assert pubsub.threadPool.numthreads == 30
    assert pubsub.threadPool.minthreads == 2


def test_a_pool_is_still_a_fixed_size_unless_a_floor_is_given():
    """Which is what every existing caller gets."""
    pubsub = PubSub.PubSub('fixed', ro.nullLogger(), numthreads=12)

    assert pubsub.threadPool.minthreads == 12


def test_monitor_passes_the_floor_down(monitors):
    monitor = Monitor.Monitor('floored', ro.nullLogger(), numthreads=24,
                              minthreads=3)

    assert monitor.threadPool.minthreads == 3
    assert monitor.threadPool.numthreads == 24


def test_a_pubsub_on_a_growing_pool_still_delivers(monitors, nameservice):
    """The property that matters: starting small must not strand anyone."""
    publisher = Monitor.Monitor('grow-pub', ro.nullLogger(), numthreads=30,
                                minthreads=2)
    subscriber = Monitor.Monitor('grow-sub', ro.nullLogger(), numthreads=30,
                                 minthreads=2)
    arrived = threading.Event()
    try:
        for monitor, transport in ((publisher, 'xmlrpc'),
                                   (subscriber, ['xmlrpc', 'g2rpc-tcp'])):
            monitor.start()
            monitor.start_server(svcname=monitor.name, host=HOST,
                                 ns=nameservice, default_auth=False,
                                 transport=transport, usethread=True,
                                 wait=True)

        subscriber.subscribe_cb(lambda value, names, channels: arrived.set(),
                                ['grow-pub'])
        publisher.subscribe('grow-sub', ['grow-pub'], {'unsub': False})
        time.sleep(0.5)
        publisher.update('TSCS', {'a': 1}, ['grow-pub'])

        assert arrived.wait(20), 'the update reached the local callback'
        # It grew past its floor to carry its own permanent tasks.
        assert len(subscriber.threadPool.running) > 2
    finally:
        for monitor in (subscriber, publisher):
            try:
                monitor.stop_server()
            except Exception:
                pass
            monitor.stop()


# ------------------------------------------------- the thread budget --

def test_transports_asked_for_that_cannot_be_served_are_refused():
    """Listeners and delivery daemons each hold a worker for the life of
    the service.  When they add up to the whole pool the service accepts
    calls and never answers them, on every protocol, silently.  Serving
    fewer than were asked for would be as quiet, so this raises."""
    pubsub = PubSub.PubSub('starved', ro.nullLogger(), numthreads=8)

    with pytest.raises(PubSub.PubSubError) as caught:
        pubsub._affordable_transports(['xmlrpc', 'g2rpc-tcp'], True,
                                      asked_for=True)
    assert 'numthreads' in str(caught.value)


def test_the_default_gives_way_to_a_pool_that_cannot_serve_it(recording):
    """A default must not break a service that worked.  This one runs on
    XML-RPC alone, which is exactly where it already was."""
    pubsub = PubSub.PubSub('starved2', recording, numthreads=8)

    kept = pubsub._affordable_transports(['xmlrpc', 'g2rpc-tcp'], True,
                                         asked_for=False)

    assert kept == ['xmlrpc']
    assert any('numthreads' in m for m in recording.warnings)


def test_a_pool_that_can_serve_them_keeps_them_all():
    pubsub = PubSub.PubSub('adequate', ro.nullLogger(), numthreads=12)

    assert pubsub._affordable_transports(['xmlrpc', 'g2rpc-tcp'], True,
                                         asked_for=False) == ['xmlrpc',
                                                              'g2rpc-tcp']


def test_the_budget_counts_the_delivery_daemons_too():
    """Fewer daemons leaves room for more listeners in the same pool."""
    pubsub = PubSub.PubSub('fewer', ro.nullLogger(), numthreads=8)
    pubsub.outlimit = 2

    assert pubsub._affordable_transports(['xmlrpc', 'g2rpc-tcp'], True,
                                         asked_for=False) == ['xmlrpc',
                                                              'g2rpc-tcp']


def test_a_pool_too_small_for_even_one_listener_is_refused(recording):
    """There is nothing to fall back to, so falling back quietly would
    leave a service that accepts calls and never answers."""
    pubsub = PubSub.PubSub('hopeless', recording, numthreads=4)

    with pytest.raises(PubSub.PubSubError):
        pubsub._affordable_transports(['xmlrpc'], True, asked_for=False)


def test_start_server_takes_one_transport_or_a_list(monitors):
    single = monitors('one', transport='xmlrpc')
    several = monitors('many', transport=['xmlrpc', 'g2rpc-tcp'])

    assert single.server.transports == ['xmlrpc']
    assert several.server.transports == ['xmlrpc', 'g2rpc-tcp']


def test_start_server_honours_the_host_it_is_given(monitors):
    """It took a host and dropped it, so a pubsub always registered the
    machine's fully qualified name no matter what it was told."""
    monitor = monitors('bound')
    assert monitor.server.host == HOST


# --------------------------------------- old and new, in both roles --

@pytest.mark.parametrize('subscriber_offers,expected', [
    (_UNSPECIFIED, 'g2rpc-tcp'),
    ('xmlrpc', 'xmlrpc'),
])
def test_the_faster_way_is_used_where_both_ends_can(monitors,
                                                    subscriber_offers,
                                                    expected):
    """g2rpc-tcp where it is offered, XML-RPC where it is not.  Neither end
    is asked and nothing is negotiated: the subscriber's registration says
    what it answers to, and the publisher takes the best of it that it can
    speak."""
    publisher = monitors('pub-m1-%s' % expected)
    subscriber = monitors('sub-m1-%s' % expected, transport=subscriber_offers)

    assert deliver(publisher, subscriber)
    assert chosen_transport(publisher, subscriber) == expected


def test_a_publisher_that_only_knows_xmlrpc_is_still_served(monitors):
    """The other direction, and why a pubsub shared with un-upgraded peers
    has to say so: a subscriber offering both is reachable to a publisher
    that asks for the old way, and one left on the default is not."""
    publisher = monitors('pub-m2')
    subscriber = monitors('sub-m2', transport=['xmlrpc', 'g2rpc-tcp'])

    assert deliver(publisher, subscriber, {'transport': 'xmlrpc'})
    assert chosen_transport(publisher, subscriber) == 'xmlrpc'


# ------------------------------------- reaching back to a publisher --

@pytest.mark.parametrize('shape', ['g2rpc-tcp', ['g2rpc-tcp', 'xmlrpc']])
def test_pubtransport_reaches_the_proxy_whichever_shape_it_takes(shape):
    """Regression.  A 'pubtransport' list was translated here into
    'prefer', which _getProxy does not look for, so an order of preference
    was quietly dropped on the way to the publisher -- while a pinned one,
    translated to 'transport', survived.  Both are handed over as given
    now, and translated once."""
    pubsub = PubSub.PubSub('reaching', ro.nullLogger(), numthreads=2)
    seen = {}
    pubsub._getProxy = lambda name, options: seen.setdefault(name,
                                                             dict(options))

    pubsub._subscribe_remote('pubX', ['c'], {'pubtransport': shape})
    assert seen['pubX'] == {'transport': shape}

    seen.clear()
    pubsub._unsubscribe_remote('pubX', ['c'], {'pubtransport': shape})
    assert seen['pubX'] == {'transport': shape}, 'and on the way back out'


# ------------------------------------------- what the option can say --

def test_a_string_pins_one_protocol(monitors):
    publisher = monitors('pub-c')
    subscriber = monitors('sub-c', transport=['xmlrpc', 'g2rpc-tcp'])

    assert deliver(publisher, subscriber, {'transport': 'xmlrpc'})
    assert chosen_transport(publisher, subscriber) == 'xmlrpc'


def test_a_list_is_an_order_of_preference(monitors):
    publisher = monitors('pub-d')
    subscriber = monitors('sub-d', transport=['xmlrpc', 'g2rpc-tcp'])

    assert deliver(publisher, subscriber,
                   {'transport': ['g2rpc-tcp', 'xmlrpc']})
    assert chosen_transport(publisher, subscriber) == 'g2rpc-tcp'


def test_a_preference_the_subscriber_cannot_meet_falls_back(monitors):
    """Which is the difference between the two shapes.  A pin the subscriber
    does not offer makes it unreachable; a list just moves down it."""
    publisher = monitors('pub-e')
    subscriber = monitors('sub-e')          # xmlrpc only

    assert deliver(publisher, subscriber,
                   {'transport': ['g2rpc-zmq', 'xmlrpc']})
    assert chosen_transport(publisher, subscriber) == 'xmlrpc'


def test_the_two_shapes_map_to_different_proxy_arguments():
    pubsub = PubSub.PubSub('shapes', ro.nullLogger(), numthreads=2)
    assert pubsub._transport_options('s', 'xmlrpc') == {'transport': 'xmlrpc'}
    assert pubsub._transport_options('s', ['a', 'b']) == {'prefer': ['a', 'b']}


# ------------------------------------------------------ the 0mq bind --

def test_a_zmq_service_binds_every_interface_not_just_loopback():
    """Regression.  One url() served both binding and connecting, and its
    empty-host fallback was the client's -- 127.0.0.1 -- so a service that
    meant to listen everywhere listened on loopback, registered an address
    its clients could not reach, and looked like it was down."""
    spec = ro_transport.get('g2rpc-zmq')

    assert spec.bind_url('', 8000) == 'tcp://*:8000'
    assert spec.bind_url('10.0.0.1', 8000) == 'tcp://10.0.0.1:8000'
    assert spec.url('', 8000) == 'tcp://127.0.0.1:8000', \
        'a client still needs somewhere concrete to dial'


def test_a_zmq_service_is_reachable_by_the_name_it_registers():
    class Service:
        def echo(self, value):
            return value

    server = ro.remoteObjectServer(svcname='zmqbind', obj=Service(),
                                   logger=ro.nullLogger(), usethread=True,
                                   ns=False, default_auth=False,
                                   transport='g2rpc-zmq',
                                   method_list=['echo'])
    server.ro_start(wait=True, timeout=15)
    try:
        # Exactly what a client that looked the service up would dial.
        assert server.host == socket.getfqdn()
        client = ro.remoteObjectClient(server.host, server.port,
                                       name='zmqbind', transport='g2rpc-zmq',
                                       default_auth=False, timeout=10)
        assert client.echo('hi') == 'hi'
    finally:
        server.ro_stop(wait=True, timeout=15)


# -------------------------------------------------- the delivery queue --

def test_two_updates_of_equal_priority_do_not_collide():
    """Regression.  The queue held (priority, record), so equal priorities
    sent Python on to compare the records -- reaching the payload dicts,
    which do not compare, and raising inside the delivery path."""
    pubsub = PubSub.PubSub('queued', ro.nullLogger(), numthreads=2)

    # As the queue held them when the bug was live: whole records, whose
    # payload dicts are what the comparison reached.
    same = ('sub', {'a': 1}, ['p'], ['ch'], 0)
    other = ('sub', {'b': 2}, ['p'], ['ch'], 0)
    pubsub._enqueue(100.0, same)
    pubsub._enqueue(100.0, other)          # used to raise TypeError here

    assert pubsub.outqueue.get()[2][1] == {'a': 1}, 'and FIFO within a tie'
    assert pubsub.outqueue.get()[2][1] == {'b': 2}


def test_priority_still_orders_the_queue():
    pubsub = PubSub.PubSub('ordered', ro.nullLogger(), numthreads=2)
    for priority in (3.0, 1.0, 2.0):
        pubsub._enqueue(priority, ('sub', {'p': priority}, [], [], 0))

    assert [pubsub.outqueue.get()[2][1]['p'] for _ in range(3)] == [1.0, 2.0,
                                                                   3.0]


def test_the_queue_inspector_still_reports_subscribers():
    """The queue carries a subscriber rather than one update now -- what to
    send is decided when a delivery thread gets there -- so the inspector
    reads it out of a different place and reports the same thing."""
    pubsub = PubSub.PubSub('elts', ro.nullLogger(), numthreads=2)
    pubsub._enqueue(5.0, 'whoever')

    assert pubsub.get_qelts() == [(5.0, 'whoever')]


# ------------------------------------------------- doing work in place --

def test_publishing_does_not_hand_the_work_to_a_thread():
    """notify() only works out who wants the value and queues it -- a few
    microseconds -- while handing that to the pool cost 63us to defer it.
    The sending is the delivery threads' job either way."""
    pubsub = PubSub.PubSub('inline', ro.nullLogger(), numthreads=2)
    pubsub.add_channel('ch')

    seen = []
    original = pubsub._named_update

    def watching(value, names, channels, priority=0):
        seen.append(threading.current_thread())
        return original(value, names, channels, priority=priority)

    pubsub._named_update = watching
    pubsub.notify({'a': 1}, ['ch'])

    assert seen == [threading.current_thread()], \
        'notify() should do this on the caller thread, not defer it'


def test_a_debug_message_is_not_built_when_debug_is_off():
    """The delivery path formatted its arguments before calling the logger,
    so a large status value was rendered on every update whether or not
    anyone was reading it."""
    class Recording:
        level_asked = None

        def isEnabledFor(self, level):
            self.level_asked = level
            return False

        def debug(self, msg):
            raise AssertionError('should not have been called')

    class Exploding:
        def __str__(self):
            raise AssertionError('should not have been rendered')

    pubsub = PubSub.PubSub('quiet', Recording(), numthreads=2)
    pubsub._debug('value=%s', Exploding())
    assert pubsub.logger.level_asked == logging.DEBUG


def test_a_logger_that_cannot_say_still_gets_its_message():
    """Not every logger here is a logging.Logger; the minimal ones take a
    string and have no isEnabledFor."""
    class Minimal:
        def __init__(self):
            self.messages = []

        def debug(self, msg):
            self.messages.append(msg)

    pubsub = PubSub.PubSub('minimal', Minimal(), numthreads=2)
    pubsub._debug('value=%s', 42)
    assert pubsub.logger.messages == ['value=42']


def test_a_remote_update_is_handled_on_the_calling_worker():
    """The RPC server already gave this call a thread.  Storing the value
    takes microseconds, so a second hand-off cost more than the work."""
    monitor = Monitor.Monitor('inplace', ro.nullLogger(), numthreads=4)
    monitor.start()
    try:
        seen = []
        original = monitor.do_update

        def watching(path, value):
            seen.append(threading.current_thread())
            return original(path, value)

        monitor.do_update = watching
        monitor.remote_update(
            {'msg': 'update', 'path': 'A.B', 'value': {'v': 1}},
            ['somebody'], ['ch'])

        assert seen == [threading.current_thread()]
        assert dict(monitor['A.B']) == {'v': 1}
    finally:
        monitor.stop()


def test_a_subscriber_that_cannot_store_does_not_stall_its_own_feed():
    """Doing the work in place made a store failure reach the publisher,
    which is the wrong trade: the publisher marks the partner failed, every
    later update queues behind the bad one, and it retries a value that can
    never be stored.  A status stream would rather lose one value than
    stop."""
    monitor = Monitor.Monitor('poison', ro.nullLogger(), numthreads=4)
    monitor.start()
    try:
        def boom(path, value):
            raise RuntimeError('store is broken')

        monitor.do_update = boom
        result = monitor.remote_update(
            {'msg': 'update', 'path': 'A.B', 'value': {'v': 1}},
            ['somebody'], ['ch'])

        assert result == ro.OK, 'the publisher should be told to move on'
    finally:
        monitor.stop()


def test_reporting_that_failure_does_not_itself_raise():
    """The handler logged with exc_info=, which the minimal loggers do not
    take -- so the one place meant to stop an exception raised its own, and
    the publisher saw a failure after all."""
    class Minimal:
        """As small as the loggers that get passed in really are."""

        def __init__(self):
            self.errors = []

        def error(self, msg):
            self.errors.append(msg)

        def debug(self, msg):
            pass

        def info(self, msg):
            pass

        def warning(self, msg):
            pass

        warn = warning

    monitor = Monitor.Monitor('reporting', Minimal(), numthreads=4)
    monitor.start()
    try:
        def boom(path, value):
            raise RuntimeError('store is broken')

        monitor.do_update = boom
        assert monitor.remote_update(
            {'msg': 'update', 'path': 'A.B', 'value': {'v': 1}},
            ['somebody'], ['ch']) == ro.OK
        assert monitor.logger.errors, 'and it should say so somewhere'
        assert 'store is broken' in monitor.logger.errors[0]
    finally:
        monitor.stop()


def test_publishing_does_not_fail_the_caller_over_a_fault_in_delivery():
    """The task notify() used to spawn swallowed whatever went wrong in
    there.  Doing the work in place would have handed it to the caller --
    and notify() and update() are public, so the method scan exposes them
    and that caller can be a remote one, which would see an RPC fault for a
    fault in the machinery rather than in its own value."""
    class Recording:
        def __init__(self):
            self.errors = []

        def error(self, msg):
            self.errors.append(msg)

        def debug(self, msg):
            pass

        def isEnabledFor(self, level):
            return False

    pubsub = PubSub.PubSub('faulty', Recording(), numthreads=2)
    pubsub.add_channel('ch')

    def explode(*args, **kwargs):
        raise RuntimeError('machinery broke')

    pubsub._named_update = explode
    pubsub.notify({'a': 1}, ['ch'])          # must not raise

    assert pubsub.logger.errors, 'and it should still be reported'
    assert 'machinery broke' in pubsub.logger.errors[0]


# ------------------------------------------------------- the null logger --

def test_the_null_logger_is_a_real_logger():
    """It was a hand-written stand-in taking a message and nothing else, so
    a caller writing what the standard library documents got a TypeError
    from the object whose whole job is to keep quiet."""
    from g2base import ssdlog

    logger = ro.nullLogger()

    assert isinstance(logger, logging.Logger)
    assert ro.nullLogger is ssdlog.NullLogger, \
        'one implementation, reachable by the name each caller already uses'

    for method in ('debug', 'info', 'warning', 'warn', 'error', 'critical',
                   'exception', 'log', 'isEnabledFor', 'setLevel'):
        assert callable(getattr(logger, method, None)), method


def test_the_null_logger_takes_the_usual_arguments():
    """Lazy arguments and exc_info, neither of which it used to accept --
    and exc_info is the one that bit, inside a handler written to stop an
    exception reaching a publisher."""
    logger = ro.nullLogger()

    logger.debug('a lazy %s and a %d', 'string', 2)
    try:
        raise RuntimeError('boom')
    except RuntimeError:
        logger.error('failed: %s', 'boom', exc_info=True)
        logger.exception('also fine')


def test_the_null_logger_discards_by_default():
    logger = ro.nullLogger()
    assert not logger.isEnabledFor(logging.CRITICAL), \
        'set above CRITICAL so no record is built at any level'
    assert not logger.propagate, \
        'a stand-in for having no logger, not a quiet route into the root one'


def test_the_null_logger_still_writes_when_given_a_file():
    import io

    stream = io.StringIO()
    logger = ro.nullLogger(f_out=stream)
    logger.info('written: %s', 'yes')

    assert 'written: yes' in stream.getvalue()


def test_two_null_loggers_do_not_share_handlers():
    """They are throwaways; a process that makes many should not find them
    accumulating on one name."""
    first, second = ro.nullLogger(), ro.nullLogger()

    assert first.name != second.name
    assert len(first.handlers) == 1 and len(second.handlers) == 1
