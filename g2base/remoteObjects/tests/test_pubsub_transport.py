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

import itertools
import queue
import socket
import threading
import time

import pytest

from g2base.remoteObjects import Monitor, PubSub
from g2base.remoteObjects import remoteObjectNameSvc as ns_mod
from g2base.remoteObjects import remoteObjects as ro
from g2base.remoteObjects import ro_transport

HOST = '127.0.0.1'


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
def monitors(nameservice):
    started = []

    def _make(name, transport='xmlrpc'):
        monitor = Monitor.Monitor(name, ro.nullLogger(), numthreads=12)
        monitor.start()
        monitor.start_server(svcname=name, host=HOST, ns=nameservice,
                             default_auth=False, transport=transport,
                             usethread=True, wait=True)
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
    pubsub = PubSub.PubSub('elts', ro.nullLogger(), numthreads=2)
    pubsub._enqueue(5.0, ('whoever', {'a': 1}, [], [], 0))

    assert pubsub.get_qelts() == [(5.0, 'whoever')]
