#
# test_pubsub_batching.py -- several updates in one call, in order
#
"""Delivery used to pull individual updates off a shared queue with several
threads, so two of them could be carrying updates for the same subscriber at
once and arrive in the wrong order.  Nothing on a network promises order
across calls, but there is no reason for a publisher to shuffle its own
updates before handing them over.

Sending one subscriber's updates from one thread at a time fixes that, and
gives batching for free: whatever gathers while a call is in flight travels
together on the next one.  So a batch costs no latency -- the first update
goes immediately -- and forms only when the traffic is there to form it.
"""

import threading
import time

import pytest

from g2base import Task
from g2base.remoteObjects import Monitor, PubSub
from g2base.remoteObjects import remoteObjectNameSvc as ns_mod
from g2base.remoteObjects import remoteObjects as ro

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

    def _make(name, method_list=None):
        monitor = Monitor.Monitor(name, ro.nullLogger(), numthreads=16)
        monitor.start()
        if method_list is None:
            monitor.start_server(svcname=name, host=HOST, ns=nameservice,
                                 default_auth=False, usethread=True,
                                 wait=True)
        else:
            # A subscriber exposing only some of its methods, which is how
            # an un-upgraded one looks from here.
            monitor.server = ro.remoteObjectServer(
                svcname=name, obj=monitor, logger=monitor.logger,
                ev_quit=monitor.ev_quit, host=HOST, usethread=True,
                ns=nameservice, default_auth=False,
                threadPool=monitor.threadPool, method_list=method_list)
            Task.FuncTask(monitor.server.ro_start, [], {},
                          logger=monitor.logger).init_and_start(monitor)
            monitor.server.ro_wait_start(timeout=15)
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


def watch(subscriber, delay=0.002):
    """Record the order updates are applied in, slowly enough that a later
    one could overtake an earlier one if anything let it."""
    seen = []
    original = subscriber.do_update

    def recording(path, value):
        seen.append(value.get('n'))
        time.sleep(delay)
        return original(path, value)

    subscriber.do_update = recording
    return seen


def count_batches(publisher):
    sizes = []
    original = publisher._send_batch

    def counting(subscriber, partner, proxy_obj, records):
        sizes.append(len(records))
        return original(subscriber, partner, proxy_obj, records)

    publisher._send_batch = counting
    return sizes


def publish(publisher, count):
    for i in range(count):
        publisher.update('SEQ', {'n': i}, [publisher.name])


def wait_for(seen, count, timeout=45.0):
    deadline = time.time() + timeout
    while len(seen) < count and time.time() < deadline:
        time.sleep(0.02)
    return len(seen)


# ------------------------------------------------------------- order --

def test_updates_arrive_in_the_order_they_were_published(monitors):
    """Regression.  Four delivery threads pulled updates for the same
    subscriber off a shared queue and raced, so a run of 120 arrived with
    half a dozen pairs transposed -- a reordering the publisher introduced
    itself, with no network involved."""
    publisher = monitors('ord-pub')
    subscriber = monitors('ord-sub')
    seen = watch(subscriber)

    publisher.subscribe(subscriber.name, [publisher.name], {'unsub': False})
    time.sleep(0.4)
    publish(publisher, 120)

    assert wait_for(seen, 120) == 120
    assert seen == sorted(seen), \
        'transposed pairs: %r' % ([(a, b) for a, b in zip(seen, seen[1:])
                                   if b < a][:6],)


def test_only_one_delivery_at_a_time_per_subscriber(monitors):
    """Which is what keeps the order: the batching follows from it."""
    publisher = monitors('one-pub')
    subscriber = monitors('one-sub')

    overlaps = []
    inside = []
    original = publisher._send_batch

    def watching(name, partner, proxy_obj, records):
        if inside:
            overlaps.append(name)
        inside.append(name)
        try:
            return original(name, partner, proxy_obj, records)
        finally:
            inside.remove(name)

    publisher._send_batch = watching
    seen = watch(subscriber)
    publisher.subscribe(subscriber.name, [publisher.name], {'unsub': False})
    time.sleep(0.4)
    publish(publisher, 60)

    assert wait_for(seen, 60) == 60
    assert not overlaps, 'two deliveries to one subscriber at once'


# ----------------------------------------------------------- batching --

def test_a_run_of_updates_travels_in_far_fewer_calls(monitors):
    publisher = monitors('bat-pub')
    subscriber = monitors('bat-sub')
    sizes = count_batches(publisher)
    seen = watch(subscriber)

    publisher.subscribe(subscriber.name, [publisher.name], {'unsub': False})
    time.sleep(0.4)
    publish(publisher, 120)

    assert wait_for(seen, 120) == 120
    assert len(sizes) < 20, 'expected batching, got %d calls' % (len(sizes),)
    assert max(sizes) > 1
    assert sum(sizes) == 120, 'every update accounted for exactly once'


def test_nothing_is_held_back_to_make_a_batch(monitors):
    """The first update goes on its own.  A batch is only ever what
    gathered while the previous call was in flight, so a quiet publisher
    pays nothing for the feature."""
    publisher = monitors('lone-pub')
    subscriber = monitors('lone-sub')
    sizes = count_batches(publisher)
    seen = watch(subscriber, delay=0)

    publisher.subscribe(subscriber.name, [publisher.name], {'unsub': False})
    time.sleep(0.4)
    publisher.update('SEQ', {'n': 0}, [publisher.name])

    assert wait_for(seen, 1, timeout=15) == 1
    assert sizes == [1]


def test_setup_batch_caps_how_many_travel_together(monitors):
    publisher = monitors('cap-pub')
    subscriber = monitors('cap-sub')
    publisher.setup_batch(None, limit_num=10)
    sizes = count_batches(publisher)
    seen = watch(subscriber)

    publisher.subscribe(subscriber.name, [publisher.name], {'unsub': False})
    time.sleep(0.4)
    publish(publisher, 120)

    assert wait_for(seen, 120) == 120
    assert max(sizes) <= 10
    assert sum(sizes) == 120


def test_setup_batch_refuses_a_nonsensical_size():
    pubsub = PubSub.PubSub('caps', ro.nullLogger(), numthreads=2)
    pubsub.setup_batch(None, limit_num=0)
    assert pubsub.batch_limit_num == 1


# ------------------------------------------- an un-upgraded subscriber --

def test_a_subscriber_without_the_batch_call_still_gets_everything(monitors):
    """Discovered by asking rather than announcing: the batch call is
    refused, the same updates go singly, and if that works the refusal was
    about the method rather than the subscriber."""
    publisher = monitors('old-pub')
    older = Monitor.Monitor('old-sub', ro.nullLogger(), numthreads=16)
    methods = [name for name in dir(older)
               if not name.startswith('_')
               and callable(getattr(older, name, None))
               and name != 'remote_update_many']
    subscriber = monitors('old-sub', method_list=methods)
    assert 'remote_update_many' not in subscriber.server.method_list

    seen = watch(subscriber)
    publisher.subscribe(subscriber.name, [publisher.name], {'unsub': False})
    time.sleep(0.4)
    publish(publisher, 60)

    assert wait_for(seen, 60) == 60
    assert seen == sorted(seen), 'and still in order, one at a time'


def test_the_refusal_is_learned_rather_than_retried(monitors):
    publisher = monitors('learn-pub')
    older = Monitor.Monitor('learn-sub', ro.nullLogger(), numthreads=16)
    methods = [name for name in dir(older)
               if not name.startswith('_')
               and callable(getattr(older, name, None))
               and name != 'remote_update_many']
    subscriber = monitors('learn-sub', method_list=methods)

    seen = watch(subscriber)
    publisher.subscribe(subscriber.name, [publisher.name], {'unsub': False})
    time.sleep(0.4)
    publish(publisher, 60)
    wait_for(seen, 60)

    assert not publisher._partner[subscriber.name].takes_batches


def test_a_current_subscriber_keeps_taking_batches(monitors):
    publisher = monitors('keep-pub')
    subscriber = monitors('keep-sub')
    seen = watch(subscriber)

    publisher.subscribe(subscriber.name, [publisher.name], {'unsub': False})
    time.sleep(0.4)
    publish(publisher, 60)
    wait_for(seen, 60)

    assert publisher._partner[subscriber.name].takes_batches


# ------------------------------------------------ the receiving side --

def test_remote_update_many_applies_them_in_order():
    monitor = Monitor.Monitor('many', ro.nullLogger(), numthreads=4)
    monitor.start()
    try:
        applied = []
        original = monitor.do_update

        def recording(path, value):
            applied.append(value['n'])
            return original(path, value)

        monitor.do_update = recording
        monitor.remote_update_many([
            ({'msg': 'update', 'path': 'A', 'value': {'n': i}},
             ['elsewhere'], ['ch'])
            for i in range(5)])

        assert applied == [0, 1, 2, 3, 4]
    finally:
        monitor.stop()


def test_an_empty_batch_is_harmless():
    monitor = Monitor.Monitor('empty', ro.nullLogger(), numthreads=4)
    monitor.start()
    try:
        assert monitor.remote_update_many([]) == ro.OK
    finally:
        monitor.stop()


def test_the_queue_reports_subscribers_with_work_waiting():
    """It carries a subscriber now rather than one update, since what to
    send is decided when a thread gets there."""
    pubsub = PubSub.PubSub('qelts', ro.nullLogger(), numthreads=2)
    pubsub._enqueue(5.0, 'whoever')

    assert pubsub.get_qelts() == [(5.0, 'whoever')]
