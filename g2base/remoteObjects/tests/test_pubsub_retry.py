#
# test_pubsub_retry.py -- what happens when a subscriber stops answering
#
"""A delivery that fails puts the update on the partner's backlog and sets a
timer; when it fires, one update is released and tried again.  Succeeding
releases the next, so a partner that comes back drains at one per round trip.

The shape was sound.  What it lacked was spread -- every partner that failed
on the same tick retried in lockstep -- a first delay above the noise floor,
and any word about the updates it was quietly throwing away.
"""

import threading
import time
from collections import deque as Deque

import pytest

from g2base import Bunch
from g2base.remoteObjects import Monitor, PubSub
from g2base.remoteObjects import remoteObjectNameSvc as ns_mod
from g2base.remoteObjects import remoteObjects as ro

HOST = '127.0.0.1'


class Recording:
    """A logger that keeps what it is told, and takes what a logger takes."""

    def __init__(self):
        self.warnings = []
        self.errors = []

    def warning(self, msg):
        self.warnings.append(msg)

    warn = warning

    def error(self, msg):
        self.errors.append(msg)

    def info(self, msg):
        pass

    def debug(self, msg):
        pass

    def isEnabledFor(self, level):
        return False


@pytest.fixture
def nameservice():
    service = ns_mod.remoteObjectNameService('names', ro.nullLogger(), HOST)
    previous, ro.default_ns = ro.default_ns, service
    yield service
    ro.default_ns = previous


# --------------------------------------------------------- the schedule --

def test_the_first_retry_is_above_the_noise_floor():
    """It was a millisecond, which is below any round trip worth the name --
    so against a network fault the first several retries were spent before
    anything could have changed."""
    pubsub = PubSub.PubSub('sched', ro.nullLogger(), numthreads=2)

    assert pubsub.redelivery_delay >= 0.01


def test_the_schedule_still_backs_off_to_the_cap():
    pubsub = PubSub.PubSub('sched2', ro.nullLogger(), numthreads=2)

    delay, seen = pubsub.redelivery_delay, []
    for _ in range(10):
        seen.append(delay)
        delay = min(pubsub.max_delivery_delay,
                    delay * pubsub.redelivery_increase_factor)

    assert seen == sorted(seen), 'it should only ever grow'
    assert seen[-1] == pubsub.max_delivery_delay
    assert len(set(seen)) > 3, 'and take a few steps getting there'


def test_retries_are_spread_rather_than_run_in_lockstep():
    """Which is what a name service or a host restarting looks like: a
    crowd of partners failing on the same tick, and without spread they all
    come back on the same tick too, all the way up the backoff."""
    pubsub = PubSub.PubSub('jitter', ro.nullLogger(), numthreads=2)

    assert pubsub.redelivery_jitter > 0

    # what the failure path computes, for one step of the schedule
    import random
    base = 0.4
    spread = base * pubsub.redelivery_jitter
    fired = [max(0.0, base + random.uniform(-spread, spread))
             for _ in range(200)]

    assert len(set(fired)) > 100, 'they should not all land together'
    assert min(fired) >= 0.0
    assert max(fired) <= base + spread


# ------------------------------------------------------- the backlog --

def make_partner(maxlen=10):
    return Bunch.Bunch(backlog=Deque(maxlen=maxlen), dropped=0)


def records(n, start=0):
    return [('sub', {'n': i}, [], [], 0) for i in range(start, start + n)]


def test_a_full_backlog_says_what_it_dropped():
    """It dropped silently, so a subscriber could lose hundreds of updates
    with nothing said anywhere."""
    pubsub = PubSub.PubSub('drop', Recording(), numthreads=2)
    partner = make_partner(maxlen=10)

    pubsub._backlog_add('subX', partner, records(14))

    assert partner.dropped == 4
    assert pubsub.logger.warnings
    assert 'dropping the oldest' in pubsub.logger.warnings[0]


def test_it_is_the_oldest_that_goes():
    """The right end for status: what a subscriber missed matters less than
    where things now stand."""
    pubsub = PubSub.PubSub('drop2', Recording(), numthreads=2)
    partner = make_partner(maxlen=10)

    pubsub._backlog_add('subX', partner, records(14))

    assert partner.backlog[0][1] == {'n': 4}
    assert partner.backlog[-1][1] == {'n': 13}


def test_a_backlog_with_room_drops_nothing_and_says_nothing():
    pubsub = PubSub.PubSub('drop3', Recording(), numthreads=2)
    partner = make_partner(maxlen=10)

    pubsub._backlog_add('subX', partner, records(4))

    assert partner.dropped == 0
    assert not pubsub.logger.warnings


def test_it_does_not_warn_once_per_dropped_update():
    """A subscriber that stays down would otherwise fill the log with one
    line per update for as long as it is down."""
    pubsub = PubSub.PubSub('drop4', Recording(), numthreads=2)
    partner = make_partner(maxlen=10)

    for _ in range(300):
        pubsub._backlog_add('subX', partner, records(1))

    assert partner.dropped > 250
    assert len(pubsub.logger.warnings) < 5


# ------------------------------------------------- rebuilding the proxy --

def test_the_proxy_is_rebuilt_once_per_failure_not_once_per_retry(
        nameservice):
    """A proxy looked up by name re-resolves itself when a call fails --
    call_failover() asks the name service again -- so replacing it on every
    retry discarded one that had just healed and paid for another lookup to
    get back where it was."""
    publisher = Monitor.Monitor('rb-pub', ro.nullLogger(), numthreads=8)
    publisher.start()
    publisher.start_server(svcname='rb-pub', host=HOST, ns=nameservice,
                           default_auth=False, usethread=True, wait=True)
    subscriber = Monitor.Monitor('rb-sub', ro.nullLogger(), numthreads=8)
    subscriber.start()
    subscriber.start_server(svcname='rb-sub', host=HOST, ns=nameservice,
                            default_auth=False, usethread=True, wait=True)
    try:
        publisher.subscribe('rb-sub', ['rb-pub'], {'unsub': False})
        time.sleep(0.4)

        rebuilds = []
        original = publisher.proxy_error

        def counting(name, partner):
            rebuilds.append(name)
            return original(name, partner)

        publisher.proxy_error = counting

        # Take the subscriber away and let several retries happen.
        subscriber.stop_server()
        subscriber.stop()
        for i in range(4):
            publisher.update('GONE.%d' % i, {'n': i}, ['rb-pub'])
        time.sleep(2.5)

        partner = publisher._partner['rb-sub']
        assert partner.time_failure is not None, 'it should be failing'
        assert len(rebuilds) <= 1, \
            'rebuilt %d times in one failure' % (len(rebuilds),)
    finally:
        try:
            publisher.stop_server()
        except Exception:
            pass
        publisher.stop()


def test_a_subscriber_that_comes_back_is_caught_up(nameservice):
    """The part that has to keep working: the backoff exists to bridge an
    outage, not to give up on one."""
    publisher = Monitor.Monitor('cb-pub', ro.nullLogger(), numthreads=8)
    publisher.start()
    publisher.start_server(svcname='cb-pub', host=HOST, ns=nameservice,
                           default_auth=False, usethread=True, wait=True)

    def start_subscriber(port=None):
        monitor = Monitor.Monitor('cb-sub', ro.nullLogger(), numthreads=8)
        monitor.start()
        monitor.start_server(svcname='cb-sub', host=HOST, port=port,
                             ns=nameservice, default_auth=False,
                             usethread=True, wait=True)
        return monitor

    subscriber = start_subscriber()
    port = subscriber.server.port
    try:
        publisher.subscribe('cb-sub', ['cb-pub'], {'unsub': False})
        time.sleep(0.4)

        subscriber.stop_server()
        subscriber.stop()
        for i in range(5):
            publisher.update('LATE.%d' % i, {'n': i}, ['cb-pub'])
        time.sleep(1.0)
        assert publisher._partner['cb-sub'].time_failure is not None

        again = start_subscriber(port=port)
        try:
            deadline = time.time() + 30.0
            while time.time() < deadline:
                if all('LATE.%d' % i in again for i in range(5)):
                    break
                time.sleep(0.1)
            missing = [i for i in range(5) if 'LATE.%d' % i not in again]
            assert not missing, 'never caught up on %r' % (missing,)
        finally:
            try:
                again.stop_server()
            except Exception:
                pass
            again.stop()
    finally:
        try:
            publisher.stop_server()
        except Exception:
            pass
        publisher.stop()
