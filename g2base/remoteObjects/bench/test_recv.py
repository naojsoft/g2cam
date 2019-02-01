import sys
import time
import threading

from g2base import ssdlog

count = 0
time_start = 0.0
last_time = 0.0
latency = dict(total=0.0, _min=10000.0, _max=0.0)

def reset_counters():
    global count, time_start, last_time, latency
    count = 0
    time_start = 0.0
    last_time = 0.0
    latency = dict(total=0.0, _min=10000.0, _max=0.0)

def subscribe_cb(ps, channel, payload, logger):
    global count, time_start, last_time, latency
    cur_time = time.time()
    count += 1
    if cur_time - last_time > 1.0:
        last_time = cur_time
        logger.info("received %d messages" % (count))

    if payload['state'] == 'start':
        time_start = time.time()

    elif payload['state'] == 'stop':
        elapsed = time.time() - time_start
        logger.info("%.2f messages/sec" % (count/elapsed))
        logger.info("latency: avg: %f min: %f max: %f" % (
            latency['total'] / count, latency['_min'], latency['_max']))
        reset_counters()

    else:
        _lat = max(0, cur_time - payload['time'])
        latency['total'] += _lat
        latency['_min'] = min(_lat, latency['_min'])
        latency['_max'] = max(_lat, latency['_max'])


def main(options, args):

    if options.backend == 'redis':
        from g2base.remoteObjects.pubsubs.pubsub_redis import PubSub
    elif options.backend == 'nats':
        from g2base.remoteObjects.pubsubs.pubsub_nats import PubSub
    elif options.backend == 'zmq':
        from g2base.remoteObjects.pubsubs.pubsub_zmq import PubSub
    else:
        print("Please specify a --backend")
        sys.exit(1)

    logger = ssdlog.make_logger('recv', options)

    ps = PubSub('localhost', logger=logger)
    ps.subscribe('test')

    # add a callback for messages to 'foo' channel
    ps.add_callback('test', subscribe_cb, logger)

    # start a thread in a loop listening for messages
    logger.info("waiting for messages...")
    ps.subscribe_loop(threading.Event())


if __name__ == '__main__':
    from optparse import OptionParser

    usage = "usage: %prog [options]"
    optprs = OptionParser(usage=usage)

    optprs.add_option("--backend", dest="backend", metavar="NAME",
                      default=None,
                      help="Specify backend transport")
    optprs.add_option("--debug", dest="debug", default=False,
                      action="store_true",
                      help="Enter the pdb debugger on main()")
    optprs.add_option("--profile", dest="profile", action="store_true",
                      default=False,
                      help="Run the profiler on main()")
    ssdlog.addlogopts(optprs)

    (options, args) = optprs.parse_args(sys.argv[1:])

    if len(args) != 0:
        optprs.error("incorrect number of arguments")

    # Are we debugging this?
    if options.debug:
        import pdb

        pdb.run('main(options, args)')

    # Are we profiling this?
    elif options.profile:
        import profile

        print(("%s profile:" % sys.argv[0]))
        profile.run('main(options, args)')

    else:
        main(options, args)
