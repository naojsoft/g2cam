import sys
import threading
import time

from g2base import ssdlog, Bunch


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

    if options.marshal is None:
        print("Please specify a --marshal")
        sys.exit(1)

    pack_info = Bunch.Bunch(ptype=options.marshal)

    logger = ssdlog.make_logger('send', options)

    ps = PubSub('localhost', logger=logger)

    try:
        start_time = time.time()
        stop_time = start_time + 10.0

        d = {'mtype': 'test', 'state': 'start',
             'a': 45, 'b': 34.5, 'c': "foo",
             'e': 1000000000000000, 'f':[1, 2, 56.7],
             'g': True, 'h': False, 'i': None,
             }
        # test of sending binary data
        d['d'] = b"goo"

        logger.info("sending messages...")
        ps.publish('test', d, pack_info)

        d['state'] = 'test'

        cur_time = time.time()
        while cur_time < stop_time:
            d['time'] = cur_time
            ps.publish('test', d, pack_info)
            cur_time = time.time()

        d['state'] = 'stop'
        ps.publish('test', d, pack_info)
        ps.flush()

    finally:
        pass


if __name__ == '__main__':
    from optparse import OptionParser

    usage = "usage: %prog [options]"
    optprs = OptionParser(usage=usage)

    optprs.add_option("--backend", dest="backend", metavar="NAME",
                      default=None,
                      help="Specify backend transport")
    optprs.add_option("--marshal", dest="marshal", metavar="NAME",
                      default=None,
                      help="Specify method of marshalling")
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
