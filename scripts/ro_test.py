#!/usr/bin/env python
#
# Remote objects tests
#

import sys
import time
from g2base.remoteObjects import remoteObjects as ro
from g2base import Task, ssdlog


class TestRO(ro.remoteObjectServer):

    def __init__(self, options, logger, threadPool, usethread=True):

        authDict = {}
        if options.auth:
            auth = options.auth.split(':')
            authDict[auth[0]] = auth[1]

        # Superclass constructor
        ro.remoteObjectServer.__init__(self, svcname=options.svcname,
                                       logger=logger,
                                       port=options.port,
                                       usethread=usethread,
                                       authDict=authDict,
                                       secure=options.secure,
                                       transport=options.transport,
                                       threadPool=threadPool,
                                       cert_file=options.cert)

    def search(self, ra, dec, radius, mag):
        # For testing call overhead time
        # comment out print statement to measure call overhead
        print("ra=%f dec=%f radius=%f mag=%f" % (ra, dec, radius, mag))
        return ra


    def test(self, ra, dec, radius, mag):
        # For testing call overhead time
        print("ra=%f dec=%f radius=%f mag=%f" % (ra, dec, radius, mag))
        return ra

def client2(options, logger):

    auth = None
    if options.auth:
        auth = options.auth.split(':')

    # Get handle to server
    testro = ro.remoteObjectProxy(options.svcname, auth=auth,
                                  logger=logger,
                                  secure=options.secure,
                                  timeout=2.0)

    time1 = time.time()

    for i in range(options.count):
        res = testro.test(1.0, 2.0, 3.0, 4.0)

    tottime = time.time() - time1
    time_per_call = tottime / options.count
    calls_per_sec = int(1.0 / time_per_call)

    print("Time taken: %f secs total  %f sec per call  %d calls/sec" % \
          (tottime, time_per_call, calls_per_sec))


def main(options, args):

    ro.init()

    # Create top level logger.
    logger = ssdlog.make_logger('ro_test', options)

    select = options.action

    if select == 'server':

        threadPool = Task.ThreadPool(numthreads=options.numthreads,
                                     logger=logger)
        threadPool.startall(wait=True)

        testro = TestRO(options, logger, threadPool=threadPool,
                        usethread=False)

        print("Starting TestRO service...")
        try:
            testro.ro_start()

        except KeyboardInterrupt:
            print("Shutting down...")
            testro.ro_stop()
            threadPool.stopall(wait=True)

    elif select == 'calls':
        client2(options, logger)

    else:
        print("I don't know how to do '%s'" % select)
        sys.exit(1)

    print("Program exit.")
    sys.exit(0)

if __name__ == '__main__':

    # Parse command line options
    from argparse import ArgumentParser

    usage = "%(prog)s [options]"
    parser = ArgumentParser(usage=usage)
    parser.add_argument('--version', action='version',
                        version='%(prog)s')

    parser.add_argument("--action", dest="action",
                      help="Action is server|calls")
    parser.add_argument("--auth", dest="auth",
                      help="Use authorization; arg should be user:passwd")
    parser.add_argument("--cert", dest="cert",
                      help="Path to key/certificate file")
    parser.add_argument("--count", dest="count", type=int,
                      default=1,
                      help="Iterate NUM times", metavar="NUM")
    parser.add_argument("--debug", dest="debug", default=False,
                      action="store_true",
                      help="Enter the pdb debugger on main()")
    parser.add_argument("--numthreads", dest="numthreads", type=int,
                      default=10, metavar="NUM",
                      help="Use NUM threads in thread pool")
    parser.add_argument("--port", dest="port", type=int,
                      help="Register using PORT", metavar="PORT")
    parser.add_argument("--profile", dest="profile", action="store_true",
                      default=False,
                      help="Run the profiler on main()")
    parser.add_argument("--secure", dest="secure", action="store_true",
                      default=False,
                      help="Use SSL encryption")
    parser.add_argument("--svcname", dest="svcname",
                      default='ro_test',
                      help="Register using service NAME", metavar="NAME")
    parser.add_argument("--transport", dest="transport", metavar='PROTOCOL',
                      default=ro.default_transport,
                      help="Choose PROTOCOL for transport")
    ssdlog.addlogopts(parser)

    (options, args) = parser.parse_known_args(sys.argv[1:])

    # Are we debugging this?
    if options.debug:
        import pdb

        pdb.run('main(options, args)')

    # Are we profiling this?
    elif options.profile:
        import profile

        print("%s profile:" % sys.argv[0])
        profile.run('main(options, args)')

    else:
        main(options, args)

#END
