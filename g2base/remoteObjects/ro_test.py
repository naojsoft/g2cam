#!/usr/bin/env python
#
# Remote objects tests
#
# Eric Jeschke (eric@naoj.org)
#

import sys, time
import logging

from g2base import Task, ssdlog
from g2base.remoteObjects import remoteObjects as ro

class TestRO(ro.remoteObjectServer):

    def __init__(self, options, logger, threadPool, threaded=True,
                 usethread=False):

        authDict = {}
        if options.auth:
            auth = options.auth.split(':')
            authDict[auth[0]] = auth[1]

        # Superclass constructor
        ro.remoteObjectServer.__init__(self, svcname=options.svcname,
                                       logger=logger,
                                       port=options.port,
                                       threaded_server=threaded,
                                       usethread=usethread,
                                       authDict=authDict,
                                       secure=options.secure,
                                       transport=options.transport,
                                       threadPool=threadPool,
                                       cert_file=options.cert)

    def search(self, ra, dec, radius, mag):
        # For testing call overhead time
        # comment out print statement to measure call overhead
        print "ra=%f dec=%f radius=%f mag=%f" % (ra, dec, radius, mag)
        return ra


    def test(self, ra, dec, radius, mag):
        # For testing call overhead time
        print "ra=%f dec=%f radius=%f mag=%f" % (ra, dec, radius, mag)
        return ra

def client2(options, logger):

    datafile = None
    if options.datafile:
        datafile = open(options.datafile, 'a')
            
    auth = None
    if options.auth:
        auth = options.auth.split(':')

    # Get handle to server
    testro = ro.remoteObjectProxy(options.svcname, auth=auth,
                                  logger=logger,
                                  secure=options.secure,
                                  timeout=2.0)

    time1 = time.time()

    for i in xrange(options.count):
        res = testro.test(1.0, 2.0, 3.0, 4.0)

    tottime = time.time() - time1
    time_per_call = tottime / options.count
    calls_per_sec = int(1.0 / time_per_call)

    print "Time taken: %f secs total  %f sec per call  %d calls/sec" % \
          (tottime, time_per_call, calls_per_sec)

    if datafile:
        # total bytes, count, total time, encode time, avg rate
        datafile.write("%d %d %f %f %f\n" % (
            size*options.count, options.count, tottime, time2-time1,
            amount/tottime))
        datafile.close()


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
                        threaded=True, usethread=False)

        print "Starting TestRO service..."
        try:
            testro.ro_start()

        except KeyboardInterrupt:
            print "Shutting down..."
            testro.ro_stop()
            threadPool.stopall(wait=True)

    elif select == 'calls':
        client2(options, logger)

    else:
        print "I don't know how to do '%s'" % select
        sys.exit(1)

    print "Program exit."
    sys.exit(0)
            
if __name__ == '__main__':

    # Parse command line options with nifty new optparse module
    from optparse import OptionParser

    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage, version=('%%prog'))
    
    parser.add_option("--action", dest="action",
                      help="Action is server|calls")
    parser.add_option("--auth", dest="auth",
                      help="Use authorization; arg should be user:passwd")
    parser.add_option("--cert", dest="cert",
                      help="Path to key/certificate file")
    parser.add_option("--count", dest="count", type="int",
                      default=1,
                      help="Iterate NUM times", metavar="NUM")
    parser.add_option("--datafile", dest="datafile", metavar='FILE',
                      help="Write statistics to FILE")
    parser.add_option("--debug", dest="debug", default=False,
                      action="store_true",
                      help="Enter the pdb debugger on main()")
    parser.add_option("--numthreads", dest="numthreads", type="int",
                      default=10,
                      help="Use NUM threads in thread pool", metavar="NUM")
    parser.add_option("--port", dest="port", type="int",
                      help="Register using PORT", metavar="PORT")
    parser.add_option("--profile", dest="profile", action="store_true",
                      default=False,
                      help="Run the profiler on main()")
    parser.add_option("--secure", dest="secure", action="store_true",
                      default=False,
                      help="Use SSL encryption")
    parser.add_option("--svcname", dest="svcname",
                      default='ro_test',
                      help="Register using service NAME", metavar="NAME")
    parser.add_option("--transport", dest="transport", metavar='PROTOCOL',
                      default=ro.default_transport,
                      help="Choose PROTOCOL for transport")
    ssdlog.addlogopts(parser)

    (options, args) = parser.parse_args(sys.argv[1:])

    # Are we debugging this?
    if options.debug:
        import pdb

        pdb.run('main(options, args)')

    # Are we profiling this?
    elif options.profile:
        import profile

        print "%s profile:" % sys.argv[0]
        profile.run('main(options, args)')

    else:
        main(options, args)
       
#END


    

    
