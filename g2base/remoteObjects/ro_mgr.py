#!/usr/bin/python
#
"""
Examples of programmatic control of processes using remoteObjectManagerSvc.py

Instructions:
  1. In one xterm run
     $ ./remoteObjectManagerSvc.py --stderr --loglevel=0

  2. In another xterm run this program.

  Examples:
     ./ro_mgr.py --action=add --name='foo' --cmd="/usr/bin/ed"
     ./ro_mgr.py --action=add --name='bar' --cmd="/usr/bin/vi"

     ./ro_mgr.py --action=startall

     ./ro_mgr.py --action=stop --name=foo
     ./ro_mgr.py --action=start --name=foo

     ./ro_mgr.py --action=pid --name=bar

     ./ro_mgr.py --action=uptime --name=bar

     ./ro_mgr.py --action=load

     ./ro_mgr.py --action=shutdown

     Also, try killing the pid of one of the processes and watch it
     restart.

     In your program just copy the get_ms_handle() function and use it
     as illustrated in the examples below.
"""
import sys
from g2base.remoteObjects import remoteObjects as ro


def get_ms_handle(host=None):
    if not host:
        host = ro.get_myhost()

    # Create a handle to the manager service
    ms = ro.remoteObjectClient(host, ro.managerServicePort)

    return ms


if __name__ == '__main__':

    # Parse command line options
    from argparse import ArgumentParser

    usage = "%(prog)s [options] command [args]"
    parser = ArgumentParser(usage=usage)
    parser.add_argument('--version', action='version',
                        version='%(prog)s')

    parser.add_argument("--action", dest="action", default='nop',
                      metavar="ACTION",
                      help="Action is ACTION")
    parser.add_argument("--cmd", dest="cmdline", default=None,
                      metavar="CMD",
                      help="Specify command to be run.")
    parser.add_argument("--name", dest="name", default=None,
                      metavar="NAME",
                      help="Perform action on NAME")

    (options, args) = parser.parse_known_args(sys.argv[1:])

    ms = get_ms_handle()

    if options.action == 'add':
        res = ms.add(options.name, options.cmdline)
        res = ms.set_restart(options.name, True, 10, 5.0)

    elif options.action == 'clear':
        res = ms.clear()

    elif options.action == 'start':
        res = ms.start(options.name)

    elif options.action == 'startall':
        res = ms.startall()

    elif options.action == 'stop':
        res = ms.stop(options.name)

    elif options.action == 'stopall':
        res = ms.stopall()

    elif options.action == 'restart':
        res = ms.restart(options.name)

    elif options.action == 'restartall':
        res = ms.restartall()

    elif options.action == 'shutdown':
        res = ms.shutdown()

    elif options.action == 'pid':
        res = ms.getpid(options.name)

    elif options.action == 'uptime':
        res = ms.gettime(options.name)

    elif options.action == 'names':
        res = ms.getNames()

    elif options.action == 'load':
        res = ms.getLoadAvg()

    else:
        print("I don't understand action=%s" % options.action)
        sys.exit(1)

    print(res)

#END
