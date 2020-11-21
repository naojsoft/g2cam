"""
Example to stream status items from Gen2

IMPORTANT:

[1] If you need only a limited number of status items and you don't need
updates any faster than 10 sec interval, please consider using the
StatusClient() class--(see the "fetch_status.py" example).

[2] The StatusStream class gives you a roughly 1Hz stream of updated
status items from Gen2.  It does not give you ALL status items in each
update--ONLY the items that have CHANGED since the last update.  You
may need therefore to use the StatusClient class to fetch your initial
values for status items, as shown in the example below.

[3] Do not rely on the internals of the StatusStream class!!!
The implementation details are subject to change suddenly!

"""
import sys
import argparse
import threading
import queue as Queue

from g2cam.status.client import StatusClient
from g2cam.status.stream import StatusStream


def main(options, args):
    # setup (use the Gen2 host, user name and password you are advised by
    # observatory personnel)
    sc = StatusClient(host=options.host, username=options.username,
                      password=options.password)
    sc.connect()

    # fetch a dictionary
    d = {'FITS.SBR.RA': None, 'FITS.SBR.DEC': None}
    sc.fetch(d)

    ss = StatusStream(host=options.streamhost, username=options.stream_username,
                      password=options.stream_password)
    ss.connect()

    # create a queue to receive the status updates
    status_q = Queue.Queue()
    # shared event to signal termination of processing
    ev_quit = threading.Event()

    # start a thread to put status updates on the queue
    t = threading.Thread(target=ss.subscribe_loop, args=[ev_quit, status_q])
    t.start()

    # consume items from queue
    try:
        print("consuming on queue...")
        while not ev_quit.is_set():
            envelope = status_q.get()
            changed = envelope['status']
            d.update({k: changed[k] for k in d if k in changed})
            print(d)

    except KeyboardInterrupt as e:
        ev_quit.set()


if __name__ == '__main__':

    # Parse command line options
    argprs = argparse.ArgumentParser()

    argprs.add_argument("--streamhost", dest="streamhost", metavar="HOST",
                        default='localhost',
                        help="Fetch streaming status from HOST")
    argprs.add_argument("--streamuser", dest="stream_username", default="none",
                        metavar="USERNAME",
                        help="Authenticate using USERNAME")
    argprs.add_argument("-sp", "--streampass", dest="stream_password",
                        default="none",
                        metavar="PASSWORD",
                        help="Authenticate for streams using PASSWORD")
    argprs.add_argument("--host", dest="host", metavar="HOST",
                        default='localhost',
                        help="Fetch status from HOST")
    argprs.add_argument("--user", dest="username", default="none",
                        metavar="USERNAME",
                        help="Authenticate using USERNAME")
    argprs.add_argument("-p", "--pass", dest="password", default="none",
                        metavar="PASSWORD",
                        help="Authenticate using PASSWORD")
    (options, args) = argprs.parse_known_args(sys.argv[1:])

    if len(args) != 0:
        argprs.error("incorrect number of arguments")

    main(options, args)
