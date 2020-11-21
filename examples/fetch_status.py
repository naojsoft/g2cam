"""
Example to fetch status items from Gen2

IMPORTANT:

[1] If you are requesting many status items, do it in one large call,
instead of several calls.  That way the status items will be consistent
with each other in time.

[2] If you need a continuous status feed (more frequent than once every
10 sec on average), please consider using the StatusStream() class--
(see the "stream_status.py" example).  It will push a continuous stream of
status items to you and is more efficient for that purpose.

[3] Do not rely on the internals of the StatusClient class!!!  The
implementation details are subject to change suddenly!

"""
import sys
import argparse

from g2cam.status.client import StatusClient


def main(options, args):
    # setup (use the Gen2 host, user name and password you are advised by
    # observatory personnel)
    st = StatusClient(host=options.host, username=options.username,
                      password=options.password)
    st.connect()

    # fetch a dictionary
    d = {'FITS.SBR.RA': None, 'FITS.SBR.DEC': None}
    st.fetch(d)
    print(d)

    # fetch a list of aliases
    l = ['FITS.SBR.RA', 'FITS.SBR.DEC']
    print('list:', l)
    print('status:', st.fetch_list(l))


if __name__ == '__main__':

    # Parse command line options
    argprs = argparse.ArgumentParser()

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
