#
# remoteObjectNameSvc.py -- remote object name service.
#
# E. Jeschke
#
# TODO:
# [ ] How to handle authentication for name service
#
"""
This is the name service for the remoteObjects middleware.

Each service created by remoteObjectService registers itself here so that
it can be looked up by name instead of (host, port).

Synchronization between name servers is accomplished using the Monitor
service (built on top of PubSub).  The data is stored in the following
hierarchy in the local "white pages":

name
    host:port
        transport
        encoding
        secure
        registrar
        pingtime
        keep

Updates come in on the channel 'names' and these propagate between the
name servers.

"""
import sys, time
import threading

from g2base.remoteObjects import remoteObjects as ro
from g2base.remoteObjects.pubsubs.pubsub_redis import PubSub
from g2base import ssdlog, Task, Bunch

# Our version
version = '20230515.0'


# For generating errors from this module
#
class nameServiceError(Exception):
    pass


# TODO:
#  - The registration of names is done via individual pubsub updates.
#    This may be too fine grained of a method.  When we want to sync all
#    of our own registrations to another node it would be better to send
#    them all in a single update.
#
class remoteObjectNameService(object):

    def __init__(self, pubsub, logger, myhost, purge_delta=30.0):

        self.logger = logger
        self.pubsub = pubsub
        self.myhost = myhost

        # How long we don't hear from somebody before we drop them
        self.purge_delta = purge_delta

        # self state mutex
        self.lock = threading.RLock()

        # the "white pages"
        self.wp = dict()

        # The name we publish under
        self.channel = 'names'
        self.pubsub.subscribe(self.channel)
        self.pack_info = Bunch.Bunch(ptype='msgpack')
        self.pubsub.add_callback(self.channel,
                                 self.update_offsite_registrations)


    def _register(self, name, host, port, registrar, options, hosttime,
                  replace=True):
        tag = f'{host:s}:{port:d}'
        rec = dict()

        with self.lock:
            try:
                dct = self.wp.setdefault(name, {})
                if tag not in dct:
                    self.logger.info(f"Registering remote object service {host:s}:{port:d} under name '{name:s}'")
                    dct[tag] = rec
                else:
                    rec = dct[tag]

                # TEMP: until we can deprecate former api
                if isinstance(options, bool):
                    self.logger.warn("Deprecated API used!")
                    secure = options
                    transport = ro.default_transport
                    encoding = ro.default_encoding
                    options = {}
                elif isinstance(options, dict):
                    secure = options.get('secure', ro.default_secure)
                    transport = options.get('transport', ro.default_transport)
                    encoding = options.get('encoding', ro.default_encoding)
                else:
                    raise nameServiceError("flags argument (%s) should be a dict" % (
                        str(flags)))

                keep = options.get('keep', False)
                rec.update(dict(name=name, host=host, port=port,
                                registrar=registrar, pingtime=hosttime,
                                transport=transport, encoding=encoding,
                                secure=secure, keep=keep))

                if registrar == self.myhost:
                    # only notify others of our own registrations
                    self.pubsub.publish(self.channel, rec, self.pack_info)

                return rec

            except Exception as e:
                self.logger.error(f"Failed to register '{name:s}': {e}",
                                  exc_info=True)


    def register(self, name, host, port, options, replace=True):
        """Register a new name at a certain host and port."""

        hosttime = time.time()
        self._register(name, host, port, self.myhost, options, hosttime,
                       replace=replace)
        return 0

    def ping(self, name, host, port, options, hosttime):
        """Get pinged by service.  Register this service if we have never
        heard of it, otherwise just record the time that we heard from it.
        """
        now = time.time()
        self.logger.info("Ping from '%s' (%s:%d) -- %.4f [%.4f]" % (
            name, host, port, hosttime, now-hosttime))

        self._register(name, host, port, self.myhost, options, hosttime)

    def unregister(self, name, host, port):
        """Unregister a new name at a certain host and port."""

        tag = f'{host:s}:{port:d}'

        with self.lock:
            dct = self.wp.get(name, {})
            if tag not in dct:
                return

            self.logger.info(f"Unregistering remote object service {host:s}:{port:d} under name '{name:s}'")
            del dct[tag]

    def clearName(self, name):
        """Clear all registrations associated with _name_."""

        with self.lock:
            if name in self.wp:
                del self.wp[name]
        return 0

    def clearAll(self):
        """Clear all name registrations."""

        with self.lock:
            self.wp = dict()
            self.register_self()

        return 0

    def register_self(self):
        options = dict(secure=ro.default_secure,
                       transport=ro.ns_transport,
                       encoding=ro.ns_encoding,
                       keep=True)

        hosttime = time.time()
        return self._register(self.channel, self.myhost, ro.nameServicePort,
                              self.myhost, options, hosttime)

    def getNames(self):
        """Return a list of all registered names."""

        with self.lock:
            names = list(self.wp.keys())
        return names

    def getNamesSorted(self):
        """Returns a sorted list of all registered names."""

        names = self.getNames()
        names.sort()
        return names

    def purgeDead(self, name):
        """Purge all registered instances of _name_ that haven't been heard
        from in purge_delta seconds."""

        with self.lock:
            svcs = self.getInfo(name)
            for d in svcs:
                if 'pingtime' not in d:
                    # No pingtime?  Give `em the boot!
                    self.unregister(d['name'], d['host'], d['port'])

                else:
                    # Haven't heard from them in a while?  Ditto!
                    delta = time.time() - d['pingtime']
                    if delta > self.purge_delta and not d['keep']:
                        self.unregister(d['name'], d['host'], d['port'])

            # if there are no more live hosts left, drop the service name
            # as well
            svcs = self.wp.get(name, {})
            if len(svcs) == 0:
                del self.wp[name]

    def purgeAll(self):
        """Iterate over all known registrations and perform a purge
        operation from those we haven't heard from lately."""

        with self.lock:
            for name in self.getNames():
                # # TEMP HACK UNTIL WE GET BUMP OUR OWN PING TIME
                # if name in ('names',):
                #     continue
                self.purgeDead(name)

    def purgeLoop(self, interval, ev_quit):
        """Loop invoked to periodically purge data from white pages."""
        while not ev_quit.is_set():
            time_end = time.time() + interval

            try:
                self.purgeAll()
            except Exception as e:
                self.logger.error("Purge loop error: %s" % (str(e)))

            sleep_time = max(0, time_end - time.time())
            time.sleep(sleep_time)

    def getHosts(self, name):
        """Return a list of all (host, port) pairs associated with
        a registered name.
        TO BE DEPRECATED--DO NOT USE.  USE getInfo() INSTEAD."""

        with self.lock:
            dct = self.wp.get(name, {})
            if len(dct) == 0:
                return []
            instances = list(dct.keys())
        return [(h, int(p)) for h, p in [key.split(':') for key in instances]]

    def getInfo(self, name):
        """Return a list of dicts of service info associated with
        a registered name.  This should be used in preference to
        getHosts for most applications."""

        with self.lock:
            dct = self.wp.get(name, {})
            if len(dct) == 0:
                return []

            res = []
            for key, val_d in dct.items():
                host, port = key.split(':')
                port = int(port)
                secure = val_d.get('secure', ro.default_secure)
                transport = val_d.get('transport', ro.default_transport)
                encoding = val_d.get('encoding', ro.default_encoding)
                pingtime = val_d.get('pingtime', 0)
                registrar = val_d['registrar']
                keep = val_d.get('keep', False)
                res.append(dict(name=name, host=host, port=port,
                                secure=secure, keep=keep,
                                transport=transport, encoding=encoding,
                                pingtime=pingtime, registrar=registrar))

        return res

    def _getInfoPred(self, pred_fn):
        """Return a list of info (dicts) for any services that match
        predicate function _pred_fn_."""
        res = []
        for name in self.getNames():
            infolist = self.getInfo(name)

            for d in infolist:
                if pred_fn(d):
                    res.append(d)

        return res

    def getInfoHost(self, host):
        """Return the info for any services registered on _host_."""
        return self._getInfoPred(lambda d: d['host'] == host)

    def getNamesHost(self, host):
        """Return the names for any services registered on _host_."""
        res = self.getInfoHost(host)
        return [d['name'] for d in res]

    def update_offsite_registrations(self, pubsub, channel, rec):
        """This is called when we get an update from other name services."""
        if rec['registrar'] != self.myhost:
            self.logger.info(f"notified of service on another node: {rec:s}")
            options = {key: rec[key] for key in ['secure', 'transport',
                                                 'encoding', 'keep']}
            self._register(rec['name'], rec['host'], rec['port'],
                           rec['registrar'], options, rec['pingtime'])

    # def _syncInfoPred(self, pred_fn):
    #     """Ping any services that match predicate function _pred_fn_."""
    #     res = self._getInfoPred(pred_fn)
    #     for d in res:
    #         self.ping(d['name'], d['host'], d['port'],
    #                   { 'secure': d['secure'],
    #                     'transport': d['transport'],
    #                     'encoding': d['encoding'],
    #                     },
    #                   d['pingtime'])

    # def syncInfoSelf(self):
    #     """Ping any services that registered with us."""
    #     self._syncInfoPred(lambda d: d['host'] == self.myhost)


#------------------------------------------------------------------
# MAIN PROGRAM
#
def main(options, args):

    # Create top level logger.
    logger = ssdlog.make_logger('names', options)

    try:
        myhost = ro.get_myhost()

    except Exception as e:
        raise nameServiceError("Can't get my own hostname: %s" % str(e))

    ev_quit = threading.Event()

    # Create a pubsub instance
    pubsub = PubSub(host='localhost', logger=logger)

    nsobj = remoteObjectNameService(pubsub, logger, myhost,
                                    purge_delta=options.purge_delta)

    t_pool = Task.ThreadPool(logger=logger, ev_quit=ev_quit,
                             numthreads=options.numthreads)

    # Create remote object server for this object.
    # svcname to None temporarily because we get into infinite loop
    # try to register ourselves.
    nssvc = ro.remoteObjectServer(name='names', obj=nsobj, svcname=None,
                                  transport=ro.ns_transport,
                                  encoding=ro.ns_encoding,
                                  port=options.port, logger=logger,
                                  usethread=True, threadPool=t_pool,
                                  ev_quit=ev_quit,
                                  #authDict=authDict,
                                  secure=options.secure,
                                  cert_file=options.cert)

    server_started = False
    try:
        try:
            logger.info("Starting thread pool ...")
            t_pool.startall(wait=True)

            logger.info("Starting pubsub...")
            task = Task.FuncTask2(pubsub.subscribe_loop, ev_quit)
            t_pool.addTask(task)

            server_started = True

            logger.info("Starting name service..")
            nssvc.ro_start(wait=True)

            # Register ourself
            logger.info("Registering self..")
            nsobj.register_self()

        except Exception as e:
            logger.error(str(e), exc_info=True)
            raise e

        logger.info("Entering main loop..")
        try:
            nsobj.purgeLoop(options.purge_interval, ev_quit)

        except KeyboardInterrupt:
            logger.error("Received keyboard interrupt!")

        except Exception as e:
            logger.error(str(e))

    finally:
        logger.info("Stopping remote objects name service...")
        if server_started:
            ev_quit.set()
            t_pool.stopall(wait=True)

    logger.info("Exiting remote objects name service...")
    #ev_quit.set()
    sys.exit(0)
