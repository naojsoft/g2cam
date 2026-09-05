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
        protocol
        encoding
        secure
        registrar
        pingtime
        keep

Registrations from services that have not been upgraded carry a ``transport``
field instead of ``protocol``; see :py:func:`normalize_options`.  Lookups
report both, since clients that have not been upgraded read ``transport``.

Updates come in on the channel 'names' and these propagate between the
name servers.

"""
import sys
import time
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


def legacy_transport_for(protocol):
    """The value an un-upgraded client expects to see for _protocol_.

    Clients that have not been upgraded read the ``transport`` field, so it
    goes on being reported.  A protocol with no older equivalent reports its
    own name: such a client cannot speak it under any label, and refusing
    something it does not recognise is better than being handed 'xmlrpc' and
    talking XML at a service that does not speak it.
    """
    try:
        spec = ro.ro_transport.get(protocol)
    except Exception:
        return protocol
    return spec.legacy_transport or spec.name


def normalize_alternates(alternates, logger):
    """Put a registration's *other* ways in.

    A service may listen on several protocols at once -- XML-RPC for
    whatever has not been upgraded, something faster for whatever has.  They
    are one service, not several providers of it, so they belong on one
    registration rather than as separate entries that a caller would treat
    as alternatives to fail over between.

    A malformed entry is dropped rather than refused.  The registration as a
    whole is still good, and losing one way in beats losing the service.

    :return: a list of dicts with ``protocol``, ``port`` and ``encoding``.
    """
    if not alternates:
        return []
    if not isinstance(alternates, (list, tuple)):
        logger.warning("registration's alternates (%s) should be a list; "
                       "ignoring them" % (alternates,))
        return []

    kept = []
    for entry in alternates:
        if not isinstance(entry, dict):
            logger.warning("ignoring malformed alternate %s" % (entry,))
            continue
        protocol, port = entry.get('protocol'), entry.get('port')
        if not isinstance(protocol, str) or not isinstance(port, int):
            logger.warning("ignoring alternate without a protocol and port: "
                           "%s" % (entry,))
            continue

        encoding = entry.get('encoding')
        try:
            encoding = ro.ro_transport.get(protocol).check_encoding(encoding)
        except Exception:
            pass                # newer than us, or not ours to refuse

        kept.append(dict(protocol=protocol, port=port, encoding=encoding))
    return kept


def normalize_options(options, logger):
    """Put a registration's options into the current shape.

    Registrations arrive from services of two vintages.  A current one sends
    ``protocol``, and ``encoding`` where its protocol has a choice to make.
    An older one sends only ``transport`` -- a field named for the transport
    that in fact held protocol names, which is why 'xmlrpc' sat in it
    alongside 'socket'.  Its three possible values were the three transport
    modules that existed, so the translation is complete.

    :return: a dict with ``protocol``, ``encoding``, ``secure``, ``keep``
        and ``alternates``.
    """
    if isinstance(options, bool):
        # Older still: options was the bare 'secure' flag.
        logger.warning("Deprecated registration API used (options as bool)")
        options = dict(secure=options)
    elif not isinstance(options, dict):
        raise nameServiceError("options argument (%s) should be a dict"
                               % (options,))

    protocol = options.get('protocol')
    if protocol is None:
        # An un-upgraded service.  Its 'transport' is a protocol name.
        legacy = options.get('transport', ro.default_transport)
        protocol = ro.ro_transport.resolve_legacy_transport(legacy)
        logger.debug("registration without a protocol field; reading "
                     "transport '%s' as protocol '%s'" % (legacy, protocol))

    encoding = options.get('encoding')
    try:
        # Let the protocol say what it actually puts on the wire, rather
        # than recording whatever the registrant's module default was.
        encoding = ro.ro_transport.get(protocol).check_encoding(encoding)
    except Exception:
        # An unknown protocol is not this name service's business to refuse:
        # it may simply be newer than we are.  Record it as given.
        pass

    return dict(protocol=protocol, encoding=encoding,
                secure=options.get('secure', ro.default_secure),
                keep=options.get('keep', False),
                alternates=normalize_alternates(options.get('alternates'),
                                                logger))


class remoteObjectNameService:

    def __init__(self, svcname, pubsub, logger, myhost, purge_delta=30.0):

        # The name we publish under
        self.channel = svcname
        self.logger = logger
        self.pubsub = pubsub
        self.myhost = myhost

        # How long we don't hear from somebody before we drop them
        self.purge_delta = purge_delta

        # self state mutex
        self.lock = threading.RLock()

        # the "white pages"
        self.wp = dict()

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

                opts = normalize_options(options, self.logger)

                rec.update(dict(name=name, host=host, port=port,
                                registrar=registrar, pingtime=hosttime,
                                protocol=opts['protocol'],
                                encoding=opts['encoding'],
                                secure=opts['secure'], keep=opts['keep'],
                                alternates=opts['alternates']))

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
                       protocol=ro.ns_transport,
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
                protocol = val_d.get('protocol') or val_d.get(
                    'transport', ro.default_transport)
                encoding = val_d.get('encoding')
                pingtime = val_d.get('pingtime', 0)
                registrar = val_d['registrar']
                keep = val_d.get('keep', False)
                res.append(dict(name=name, host=host, port=port,
                                secure=secure, keep=keep,
                                protocol=protocol,
                                # Deprecated, and still reported: this is the
                                # field un-upgraded clients read.
                                transport=legacy_transport_for(protocol),
                                encoding=encoding,
                                # The other ways into the same service.  A
                                # client that does not know to look finds
                                # the top-level fields as before, which is
                                # why the primary is the compatible one.
                                alternates=val_d.get('alternates', []),
                                pingtime=pingtime, registrar=registrar))

        return res

    def _getInfoPred(self, pred_fn):
        """Return a list of info (dicts) for any services that match
        predicate function _pred_fn_."""
        res = []
        with self.lock:
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

    def getInfoMine(self):
        """Return the info for any services that we registered."""
        return self._getInfoPred(lambda d: d['registrar'] == self.myhost)

    def update_offsite_registrations(self, pubsub, channel, env):
        """This is called when we get an update from other name services."""
        if env['registrar'] != self.myhost:
            self.logger.info("notified of service on another node: %s" % env['registrar'])
            for rec in env['names']:
                options = {key: rec[key]
                           for key in ['secure', 'protocol', 'transport',
                                       'encoding', 'keep']
                           if key in rec}
                self._register(rec['name'], rec['host'], rec['port'],
                               rec['registrar'], options, rec['pingtime'])

    def share_our_registrations(self):
        """This is called to update other name services with names we
           registered.
        """
        my_regs = self.getInfoMine()
        env = dict(registrar=self.myhost, names=my_regs)
        self.pubsub.publish(self.channel, env, self.pack_info)

    def publish_loop(self, interval, ev_quit):
        """Loop invoked to periodically publish our registrations from
        white pages to other name services.
        """
        while not ev_quit.is_set():
            time_end = time.time() + interval

            try:
                self.share_our_registrations()
            except Exception as e:
                self.logger.error(f"publish loop error: {e}", exc_info=True)

            sleep_time = max(0, time_end - time.time())
            time.sleep(sleep_time)


#------------------------------------------------------------------
# MAIN PROGRAM
#
def main(options, args):

    # Create top level logger.
    logger = ssdlog.make_logger(options.svcname, options)

    try:
        myhost = ro.get_myhost()

    except Exception as e:
        raise nameServiceError("Can't get my own hostname: %s" % str(e))

    ev_quit = threading.Event()

    # Create a pubsub instance
    pubsub = PubSub(host=options.pubsub_host, port=options.pubsub_port,
                    logger=logger)

    nsobj = remoteObjectNameService(options.svcname, pubsub, logger, myhost,
                                    purge_delta=options.purge_delta)

    t_pool = Task.ThreadPool(logger=logger, ev_quit=ev_quit,
                             numthreads=options.numthreads)

    # Create remote object server for this object.
    # svcname to None temporarily because we get into infinite loop
    # try to register ourselves.
    nssvc = ro.remoteObjectServer(name=options.svcname, obj=nsobj,
                                  svcname=None,  #?!!
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

            logger.info("Starting pubsub subscribe loop...")
            task = Task.FuncTask2(pubsub.subscribe_loop, ev_quit)
            t_pool.addTask(task)

            logger.info("Starting pubsub publish loop...")
            task = Task.FuncTask2(nsobj.publish_loop,
                                  options.publish_interval, ev_quit)
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
