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

Name servers keep each other current by asking each other directly: each
one periodically takes every other's own registrations and merges them.
There is no broker in the middle.  There used to be -- the Monitor service,
over Redis -- and the exchange it did is the exchange that happens here now,
minus a process to run, a dependency to install, and a second way into the
white pages that nothing authenticated.

The data is stored in the following hierarchy in the local "white pages":

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

A name server is told about one other at startup (--peer) and finds the
rest from it; see :py:meth:`~remoteObjectNameService.peers`.

"""
import sys
import time
import uuid
import random
import threading

from g2base.remoteObjects import remoteObjects as ro
from g2base import ssdlog, Task

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
        # 'xmlrpc' rather than the module default: a registration with no
        # protocol field came from a service old enough not to write one,
        # and that is what such a service speaks whatever the default has
        # since become.
        legacy = options.get('transport', 'xmlrpc')
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


#: What a registration carries from one name server to another.
#:
#: Here rather than spelled out at the point of use because the two have to
#: agree and did not: 'alternates' was added to a registration and not to
#: the list that shares it, so a service's second way in reached the name
#: server it registered with and no other.  A caller resolving through a
#: peer then saw only the primary -- which is the oldest protocol on offer,
#: chosen to be what an un-upgraded caller can speak -- and quietly used
#: that.
#:
#: 'transport' is in the list because a registration from an un-upgraded
#: service carries it in place of 'protocol', and normalize_options reads it
#: when 'protocol' is absent.
SHARED_FIELDS = ('secure', 'protocol', 'transport', 'encoding', 'keep',
                 'alternates')


#: Namespace for name server ids, so that deriving one from a host name
#: cannot collide with a UUID derived from the same string anywhere else.
NODE_NAMESPACE = uuid.UUID('6f9619ff-8b86-d011-b42d-00c04fc964ff')

#: Names that mean "whoever is asking", and so can never name a peer.  They
#: would be caught anyway on the first connection, by the id that comes
#: back -- but not before being passed to other name servers, where each one
#: means a different machine and all of them mean the wrong one.
LOOPBACK_NAMES = frozenset(['localhost', 'localhost.localdomain',
                            '127.0.0.1', '::1', 'ip6-localhost'])


def derive_node_id(myhost, port, configured=None):
    """Positively identify one name server.

    A name server has always been known by its host name, which answers the
    wrong question twice: two name servers on one host share a host name,
    and one name server reached by its short name and by its FQDN is two
    different strings.  Neither is hypothetical -- the first is what makes a
    cluster impossible to rehearse on one machine, and the second is a
    standing invitation for a node to treat itself as a peer.  An id is
    carried in the registration, so whoever reads it is comparing what the
    node says it is rather than how they happened to reach it.

    Derived rather than random, because it has to survive a restart.  A name
    server that came back with a new id would not recognise its own
    registrations in a peer's copy and would merge them back as somebody
    else's, resurrecting exactly the services it had just forgotten.  That
    makes an id that outlives the process a correctness requirement rather
    than a convenience.

    Derived from the host and the port rather than from anything about the
    machine: /etc/machine-id would survive a rename, but it is the same on
    both of two name servers running on one host, which is the case this
    exists to tell apart.

    :param configured: An id given explicitly, which is what a site should
        use if it renames a host and wants the name server on it to go on
        being the same one.
    """
    if configured:
        return configured
    return str(uuid.uuid5(NODE_NAMESPACE, '%s:%d' % (myhost, port)))


class remoteObjectNameService:

    def __init__(self, svcname, logger, myhost, purge_delta=30.0,
                 peer_hosts=None, peer_timeout=2.0, node_id=None,
                 port=None):

        # The name we publish under
        self.channel = svcname
        self.logger = logger
        self.myhost = myhost

        # Who we are, as distinct from where we are.  See derive_node_id.
        self.node_id = derive_node_id(
            myhost, ro.nameServicePort if port is None else port,
            configured=node_id)

        # How long we don't hear from somebody before we drop them
        self.purge_delta = purge_delta

        # Hosts that turned out to be us reached by another name.  Kept so
        # that we stop dialling them, and so that learning the same alias
        # again from a peer does not put it back.  Declared before the seeds
        # because filtering those already asks whether a name is ours.
        self._not_peers = set()

        # Name servers we were told about at startup.  These are where we go
        # to be introduced, not who we exchange registrations with: that is
        # peers(), which grows as we learn of others and does not shrink back
        # to this list if a seed goes away.
        self.seed_hosts = [host for host in (peer_hosts or [])
                           if not self.is_self(host)]

        # Short, because a round asks every peer in turn and one that has
        # died should cost that round a moment rather than an interval.
        self.peer_timeout = peer_timeout
        self._peer_clients = {}

        # Name servers we have been told about by other name servers.  Kept
        # apart from the white pages because we have not heard from these
        # ourselves -- they are places to go and ask, not registrations.
        self._known_peers = set(self.seed_hosts)

        # self state mutex
        self.lock = threading.RLock()

        # the "white pages"
        self.wp = dict()



    def _register(self, name, host, port, registrar, options, hosttime,
                  replace=True, registrar_id=None):
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
                                registrar=registrar,
                                registrar_id=registrar_id, pingtime=hosttime,
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
                       replace=replace, registrar_id=self.node_id)
        return 0

    def ping(self, name, host, port, options, hosttime):
        """Get pinged by service.  Register this service if we have never
        heard of it, otherwise just record the time that we heard from it.
        """
        now = time.time()
        self.logger.info("Ping from '%s' (%s:%d) -- %.4f [%.4f]" % (
            name, host, port, hosttime, now-hosttime))

        self._register(name, host, port, self.myhost, options, hosttime,
                       registrar_id=self.node_id)

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
        """Record what we answer to, as any other service would.

        Nothing needs to read this to find us -- a client that could read it
        has found us already -- but a registration that described only half
        the service would be a lie to anything listing what is running.
        """
        hosttime = time.time()
        return self._register(self.channel, self.myhost, ro.nameServicePort,
                              self.myhost, self._self_options(), hosttime,
                              registrar_id=self.node_id)

    def _self_options(self):
        """How we describe ourselves, to our own white pages or a peer's.

        One description rather than two: a name server that told its peers
        something different from what it recorded locally would be reported
        differently depending on which of them a client happened to ask.
        """
        options = dict(secure=ro.default_secure,
                       protocol=ro.ns_transport,
                       encoding=ro.ns_encoding,
                       keep=True)
        if ro.ns_rpc_transport:
            options['alternates'] = [dict(protocol=ro.ns_rpc_transport,
                                          port=ro.nameServiceRpcPort,
                                          encoding=ro.ns_rpc_encoding)]
        return options

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
                    'transport', 'xmlrpc')
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
                                pingtime=pingtime, registrar=registrar,
                                # Which name server heard it first-hand, as
                                # distinct from what that name server is
                                # called.  Absent from a record written
                                # before name servers had ids.
                                registrar_id=val_d.get('registrar_id')))

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

    def is_self(self, host):
        """Whether _host_ names this name server rather than another one.

        The reliable answer costs a connection -- ask, and see whose id
        comes back -- and this is the part that can be known without one: a
        loopback name means the asker, so it names us here and someone else
        everywhere it might be repeated.
        """
        return (host == self.myhost
                or host.lower() in LOOPBACK_NAMES
                or host in self._not_peers)

    def is_ours(self, rec):
        """Whether we are the one who heard this registration first-hand.

        By id where the record carries one, and by host otherwise -- a
        registration written before name servers had ids, or by one that
        still does not.  The id is the better answer to a question the host
        name only approximately answers: two name servers on one host share
        a host name, and one name server has as many host names as DNS cares
        to give it.
        """
        their_id = rec.get('registrar_id')
        if their_id is not None:
            return their_id == self.node_id
        return rec.get('registrar') == self.myhost

    def getNodeId(self):
        """Who this name server is, independently of what it is called."""
        return self.node_id

    def getInfoMine(self):
        """Return the info for any services that we registered."""
        return self._getInfoPred(self.is_ours)

    def merge_registrations(self, env):
        """Take another name server's registrations into our white pages.

        Idempotent, because a peer states everything it holds rather than
        what has changed: a merge that arrives twice, late, or out of order
        says the same thing.  That is what lets a missed round repair
        itself on the next one instead of needing to be retried, and what
        made it safe to run this beside the broker it replaced while a
        cluster was converted.

        :return: how many registrations were taken.
        """
        registrar = env.get('registrar')
        if env.get('registrar_id') == self.node_id:
            return 0

        self.logger.info("notified of services on another node: %s"
                         % (registrar,))
        merged = 0
        for rec in env['names']:
            if self.is_ours(rec):
                # Ours, come back to us the long way round.  Whatever it
                # says, we heard it first-hand and they did not.
                continue
            try:
                options = {key: rec[key] for key in SHARED_FIELDS
                           if key in rec}
                self._register(rec['name'], rec['host'], rec['port'],
                               rec['registrar'], options, rec['pingtime'],
                               registrar_id=rec.get('registrar_id'))
                merged += 1
            except KeyError as e:
                # One malformed registration costs itself, not the rest of
                # the batch it arrived in.
                self.logger.warning(
                    "ignoring a registration from '%s' with no %s"
                    % (registrar, e))
        return merged

    # ------------------------------------------------- peer name servers --
    #
    # How name servers keep each other current: each asks every other for
    # its own registrations.
    #
    # It is a *pull*: we ask each peer for its own registrations rather than
    # pushing ours at it.  getInfoMine() is a read, and already exists, so
    # exchanging registrations this way adds no method that writes into a
    # name server -- which matters for the thing every other service trusts
    # to tell it where everything is.  It also fails locally: a peer that
    # has died costs us a logged timeout rather than a stalled publisher.
    #
    # Every node asks every other, rather than routing through one of them.
    # At the sizes this runs at -- a dozen name servers, ~120 services, a
    # registration of about 220 bytes packed -- a full exchange is some 28
    # KB/s across the whole cluster, so a hub would concentrate the same
    # traffic into one process and buy nothing but a thing to fail.

    def peers(self):
        """The other name servers we know of.

        Our own white pages are the register: every name server records
        itself under the name we all share, so the directory of services is
        also the directory of directories, kept current by the same exchange
        as everything else.  The seeds are included whether or not they have
        answered yet -- that is what introduces a cluster that starts all at
        once, where nobody has heard of anybody.
        """
        with self.lock:
            known = {rec['host']
                     for rec in self.wp.get(self.channel, {}).values()}
        known.update(self._known_peers)
        known = {host for host in known if not self.is_self(host)}
        return sorted(known)

    def getPeers(self):
        """Every name server this one knows of, itself included.

        A directory question, not a registration, and the distinction is the
        point.  This answer may be passed on second-hand while registrations
        may not, because the two are believed differently: a relayed service
        record is a claim you have to take on trust, and a relayed name
        server address is a hint that proves itself the moment you try it,
        or does not.

        Membership has to travel more than one hop or a cluster seeded from
        a single host never finishes introducing itself -- the seed knows
        everyone, but it heard about them first-hand and so has nothing to
        pass on under the one-hop rule.
        """
        with self.lock:
            hosts = {rec['host']
                     for rec in self.wp.get(self.channel, {}).values()}
        hosts.update(self._known_peers)
        hosts.add(self.myhost)
        hosts -= self._not_peers
        hosts -= LOOPBACK_NAMES
        return sorted(hosts)

    def learn_peers_from(self, host):
        """Ask one peer which name servers it knows of.

        :return: how many we had not heard of.
        """
        try:
            found = self._peer_client(host).getPeers()
        except Exception as e:
            self.forget_peer(host)
            self.logger.debug("could not ask '%s' who else it knows: %s"
                              % (host, e))
            return 0

        fresh = ({h for h in found if not self.is_self(h)}
                 - self._known_peers)
        if fresh:
            self._known_peers.update(fresh)
            self.logger.info("learned of name servers: %s"
                             % (', '.join(sorted(fresh)),))
        return len(fresh)

    def _peer_client(self, host):
        """A handle to another name server, made once and kept.

        Tried in the order a client tries them -- the faster way in first,
        the one every name server has always spoken last -- because during
        an upgrade a peer may be older than we are.
        """
        client = self._peer_clients.get(host)
        if client is not None:
            return client

        last = None
        for port, protocol, encoding in ro.ns_endpoints():
            try:
                client = ro.remoteObjectClient(
                    host=host, port=port, transport=protocol,
                    encoding=encoding, name=self.channel,
                    timeout=self.peer_timeout)
                # Asking who answered, rather than merely whether anything
                # did: the probe has to happen anyway, and this is where we
                # find out that a host we were told to talk to is us under
                # another name.
                try:
                    their_id = client.getNodeId()
                except Exception:
                    # Older than us, and so has no id to give.  Its records
                    # will say who wrote them by host name, which is what we
                    # had before and is good enough to go on with.
                    their_id = None
            except Exception as e:
                last = e
                continue

            if their_id is not None and their_id == self.node_id:
                # Remembered, so that this costs one connection rather than
                # one per round: a seed is dialled every round until it
                # answers, and this one will answer every time.
                self._not_peers.add(host)
                self._known_peers.discard(host)
                self.logger.info("'%s' is this same name server under "
                                 "another name; not peering with it"
                                 % (host,))
                raise nameServiceError(
                    "'%s' is this same name server under another name" % (host,))

            self._peer_clients[host] = client
            return client

        raise nameServiceError("no way in to the name server on '%s': %s"
                               % (host, last))

    def forget_peer(self, host):
        """Drop a peer's handle, so the next round dials it again.

        A peer that stopped answering may come back on a different protocol
        -- it may have been upgraded, which is the whole reason there is
        more than one way in.
        """
        self._peer_clients.pop(host, None)

    def pull_from_peer(self, host):
        """Merge one peer's own registrations.

        What comes back is only what that peer heard first-hand.  Keeping it
        to one hop is what lets `registrar` mean "this node was told by the
        service itself", and it makes loops impossible without anything
        having to detect them.

        :return: how many registrations were taken; 0 if it could not be
            reached, which is not an error -- a name server going away is an
            ordinary event, and its registrations will age out.
        """
        try:
            names = self._peer_client(host).getInfoMine()
        except Exception as e:
            self.forget_peer(host)
            self.logger.warning("could not reach the name server on '%s': %s"
                                % (host, e))
            return 0

        return self.merge_registrations(dict(registrar=host, names=names))

    def announce_peer(self, host, port, options):
        """Another name server introducing itself to us.

        Recorded against the announcer rather than against us, which is the
        difference between this and register().  We did hear it first-hand,
        but so does everyone else when they pull from it, and a record we
        claimed as ours would be one we then passed on as ours -- which is
        the single thing the exchange must never do, because it is what
        keeps a record one hop from its registrar.

        It says only where a name server is.  Everything that name server
        knows arrives the other way, by us asking it.
        """
        self._register(self.channel, host, port, host, options, time.time())
        return 0

    def announce_to_seeds(self):
        """Tell the hosts we were seeded with that we are here.

        The one thing a pull cannot do for itself: nobody can ask us for our
        registrations until they know we exist.  So we register with a seed
        exactly as any other service registers with a name server -- one
        record, naming only ourselves, through the method every service
        already calls.

        Only the seeds, not every peer: the rest learn of us when the seed
        shares what it heard, and asking every peer every round would be a
        write where a read will do.
        """
        options = self._self_options()
        reached = 0
        for host in self.seed_hosts:
            if self.is_self(host):
                # Either it always named us -- a loopback name -- or we
                # found out it did when we first dialled it.
                continue
            try:
                self._peer_client(host).announce_peer(
                    self.myhost, ro.nameServicePort, options)
                reached += 1
            except Exception as e:
                self.forget_peer(host)
                self.logger.debug("could not announce to '%s': %s"
                                  % (host, e))
        return reached

    def exchange_with_peers(self):
        """One round: be introduced, then ask everyone we know of.

        Asked in a different order each time so that a peer which is slow
        rather than dead does not always delay the same successors.
        """
        self.announce_to_seeds()
        hosts = self.peers()
        random.shuffle(hosts)
        merged = 0
        for host in hosts:
            merged += self.pull_from_peer(host)
            self.learn_peers_from(host)
        return merged

    def peer_loop(self, interval, ev_quit):
        """Exchange registrations with the other name servers, forever.

        The interval is jittered because a dozen name servers started
        together by the same boot manager would otherwise stay in phase and
        ask each other everything at the same instant, forever.
        """
        while not ev_quit.is_set():
            time_end = time.time() + interval * random.uniform(0.85, 1.15)

            try:
                self.exchange_with_peers()
            except Exception as e:
                self.logger.error("peer loop error: %s" % (e,), exc_info=True)

            ev_quit.wait(max(0, time_end - time.time()))


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

    peer_hosts = [host.strip()
                  for host in (getattr(options, 'peer', None) or '').split(',')
                  if host.strip()]

    nsobj = remoteObjectNameService(options.svcname, logger, myhost,
                                    purge_delta=options.purge_delta,
                                    peer_hosts=peer_hosts,
                                    peer_timeout=getattr(options,
                                                         'peer_timeout', 2.0),
                                    node_id=getattr(options, 'node_id', None),
                                    port=options.port)

    logger.info("This name server is %s (%s:%d)"
                % (nsobj.node_id, myhost, options.port))

    # Several of these workers never come back: the peer exchange loop where
    # one is running, the server's own command loop and one serve loop per
    # listener all run until ev_quit.  The floor need not cover them -- the
    # pool grows as they are submitted, and cannot shrink back through them
    # afterwards, since only an idle worker ever retires.  It is here to
    # leave a few workers ready to answer a lookup without growing the pool
    # first; the service settles at whatever it actually holds.
    t_pool = Task.ThreadPool(logger=logger, ev_quit=ev_quit,
                             numthreads=options.numthreads,
                             minthreads=8)

    # Create remote object server for this object.
    # svcname to None temporarily because we get into infinite loop
    # try to register ourselves.
    # Two ways in, on two agreed ports: XML-RPC on the one every client
    # knows, and the current default beside it for those that can speak it.
    # A client tries the second first and falls back, which is why they are
    # separate ports rather than alternates in a registration -- there is no
    # registration to read before you have found the name service.
    ways, ports = [ro.ns_transport], [options.port]
    if ro.ns_rpc_transport:
        ways.append(ro.ns_rpc_transport)
        ports.append(options.rpcport)

    nssvc = ro.remoteObjectServer(name=options.svcname, obj=nsobj,
                                  svcname=None,  #?!!
                                  transport=ways,
                                  encoding=ro.ns_encoding,
                                  port=ports, logger=logger,
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

            # Always, even with nothing to start from.  A name server given
            # no seeds is still found by the ones that were given its name,
            # and once one has announced itself the loop is what pulls its
            # registrations; without it the exchange would only ever work in
            # the direction it was configured.
            if nsobj.seed_hosts:
                logger.info("Starting peer exchange loop (seeded with %s)..."
                            % (', '.join(nsobj.seed_hosts),))
            else:
                # Ordinary on a single host, where there is no second name
                # server to exchange with.  Worth saying either way, since
                # the alternative is noticing when half a cluster cannot
                # find a service.
                logger.warning(
                    "No other name server to start from%s: this one will "
                    "exchange registrations only with whoever introduces "
                    "themselves to it"
                    % (" (--peer named only this host)" if peer_hosts else ""))
            task = Task.FuncTask2(nsobj.peer_loop,
                                  options.peer_interval, ev_quit)
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
