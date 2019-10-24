#! /usr/bin/env python
#
# ro_name_svc.py -- remote object name service.
#
# Eric Jeschke (eric@naoj.org)
#
"""
This is the name service for the remote objects middleware wrapper.

Each service created by remoteObjectService registers itself here so that
it can be looked up by name instead of (host, port).

"""
import sys, time, os
import threading
from datetime import datetime, timedelta
import configparser

from g2base.remoteObjects import remoteObjects as ro
from g2base import ssdlog, Task

pymongo = None

# Our version
version = '20191017.0'


# For generating errors from this module
#
class nameServiceError(Exception):
    pass


class remoteObjectNameService(object):

    def __init__(self, db, ev_quit, myhost, purge_delta=30.0):

        self.db = db
        self.logger = db.logger
        self.myhost = myhost
        self.ev_quit = ev_quit

        # How long we don't hear from somebody before we drop them
        self.purge_delta = purge_delta

    def reconnect(self):
        try:
            self.db.reconnect()

        except Exception as e:
            errmsg = "Error reconnecting to db: {}".format(str(e))
            self.logger.error(errmsg, exc_info=True)
            raise nameServiceError(errmsg)

    def _register(self, name, host, port, options, hosttime, replace=True):

        # TEMP: until we can deprecate former API--some old clients pass
        # "True" for this parameter instead of a dict
        if isinstance(options, bool):
            self.logger.warning("Deprecated API used!")
            secure = options
            transport = ro.default_transport
            encoding = ro.default_encoding
            keep = False

        elif isinstance(options, dict):
            secure = options.get('secure', ro.default_secure)
            transport = options.get('transport', ro.default_transport)
            encoding = options.get('encoding', ro.default_encoding)
            keep = options.get('keep', False)

        else:
            raise nameServiceError("options argument ({}) should be a dict".format(
                str(options)))

        # prepare record for database
        rec = dict(name=name, host=host, port=port, pingtime=hosttime,
                   secure=secure, transport=transport, encoding=encoding,
                   keep=keep)

        if self.db.auto_expire:
            # this field only needed for MongoDB
            tstamp = datetime.utcnow()
            if keep:
                # for records that should never time out, assign a null time stamp
                tstamp = None
            else:
                tstamp = tstamp + timedelta(seconds=self.purge_delta)
            rec['expires'] = tstamp

        try:
            self.db.update(name, rec)

        except Exception as e:
            self.logger.error("Error storing name record in db: %s" % (
                str(e)))
        self.logger.debug("updated record for '{}' ({})".format(
            name, rec))

    def register(self, name, host, port, options, replace=True):
        """Register a new name at a certain host and port."""

        hosttime = time.time()
        return self._register(name, host, port, options, hosttime,
                              replace=replace)

    def ping(self, name, host, port, options, hosttime):
        """Get pinged by service.  Register this service if we have never
        heard of it, otherwise just record the time that we heard from it.
        """
        now = time.time()
        self.logger.info("ping from '%s' (%s:%d) -- %.4f [%.4f]" % (
            name, host, port, hosttime, now-hosttime))

        return self._register(name, host, port, options, hosttime)


    def unregister(self, name, host, port):
        """Unregister a new name at a certain host and port."""
        # update database
        try:
            self.db.delete_one(name, host, port)
            self.logger.info("unregistered '{}' at {}:{}".format(name, host, port))
            return 1

        except Exception as e:
            self.logger.error("Failed to unregister '{}': {}".format(
                name, str(e)))
            return 0


    def clearName(self, name):
        """Clear all registrations associated with _name_."""

        # update database
        try:
            self.db.delete_name(name)
            self.logger.info("unregistered '{}'".format(name))
            return 1

        except Exception as e:
            self.logger.error("Failed to unregister '{}': {}".format(
                name, str(e)))
            return 0


    def clearAll(self):
        """Clear all name registrations."""

        # update database
        try:
            self.db.delete_all()
            return 1

        except Exception as e:
            self.logger.error("Failed to unregister all names: {}".format(
                name, str(e)))
            return 0


    def getInfo(self, name):
        """Return a list of dicts of service info associated with
        a registered name.  This should be used in preference to
        getHosts for most applications."""

        try:
            res = self.db.lookup(name)
            return res

        except Exception as e:
            errmsg = "Error looking up name '{}' from db: {}".format(name, str(e))
            self.logger.error(errmsg, exc_info=True)
            raise nameServiceError(errmsg)


    def getNames(self):
        """Return a list of all registered names."""

        try:
            cur = self.db.get_all()

        except Exception as e:
            errmsg = "Error looking up names from db: {}".format(str(e))
            self.logger.error(errmsg, exc_info=True)
            raise nameServiceError(errmsg)

        res = [dct['name'] for dct in cur]
        return list(set(res))


    def getNamesSorted(self):
        """Returns a sorted list of all registered names."""

        res = self.getNames()
        res.sort()

        return res


    def getHosts(self, name):
        """Return a list of all (host, port) pairs associated with
        a registered name."""

        cur = self.getInfo(name)
        return [(dct['host'], dct['port']) for dct in cur]


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

    def _syncInfoPred(self, pred_fn):
        """Ping any services that match predicate function _pred_fn_."""
        res = self._getInfoPred(pred_fn)
        for d in res:
            self.ping(d['name'], d['host'], d['port'],
                      { 'secure': d['secure'],
                        'transport': d['transport'],
                        'encoding': d['encoding'],
                        },
                      d['pingtime'])

    def syncInfoSelf(self):
        """Ping any services that registered with us."""
        self._syncInfoPred(lambda d: d['host'] == self.myhost)


    def purgeDead(self, name):
        """Purge all registered instances of _name_ that haven't been heard
        from in purge_delta seconds."""

        info_list = self.getInfo(name)

        for info in info_list:
            if 'pingtime' not in info:
                # No pingtime?  Give `em the boot!
                self.unregister(name, info['host'], info['port'])

            else:
                # Haven't heard from them in a while?  Ditto!
                delta = time.time() - info['pingtime']
                if (delta > self.purge_delta) and (not info['keep']):
                    self.logger.info("haven't heard from '{}' in {} seconds--purging them".format(name, self.purge_delta))
                    self.unregister(name, info['host'], info['port'])


    def purgeAll(self):
        """Iterate over all known registrations and perform a purge
        operation from those we haven't heard from lately."""

        name_list = self.getNames()

        for name in name_list:
            self.purgeDead(name)


    def purgeLoop(self, interval):
        """Loop to periodically purge stale names data."""

        while not self.ev_quit.is_set():
            time_end = time.time() + interval

            try:
                if not self.db.auto_expire:
                    # For MongoDB we are using the TTL feature of records
                    # to have them auto expire, so the purge loop is not
                    # needed
                    self.purgeAll()

            except Exception as e:
                self.logger.error("Purge loop error: {}".format(str(e)))

            sleep_time = max(0, time_end - time.time())
            time.sleep(sleep_time)


#------------------------------------------------------------------
# Name service database classes
#------------------------------------------------------------------

class SimpleNameSvcDB(object):
    """Simple, in-memory DB where records are stored as dicts in a two-level
    hierarchy.  Top-level is keyed by name, and second level is keyed by
    (host, port).
    """

    def __init__(self, logger):
        self.logger = logger
        self.auto_expire = False

        self.lock = threading.RLock()
        self.names = {}

    def connect(self):
        pass

    def reconnect(self):
        pass

    def update(self, name, rec):
        subkey = (rec['host'], rec['port'])
        with self.lock:
            dct = self.names.setdefault(name, {})
            dct[subkey] = rec

    def delete_one(self, name, host, port):
        subkey = (host, port)
        with self.lock:
            dct = self.names.setdefault(name, {})
            try:
                del dct[subkey]
            except KeyError:
                pass

    def delete_name(self, name):
        with self.lock:
            try:
                del self.names[name]
            except KeyError:
                pass

    def delete_all(self):
        with self.lock:
            self.names = {}

    def lookup(self, name):
        with self.lock:
            if not name in self.names:
                return []
            return list(self.names[name].values())

    def get_all(self):
        with self.lock:
            return [dct[subkey] for dct in self.names.values()
                    for subkey in dct]


class MongoNameSvcDB(object):
    """Records are stored as simple documents in a Mongo DB.
    Info for accessing the Mongo instance is read from a JSON config file
    in the CONFHOME area.
    """

    def __init__(self, logger):
        self.logger = logger
        self.auto_expire = True

        # Read configuration file for accessing database
        if not 'CONFHOME' in os.environ:
            raise nameServiceError("CONFHOME env variable not set!")

        config = configparser.ConfigParser()
        try:
            conf_file = os.path.join(os.environ['CONFHOME'], 'ro',
                                     'ro.cfg')
            config.read(conf_file)
            cfg = config['name_service']

        except Exception as e:
            errmsg = "Failed to read config file '{}': {}".format(conf_file, e)
            self.logger.error(errmsg, exc_info=True)
            raise nameServiceError(errmsg)

        self.db_host = cfg['db_host']
        self.db_port = int(cfg['db_port'])
        self.db_user = cfg.get('db_user', None)
        self.db_pswd = cfg.get('db_pswd', None)
        self.db_auth = cfg.get('db_auth', None)
        self.db_auth_src = cfg.get('auth_src', None)

        # for Mongo db
        self.g2_db = 'gen2'
        self.coll_name = 'names'
        self.mdb_client = None
        self.mdb_db = None
        self.mdb_coll = None

    def connect(self):
        kwargs = {}
        if self.db_user is not None:
            kwargs = dict(username=self.db_user, password=self.db_pswd,
                          authSource=self.db_auth_src,
                          authMechanism=self.db_auth)
        self.mdb_client = pymongo.MongoClient(self.db_host, self.db_port,
                                      **kwargs)
        self.mdb_db = self.mdb_client[self.g2_db]
        self.mdb_coll = self.mdb_db[self.coll_name]
        #print(self.mdb_coll.index_information())
        # this will silently succeed on subsequent attempts
        self.mdb_coll.create_index('expires', expireAfterSeconds=0)

    def reconnect(self):
        return self.connect()

    def update(self, name, rec):
        host, port = rec['host'], rec['port']
        self.mdb_coll.update_one(dict(name=name, host=host, port=port),
                                 {'$set': rec}, upsert=True)

    def delete_one(self, name, host, port):
        self.mdb_coll.delete_one(dict(name=name, host=host, port=port))

    def delete_name(self, name):
        self.mdb_coll.delete_many(dict(name=name))

    def delete_all(self):
        self.mdb_coll.delete_many({})

    def lookup(self, name):
        cur = self.mdb_coll.find(dict(name=name), dict(_id=0))
        return [dict(dct) for dct in cur]

    def get_all(self):
        cur = self.mdb_coll.find({}, dict(_id=0))
        return [dict(dct) for dct in cur]


#------------------------------------------------------------------
# MAIN PROGRAM
#------------------------------------------------------------------
#
def main(options, args):
    global pymongo

    # Create top level logger.
    logger = ssdlog.make_logger('names', options)

    try:
        myhost = ro.get_myhost()

    except Exception as e:
        raise nameServiceError("Can't get my own hostname: %s" % str(e))

    # Get authorization params, if provided for our name service
    authDict = {}
    ## if options.auth is not None:
    ##     auth = options.auth.split(':')
    ##     authDict[auth[0]] = auth[1]

    ev_quit = threading.Event()

    # database object holding our name info
    if options.dbtype == 'mongo':
        import pymongo
        logger.info("using MongoDB as names database")
        db = MongoNameSvcDB(logger)
    else:
        logger.info("using in-memory names database")
        db = SimpleNameSvcDB(logger)

    try:
        # try to connect to names database
        db.connect()

    except Exception as e:
        logger.error("failed to make connection to names db: {}".format(e),
                     exc_info=True)
        sys.exit(1)

    nsobj = remoteObjectNameService(db, ev_quit, myhost,
                                    purge_delta=options.purge_interval)
    thread_pool = Task.ThreadPool(logger=logger, ev_quit=ev_quit,
                                  numthreads=options.numthreads)
    thread_pool.startall(wait=True)

    method_list = ['reconnect', 'register', 'ping', 'unregister', 'clearName',
                   'clearAll', 'getNames', 'getNamesSorted', 'getHosts',
                   'getInfo', 'getInfoHost', 'getNamesHost',
                   ]

    # Create remote object server for this object.
    # svcname to None temporarily because we get into infinite loop
    # try to register ourselves.
    nssvc = ro.remoteObjectServer(name='names', obj=nsobj, svcname='names',
                                  ev_quit=ev_quit,
                                  transport=ro.ns_transport,
                                  encoding=ro.ns_encoding,
                                  port=options.port, logger=logger,
                                  usethread=True, threadPool=thread_pool,
                                  authDict=authDict,
                                  method_list=method_list)

    server_started = False
    try:
        try:
            # Register ourself
            logger.info("Registering self..")
            nsopts = dict(secure=False,
                          transport=ro.ns_transport,
                          encoding=ro.ns_encoding,
                          keep=True)
            nsobj.register('names', myhost, options.port, nsopts)

            logger.info("Starting name service..")
            nssvc.ro_start(wait=True)

        except Exception as e:
            logger.error(str(e))
            raise e

        logger.info("Entering main loop..")
        try:
            nsobj.purgeLoop(options.purge_interval)

        except KeyboardInterrupt:
            logger.error("Received keyboard interrupt!")

        except Exception as e:
            logger.error("Fatal error in purge loop: {}".format(str(e)),
                         exc_info=True)

    finally:
        ev_quit.set()
        logger.info("Stopping remote objects name service...")

    logger.info("Exiting remote objects name service...")
    nssvc.ro_stop()

    sys.exit(0)
