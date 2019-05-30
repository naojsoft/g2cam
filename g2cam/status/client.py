
from pymongo import MongoClient

status_not_found = '##NODATA##'

class StatusClient(object):
    """
    Status client for Subaru Gen2 Observation Control System

    USAGE:

    >>> from g2cam.status.client import StatusClient

    # setup (use the Gen2 host, user name and password you are advised by
    # observatory personnel)
    >>> st = StatusClient(host="host", username="user", password="pass")
    >>> st.connect()

    # fetch a dictionary
    >>> d = {'FITS.SBR.RA': None, 'FITS.SBR.DEC': None}
    >>> st.fetch(d)
    >>> d
    {'FITS.SBR.RA': '04:08:07.209', 'FITS.SBR.DEC': '+19:43:31.23'}

    # fetch a list of aliases
    >>> st.fetch_list(['FITS.SBR.RA', 'FITS.SBR.DEC'])
    ['04:08:07.209', '+19:43:31.23']

    IMPORTANT:

    [1] If you are requesting many status items, do it in one large call,
    instead of several calls.  That way the status items will be consistent
    with each other in time.

    [2] If you need a continuous status feed (more frequent than once every
    10 sec on average), please consider using the StatusSubscriber()
    class--it will push a continuous stream of status items to you and is
    more efficient for that purpose.

    [3] Do not rely on the internals of this class API!!!  The database
    name, location, type and details are subject to change over time!

    """
    def __init__(self, host='localhost', port=29013,
                 username=None, password=None,
                 auth_mech='SCRAM-SHA-256', auth_src='gen2'):
        self.db_host = host
        self.db_port = port
        self.db_user = username
        self.db_pswd = password
        self.db_auth = auth_mech
        self.db_auth_src = auth_src

        # table of conversion functions
        self._cvt = dict(int=int)

        # for Mongo status db
        self.g2_db = 'gen2'
        self.g2_status = 'status'
        self.mdb_doc = {'owner': 'gen2'}
        self.mdb_client = None
        self.mdb_db = None
        self.mdb_coll = None

    def connect(self):
        kwargs = {}
        if self.db_user is not None:
            kwargs = dict(username=self.db_user, password=self.db_pswd,
                          authSource=self.db_auth_src,
                          authMechanism=self.db_auth)
        self.mdb_client = MongoClient(self.db_host, self.db_port,
                                      **kwargs)
        self.mdb_db = self.mdb_client[self.g2_db]
        self.mdb_coll = self.mdb_db[self.g2_status]

    def reconnect(self):
        return self.connect()

    def fetch(self, status_dict):
        """Fetch a dictionary of status aliases from Gen2

        Parameters
        ----------
        status_dict : dict
            A dictionary where the keys are Gen2 status aliases

        The dictionary is filled in with values for each key.
        """
        try:
            # fetch status in store
            proj = {key.replace('.', '__'): True
                    for key in status_dict}
            doc = self.mdb_coll.find(self.mdb_doc, proj).next()

            # update results for aliases including those not found
            res = {key: doc.get(key.replace('.', '__'), status_not_found)
                   for key in status_dict}

            # convert types that could not be stored as Python values
            # back into Python ones
            res = {key: (val if not isinstance(val, dict)
                   else self._cvt[val['ptype']](val['value']))
                   for key, val in res.items()}

            status_dict.update(res)

        except Exception as e:
            raise ValueError("Error fetching status in db: %s" % (
                str(e)))

    def fetch_list(self, aliases):
        """Fetch a list of status aliases from Gen2

        Parameters
        ----------
        aliases : list
            A list of Gen2 status aliases

        A list of values corresponding to each alias is returned.
        """
        d = {}.fromkeys(aliases)
        self.fetch(d)
        return [d[key] for key in aliases]

    def close(self):
        return self.mdb_client.close()
        self.mdb_client = None
        self.mdb_db = None
        self.mdb_coll = None

# END
