from collections.abc import Mapping
import ast

import yaml

from g2base import Bunch

# class used for nodes
Klass = Bunch.threadSafeBunch


class NestedBunch:
    """Basically a dictionaries of dictionaries with a few convenience methods
    for getting data in and out.
    """

    def __init__(self, fillDict=None, dbpath=None, pathsep='.'):
        self.dbpath = dbpath
        self.pathsep = pathsep

        self.sb = Klass()
        if fillDict:
            self._lload(None, fillDict)

        super().__init__()


    def _rsplit(self, path):
        res = path.rsplit(self.pathsep, 1)
        if len(res) == 1:
            # No leading path
            return (None, path)
        return res


    def _lload(self, path, valDict):
        for key, value in valDict.items():
            if path:
                newpath = path + self.pathsep + key
            else:
                newpath = key

            if not isinstance(value, dict):
                self.update(newpath, value)
            else:
                self.update(newpath, {})
                self._lload(newpath, value)


    def _to_plain(self, sb):
        """The tree as plain nested dicts, which is what gets written.

        The nodes are Bunches; yaml has no idea what one is, and would
        not be readable if it did.
        """
        out = {}
        for key in sb.keys():
            value = sb[key]
            out[key] = self._to_plain(value) if isinstance(value, Klass) \
                else value
        return out


    def writeout(self, filepath):
        with open(filepath, 'w') as out_f:
            yaml.safe_dump(self._to_plain(self.sb), out_f,
                           default_flow_style=False)


    def readin(self, filepath):
        with open(filepath, 'r') as in_f:
            text = in_f.read()

        # A file written before this was yaml is a python repr on one
        # line -- pprint calls repr() on a Bunch rather than descending
        # into it -- and it has to be read as one.  yaml would take it
        # without complaining and get it wrong: None comes back as the
        # string 'None', and a tuple (1, 2) becomes the value '(1' plus
        # a key '2)'.  Block-style yaml never begins with a brace, so
        # the first character says which this is.
        stripped = text.lstrip()
        if stripped.startswith('{'):
            d = ast.literal_eval(stripped)
        else:
            d = yaml.safe_load(text) or {}

        # update
        self._lload(None, d)


    def save(self):
        if self.dbpath:
            self.writeout(self.dbpath)


    def restore(self):
        if self.dbpath:
            self.readin(self.dbpath)


    def get_node(self, path, create=False):
        """Get the leaf dict for dotted path _path_.
        """

        sb = self.sb
        for key in path.split(self.pathsep):

            if key not in sb:
                if not create:
                    raise KeyError("No such key: '%s'" % path)

                sub_sb = Klass()
                sb[key] = sub_sb
                sb = sub_sb

            else:
                sb = sb[key]
                if not isinstance(sb, Klass):
                    raise KeyError("No such key: '%s'" % path)

        return sb


    def getitem(self, path):
        try:
            (pfx, key) = self._rsplit(path)
            if pfx is None:
                sb = self.sb
            else:
                sb = self.get_node(pfx, create=False)

            return sb[key]

        except KeyError:
            # Reraise KeyError with correct path as user sees it
            raise KeyError(path)


    def getitems(self, path):
        """Get the status values associated with this path in the table.
        If successful, then return a dictionary of the items found, otherwise
        return an error code.
        """

        resDict = {}
        try:
            if self.isLeaf(path):
                resDict[path] = self.getitem(path)
            else:
                for key in self.keys(path=path):
                    resDict[key] = self.getitem(key)

            return resDict

        except KeyError:
            raise KeyError(path)


    def getitems_suffixOnly(self, path):
        """Get the status values associated with this path in the table.
        If successful, then return a dictionary of the items found, otherwise
        return an error code.
        """

        resDict = {}
        pathlen = len(path) + 1
        try:
            if self.isLeaf(path):
                resDict[path] = self.getitem(path)
            else:
                for key in self.keys(path=path):
                    resDict[key[pathlen:]] = self.getitem(key)

            return resDict

        except KeyError:
            raise KeyError(path)


    def getTree(self, path):
        """Get the status values associated with this path in the table.
        If successful, then return a dictionary of the items found, otherwise
        return an error code.
        """

        try:
            if self.isLeaf(path):
                return self.getitem(path)

            resDict = {}
            for key in self.topKeys(path=path):
                path = key.split(self.pathsep)
                resDict[path[-1]] = self.getTree(key)

            return resDict

        except KeyError:
            raise KeyError(path)


    def isLeaf(self, path):
        """Returns True if _path_ is a leaf element in the nestedDict.
        May throw a KeyError
        """

        val = self.getitem(path)
        return not isinstance(val, Klass)


    def update(self, path, value):
        """Update the entry(s) in the proper dict for that path.
        """

        (pfx, key) = self._rsplit(path)
        if pfx is None:
            sb = self.sb
        else:
            sb = self.get_node(pfx, create=True)

        _set_sb(sb, key, value)


    def setitem(self, path, value):
        return self.update(path, value)


    def setvals(self, path, **values):
        """Similar method to update(), but uses keyword arguments as the
        set of values to store.
        """
        return self.update(path, values)


    def delitem(self, path):
        """Delete the elements associated with this path at the leaf.
        """

        sub_sb = self.sb

        for key in path.split(self.pathsep):
            sb = sub_sb
            if key not in sb:
                raise KeyError("No such key: '%s'" % path)

            else:
                sub_sb = sb[key]
                if not isinstance(sb, Klass):
                    raise KeyError("No such key: '%s'" % path)

        sb.delitem(key)


    def __getitem__(self, path):
        """Called for dictionary style access of this object.
        """
        return self.getitem(path)


    def __setitem__(self, path, value):
        """Called for dictionary style access with assignment.
        """
        return self.setitem(path, value)


    def __delitem__(self, path):
        """Deletes key/value pairs from object.
        """
        return self.delitem(path)


    def __contains__(self, path):
        return self.has_key(path)


    def __str__(self):
        """Implements the str() operation on a nestedDict.
        """
        return str(self.sb)


    def __repr__(self):
        """Implements the str() operation on a nestedDict.
        """
        return repr(self.sb)


    def topKeys(self, path=None):
        """Returns the top-level keys of _path_ in the nestedDict
        (or the bundle itself if no path==None).
        """
        if not path:
            sb = self.sb
        else:
            sb = self.getitem(path)

        keys = []
        for key in sb.keys():
            if path:
                pkey = self.pathsep.join((path, key))
            else:
                pkey = key
            keys.append(pkey)

        return keys


    def bottomKeys(self, path=None):
        """Returns the bottom-level keys of _path_ in the nestedDict
        (or the bundle itself if no path==None).
        """
        if path is None:
            idx = 0
        else:
            idx = len(path) + 1

        res = []
        for key in self.keys(path=path):
            res.append(key[idx:])

        return res


    def midKeys(self, path=None):
        """Returns the middle-level keys of _path_ in the nestedDict
        (or the bundle itself if no path==None).
        """
        if path is None:
            idx = 0
        else:
            idx = len(path) + 1

        res = []
        for key in self.topKeys(path=path):
            res.append(key[idx:])

        return res


    def keys(self, path=None):
        """Returns the keys of _path_ in the nestedDict
        (or the bundle itself if no path==None).
        """
        if not path:
            sb = self.sb
        else:
            sb = self.getitem(path)

        keys = []
        for key in sb.keys():
            if path:
                pkey = self.pathsep.join((path, key))
            else:
                pkey = key
            if isinstance(sb[key], Klass):
                keys.extend(self.keys(pkey))
            else:
                keys.append(pkey)

        return keys


    def has_key(self, path):
        """Returns True if the nestedDict contains an element with
        the given _path_.
        """
        try:
            self.getitem(path)
            return True

        except KeyError:
            return False


    def has_val(self, path, key):
        pkey = self.pathsep.join((path, key))
        return pkey in self



def _set_sb(sb, key, value):
    if isinstance(value, Mapping):
        if key not in sb:
            sub_sb = Klass()
            sb[key] = sub_sb
        else:
            sub_sb = sb[key]
            if not isinstance(sub_sb, Klass):
                raise ValueError("Expected a node, but found a <%s> [%s]" % (
                    type(sub_sb), str(sub_sb)))

        for key, val in value.items():
            _set_sb(sub_sb, key, val)

    else:
        sb[key] = value
