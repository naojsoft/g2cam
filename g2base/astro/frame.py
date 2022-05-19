import os
import re

from g2base import Bunch


frame_regex1 = re.compile('^(\w{3})([A-Za-z])(\d{8})$')
frame_regex2 = re.compile('^(\w{3})([A-Za-z])(\d{1})(\d{7})$')
frame_templ = "%3.3s%1.1s%1.1s%07d"
max_frame_count = 9999999


class FitsFrameIdError(Exception):
    pass


# OLD STYLE

def getFrameInfoFromPath(fitspath):
    # Extract frame id from file path
    (fitsdir, fitsname) = os.path.split(fitspath)
    ridx = fitsname.rindex('.fits')
    frameid, ext = fitsname[:ridx], fitsname[ridx + 1:]

    match = frame_regex1.match(frameid)
    if match:
        (inscode, frametype, frame_no) = match.groups()
        frame_no = int(frame_no)

        frameid = frameid.upper()
        inscode = inscode.upper()

        return Bunch.Bunch(frameid=frameid, fitsname=fitsname,
                           fitsdir=fitsdir, inscode=inscode,
                           frametype=frametype, frame_no=frame_no)

    raise FitsFrameIdError("path does not match Subaru FITS specification: '%s'" % (
            fitspath))

# NEW STYLE
# Use this class over the old module method if possible

class Frame(object):

    def __init__(self, path=None):
        self.filename = None
        self.extension = None
        self.directory = None
        self.inscode = None
        self.frametype = None
        self.prefix = None
        self.number = None

        if path != None:
            self.create_from_path(path)

    # this is like the number but includes the prefix
    @property
    def count(self):
        return int(self.frameid[4:])

    @property
    def frameid(self):
        return frame_templ % (self.inscode, self.frametype, self.prefix,
                              self.number)

    @property
    def path(self):
        return os.path.join(self.directory, self.filename)

    def from_frameid(self, frameid):

        match = frame_regex2.match(frameid)
        if not match:
            raise ValueError("Frame id (%s) does not match frame spec" % (
                frameid))

        (inscode, frametype, framepfx, frame_no) = match.groups()

        self.inscode = inscode.upper()
        self.frametype = frametype.upper()
        self.prefix = str(framepfx)
        self.number = int(frame_no)

    def from_parts(self, inscode, frametype, prefix, number):
        self.inscode = inscode.upper()
        self.frametype = frametype.upper()
        self.prefix = prefix
        self.number = int(number)

    def create_from_path(self, path):

        # Extract frame id from file path
        (fitsdir, filename) = os.path.split(path)
        if '.fits' in path:
            ridx = filename.rindex('.fits')
            frameid, ext = filename[:ridx], filename[ridx + 1:].strip()
        else:
            frameid, ext = filename, '.fits'

        self.filename = filename
        if len(ext) > 0:
            self.extension = ext
        self.directory = fitsdir

        self.from_frameid(frameid)

    limit = 9999999

    def add(self, count):
        res = self.number + count
        if res > self.limit:
            # bump prefix
            pfx_int = ord(self.prefix) - ord('0') + 1
            if pfx_int > 9:
                raise ValueError("Count exceeds digit space")
            self.prefix = chr(ord('0') + pfx_int)
            self.number = res - (self.limit + 1)

        else:
            self.number = res

    def get_primary_hdu(self, fits_f):
        if self.extension.endswith('.fz'):
            return fits_f[1]
        return fits_f[0]

    def __repr__(self):
        return str(self)

    def __str__(self):
        return self.frameid

#END
