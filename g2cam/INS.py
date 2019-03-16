#! /usr/bin/env python
#
# Instrument configuration file.
#
# Eric Jeschke (eric@naoj.org)
#
from __future__ import print_function
import sys, os
import re

from g2base.astro.frame import Frame as AstroFrame

# NOTE: fov is specified as a RADIUS in DEGREES

insinfo = {
    'IRCS': {
        'name': 'IRCS',
        'number': 1,
        'code': 'IRC',
        'active': True,
        'interface': ('daqtk', 1.0),
        'fov': 0.011785,
        'frametypes': 'AQ',
        'description': u'Infra Red Camera and Spectrograph',
        },
    'AO': {
        'name': 'AO',
        'number': 2,
        'code': 'AOS',
        'active': False,
        'interface': ('daqtk', 1.0),
        'fov': 0.007000,
        'frametypes': 'AQ',
        'description': u'Subaru 36-elements Adaptive Optics',
        },
    'CIAO': {
        'name': 'CIAO',
        'number': 3,
        'code': 'CIA',
        'active': False,
        'interface': ('daqtk', 1.0),
        'fov': 0.007000,
        'frametypes': 'AQ',
        'description': u'Coronagraphic Imager with Adaptive Optics',
        },
    'OHS': {
        'name': 'OHS',
        'number': 4,
        'code': 'OHS',
        'active': False,
        'interface': ('daqtk', 1.0),
        'fov': 0.016667,
        'frametypes': 'AQ',
        'description': u'Cooled Infrared Spectrograph and Camera for OHS',
        },
    'FOCAS': {
        'name': 'FOCAS',
        'number': 5,
        'code': 'FCS',
        'active': True,
        'interface': ('daqtk', 1.0),
        'fov': 0.05,
        'frametypes': 'AQ',
        'description': u'Faint Object Camera And Spectrograph',
        },
    'HDS': {
        'name': 'HDS',
        'number': 6,
        'code': 'HDS',
        'active': True,
        'interface': ('daqtk', 1.0),
        'fov': 0.002778,
        'frametypes': 'AQ',
        'description': u'High Dispersion Spectrograph',
        },
    'COMICS': {
        'name': 'COMICS',
        'number': 7,
        'code': 'COM',
        'active': True,
        'interface': ('daqtk', 1.0),
        'fov': 0.007,
        'frametypes': 'AQ',
        'description': u'Cooled Mid-Infrared Camera and Spectrograph',
        },
    'SPCAM': {
        'name': 'SPCAM',
        'number': 8,
        'code': 'SUP',
        'active': True,
        'interface': ('daqtk', 1.0),
        'fov': 0.22433,
        'frametypes': 'AQ',
        'frames_per_exp': dict(A=10),
        'description': u'Subaru Prime Focus Camera',
        },
    'SUKA': {
        'name': 'SUKA',
        'number': 9,
        'code': 'SUK',
        'active': True,
        'interface': ('g2cam', 1.0),
        'fov': 0.004222,
        'frametypes': 'AQ',
        'description': u'Simulation Instrument',
        },
    'MIRTOS': {
        'name': 'MIRTOS',
        'number': 10,
        'code': 'MIR',
        'active': False,  #???
        'interface': ('daqtk', 1.0),
        'fov': 0.004222,
        'frametypes': 'AQ',
        'description': u'',
        },
    'VTOS': {
        'name': 'VTOS',
        'number': 11,
        'code': 'VTO',
        'active': False,  #???
        'interface': ('daqtk', 1.0),
        'fov': 0.004222,
        'frametypes': 'AQ',
        'description': u'',
        },
    'CAC': {
        'name': 'CAC',
        'number': 12,
        'code': 'CAC',
        'active': False,
        'interface': ('daqtk', 1.0),
        'fov': 0.011785,
        'frametypes': 'AQ',
        'description': u'Commissioning instrument',
        },
    'SKYMON': {
        'name': 'SKYMON',
        'number': 13,
        'code': 'SKY',
        'active': False,
        'interface': ('daqtk', 1.0),
        'fov': 0.023570,
        'frametypes': 'AQ',
        'description': u'Original Sky Monitor',
        },
    'PI1': {
        'name': 'PI1',
        'number': 14,
        'code': 'PI1',
        'active': False,  #???
        'interface': ('daqtk', 1.0),
        'fov': 0.023570,
        'frametypes': 'AQ',
        'description': u'',
        },
    'K3D': {
        'name': 'K3D',
        'number': 15,
        'code': 'K3D',
        'active': True,
        'interface': ('daqtk', 1.0),
        'fov': 0.023570,
        'frametypes': 'AQ',
        'description': u'',
        },
    'SCEXAO': {
        'name': 'SCEXAO',
        'number': 16,
        'code': 'SCX',
        'active': True,
        'interface': ('g2cam', 1.0),
        'fov': 0.045,
        'frametypes': 'AQ',
        'description': u'Subaru Coronagraphic Extreme Adaptive Optics',
        },
    'MOIRCS': {
        'name': 'MOIRCS',
        'number': 17,
        'code': 'MCS',
        'active': True,
        'interface': ('g2cam', 1.0),
        'fov': 0.033333,
        'frametypes': 'AQ',
        'description': u'Multi-Object Infra-Red Camera and Spectrograph',
        },
    'FMOS': {
        'name': 'FMOS',
        'number': 18,
        'code': 'FMS',
        'active': True,
        'interface': ('g2cam', 1.0),
        'fov': 0.22433,
        'frametypes': 'AQ',
        'description': u'Fiber Multi-Object Spectrograph',
        },
    'FLDMON': {
        'name': 'FLDMON',
        'number': 19,
        'code': 'FLD',
        'active': True,
        'interface': ('g2cam', 1.0),
        'fov': 0.023570,
        'frametypes': 'AQ',
        'description': u'High Intensity Field Imager',
        },
    'AO188': {
        'name': 'AO188',
        'number': 20,
        'code': 'AON',
        'active': True,
        'interface': ('g2cam', 1.0),
        'fov': 0.045,
        'frametypes': 'AQ',
        'description': u'Adaptive Optics 188',
        },
    'HICIAO': {
        'name': 'HICIAO',
        'number': 21,
        'code': 'HIC',
        'active': True,
        'interface': ('daqtk', 1.0),
        'fov': 0.023570,
        'frametypes': 'AQ',
        'description': u'High Contrast Instrument for the Subaru Next Generation Adaptive Optics',
        },
    'WAVEPLAT': {
        'name': 'WAVEPLAT',
        'number': 22,
        'code': 'WAV',
        'active': True,
        'interface': ('daqtk', 1.0),
        'fov': 0.023570,
        'frametypes': 'AQ',
        'description': u'Nasmyth Waveplate Unit for IR',
        },
    'LGS': {
        'name': 'LGS',
        'number': 23,
        'code': 'LGS',
        'active': True,
        'interface': ('g2cam', 1.0),
        'fov': 0.023570,
        'frametypes': 'AQ',
        'description': u'Laser Guide Star Control',
        },
    'HSC': {
        'name': 'HSC',
        'number': 24,
        'code': 'HSC',
        'active': True,
        'fov': 0.83,
        'interface': ('g2cam', 1.0),
        'frametypes': 'AQ',
        'frames_per_exp': dict(A=200),
        'description': u'Hyper-Suprime Cam',
        },
    'K3C': {
        'name': 'K3C',
        'number': 25,
        'code': 'K3C',
        'active': True,
        'interface': ('g2cam', 1.0),
        'fov': 0.023570,
        'frametypes': 'AQ',
        'description': u'Kyoto 3D 2nd Generation Sensor',
        },
    'CHARIS': {
        'name': 'CHARIS',
        'number': 26,
        'code': 'CRS',
        'active': True,
        'interface': ('g2cam', 1.0),
        'fov': 0.023570,
        'frametypes': 'AQ',
        'description': u'Coronagraphic High Angular Resolution Imaging Spectrograph',
        },
    'PFS': {
        'name': 'PFS',
        'number': 27,
        'code': 'PFS',
        'active': True,
        'interface': ('g2cam', 1.0),
        'fov': 0.023570,
        'frametypes': 'ABCD',
        'frames_per_exp': dict(A=100, B=100, C=100, D=100),
        'description': u'Prime Focus Spectrograph',
        },
    'IRD': {
        'name': 'IRD',
        'number': 28,
        'code': 'IRD',
        'active': True,
        'interface': ('g2cam', 1.0),
        'fov': 0.023570,
        'frametypes': 'AQ',
        'description': u'',
        },
    'SWIMS': {
        'name': 'SWIMS',
        'number': 29,
        'code': 'SWS',
        'active': True,
        'interface': ('g2cam', 1.0),
        'fov': 0.023570,
        'frametypes': 'BCRS',
        'frames_per_exp': dict(B=10, C=10, R=10, S=10),
        'description': u'',
        },
    'MIMIZUKU': {
        'name': 'MIMIZUKU',
        'number': 30,
        'code': 'MMZ',
        'active': True,
        'interface': ('g2cam', 1.0),
        'fov': 0.023570,
        'frametypes': 'AQ',
        'description': u'',
        },
    'VAMPIRES': {
        'name': 'VAMPIRES',
        'number': 31,
        'code': 'VMP',
        'active': True,
        'interface': ('g2cam', 1.0),
        'fov': 0.023570,
        'frametypes': 'AQ',
        'description': u'VAMPIRES (interferometry with differential polarimetry)',
        },
    'MEC': {
        'name': 'MEC',
        'number': 32,
        'code': 'MEC',
        'active': False,
        'interface': ('g2cam', 1.0),
        'fov': 0.023570,
        'frametypes': 'AQ',
        'description': u'',
        },
    'VGW': {
        'name': 'VGW',
        'number': 33,
        'code': 'VGW',
        'active': True,
        'interface': ('g2cam', 1.0),
        'fov': 0.023570,
        'frametypes': 'AQ',
        'description': u'',
        },
    'TELSIM': {
        'name': 'TELSIM',
        'number': 34,
        'code': 'TSM',
        'active': True,
        'interface': ('g2cam', 1.0),
        'fov': 0.023570,
        'frametypes': 'AQ',
        'description': u'Telescope simulator at the DD command level',
        },
    }


class INSdata(object):
    """Class that allows you to query various information about Subaru instruments.
    """

    def __init__(self, info=None):

        # Make a map to the instrument info by name
        self.nameMap = insinfo

        # Update from supplementary info provided by config file
        if info == None:
            try:
                # Yuk--use g2soss module instead of envvar?
                infopath = os.path.join(os.environ['GEN2COMMON'],
                                        'db', 'inscfg')
                if os.path.exists(infopath):
                    info = infopath
            except:
                pass

        # Update from supplementary info provided by the user
        if info:
            if isinstance(info, str):
                with open(info, 'r') as in_f:
                    buf = in_f.read()
                    info = eval(buf)

            assert isinstance(info, dict), Exception("argument must be a dict")
            for key in info.keys():
                if key in self.nameMap:
                    self.nameMap[key].update(info[key])
                else:
                    self.nameMap[key] = info[key]

        # Make a map to the instrument info by number
        numberMap = {}
        for d in insinfo.values():
            numberMap[d['number']] = d
        self.numberMap = numberMap

        # Make a map to the instrument info by 3letter mnemonic
        codeMap = {}
        for d in insinfo.values():
            codeMap[d['code']] = d
        self.codeMap = codeMap


    def getCodeByName(self, insname):
        """Get the 3-letter code used for a particular instrument name (_insname_).
        Returns a string or raises a KeyError if the instrument is not found.
        """

        insname = insname.upper()

        return self.nameMap[insname]['code']


    def getNumberByName(self, insname):
        """Get the number used for a particular instrument name (_insname_).
        Returns an integer or raises a KeyError if the instrument is not found.
        """

        insname = insname.upper()

        return self.nameMap[insname]['number']


    def getNameByCode(self, code):
        """Get the name used for a particular instrument by _code_.
        Returns a string or raises a KeyError if the instrument is not found.
        """

        code = code.upper()

        return self.codeMap[code]['name']


    def getNumberByCode(self, code):
        """Get the number used for a particular instrument _code_.
        Returns an integer or raises a KeyError if the instrument is not found.
        """

        code = code.upper()

        return self.codeMap[code]['number']


    def getNameByNumber(self, number):
        """Get the name used for a particular instrument by _number_.
        Returns a string or raises a KeyError if the instrument is not found.
        """

        return self.numberMap[number]['name']


    def getCodeByNumber(self, number):
        """Get the 3-letter code used for a particular instrument by _number_.
        Returns a string or raises a KeyError if the instrument is not found.
        """

        return self.numberMap[number]['code']


    def getNumbers(self, active=True):
        """Returns all the known instrument numbers.
        """

        res = []
        for (insname, d) in insinfo.items():
            if active:
                if d['active']:
                    res.append( self.nameMap[insname]['number'] )
            else:
                res.append( self.nameMap[insname]['number'] )

        res.sort()
        return res


    def getNames(self, active=True):
        """Returns all the known instrument names.
        """

        res = []
        for (insname, d) in insinfo.items():
            if active:
                if d['active']:
                    res.append( self.nameMap[insname]['name'] )
            else:
                res.append( self.nameMap[insname]['name'] )

        res.sort()
        return res


    def getCodes(self, active=True):
        """Returns all the known instrument codes.
        """

        res = []
        for (insname, d) in insinfo.items():
            if active:
                if d['active']:
                    res.append( self.nameMap[insname]['code'] )
            else:
                res.append( self.nameMap[insname]['code'] )

        res.sort()
        return res


    def getOBCPInfoByName(self, insname):
        """Get the OBCP configuration used for a particular instrument by
        _insname_.
        Returns a dict or raises a KeyError if the instrument is not found.
        """

        insname = insname.upper()

        d = self.nameMap[insname]

        res = {}
        res.update(d)

        return res


    def getOBCPInfoByNumber(self, number):
        insname = self.getNameByNumber(number)

        return self.getOBCPInfoByName(insname)


    def getOBCPInfoByCode(self, inscode):
        insname = self.getNameByCode(inscode)

        return self.getOBCPInfoByName(insname)


    def getNameByFrameId(self, frameid):

        fr = AstroFrame()
        fr.from_frameid(frameid)

        insname = self.getNameByCode(fr.inscode)
        return insname


    def getFileByFrameId(self, frameid):

        insname = self.getNameByFrameId(frameid)

        try:
            datadir = os.environ['DATAHOME']
        except KeyError:
            raise Exception("Environment variable 'DATAHOME' not set")

        return os.path.join(datadir, insname, frameid + '.fits')


    def getFrametypesByName(self, insname):
        """Get the frame types used for a particular instrument by _insname_.
        Returns a list.
        """
        insname = insname.upper()

        d = self.nameMap[insname]
        try:
            types = d['frametypes']

        except KeyError:
            types = 'AQ'

        return list(types)


    def getFramesPerExpByName(insname, frtype):
        insname = insname.upper()

        d = self.nameMap[insname]
        try:
            expnum = d['frames_per_exp'][frtype]

        except KeyError:
            expnum = 1

        return expnum


    def getInterfaceByName(self, insname):
        """Get the interface used for a particular instrument by
        _insname_.
        Returns a string or raises a KeyError if the instrument is not found.
        """
        insname = insname.upper()

        d = self.nameMap[insname]
        return d['interface']

    def getFovByName(self, insname):
        """Get the field of view (FOV) for a particular instrument by
        _insname_. The value is the radius of the FOV and is expressed
        in degrees. The FOV values in were copied from the Gen2
        AgAutoSelect.cfg configuration file.
        Returns a float or raises a KeyError if the instrument is not found.
        """
        insname = insname.upper()

        d = self.nameMap[insname]
        return d['fov']


def main(options, args):

    ins_data = INSdata()

    show = options.show.lower()
    ins_list = options.ins.upper().strip()

    if ins_list == 'ACTIVE':
        ins_list = ins_data.getNumbers(active=True)

    elif ins_list == 'ALL':
        ins_list = ins_data.getNumbers(active=False)

    else:
        ins_list = ins_list.split(',')
        try:
            # Try interpreting instrument list as numbers
            ins_list = [int(elt) for elt in ins_list]
        except ValueError:
            try:
                # Try interpreting instrument list as names
                ins_list = [ins_data.getNumberByName(elt)
                            for elt in ins_list]
            except KeyError:
                try:
                    # Try interpreting instrument list as codes
                    ins_list = [ins_data.getNumberByCode(elt)
                                for elt in ins_list]
                except KeyError:
                    raise Exception("I don't understand the type of items in '%s'" % options.ins)


    for num in ins_list:
        info = ins_data.getOBCPInfoByNumber(num)
        if show == 'all':
            print(info)

        else:
            try:
                print(info[show])
            except KeyError:
                raise Exception("I don't understand --show=%s" % show)


if __name__ == '__main__':

    # Parse command line options
    from optparse import OptionParser

    usage = "usage: %prog [options]"
    optprs = OptionParser(usage=usage, version=('%%prog'))

    optprs.add_option("--debug", dest="debug", default=False,
                      action="store_true",
                      help="Enter the pdb debugger on main()")
    optprs.add_option("--show", dest="show", default='name',
                      help="Show name|code|number")
    optprs.add_option("--ins", dest="ins", default='active',
                      help="List of names/codes/numbers or 'all' or 'active'")
    optprs.add_option("--profile", dest="profile", action="store_true",
                      default=False,
                      help="Run the profiler on main()")

    (options, args) = optprs.parse_args(sys.argv[1:])

    if len(args) != 0:
        optprs.error("incorrect number of arguments")


    # Are we debugging this?
    if options.debug:
        import pdb

        pdb.run('main(options, args)')

    # Are we profiling this?
    elif options.profile:
        import profile

        print("%s profile:" % sys.argv[0])
        profile.run('main(options, args)')

    else:
        main(options, args)


#END
