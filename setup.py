#! /usr/bin/env python
#
from distutils.core import setup
from g2cam.version import version
import os

srcdir = os.path.dirname(__file__)

try:  # Python 3.x
    from distutils.command.build_py import build_py_2to3 as build_py
except ImportError:  # Python 2.x
    from distutils.command.build_py import build_py

def read(fname):
    buf = open(os.path.join(srcdir, fname), 'r').read()
    return buf

# not yet working...
def get_docs():
    docdir = os.path.join(srcdir, 'doc')
    res = []
    # ['../../doc/Makefile', 'doc/conf.py', 'doc/*.rst',
    #                              'doc/manual/*.rst', 'doc/figures/*.png']
    return res

setup(
    name = "g2cam",
    version = version,
    author = "OCS Group, Subaru Telescope, NAOJ",
    author_email = "ocs@naoj.org",
    description = ("A toolkit for interfacing with the Subaru Telescope Observation Control System."),
    long_description = read('README.txt'),
    license = "BSD",
    keywords = "subaru, telescope, instrument, toolkit, interface",
    url = "http://naojsoft.github.com/g2cam",
    packages = ['g2cam', 'g2base',
                # Misc g2cam
                'g2cam.util',
                # Misc g2base
                'g2base.remoteObjects',
                ],
    package_data = { #'g2cam.doc': ['manual/*.html'],
                     },
    scripts = ['scripts/g2cam', 'scripts/stubgen', 'scripts/ro_shell'],
    classifiers = [
        "License :: OSI Approved :: BSD License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Topic :: Scientific/Engineering :: Astronomy",
    ],
    cmdclass={'build_py': build_py}
)
