G2CAM ABOUT
-----------

COPYRIGHT AND LICENSE
---------------------
Copyright (c) 2014  National Astronomical Observatory of Japan.  All rights reserved.

g2cam is distributed under an open-source BSD licence.  Please see the
file LICENSE.txt in the top-level directory for details.

BUILDING AND INSTALLATION
-------------------------
g2cam uses a standard distutils based install, e.g.

    $ python setup.py build

or

    $ python setup.py install

If you want to install to a specific area, do

    $ python setup.py install --prefix=/some/path

The files will then end up under /some/path

RUNNING
-------
Run the example cam from the top level source directory where you 
unpacked g2cam, e.g.

$ g2cam --loglevel=20 --stderr --cam=SIMCAM

DOCUMENTATION
-------------
Please see the manual in directory 'doc'

