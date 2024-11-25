G2CAM ABOUT
-----------
g2cam is a Python module for interfacing instruments to Subaru Telescope

COPYRIGHT AND LICENSE
---------------------
Copyright (C) 2014-2024 Subaru Telescope, National Astronomical
  Observatory of Japan.  All rights reserved.

g2cam is distributed under an open-source BSD licence.  Please see the
file LICENSE.txt in the top-level directory for details.

BUILDING AND INSTALLATION
-------------------------
g2cam uses a standard distutils based install, e.g.

    $ pip install .

If you want to install to a specific area, do

    $ pip install --root=/some/path

The files will then end up under /some/path in the appropriate sub-folders.

RUNNING
-------
Run the example cam from the top level source directory where you 
unpacked g2cam, e.g.

$ g2cam --loglevel=20 --stderr --cam=SIMCAM

DOCUMENTATION
-------------
Please see the manual in directory 'doc'

