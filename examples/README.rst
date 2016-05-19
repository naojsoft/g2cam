==========================
Running the SIMCAM example
==========================

Installation of g2cam
---------------------

First, make sure g2cam is installed correctly:

    python setup.py install

you can use a --prefix parameter if you want to install it somewhere
specifically, e.g.

    python setup.py install --prefix=/home/inst

you will then need to make sure that your PATH and PYTHONPATH are set
correctly in order to run g2cam:

    export PATH=$PATH:/home/inst/bin
    export PYTHONPATH=$PYTHONPATH:/home/inst/lib/python2.7/site-packages

Running g2cam
-------------

Run g2cam as follows from a terminal:

    g2cam --loglevel=20 --stderr --cam=SIMCAM --gen2host=GEN2HOST

g2cam needs at least one port open for incoming connections.  Depending
on needs, you many need more than one port.  If you use a firewall, it
is recommended to open two ports and to specify them on the g2cam
command line as:

    --monport=9080 --port=9081



