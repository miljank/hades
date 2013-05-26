#!/usr/bin/env python


"""disutils setup/install script for hades"""

import os
from   distutils.core import setup

import hades

this_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_dir, 'README.md')) as f:
    long_description = '\n{0}'.format(f.read())

setup(
    url              = 'https://github.com/miljank/hades',
    name             = 'hades',
    author           = 'Miljan Karadzic',
    version          = hades.__version__,
    license          = 'GPLv2',
    platform         = 'Linux',
    packages         = ['hades'],
    requires         = ['pyinotify'],
    provides         = ['hades'],
    keywords         = 'asynchronous job processing json tasks'.split(),
    description      = 'A daemon for asynchronous job processing',
    author_email     = 'miljank _at_ gmail.com',
    download_url     = 'http://pypi.python.org/pypi/hades',
    long_description = long_description,
    classifiers      = [
        'Operating System :: Unix',
        'Operating System :: POSIX :: Linux',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
