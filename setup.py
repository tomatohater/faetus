#!/usr/bin/env python

from distutils.core import setup
from faetus.constants import version

setup(name='faetus',
      version=version,
      description='An FTP interface to Amazon S3 file storage.',
      author='Drew Engelson',
      author_email='drew@engelson.net',
      url='http://tomatohater.com',
      packages=['faetus'],
      scripts=['bin/faetus-server']
     )
