#!/usr/bin python

from distutils.core import setup

setup(name='Godavari (Python 2.x)',
      version='1.0',
      description='Godavari - a grid computing framework',
      author='Narahari Allamraju',
      author_email='anarahari@gmail.com',
      url='http://supercoderz.in',
      packages=['godavari', 'godavari_examples'],
      scripts=['scripts/godavari_admin.py'],
     )
