#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-daisycon',
      version="0.1.3",
      description='Singer.io tap for extracting data from the Daisycon API',
      author='Horze',
      url='http://horze.de',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_daisycon'],
      install_requires=[
          'singer-python==5.9.0',
          'backoff==1.8.0',
          'requests==2.24.0'
      ],
      entry_points={
          "console_scripts":
          ["tap-daisycon=tap_daisycon:main"]
      },
      packages=find_packages(),
      package_data = {
          'tap_daisycon': [
              'schemas/*.json',
          ],
      },
      extras_require={
          'dev': [
              'pylint',
              'ipdb',
          ]
      },
)