try:
  import distribute_setup
  distribute_setup.use_setuptools()
except:
  pass

from setuptools import setup, find_packages
import os
import re

with open(os.path.join(os.path.dirname(__file__), 'opentick',
                       '__init__.py')) as f:
  version = re.search("__version__ = '([^']+)'", f.read()).group(1)

with open('requirements.txt', 'r') as f:
  requires = [x.strip() for x in f if x.strip()]

with open('README.rst', 'r') as f:
  readme = f.read()

setup(
    name='opentick',
    version=version,
    author='OpenTrade Solutions',
    description='OpenTick SDK',
    author_email='info@opentradesolutions.com',
    long_description=readme,
    url='https://github.com/opentradesolutions/opentick',
    license='Apache License',
    packages=find_packages(exclude=['tests']),
    install_requires=requires,
    classifiers=(
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Database :: Database Engines/Servers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Financial and Insurance Industry'
    ),
)
