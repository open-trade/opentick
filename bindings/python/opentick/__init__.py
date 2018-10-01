# -*- coding: utf-8 -*-
'''Initialize the opentick package.'''

from .client import connect, Future, Connection, Error

__all__ = [
    'connect',
    'Future',
    'Connection',
    'Error',
]

__version__ = '1.0.0'
