# -*- coding: utf-8 -*-
'''Initialize the opentick package.'''

from .client import connect, split_range, Future, Connection, Error

__all__ = [
    'split_range',
    'Future',
    'Connection',
    'Error',
]

__version__ = '1.2.0'
