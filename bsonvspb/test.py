#!/usr/bin/env python3

# -*- coding: utf-8 -*-
'''Python client for opentick.'''

import time
import json
from bson import BSON
import message_pb2

value = [99999999, 1.22222, 1.3222222, 1.422222]
values = []
for x in range(10):
  values.append(value)
msg = {'0': 'test', '1': 1, '2': values}
print('json')
now = time.time()
for x in range(100000):
  body = json.dumps(msg)
print('body size  ', len(body))
print('serialize  ', time.time() - now)
now = time.time()
for x in range(100000):
  msg = json.loads(body)
print('deserialize', time.time() - now)
print('bson')
now = time.time()
for x in range(100000):
  body = BSON.encode(msg)
print('body size  ', len(body))
print('serialize  ', time.time() - now)
now = time.time()
for x in range(100000):
  msg = BSON(body).decode()
print('deserialize', time.time() - now)
m = message_pb2.Message()
m.cmd = 'test'
m.prepared = 1
value = message_pb2.Fields()
value.values.extend([message_pb2.Field(n=1), message_pb2.Field(d=1.2)])
value.values.extend([message_pb2.Field(d=1.3), message_pb2.Field(d=1.4)])
values = []
for x in range(10):
  values.append(value)
m.values.extend(values)
print('pb:')
now = time.time()
for x in range(100000):
  body = m.SerializeToString()
print('body size  ', len(body))
print('serialize  ', time.time() - now)
now = time.time()
for x in range(100000):
  y = message_pb2.Message()
  y.ParseFromString(body)
print('deserialize', time.time() - now)
