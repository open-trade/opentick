#!/usr/bin/env python3

# -*- coding: utf-8 -*-
'''Python client for opentick.'''

import time
from bson import BSON
import message_pb2

value = [1, 1.2, 1.3, 1.4]
values = []
for x in range(10):
  values.append(value)
msg = {'0': 'test', '1': 1, '2': values}
now = time.time()
for x in range(100000):
  body = BSON.encode(msg)
print(len(body))
print(time.time() - now)
now = time.time()
for x in range(100000):
  msg = BSON(body).decode()
print(time.time() - now)
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
now = time.time()
for x in range(100000):
  body = m.SerializeToString()
print(len(body))
print(time.time() - now)
now = time.time()
for x in range(100000):
  y = message_pb2.Message()
  y.ParseFromString(body)
print(time.time() - now)
