#!/usr/bin/env python

import opentick 

conn = None
try:
  conn = opentick.connect('', 1116, 'test')
  res = conn.execute('create table test(sec int, interval int, tm timestamp, open double, high double, low double, close double, v double,vwap double, primary key(sec, interval, tm))')
except opentick.Error, e:
  print(e)
finally:
  if conn: conn.close()
