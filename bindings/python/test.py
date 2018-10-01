#!/usr/bin/env python

import datetime
import opentick 
from six.moves import xrange

conn = None
try:
  conn = opentick.connect('', 1116, 'test')
  res = conn.execute('create table if not exists test(sec int, interval int, tm timestamp, open double, high double, low double, close double, v double,vwap double, primary key(sec, interval, tm))')
  res = conn.execute('delete from test where sec=? and interval=?', 1, 2)
  tm = datetime.datetime.now()
  for i in xrange(100):
    futs = []
    for j in xrange(1000000):
      tm2 = tm + datetime.timedelta(microseconds=j)
      res = conn.execute_async('insert into test(sec, interval, tm, open) values(?, ?, ?, ?)', 1, 2, tm2, 2.2)
      futs.append(res)
    for f in futs:
      f.get()
    print(i, len(futs))
    res = conn.execute('select * from test where sec=1 and interval=2')
    print(len(res))
except opentick.Error as e:
  print(e)
finally:
  if conn: conn.close()
