#!/usr/bin/env python

import datetime
import opentick
from six.moves import xrange

conn = None
try:
  conn = opentick.connect('', 1116, 'test')
  res = conn.execute(
      'create table if not exists test(sec int, interval int, tm timestamp, open double, high double, low double, close double, v double,vwap double, primary key(sec, interval, tm))'
  )
  res = conn.execute('delete from test where sec=?', 1)
  tm = datetime.datetime.now()
  for i in xrange(100):
    futs = []
    now = datetime.datetime.now()
    print(str(now),)
    for j in xrange(100000):
      tm2 = tm + datetime.timedelta(microseconds=j)
      res = conn.execute_async(
          'insert into test(sec, interval, tm, open) values(?, ?, ?, ?)', 1, i,
          tm2, 2.2)
      futs.append(res)
    now2 = datetime.datetime.now()
    print(str(now2), str(now2 - now), 'async done')
    for f in futs:
      f.get()
    now3 = datetime.datetime.now()
    print(str(now3), str(now3 - now2), i, len(futs), 'all futures get done')
    res = conn.execute('select * from test where sec=1')
    now4 = datetime.datetime.now()
    print(str(now4), str(now4 - now3), len(res), 'retrieved')
except opentick.Error as e:
  print(e)
finally:
  if conn: conn.close()
