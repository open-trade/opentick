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
    n1 = 10
    n2 = 10000
    tm2 = None
    for j in xrange(n1):
      args_array = []
      for k in xrange(n2):
        ms = j * n2 + k
        tm2 = tm + datetime.timedelta(microseconds=ms)
        args_array.append([1, i, tm2, 2.2])
      res = conn.batch_insert_async(
          'insert into test(sec, interval, tm, open) values(?, ?, ?, ?)',
          args_array)
      futs.append(res)
    now2 = datetime.datetime.now()
    print(str(now2), str(now2 - now), 'async done')
    for f in futs:
      f.get()
    now3 = datetime.datetime.now()
    print(str(now3), str(now3 - now2), i, len(futs), 'all futures get done')
    res = []
    for j in range(i+1):
      res += conn.execute('select * from test where sec=1 and interval=? and tm>=? and tm<=?', j,
              opentick.split_range(tm, tm2, 10))
    now4 = datetime.datetime.now()
    print(str(now4), str(now4- now3), len(res), 'retrieved with ranges')
    futs = []
    for j in range(i+1):
      futs.append(conn.execute_async('select * from test where sec=1 and interval=?', j))
    now5 = datetime.datetime.now()
    print(str(now5), str(now5- now4), 'async done')
    res = []
    for f in futs:
      res += f.get()
    now6 = datetime.datetime.now()
    print(str(now6), str(now6 - now4), len(res), 'retrieved with async')
    res = conn.execute('select * from test where sec=1')
    now7 = datetime.datetime.now()
    print(str(now7), str(now7 - now4), len(res), 'retrieved with one sync')
except opentick.Error as e:
  print(e)
finally:
  if conn: conn.close()
