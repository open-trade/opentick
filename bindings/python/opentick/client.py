# -*- coding: utf-8 -*-
'''Python client for opentick.'''

import datetime
import sys
import socket
import struct
from six.moves import xrange
import six
from bson import BSON
import threading
import pytz

fromtimestamp = datetime.datetime.fromtimestamp
utc_start = fromtimestamp(0, pytz.utc)
localize = pytz.utc.localize
BUF_SIZE = 1024 * 1024


class Error(RuntimeError):
  pass


def connect(addr, port, db_name=''):
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
  sock.connect((addr, port))
  conn = Connection(sock, db_name)
  return conn


def split_range(start, end, num_parts):
  out = []
  tmp = start
  d = (end - start) / num_parts
  if isinstance(start, int):
    d = int(d)
  for i in xrange(num_parts):
    tmp2 = tmp + d
    out.append([tmp, tmp2])
    tmp = tmp2
  out[-1][-1] = end
  return out


# not thread-safe
class Connection(threading.Thread):

  def __init__(self, sock, db_name):
    threading.Thread.__init__(self)
    self.__sock = sock
    self.__prepared = {}
    self.__ticker_counter = 0
    self._mutex = threading.Lock()
    self._cond = threading.Condition()
    self._store = {}
    self.start()
    if db_name:
      self.use(db_name)

  def use(self, db_name):
    ticker = self.__get_ticker()
    cmd = {'0': ticker, '1': 'use', '2': db_name}
    self.__send(cmd)
    try:
      Future(ticker, self).get()
    except Error as e:
      self.close()
      raise e

  def close(self):
    try:
      self.__sock.shutdown(socket.SHUT_RDWR)
    except socket.error as e:
      pass
    self.__sock.close()
    self.join()

  def execute(self, sql, *args):
    if len(args) > 0:
      if isinstance(args[-1], tuple) or isinstance(args[-1], list):
        if isinstance(args[-1][0], tuple) or isinstance(args[-1][0], list):
          return self.__execute_ranges_async(sql, *args).get()
    return self.execute_async(sql, *args).get()

  def execute_async(self, sql, *args):
    prepared = None
    if len(args) > 0:
      if isinstance(args[-1], tuple) or isinstance(args[-1], list):
        if isinstance(args[-1][0], tuple) or isinstance(args[-1][0], list):
          return self.__execute_ranges_async(sql, *args)
      args = list(args)
      self.__convert_timestamp(args)
      prepared = self.__prepare(sql)
    ticker = self.__get_ticker()
    cmd = {'0': ticker, '1': 'run', '2': sql, '3': args}
    if prepared != None:
      cmd['2'] = prepared
    self.__send(cmd)
    return Future(ticker, self)

  def batch_insert(self, sql, argsArray):
    fut = self.batch_insert_async(sql, argsArray)
    fut.get()

  def batch_insert_async(self, sql, argsArray):
    if not argsArray:
      raise Error('argsArray required')
    for args in argsArray:
      self.__convert_timestamp(args)
    prepared = self.__prepare(sql)
    ticker = self.__get_ticker()
    cmd = {'0': ticker, '1': 'batch', '2': prepared, '3': argsArray}
    self.__send(cmd)
    return Future(ticker, self)

  def __execute_ranges_async(self, sql, *args):
    ranges = args[-1]
    futs = []
    for r in ranges:
      args2 = list(args[:-1]) + r
      futs.append(self.execute_async(sql, *args2))
    return Futures(futs)

  def __convert_timestamp(self, args):
    for i in xrange(len(args)):
      arg = args[i]
      if isinstance(arg, datetime.datetime):
        s = (localize(arg) - utc_start).total_seconds()
        args[i] = (int(s), int(s * 1000000) % 1000000 * 1000)

  def __prepare(self, sql):
    self._mutex.acquire()
    prepared = self.__prepared.get(sql)
    self._mutex.release()
    if prepared == None:
      ticker = self.__get_ticker()
      cmd = {'0': ticker, '1': 'prepare', '2': sql}
      self.__send(cmd)
      n = Future(ticker, self).get()
      self._mutex.acquire()
      self.__prepared[sql] = n
      prepared = n
      self._mutex.release()
    return prepared

  def __notify(self, ticker, msg):
    self._cond.acquire()
    self._store[ticker] = msg
    self._cond.notify_all()
    self._cond.release()

  '''
  def __recv(self):
    msg = self.__parse_msg()
    if msg != None: return msg
    while True:
      try:
        got = self.__sock.recv(BUF_SIZE)
        if not got:
          self.__notify(-1, Error('Connection reset by peer'))
          return None
        self.__buf += got
        msg = self.__parse_msg()
        if msg != None: return msg
      except socket.error as e:
        if e.errno != 11: # not timeout
          self.__notify(-1, None)
          continue
        self.__notify(-1, e)
        return None

  def __parse_msg(self):
    if len(self.__buf) >= 8:
      n = self.__buf[:8]
      n = struct.unpack('<Q', n)[0]
      if len(self.__buf) >= 8 + n:
        msg = self.__buf[8:8+n]
        self.__buf = self.__buf[8+n:]
        msg = BSON(msg).decode()
        return msg
    return None

  def run(self):
    self.__buf = six.b('')
    while True:
      msg = self.__recv()
      if msg == None: break
      self.__notify(msg['0'], msg)
  '''

  def run(self):
    while True:
      n = 8
      head = six.b('')
      while n > 0:
        try:
          got = self.__sock.recv(n)
        except socket.error as e:
          if e.errno == 11:  # timeout
            self.__notify(-1, None)
            continue
          self.__notify(-1, e)
          return
        if not got:
          self.__notify(-1, Error('Connection reset by peer'))
          return
        n -= len(got)
        head += got
      assert (len(head) == 8)
      n = struct.unpack('<Q', head)[0]
      body = six.b('')
      while n > 0:
        try:
          got = self.__sock.recv(n)
        except socket.error as e:
          if e.errno == 11:  # timeout
            self.__notify(-1, None)
            continue
          self.__notify(-1, e)
          return
        if not got:
          self.__notify(-1, Error('Connection reset by peer'))
          return
        n -= len(got)
        body += got
      msg = BSON(body).decode()
      self.__notify(msg['0'], msg)

  def __send(self, msg):
    out = BSON.encode(msg)
    n = len(out)
    out = struct.pack('<Q', n) + out
    n = len(out)
    self._mutex.acquire()
    while n > 0:
      try:
        n2 = self.__sock.send(out)
      except socket.error as e:
        self._mutex.release()
        raise e
      out = out[n2:]
      n -= n2
    self._mutex.release()

  def __get_ticker(self):
    self._mutex.acquire()
    n = self.__ticker_counter
    self.__ticker_counter += 1
    self._mutex.release()
    return n


class Future(object):

  def __init__(self, ticker, conn):
    self.__ticker = ticker
    self.__conn = conn

  def __get_store(self, ticker):
    out = self.__conn._store.get(ticker)
    if out != None and ticker != -1:
      del self.__conn._store[self.__ticker]
    return out

  def get(self, timeout=None): # timeout in seconds
    msg = None
    err = None
    self.__conn._cond.acquire()
    tm = datetime.datetime.now()
    while True:
      msg = self.__get_store(self.__ticker)
      err = self.__get_store(-1)
      if msg == None and err == None:
        self.__conn._cond.wait(0.1)
      else:
        break
      if (timeout or 0) > 0 and datetime.datetime.now() - tm >= datetime.timedelta(seconds=timeout):
        self.__conn._cond.release()
        raise Error('Timeout')
    self.__conn._cond.release()
    if msg:
      msg = msg['1']
      if isinstance(msg, six.string_types):
        raise Error(msg)
      if isinstance(msg, list):
        for rec in msg:
          if isinstance(rec, list):
            for i in xrange(len(rec)):
              v = rec[i]
              if isinstance(v, list) and len(v) == 2:
                rec[i] = fromtimestamp(v[0], pytz.utc) + \
                  datetime.timedelta(microseconds=v[1] / 1000)
      return msg
    if err:
      raise err

class Futures(object):
  def __init__(self, futs):
    self.__futs = futs

  def get(self, timeout=None):
    out = []
    for fut in self.__futs:
      ret = fut.get(timeout)
      if ret and len(ret) > 0:
        if len(out) > 0 and out[-1] == ret[0]:
          ret = ret[1:]
        out += ret
    return out
