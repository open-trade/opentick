# -*- coding: utf-8 -*-
'''Python client for opentick.'''

import datetime
import sys
import socket
import struct
from six.moves import xrange
from bson import BSON
import atexit
import threading

utc_start = datetime.datetime.fromtimestamp(0)


def connect(addr, port, db_name):
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  ock.connect(addr, port)
  conn = Connection(sock, db_name)
  conn.start()
  return conn


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
    if db_name:
      ticker = self.get_ticker()
      cmd = {'0': ticker, '1': 'use', '2': db_name}
      self.send(cmd)
      Future(ticker, self).get()

  def close(self):
    self.__sock.close()
    self.join()

  def execute(self, sql, *args):
    fut = self.execute_async(sql, *args)
    return fut.Get()

  def execute_async(self, sql, *args):
    prepared = None
    if len(args) > 0:
      for i in xrange(args):
        arg = args[i]
        if isinstance(arg, datetime.datetime):
          s = arg - utc_start
          args[i] = (int(s), int(s * 1000000) % 1000000)
      prepared = self.__prepared.get(sql)
      if prepared != None:
        ticker = self.get_ticker()
        cmd = {'0': ticker, '1': 'prepare', '2': sql}
        self.send(cmd)
        self.prepared[sql] = Future(ticker, self).get()
    ticker = self.get_ticker()
    cmd = {'0': ticker, '1': 'run', '2': sql, '3': args}
    if prepared == None:
      cmd['2'] = prepared
    self.send(cmd)
    f = Future(ticker, self)
    return f

  def run(self):
    while True:
      try:
        n = 4
        head = ''
        while n > 0:
          try:
            got = self.__sock.recv(n)
          except socket.error as e:
            self._mutex.acquire()
            self._store[-1] = e
            self._mutex.release()
            return
          if not got:
            return
          n -= len(got)
          head += got
        assert (len(head) == 4)
        n = struct.unpack('<I', head)[0]
        body = ''
        while n > 0:
          try:
            got = self.__sock.recv(n)
          except socket.error as e:
            self._mutex.acquire()
            self._store[-1] = e
            self._mutex.release()
            return
          if not got:
            return
          n -= len(got)
          body += got
        msg = BSON.decode(body)
        self._mutex.acquire()
        self._store[msg['0']] = msg
        self._cond.notify_all()
        self._mutex.release()
      except socket.error as e:
        if e.errno != 11:  # not timeout
          pass

  def __send(msg):
    out = BSON.encode(msg)
    n = len(out)
    out = struct.unpack('<I', n) + out
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


class Future(object):

  def __init__(self, ticker, conn):
    self.__ticker = ticker
    self.__conn = conn

  def get(self):
    while True:
      self.__conn._mutex.acquire()
      msg = self.__conn._store.get(self.__ticker)
      if msg != None:
        del self.__conn._store[self.__ticker]
        self.__conn._mutex.release()
        msg = msg['1']
        if isinstance(msg, str):
          raise msg
        if isinstance(msg, list):
          for rec in msg:
            if isinstance(rec, list):
              for i in xrange(len(rec)):
                v = rec[i]
                if isinstance(v, list) and len(v) == 2:
                  rec[i] = datetime.datetime.fromtimestamp(
                      v[0]) + datetime.timedelta(microseconds=v[1] / 1000)
        return msg
      err = self._conn._store.get(-1)
      if err:
        self.__conn._mutex.release()
        raise err
      self.__conn._mutex.release()
      self.__conn._cond.wait()
