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
      pass

  def close(self):
    self.__sock.close()
    self.join()

  def execute(self, sql, *args):
    fut = self.execute_async(sql, *args)
    return fut.Get()

  def execute_async(self, sql, *args):
    if len(args) > 0:
      pass

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
        self._store[msg[0]] = msg
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

  def __init__(self, conn, ticker):
    self.__conn = conn
    self.__ticker = ticker

  def get(self):
    while True:
      self.__conn._mutex.acquire()
      msg = self.__conn._store.get(self.__ticker)
      if msg != None:
        del self.__conn._store[self.__ticker]
        self.__conn._mutex.release()
        return msg
      err = self._conn._store.get(-1)
      if err:
        self.__conn._mutex.release()
        raise err
      self.__conn._mutex.release()
      self.__conn._cond.wait()
