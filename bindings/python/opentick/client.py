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
    self._mutex = threading.Lock()
    self._cond = threading.Condition()
    self._store = {}
    self.__token_counter = 0
    if db_name:
      pass

  def close(self):
    self.__sock.close()
    self.join()

  def execute(self, sql, *args):
    pass

  def run(self):
    while True:
      try:
        n = 4
        head = ''
        while n > 0:
          got = self.__sock.recv(n)
          if not got:
            return
          n -= len(got)
          head += got
        assert (len(head) == 4)
        n = struct.unpack('<I', head)[0]
        body = ''
        while n > 0:
          got = self.__sock.recv(n)
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


class Future(object):

  def __init__(self, conn, token):
    self.__conn = conn
    self.__token = token

  def Get(self):
    while True:
      self.__conn._mutex.acquire()
      msg = self.__conn._store.get(self.__token)
      if msg != None:
        del self.__conn._store[self.__token]
        self.__conn._mutex.release()
        return msg
      self.__conn._mutex.release()
      self.__conn._cond.wait()


def recv(sock):
  pass
