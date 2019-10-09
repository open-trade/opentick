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
import time
import logging
from numbers import Number

fromtimestamp = datetime.datetime.fromtimestamp
utc_start = fromtimestamp(0, pytz.utc)
localize = pytz.utc.localize


class Error(RuntimeError):
  pass


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


class _PleaseReconnect(Exception):
  pass


class Connection(threading.Thread):

  def __init__(self, addr, port, db_name=None, timeout=15):
    threading.Thread.__init__(self)
    self.__addr = addr
    self.__port = port
    self.__db_name = db_name
    self.__sock = None
    self.__prepared = {}
    self.__auto_reconnect = 1
    self.__ticket_counter = 0
    self.__active = True
    self.__connected = None  # None here tell Connection not initialized yet
    self._mutex = threading.Lock()
    self._cond = threading.Condition()
    self.__default_timeout = timeout
    self._store = {}
    self.daemon = True

  def start(self):
    super().start()
    try:
      self.__connect(True)
    except Exception as e:
      logging.error(e)
      self.__connected = False
      return str(e)

  def is_connected(self):
    return self.__connected

  def set_auto_reconnect(self, interval):
    self.__auto_reconnect = interval

  def use(self, db_name, wait=True):
    ticket = self.__get_ticket()
    cmd = {'0': ticket, '1': 'use', '2': db_name}
    self.__send(cmd)
    if not wait: return
    try:
      Future(ticket, self).get(self.__default_timeout)
    except Error as e:
      raise e

  def list_databases(self):
    ticket = self.__get_ticket()
    cmd = {'0': ticket, '1': 'meta', '2': 'list_databases'}
    self.__send(cmd)
    try:
      return Future(ticket, self).get(self.__default_timeout)
    except Error as e:
      raise e

  def list_tables(self):
    ticket = self.__get_ticket()
    cmd = {'0': ticket, '1': 'meta', '2': 'list_tables'}
    self.__send(cmd)
    try:
      return Future(ticket, self).get(self.__default_timeout)
    except Error as e:
      raise e

  def schema(self, table_name):
    ticket = self.__get_ticket()
    cmd = {'0': ticket, '1': 'meta', '2': 'schema ' + table_name}
    self.__send(cmd)
    try:
      return Future(ticket, self).get(self.__default_timeout)
    except Error as e:
      raise e

  def close(self):
    self.__active = False
    self.__close_socket()
    self.join()

  def execute(self, sql, args=[]):
    if len(args) > 0:
      if isinstance(args[-1], tuple) or isinstance(args[-1], list):
        if isinstance(args[-1][0], tuple) or isinstance(args[-1][0], list):
          return self.__execute_ranges_async(sql,
                                             args).get(self.__default_timeout)
    return self.execute_async(sql, args).get(self.__default_timeout)

  def execute_async(self, sql, args=[]):
    prepared = None
    if len(args) > 0:
      if isinstance(args[-1], tuple) or isinstance(args[-1], list):
        if isinstance(args[-1][0], tuple) or isinstance(args[-1][0], list):
          return self.__execute_ranges_async(sql, args)
      args = list(args)
      self.__convert_timestamp(args)
      prepared = self.__prepare(sql)
    ticket = self.__get_ticket()
    cmd = {'0': ticket, '1': 'run', '2': sql, '3': args}
    if prepared != None:
      cmd['2'] = prepared
    self.__send(cmd)
    return Future(ticket, self)

  def batch_insert(self, sql, argsArray):
    fut = self.batch_insert_async(sql, argsArray)
    fut.get(self.__default_timeout)

  def batch_insert_async(self, sql, argsArray):
    if not argsArray:
      raise Error('argsArray required')
    for args in argsArray:
      self.__convert_timestamp(args)
    prepared = self.__prepare(sql)
    ticket = self.__get_ticket()
    cmd = {'0': ticket, '1': 'batch', '2': prepared, '3': argsArray}
    self.__send(cmd)
    return Future(ticket, self)

  def __connect(self, sync=False):
    logging.info('OpenTick: connecting')
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.__sock = sock
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    if self.__default_timeout:
      microsecs = int(self.__default_timeout * 1e6)
      timeval = struct.pack('ll', int(microsecs / 1e6), int(microsecs % 1e6))
      sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVTIMEO, timeval)
    sock.connect((self.__addr, self.__port))
    microsecs = 100000
    timeval = struct.pack('ll', int(microsecs / 1e6), int(microsecs % 1e6))
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVTIMEO, timeval)
    logging.info('OpenTick: connected')
    self.__connected = True
    if self.__db_name:
      self.use(self.__db_name, sync)

  def __execute_ranges_async(self, sql, args):
    ranges = args[-1]
    futs = []
    for r in ranges:
      args2 = list(args[:-1]) + r
      futs.append(self.execute_async(sql, args2))
    return Futures(futs)

  def __convert_timestamp(self, args):
    for i in xrange(len(args)):
      arg = args[i]
      if isinstance(arg, datetime.datetime):
        if arg.tzinfo: arg = arg.astimezone(pytz.utc)
        else: arg = localize(arg)
        s = (arg - utc_start).total_seconds()
        args[i] = (int(s), int(s * 1000000) % 1000000 * 1000)

  def __prepare(self, sql):
    self._mutex.acquire()
    prepared = self.__prepared.get(sql)
    self._mutex.release()
    if prepared == None:
      ticket = self.__get_ticket()
      cmd = {'0': ticket, '1': 'prepare', '2': sql}
      self.__send(cmd)
      n = Future(ticket, self).get(self.__default_timeout)
      self._mutex.acquire()
      self.__prepared[sql] = n
      prepared = n
      self._mutex.release()
    return prepared

  def __notify(self, ticket, msg):
    self._cond.acquire()
    self._store[ticket] = msg
    self._cond.notify_all()
    self._cond.release()

  def run(self):
    while self.__active:
      if self.__connected is None:  # initializing
        time.sleep(1e-6)
        continue
      try:
        n = 4
        head = six.b('')
        while n > 0:
          try:
            got = self.__sock.recv(n)
          except socket.error as e:
            if e.errno in (11,
                           35):  # timeout or Resource temporarily unavailable
              self.__notify(-1, None)
              continue
            self.__notify(-1, e)
            raise _PleaseReconnect()
          if not got:
            self.__notify(-1, Error('Connection reset by peer'))
            raise _PleaseReconnect()
          n -= len(got)
          head += got
        assert (len(head) == 4)
        n0 = n = struct.unpack('<I', head)[0]
        if not n: continue
        body = six.b('')
        while n > 0:
          try:
            got = self.__sock.recv(n)
          except socket.error as e:
            if e.errno in (11,
                           35):  # timeout or Resource temporarily unavailable
              self.__notify(-1, None)
              continue
            self.__notify(-1, e)
            raise _PleaseReconnect()
          if not got:
            self.__notify(-1, Error('Connection reset by peer'))
            raise _PleaseReconnect()
          n -= len(got)
          body += got
        if n0 == 1 and body == six.b('H'):  # heartbeat
          try:
            self.__send()
          except socket.error as e:
            raise _PleaseReconnect()
          continue
        msg = BSON(body).decode()
        self.__notify(msg['0'], msg)
      except _PleaseReconnect as e:
        if self.__auto_reconnect < 1: return
        if not self.__active: return
        time.sleep(self.__auto_reconnect)
        if not self.__active: return
        logging.info('OpenTick: trying reconnect')
        self.__close_socket()
        try:
          self.__connect()
        except socket.error as e:
          logging.error('OpenTick: failed to connect: ' + str(e))
          continue

  def __close_socket(self):
    self.__connected = False
    self._mutex.acquire()
    self.__prepared.clear()
    self._mutex.release()
    self._cond.acquire()
    self._store.clear()
    self._cond.release()
    try:
      self.__sock.shutdown(socket.SHUT_RDWR)
    except socket.error as e:
      pass
    self.__sock.close()

  def __send(self, msg=None):
    out = None
    if not msg:
      out = six.b('')
    elif msg == six.b('H'):
      out = msg
    else:
      out = BSON.encode(msg)
    n = len(out)
    out = struct.pack('<I', n) + out
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

  def __get_ticket(self):
    self._mutex.acquire()
    n = self.__ticket_counter
    self.__ticket_counter += 1
    self._mutex.release()
    return n


class Future(object):

  def __init__(self, ticket, conn):
    self.__ticket = ticket
    self.__conn = conn

  def __get_store(self, ticket):
    out = self.__conn._store.get(ticket)
    if out != None and ticket != -1:
      del self.__conn._store[self.__ticket]
    return out

  def get(self, timeout=None):  # timeout in seconds
    msg = None
    err = None
    self.__conn._cond.acquire()
    tm = datetime.datetime.now()
    while True:
      msg = self.__get_store(self.__ticket)
      err = self.__get_store(-1)
      if msg == None and err == None:
        self.__conn._cond.wait()
      else:
        break
      if (timeout or
          0) > 0 and datetime.datetime.now() - tm >= datetime.timedelta(
              seconds=timeout):
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
              if isinstance(v, list) and len(v) == 2 and isinstance(
                  v[0], Number):
                rec[i] = fromtimestamp(v[0], pytz.utc) + \
                  datetime.timedelta(microseconds=v[1] / 1000)
      return msg
    if err:
      raise Error(err)


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
