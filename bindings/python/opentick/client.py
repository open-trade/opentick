# -*- coding: utf-8 -*-
'''Python client for opentick.'''

import datetime
import sys
import hashlib
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


# add can be host or url like user_name:password@host:port/db_name
def connect(addr='localhost',
            port=None,
            db_name=None,
            username=None,
            password=None,
            timeout=15):
  conn = Connection(addr, port, db_name, username, password, timeout)
  conn.start()
  return conn


class Connection(threading.Thread):

  def __init__(self,
               addr,
               port=None,
               db_name=None,
               username=None,
               password=None,
               timeout=15):
    threading.Thread.__init__(self)
    toks = addr.split('/')
    if db_name is None and len(toks) > 1: db_name = toks[1]
    toks = toks[0].split('@')
    if len(toks) > 1:
      addr = toks[1]
      toks = toks[0].split(':')
      if password is None and len(toks) > 1: password = toks[1]
      if username is None: username = toks[0]
    else:
      addr = toks[0]
    toks = addr.split(':')
    host = toks[0]
    if port is None and len(toks) > 1: port = int(toks[1])
    self.__addr = host
    self.__port = port or 1116
    self.__db_name = db_name
    self.__username = username
    self.__password = password
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

  def login(self, username, password, db_name=None, wait=True):
    # __username/__password/__db_name not thread safe
    self.__username = username
    self.__password = password
    args = [username, password]
    if db_name:
      self.__db_name = db_name
      args.append(db_name)
    return self.__send_cmd('login', ' '.join(args), wait)

  def delete_user(self, username):
    self.execute('delete from _meta_.user where name=\'' + username + '\'')
    self.reload_users()

  def create_user(self, username, password):
    assert (username and password)
    res = self.execute('select * from _meta_.user where name=\'' + username +
                       '\'')
    if res and res[0]:
      raise Error('User already exist')
    h = hashlib.sha1()
    h.update(six.b(password))
    print(h.hexdigest())
    self.execute("insert into _meta_.user values('%s', '%s', false, '')" %
                 (username, h.hexdigest()))
    self.reload_users()

  def list_users(self):
    return self.execute('select * from _meta_.user')

  def update_user(self, username, perm=None, is_admin=None):
    res = self.execute('select * from _meta_.user where name=\'' + username +
                       '\'')
    if not res or not res[0]:
      raise Error('User not exist')
    if perm is not None:
      if isinstance(perm, str):
        res[0][-1] = perm
      elif isinstance(perm, dict):
        orig = dict([
            x for x in [x.split('=') for x in res[0][-1].split(';')]
            if len(x) == 2
        ])
        for k, v in perm.items():
          if v is None:
            if k in orig: del orig[k]
          elif v in ('write', 'read'):
            orig[k] = v
          else:
            raise ('Invalid perm type: ' + str(v))
        res[0][-1] = ';'.join(['%s=%s' % (a, b) for a, b in orig.items()])
    if is_admin is not None: res[0][-2] = is_admin
    self.execute('insert into _meta_.user values(?, ?, ?, ?)', res[0])
    self.reload_users()

  def reload_users(self):
    self.__send_cmd('meta', 'reload_users')

  def chgpasswd(self, password, wait=True):
    assert (password)
    return self.__send_cmd('meta', 'chgpasswd ' + password, wait)

  def use(self, db_name, wait=True):
    # __db_name not thread safe
    self.__db_name = db_name
    return self.__send_cmd('use', db_name, wait)

  def list_databases(self):
    return self.__send_cmd('meta', 'list_databases')

  def list_tables(self):
    return self.__send_cmd('meta', 'list_tables')

  def schema(self, table_name):
    return self.__send_cmd('meta', 'schema ' + table_name)

  def __send_cmd(self, cmd, arg, wait=True):
    ticket = self.__get_ticket()
    self.__send({'0': ticket, '1': cmd, '2': arg})
    if not wait: return Future(ticket, self)
    try:
      return Future(ticket, self).get(self.__default_timeout)
    except Error as e:
      raise e

  def close(self):
    self.__active = False
    self.__close_socket()
    self.join()

  def execute(self, sql, args=[], cache=True):
    if len(args) > 0:
      if isinstance(args[-1], tuple) or isinstance(args[-1], list):
        if isinstance(args[-1][0], tuple) or isinstance(args[-1][0], list):
          return self.__execute_ranges_async(sql, args,
                                             cache).get(self.__default_timeout)
    return self.execute_async(sql, args, cache).get(self.__default_timeout)

  def execute_async(self, sql, args=[], cache=True):
    prepared = None
    if len(args) > 0:
      if isinstance(args[-1], tuple) or isinstance(args[-1], list):
        if isinstance(args[-1][0], tuple) or isinstance(args[-1][0], list):
          return self.__execute_ranges_async(sql, args, cache)
      args = list(args)
      self.__convert_timestamp(args)
      prepared = self.__prepare(sql)
    ticket = self.__get_ticket()
    cmd = {'0': ticket, '1': 'run', '2': sql, '3': args}
    if cache: cmd['4'] = 1
    if prepared != None:
      cmd['2'] = prepared
    self.__send(cmd)
    return Future(ticket, self)

  def batch_insert(self, sql, argsArray, batch_size=None,
                   batch_one_by_one=True):
    if batch_size and batch_one_by_one:
      while argsArray:
        x = argsArray[:batch_size]
        self.batch_insert(sql, x)
        argsArray = argsArray[batch_size:]
      return

    fut = self.batch_insert_async(sql, argsArray, batch_size)
    if batch_size:
      assert (not batch_one_by_one)
      [f.get(self.__default_timeout) for f in fut]
    else:
      fut.get(self.__default_timeout)

  def batch_insert_async(self, sql, argsArray, batch_size=None):
    if batch_size:
      futs = []
      while argsArray:
        x = argsArray[:batch_size]
        futs.append(self.batch_insert_async(sql, x))
        argsArray = argsArray[batch_size:]
      return futs

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
    if self.__username:
      self.login(self.__username, self.__password, self.__db_name, sync)
    elif self.__db_name:
      self.use(self.__db_name, sync)

  def __execute_ranges_async(self, sql, args, cache=True):
    ranges = args[-1]
    futs = []
    for r in ranges:
      args2 = list(args[:-1]) + r
      futs.append(self.execute_async(sql, args2, cache))
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
        cached = msg.get('2')
        if cached is not None:
          msg['1'] = BSON(cached).decode()['1']
          del msg['2']
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
