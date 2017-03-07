# -*- coding: utf-8 -*-
import pyodbc
from Queue import Queue
import os
from datetime import datetime
from time import time

import logging
from logging import handlers
from types import NoneType

from uploder.conf.config import Configurator
import sys

__author__ = 'Krylov.YS'

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3
PY26 = sys.version_info[:2] == (2, 6)

if PY3:
    unicode_type = str
    string_type = bytes
    basestring = str

elif PY2:
    unicode_type = unicode
    string_type = basestring
else:
    raise RuntimeError('Unsupported python version.')


class ReconnectingQueue(Queue):
    def __init__(self, conn_str, maxsize, db_module):
        # Currently tested only on pyodbc.
        if db_module == 'pyodbc':
            try:
                import pyodbc as db_lib
            except ImportError:
                db_lib = None
        if db_lib:
            self.db = db_lib
        else:
            raise ImportError('Unsupported database library')
        Queue.__init__(self, maxsize)
        self.conn_str = conn_str
        self.timeout = 5
        self.last_full_time = datetime.now()
        self.__fill_connections()

    def reconnect(self, conn=None):
        if conn:
            conn.close()
        conn = self.db.connect(self.conn_str)
        return conn

    def __fill_connections(self, ):
        while not self.full():
            conn = self.db.connect(self.conn_str)
            self.put(conn)

    def get(self, block=True, timeout=None):
        if self.qsize() == 0:
            self.__fill_connections()
        conn = Queue.get(self, block, timeout)
        test_cursor = conn.cursor()
        test_cursor.execute('select 1')
        test_cursor.fetchone()
        test_cursor.close()
        return conn

    def put(self, item, block=True, timeout=None):
        if self.qsize() >= self.maxsize and item:
            item.close()
        else:
            Queue.put(self, item, block, timeout)
            if self.qsize() == self.maxsize:
                self.last_full_time = datetime.now()
            else:
                unfull_period = (datetime.now() - self.last_full_time).seconds / 60
                if unfull_period > self.timeout:
                    self.__fill_connections()

    def get_size(self):
        return self.qsize()

    def remove_from_given(self, conn):
        for item in self.given_connections:
            if item.conn == conn:
                self.given_connections.remove(item)

    def test_timed_connections(self):
        for item in self.given_connections:
            if item.is_expired():
                item.conn.close()
                self.given_connections.remove(item)
                self.__fill_connections()


class NormyDatabase(object):
    def __init__(self, db_module, conn_string, pool_size=3, db_encoding="utf8"):
        self.connection = None
        self.cursor = None
        self.db_encoding = db_encoding
        self.db_pool = ReconnectingQueue(conn_str=conn_string, maxsize=pool_size, db_module=db_module)

    def create_basic_json_res(self, keys, result_list):
        return [dict(zip(keys, values)) for values in result_list]

    def connect(self):
        if not self.connection:
            self.connection = self.db_pool.get()

    def close(self):
        if self.connection and self.db_pool.get_size() < self.db_pool.maxsize:
            self.db_pool.put(self.connection)
            self.connection = None
        elif self.connection and self.db_pool.get_size() >= self.db_pool.maxsize:
            self.connection.close()
            self.connection = None

    def _coerce_to_utf8(self, s):
        if isinstance(s, unicode_type):
            return s
        elif isinstance(s, string_type):
            try:
                return s.decode(self.db_encoding)
            except UnicodeDecodeError:
                return s
        elif type(s) in [int, datetime, NoneType]:
            return s
        return unicode_type(s)

    def _coerce(self, data):
        if data:

            def generate_seq(seq_data):
                return tuple(self._coerce_to_utf8(item) for item in seq_data)
            if isinstance(data, tuple):
                res = generate_seq(data)
                return res
            if isinstance(data, list):
                result = []
                for tuple_item in data:
                    result.append(generate_seq(tuple_item))
                return result
            return self._coerce_to_utf8(data)
        else:
            return data

    def get_data(self, sql, *args):
        cursor = self.connection.cursor()
        cursor.execute(sql, *args)
        result = cursor.fetchall()
        cursor.close()
        result = self._coerce(result)
        return result

    def get_one_result(self, sql, *args):
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, *args)
            result = cursor.fetchone()
            cursor.close()
            if isinstance(result, pyodbc.Row):
                result = self._coerce(tuple(x for x in result))
                return result
            else:
                result = self._coerce(result)
                return result
        except pyodbc.Error as err:
            print err[1]

    def execute_query(self, sql, *args):
        cursor = self.connection.cursor()
        cursor.execute(sql, *args)
        cursor.commit()
        cursor.close()

    def get_not_all(self, data_lst, indexes):
        def prepare_idx(data):
            lst_res = []
            for idx in indexes:
                lst_res.append(data[idx])
            return tuple(lst_res)
        return [prepare_idx(x) for x in data_lst]
