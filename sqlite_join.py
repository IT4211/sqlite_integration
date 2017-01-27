# -*- coding: utf-8 -*-
"""
@Author : Seon Ho Lee (IT4211)
@E-mail : rhdqor100@live.co.kr
"""

import _sqlite_join
import sqlite3
import os
import glob


class sqlite_join():

    def __init__(self, path, output):
        self.path = os.path.abspath(path) + "\\"
        self.output = os.path.abspath(output) + "\\"
        # self.listOfDBFiles = glob.glob(self.path + "*")
        # 이거는 확장자가 없으므로 불확실한 것임!
        self.listOfDBFiles = glob.glob(self.path + "*.db")
        self.listOfDBFiles.extend(glob.glob(self.path + "*.sqlite"))

        self.base_info = dict()
        self.base_fieldnames = dict()
        self.table_info = dict()
        self.fieldnames = dict()

    def set_sqlite(self):
        self.sqlname = self.output + "result.db"
        self.con = sqlite3.connect(self.sqlname)
        self.con.text_factory = str

    def join_operation(self):
        for db in self.listOfDBFiles: # 한 src DB에 대한 작업 수행하는 영역
            try:
                self.dbname = db
                self.conn = sqlite3.connect(db)
                self.conn.text_factory = str
                self.table_list() # 하나의 DB에 대해 테이블 목록과 필드명을 얻어옴
                self.check_table()

            finally:
                if self.conn:
                    self.conn.close()

    def check_table(self):
        try:
            cursor = self.con.cursor() # base DB의 정보를 가져와서 비교를 해야 하므로!
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            baseDBTables = list(map(lambda x: x[0].lower(), cursor.fetchall())) # base DB의 테이블 목록, 이 목록과 비교해야 한다.

            same = list(set(baseDBTables) & set(self.table_lists)) # 테이블 명이 같은 목록
            different = list(set(self.table_lists) - set(baseDBTables))  # 테이블 명이 다른 목록
            self.debug("different", different)

            # 같은 것들은 테이블 스키마가 동일한 지 검증해야 하고, 다른 것들은 해당 테이블을 base table에 추가
            # 테이블 명이 동일한 경우
            self.check_schema(same)

            # 테이블 명이 동일하지 않은 경우
            self.copy_table(different)

        except sqlite3.Error as e:
            if self.con:
                self.con.rollback()
            self.debug("err", e)

        finally:
            if cursor:
                cursor.close()

    def check_schema(self, tablelist):
        # 필드 명과 테이터 타입이 동일한가?
        self.base_list()
        base_dict = dict()
        src_dict = dict()

        # 비교루틴
        for table in tablelist:
            src_field = self.fieldnames[table]
            base_field = self.base_fieldnames[table]
            if not len(list(set(base_field) - set(src_field))): # 필드가 동일한지 검사
                for e in self.base_info[table]:
                    base_dict.update(e)
                for e in self.table_info[table]:
                    src_dict.update(e)

                self.debug("base_dict", base_dict)
                self.debug("src_dict", src_dict)
                self.match_flag = True
                for k in base_dict.keys(): # 필드의 데이터 타입도 동일한지 검사
                    self.debug("test", k)
                    if base_dict[k] != src_dict[k]:
                        self.rename_table(table)
                        self.match_flag = False
                        break

                if self.match_flag: # 동일한 테이블, 레코드 추가
                    self.add_record(table)

            else: # 필드가 동일하지 않음
                self.rename_table(table)

    def add_record(self, table):
        self.debug("add", "hello?")
        src = self.conn.execute('SELECT * FROM %s' % table)
        fetchdata = src.fetchall()
        if len(fetchdata) == 0: # 추가해야 되는데, 추가할 게 없다면?
            return
        ins = None
        for row in fetchdata:
            if not ins:
                cols = tuple([k.keys()[0] for k in self.table_info[table]])
                ins = 'INSERT INTO %s %s VALUES (%s)' % (table, cols, ','.join(['?'] * len(cols)))
            try:
                self.con.execute(ins, row)
            except sqlite3.Error as e:
                self.debug("add_except", e)
        self.con.commit()


    def rename_table(self, table):
        dbname = os.path.splitext(self.dbname.split('\\')[-1])[0]
        tablename = dbname.replace('.', '') + '_' + table
        src = self.conn.execute('SELECT * FROM %s' % table)
        fetchdata = src.fetchall()
        if len(fetchdata) == 0:
            self.debug("rename_null_table", tablename)
            sch = tuple([k.keys()[0] + ' ' + str(k.values()[0]) for k in self.table_info[table]])
            sql = 'CREATE TABLE %s(%s)' % (tablename, ','.join(sch))
            self.debug("rename_sql", sql)
            try:
                self.con.execute(sql)  # 대소문자가 다른 동일한 이름의 테이블이 존재할 경우 진행을 멈춤 ;;
            except sqlite3.Error as e:
                print "err", e
            self.con.commit()
        ins = None
        for row in src.fetchall():
            if not ins:
                sch = tuple([k.keys()[0] + ' ' + str(k.values()[0]) for k in self.table_info[table]])
                cols = tuple([k.keys()[0] for k in self.table_info[table]])
                sql = 'CREATE TABLE %s(%s)' % (tablename, ','.join(sch))
                self.debug("rename_table", table)
                self.con.execute(sql)
                if len(cols) == 1:
                    ins = 'INSERT INTO %s (%s) VALUES (%s)' % (table, cols[0], ','.join(['?'] * len(cols)))
                else:
                    ins = 'INSERT INTO %s %s VALUES (%s)' % (table, cols, ','.join(['?'] * len(cols)))

            self.con.execute(ins, row)
            self.debug("rename", "test")
        self.con.commit()

    def base_list(self):
        try:
            self.base_lists = []
            cursor = self.con.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            self.base_lists += list(map(lambda x: x[0], cursor.fetchall()))
            for table in self.base_lists:
                result = cursor.execute("PRAGMA table_info('%s')" % table).fetchall()
                self.base_info[table] = list(map(lambda x, y: {x[1]:y[2]}, result, result))
                self.base_fieldnames[table] = list(map(lambda x: x.keys()[0], self.base_info[table]))

        except sqlite3.Error as e:
            if self.con:
                self.con.rollback()

        finally:
            if cursor:
                cursor.close()

    def copy_table(self, tablelist): # 아예 다른 목록들은 그대로 복사
        for table in tablelist:
            if 'sqlite' in table:
                continue
            src = self.conn.execute('SELECT * FROM %s' % table)
            fetchdata = src.fetchall()
            if len(fetchdata) == 0:
                sch = tuple([k.keys()[0] + ' ' + str(k.values()[0]) for k in self.table_info[table]])
                sql = 'CREATE TABLE %s(%s)' % (table, ','.join(sch))
                try:
                    self.debug("copy_null_table", table)
                    self.con.execute(sql) # 대소문자가 다른 동일한 이름의 테이블이 존재할 경우 진행을 멈춤 ;;
                except sqlite3.Error as e:
                    self.debug("copy_except", e)
                self.con.commit()
            ins = None
            for row in fetchdata:
                if not ins:
                    sch = tuple([k.keys()[0] + ' ' + str(k.values()[0]) for k in self.table_info[table]])
                    cols = tuple([k.keys()[0] for k in self.table_info[table]])
                    sql = 'CREATE TABLE %s(%s)' % (table, ','.join(sch))
                    self.debug("copy_table", table)
                    self.con.execute(sql)
                    if len(cols) == 1:
                        ins = 'INSERT INTO %s (%s) VALUES (%s)' % (table, cols[0], ','.join(['?'] * len(cols)))
                    else:
                        ins = 'INSERT INTO %s %s VALUES (%s)' % (table, cols, ','.join(['?'] * len(cols)))

                try:
                    self.con.execute(ins, row)
                except sqlite3.Error as e:
                    self.debug("copy_except", e) # 컬럼이 하나인 경우
                    self.debug("ins", ins)

            self.con.commit()

    def table_list(self):
        try:
            self.table_lists = []
            #dbname = db
            #self.conn = sqlite3.connect(dbname)
            cursor = self.conn.cursor()
            # DB 내에서 테이블의 목록을 전부 가져온다.
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            self.table_lists += list(map(lambda x: x[0].lower(), cursor.fetchall()))
            # 각각 테이블에서 필드명을 가져온다.
            for table in self.table_lists:
                result = cursor.execute("PRAGMA table_info('%s')" % table).fetchall()

                self.table_info[table] = list(map(lambda x, y: {x[1]:y[2]}, result, result))
                self.fieldnames[table] = list(map(lambda x: x.keys()[0], self.table_info[table]))

                #이 내용은 gist에 올리고 따로 해당 repo의 wiki에 같이 정리해두기!
                #cursor.execute("SELECT * FROM " + table)
                #self.fieldnames[table] = list(map(lambda x: x[0],  cursor.description))
                #self.table_data = cursor.fetchone()
                #self.debug("table_data", self.table_data)
                #self.debug("table", table)
                #self.debug("fieldnames", self.fieldnames)

        except sqlite3.Error as e:
            if self.conn:
                self.conn.rollback()

        finally:
            if cursor:
                cursor.close()

    def debug(self, val, message = None):
        print "[debug:" + str(val) + "]  ", message

    def __del__(self):
        self.con.close()

if __name__ == "__main__":

    args = _sqlite_join.ParseCommandLine()

    db_join = sqlite_join(args.path, args.output)
    db_join.set_sqlite()
    db_join.join_operation()
