#-*- coding: utf-8 -*-
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

        self.table_lists = []
        self.table_info = dict()
        self.fieldnames = dict()

    def set_sqlite(self):
        self.sqlname = self.output + "result.db"
        self.con = sqlite3.connect(self.sqlname)
        #self.con.text_factory = str()

    def join_operation(self):
        for db in self.listOfDBFiles: # 한 src DB에 대한 작업 수행하는 영역
            try:
                self.conn = sqlite3.connect(db)
                #self.conn.text_factory = str()
                self.table_list(db) # 하나의 DB에 대해 테이블 목록과 필드명을 얻어옴
                # 그러면 그 테이블에 대해서 정보를 비교해야 겠군, result.db랑!
                self.check_table()

            finally:
                if self.conn:
                    self.conn.close()


    def check_table(self):
        try:
            cursor = self.con.cursor() # base DB의 정보를 가져와서 비교를 해야 하므로!
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            baseDBTables = list(map(lambda x: x[0], cursor.fetchall())) # base DB의 테이블 목록, 이 목록과 비교해야 한다.
            same = list(set(baseDBTables) & set(self.table_lists)) # 테이블 명이 같은 목록
            #different = list(set(baseDBTables) - set(self.table_lists)) # 테이블 명이 다른 목록
            different = list(set(self.table_lists) - set(baseDBTables))  # 테이블 명이 다른 목록

            # 같은 것들은 테이블 스키마가 동일한 지 검증해야 하고, 다른 것들은 해당 테이블을 base table에 추가
            # 추가하고 스키마 검증 함수 호출 하는 걸로!

            self.copy_table(different)

        except sqlite3.Error as e:
            if self.con:
                self.con.rollback()

        finally:
            if cursor:
                cursor.close()


    def copy_table(self, tablelist):
        print "different table list:    ", tablelist
        for table in tablelist:
            src = self.conn.execute('SELECT * FROM %s' % table)
            ins = None
            #dst = self.con.cursor()
            for row in src.fetchall():
                self.debug("row", row)
                if not ins:
                    cols = tuple([k.keys() for k in self.table_info[table]])
                    self.debug("cols", cols)
                    sql = 'CREATE TABLE ' + table + str(cols) # 테이블 만들어 줘야 하는데, 자료형이 없네? ㅋ
                    self.con.execute(sql)
                    ins = 'INSERT INTO %s %s VALUES (%s)' % (table, cols, ','.join(['?'] * len(cols)))
                    self.debug("ins", ins)

                c = row
                self.con.execute(ins, c)
            self.con.commit()

    def table_list(self, db):
        try:
            #dbname = db
            #self.conn = sqlite3.connect(dbname)
            cursor = self.conn.cursor()
            # DB 내에서 테이블의 목록을 전부 가져온다.
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            self.table_lists += list(map(lambda x: x[0], cursor.fetchall()))
            # 각각 테이블에서 필드명을 가져온다.
            for table in self.table_lists:
                result = cursor.execute("PRAGMA table_info('%s')" % table).fetchall()

                self.table_info[table] = list(map(lambda x, y: {x[1]:y[2]}, result, result))
                self.fieldnames[table] = list(map(lambda x: x.keys()[0], self.table_info[table]))

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

