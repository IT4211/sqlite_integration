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
        #self.listOfDBFiles = glob.glob(self.path + "*") # 이거는 확장자가 없으므로 불확실한 것임!
        self.listOfDBFiles = glob.glob(self.path + "*.db")
        self.listOfDBFiles.extend(glob.glob(self.path + "*.sqlite"))


    def set_sqlite(self):
        self.sqlname = self.output + "result.db"
        self.con = sqlite3.connect(self.sqlname)
        self.con.text_factory = str()

    def join_operation(self):
        for db in self.listOfDBFiles:
            self.table_list(db) # 하나의 DB에 대해 테이블 목록을 얻어옴
            # 그러면 그 테이블에 대해서 정보를 비교해야 겠군, result.db랑!


    def check_table(self):
        pass

    def table_list(self, db):
        try:
            dbname = db
            conn = sqlite3.connect(dbname)
            cursor = conn.cursor()
            # DB 내에서 테이블의 목록을 전부 가져온다.
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            self.table_lists = cursor.fetchall()
            print self.table_lists

        except sqlite3.Error as e:
            if self.con:
                self.con.rollback()

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





