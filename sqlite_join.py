#-*- coding: utf-8 -*-
"""
@Author : Seon Ho Lee (IT4211)
@E-mail : rhdqor100@live.co.kr
"""

import _sqlite_join
import sqlite3
import glob


class sqlite_join():

    def __init__(self, path, output):
        self.path = path
        self.output = output
        self.listOfDBFiles = glob.glob(path + "*.db")
        self.listOfDBFiles.extend(glob.glob(path + "*.sqlite"))

    def set_sqlite(self):
        self.sqlname = self.output + "result.db"
        self.con = sqlite3.connect(self.sqlname)
        self.con.text_factory = str()

    def table_list(self):
        try:
            cursor = self.con.cursor()
            # DB 내에서 테이블의 목록을 전부 가져온다.
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            self.table_list += cursor.fetchall()

        except sqlite3.Error as e:
            if self.con:
                self.con.rollback()

        finally:
            if cursor:
                cursor.close()

    def debug(self, val, message = ""):
        print "[debug:" + str(val) + "]  " + message

    def __del__(self):
        self.con.close()

if __name__ == "__main__":

    args = _sqlite_join.ParseCommandLine()

    db_integration = sqlite_join(args.path, args.output)
    sqlite_join.set_sqlite()
