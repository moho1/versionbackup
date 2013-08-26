#!/usr/bin/python2
# -*- coding: iso-8859-1 -*-
# Author: Moritz Holtz

import os
import sys
import hashlib
import sqlite3

def index(path):
    """The sha512 hashs are being calculated and inserted into the database
"""
    conn = sqlite3.connect("tmp.db")
    c = conn.cursor()
    c.execute("drop table if exists sums")
    c.execute("create table sums (sum text, filename text)")
    for root, dirs, files in os.walk(path):
        for name in files:
            filename = root + "/" + name
            try:
                data = open(filename, "r")
                print(filename)
                filehash = hashlib.sha512(data.read()).hexdigest()

            except IOError:
                print("IOError")
                return

            except MemoryError:
                print("MemoryError")
                return

            c.execute("insert into sums values ( ?, ? )", (filehash, buffer(filename)))
            data.close()
    conn.commit()
    conn.close()

if __name__ == "__main__":
    index(sys.argv[1])

