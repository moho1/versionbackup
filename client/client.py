#!/usr/bin/python2
# -*- coding: iso-8859-1 -*-
# Author: Moritz Holtz

import os
import hashlib
import sqlite3
            
def index(path):
    """The sha512 hashs are being calculated and inserted into the database
"""
    conn = sqlite3.connect("tmp.db")
    conn.text_factory = str
    c = conn.cursor()
    c.execute("drop table if exists sums")
    c.execute("create table sums (sum text, filename text)")
    for root, dirs, files in os.walk(path):
        for name in files:
            try:
            	data = open(root + "/" + name, "r")
            	print(root + "/" + name)
            	filename = root + "/" + name
            	filehash = hashlib.sha512(data.read()).hexdigest()

            except IOError:
		print("IOError")
                continue

            except MemoryError:
                print("MemoryError")
                continue

            c.execute("insert into sums values ( ?, ? )", (filehash, filename))
            data.close()
    conn.commit()
    conn.close()

