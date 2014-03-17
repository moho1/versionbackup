#!/usr/bin/python
# Author: Moritz Holtz

import os
import sys
import hashlib
import sqlite3
import socket
import functools

fail=False

def sha512sum(filename):
    with open(filename, mode='rb') as f:
        d = hashlib.sha512()
        for buf in iter(functools.partial(f.read, 4096), b''):
            d.update(buf)
    return d.hexdigest()

def index(path, tmpdb):
    """The sha512 hashs are being calculated and inserted into the database
"""
    conn = sqlite3.connect(tmpdb)
    c = conn.cursor()

    c.execute("drop table if exists files")
    c.execute("drop table if exists dirs")

    c.execute("create table files (sum text, filename text, mode integer, uid integer, gid integer, mtime integer )")
    c.execute("create table dirs ( dirname text, mode integer, uid integer, gid integer, mtime integer )")

    for root, dirs, files in os.walk(path):
        for name in files:
            filename = root + "/" + name
            try:
                print(filename)
                filehash = sha512sum(filename)
                statobj = os.stat(filename)
                mode = statobj.st_mode
                uid = statobj.st_uid
                gid = statobj.st_gid
                mtime = statobj.st_mtime

            except IOError:
#               print("IOError")
                sys.__stderr__.write("IOError in file " + filename + "\n")
                fail=True
                continue

            except MemoryError:
#                print("MemoryError")
                sys.__stderr__.write("MemoryError in file " + filename + "\n")
                fail=True
                continue
            
            c.execute("insert into files values ( ?, ?, ?, ?, ?, ? )", (filehash, filename, mode, uid, gid, mtime))

        for name in dirs:
            dirname = root + "/" + name
            statobj = os.stat(dirname)
            mode = statobj.st_mode
            uid = statobj.st_uid
            gid = statobj.st_gid
            mtime = statobj.st_mtime
            c.execute("insert into dirs values ( ?, ?, ?, ?, ? )", (dirname, mode, uid, gid, mtime))
    conn.commit()
    conn.close()

def connectToServer(ip, port, ipIsSocket=False):
    sockettype = socket.AF_INET
    if ipIsSocket: sockettype = socket.AF_UNIX
    s = socket.socket(sockettype, socket.SOCK_STREAM)
    s.connect((ip, port))
    return s

def senddata(s, db):
    dbdata = open(db,"r")
    s.sendall(dbdata.read())
    dbdata.close()
    while True:
        filename = ""
        while True:
            tmp = s.recv(1024)
            if not data: break
            filename += tmp
        if filename == "EOF": break
        pointer = open(filename, "r")
        s.sendall(pointer.read())
        pointer.close()

def connectioncleanup(s):
    s.close()

if __name__ == "__main__":
    tmpdb = "tmp.db"
    index(sys.argv[1], tmpdb)
#    s = connectToServer()
#    senddata(s, tmpdb)
#    connectioncleanup(s)


    if fail:
        sys.exit(1)
    sys.exit(0)

