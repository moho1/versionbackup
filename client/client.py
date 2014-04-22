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

def dbinit(dbconn):
    c = dbconn.cursor()
    
    c.execute("drop table if exists files")
    c.execute("drop table if exists dirs")

    c.execute("create table files (sum text, filename text, mode integer, uid integer, gid integer, mtime integer )")
    c.execute("create table dirs ( dirname text, mode integer, uid integer, gid integer, mtime integer )")

    dbconn.commit()
    c.close()

def getmeta(filename):
    statobj = os.stat(filename)
    mode = statobj.st_mode
    uid = statobj.st_uid
    gid = statobj.st_gid
    mtime = statobj.st_mtime
    return (mode, uid, gid, mtime)

def insertfile(data, dbcsr):
    dbcsr.execute("insert into files values ( ?, ?, ?, ?, ?, ? )", data)

def insertdir(data, dbcsr):
    dbcsr.execute("insert into dirs values ( ?, ?, ?, ?, ? )", data)

def index(path, dbconn):
    c = dbconn.cursor()
    for root, dirs, files in os.walk(path):
        for name in files:
            filename = root + "/" + name
            print("File:", filename)
            filehash = sha512sum(filename)
            (mode, uid, gid, mtime) = getmeta(filename)
            insertfile((filehash, filename, mode, uid, gid, mtime), c)

        for name in dirs:
            dirname = root + "/" + name
            print("Dir:", dirname)
            (mode, uid, gid, mtime) = getmeta(dirname)
            insertdir((dirname, mode, uid, gid, mtime), c)
    dbconn.commit()
    c.close()
    
#def connectToServer(ip, port, ipIsSocket=False):
#    sockettype = socket.AF_INET
#    if ipIsSocket: sockettype = socket.AF_UNIX
#    s = socket.socket(sockettype, socket.SOCK_STREAM)
#    s.connect((ip, port))
#    return s
#
#def senddata(s, db):
#    dbdata = open(db,"r")
#    s.sendall(dbdata.read())
#    dbdata.close()
#    while True:
#        filename = ""
#        while True:
#            tmp = s.recv(1024)
#            if not data: break
#            filename += tmp
#        if filename == "EOF": break
#        pointer = open(filename, "r")
#        s.sendall(pointer.read())
#        pointer.close()
#
#def connectioncleanup(s):
#    s.close()

if __name__ == "__main__":
    conn = sqlite3.connect("tmp.db")
    dbinit(conn)
    index(sys.argv[1], conn)
    conn.close()


#    tmpdb = "tmp.db"
#    index(sys.argv[1], tmpdb)
#    s = connectToServer()
#    senddata(s, tmpdb)
#    connectioncleanup(s)


    if fail:
        sys.exit(1)
    sys.exit(0)

