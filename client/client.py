#!/usr/bin/python
# Author: Moritz Holtz

import os
import sys
import hashlib
import sqlite3
import socket
import functools

fail=False

class Index:
	
	dbconn = None
	dbcursor = None
	path = ""

	def __init__(self, path, dbconn):
		self.dbconn = dbconn
		self.path = path
		self.dbinit()

	def __destroy__(self):
		self.dbclose()

	def sha512sum(self, filename):
		with open(filename, mode='rb') as f:
			d = hashlib.sha512()
			for buf in iter(functools.partial(f.read, 4096), b''):
				d.update(buf)
		return d.hexdigest()

	def dbinit(self):
		self.dbcursor = self.dbconn.cursor()
		
		self.dbcursor.execute("drop table if exists files")
		self.dbcursor.execute("drop table if exists dirs")

		self.dbcursor.execute("create table files (sum text, filename text, mode integer, uid integer, gid integer, mtime integer )")
		self.dbcursor.execute("create table dirs ( dirname text, mode integer, uid integer, gid integer, mtime integer )")

	def getmeta(self, filename):
		statobj = os.stat(filename)
		mode = statobj.st_mode
		uid = statobj.st_uid
		gid = statobj.st_gid
		mtime = statobj.st_mtime
		return (mode, uid, gid, mtime)

	def index(self):
		for root, dirs, files in os.walk(self.path):
			for name in files:
				filename = root + "/" + name
				print("File:", filename)
				filehash = self.sha512sum(filename)
				(mode, uid, gid, mtime) = self.getmeta(filename)
				self.dbcursor.execute("insert into files values ( ?, ?, ?, ?, ?, ? )", (filehash, filename, mode, uid, gid, mtime))

			for name in dirs:
				dirname = root + "/" + name
				print("Dir:", dirname)
				(mode, uid, gid, mtime) = self.getmeta(dirname)
				self.dbcursor.execute("insert into dirs values ( ?, ?, ?, ?, ? )", (dirname, mode, uid, gid, mtime))
		self.dbconn.commit()
	
	def dbclose(self):
		self.dbconn.close()

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
	conn = sqlite3.connect("tmp.db")
	indexobj = Index(sys.argv[1], conn)
	indexobj.index()


#	tmpdb = "tmp.db"
#	index(sys.argv[1], tmpdb)
#	s = connectToServer()
#	senddata(s, tmpdb)
#	connectioncleanup(s)


	if fail:
		sys.exit(1)
	sys.exit(0)

