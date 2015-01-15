#!/usr/bin/env python3

class Indexer(object):
  def __init__(self, dbconn):
    self.dbconn = dbconn
    self.csr = dbconn.cursor()
  
  def __enter__(self):
    return self
  
  def __exit__(self, type, value, traceback):
    self.csr.close()
  
  def index(self, indexdbconn):
    self.indexdbconn = indexdbconn
    self.indexcsr = indexdbconn.cursor()
    
    self.indexcsr.execute('SELECT id, hash, size FROM chunks')
    fetched = 1
    while fetched > 0:
      data = self.indexcsr.fetchmany()
      fetched = len(data)
      for el in data:
        self.insertchunk(el)
    
    setrel = {}
    self.indexcsr.execute('SELECT rowid, host, time, configname FROM backupsets')
    fetched = 1
    while fetched > 0:
      data = self.indexcsr.fetchmany()
      fetched = len(data)
      for el in data:
        (id, host, time, config) = el
        self.csr.execute('INSERT INTO backupsets (host, time, configname) VALUES (%(host)s, %(time)s, %(config)s)',
                         {"host": host, "time": time, "config": config})
        setrel[id] = self.csr.lastrowid
    
    self.indexcsr.execute('SELECT files.hash, files.chunkchain, backupset, name, mode, uid, gid, mtime FROM set2files INNER JOIN files on set2files.file = files.rowid')
    fetched = 1
    while fetched > 0:
      data = self.indexcsr.fetchmany()
      fetched = len(data)
      for el in data:
        self.insertfile(el, setrel)
    
    self.indexcsr.execute('SELECT backupset, name, mode, uid, gid, mtime FROM dirs')
    fetched = 1
    while fetched > 0:
      data = self.indexcsr.fetchmany()
      fetched = len(data)
      for el in data:
        (origsetid, path, mode, uid, gid, mtime) = el
        self.csr.execute('INSERT INTO dirs (backupset, name, mode, uid, gid, mtime) VALUES (%(set)s, %(name)s, %(mode)s, %(uid)s, %(gid)s, %(mtime)s)',
                         {"set": setrel[origsetid], "name": path, "mode": mode, "uid": uid, "gid": gid, "mtime": mtime})
    
    self.indexcsr.execute('SELECT backupset, name, dest, mode, uid, gid, mtime FROM links')
    fetched = 1
    while fetched > 0:
      data = self.indexcsr.fetchmany()
      fetched = len(data)
      for el in data:
        (origsetid, path, dest, mode, uid, gid, mtime) = el
        self.csr.execute('INSERT INTO links (backupset, name, dest, mode, uid, gid, mtime) VALUES (%(set)s, %(name)s, %(dest)s, %(mode)s, %(uid)s, %(gid)s, %(mtime)s)',
                         {"set": setrel[origsetid], "name": path, "dest": dest, "mode": mode, "uid": uid, "gid": gid, "mtime": mtime})
  
  def insertfile(self, elem, setrel):
    (hash, origchunkchain, origsetid, path, mode, uid, gid, mtime) = elem
    self.csr.execute("select count (*) from files where hash=%(hash)s", {"hash":hash})
    (count,) = self.csr.fetchone()
    if count > 0:
      if count >1:
        raise Exception("got a count > 1 for filehash " + hash)
      self.csr.execute("select id from files where hash=%(hash)s", {"hash":hash})
      (fileid,) = self.csr.fetchone()
    else:
      first = True
      workchunk = origchunkchain
      while workchunk != None:
        self.indexcsr.execute("select chunk, nextchunk from chunkchains where rowid=%(chainid)s", {"chainid": workchunk})
        (chunkid, nextchunk) = self.indexcsr.fetchone()
        self.indexcsr.execute("select chunkhash from chunks where rowid=%(chunkid)s", {"chunkid":chunkid})
        (chunkhash,) = self.indexcsr.fetchone()
        self.csr.execute("select id from chunks where hash=%s(hash)s", {"hash": chunkhash})
        (id,) = self.indexcsr.fetchone()
        self.csr.execute("insert into chunkchains (chunk, nextchunk) values (%(chunkid)s)", {"chunkid":id})
        thisid = self.csr.lastrowid
        if not first:
          self.csr.execute("update chunkchains set nextchunk=%(thisid)s where id=%(lastid)s", {"thisid": thisid, "lastid":lastid})
        lastid = thisid
        if first:
          firstid = lastid
        workchunk = nextchunk
      self.csr.execute("insert into files (hash, chunkchain) values (%(hash)s, %(first)s")
      fileid = self.scr.lastrowid
      
    self.csr.execute('INSERT INTO set2files (backupset, file, name, mode, uid, gid, mtime) VALUES (%(set)s, %(fileid)s, %(name)s, %(mode)s, %(uid)s, %(gid)s, %(mtime)s)',
                     {"set": setrel[origsetid], "fileid": fileid, "name": path, "mode": mode, "uid": uid, "gid": gid, "mtime": mtime})
    
        
  def insertchunk(self, elem):
    (origchunkid, hash, size) = elem
    self.csr.execute("select count (*) from chunks where hash=%(hash)s", {"hash":hash})
    (count,) = self.csr.fetchone()
    if count > 0:
      if count >1:
        raise Exception("got a count > 1 for chunkhash " + hash)
      self.csr.execute("select id, present from chunks where hash=%(hash)s", {"hash":hash})
      (chunkid, present) = self.csr.fetchone()
      if present:
        continue
    else:
      self.csr.execute("insert into chunks (hash, size) values (%(hash)s, %(size)s)",
                       {"hash":hash, "size":size})
      self.csr.execute("select id from chunks where hash=%(hash)s", {"hash":hash})
      (chunkid,) = self.csr.fetchone()
    
    self.indexcsr.execute("select host, configname, path, offset, size from chunkorigin where chunk=%(origchunkid)s",
                          {"origchunkid":origchunkid})
    fetched = 1
    while fetched > 0:
      data = self.indexcsr.fetchmany()
      fetched = len(data)
      for el in data:
        (host, configname, path, offset, size) = el
        self.csr.execute("""insert into chunkorigin (chunkid, host, configname, path, offset, size) values 
                            (%(chunkid)s, %(host)s, %(configname)s, %(path)s, %(offset)s, %(size)s)""",
                        {"id":chunkid, "hash":hash, "configname":configname, "path":path, "offset":offset, "size":size})
