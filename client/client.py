#!/usr/bin/env python3
# Author: Moritz Holtz

import os
import sys
import hashlib
import sqlite3
import stat
import time
import lzma

class Backuper(object):
  def __init__(self, config, dbconn):
    self.dbconn = dbconn
    self.csr = dbconn.cursor()
    self.dbinit()
    self.config = config
    self.setid = self.newset()
  
  def __enter__(self):
    return self
  
  def __exit__(self, type, value, traceback):
    self.csr.close()
  
  def index(self, path):
    for root, dirs, files in os.walk(path):
      for name in files:
        filename = (os.path.join(root, name)).encode("utf-8", "replace")
        print("File: " + filename.decode("utf-8"))
        self.indexfile(filename)

      for name in dirs:
        dirname = (os.path.join(root, name)).encode("utf-8", "replace")
        print("Dir: " + dirname.decode("utf-8"))
        try:
          (mode, uid, gid, mtime) = getmeta(dirname)
        except FileNotFoundError:
          print("Could not find file \"" + str(filename) + "\", maybe it was deleted or it has unprintable characters?")
          continue
        except PermissionError:
          print("Could not read file \"" + str(filename) + "\", permission denied.")
          continue
        except KeyboardInterrupt:
          print("Caught Keyboard Interrupt, shutting down!")
          sys.exit(1)
        except:
          print("Something strange happend with file \"" + str(filename) + "\".")
          continue
        self.insertdir(dirname, mode, uid, gid, mtime)
    self.dbconn.commit()

  def indexfile(self, filename):
    try:
      (mode, uid, gid, mtime) = getmeta(filename)
    except FileNotFoundError:
      print("Could not find file \"" + str(filename) + "\", maybe it was deleted or it has unprintable characters?")
      return
    except PermissionError:
      print("Could not read file \"" + str(filename) + "\", permission denied.")
      return
    except KeyboardInterrupt:
      print("Caught Keyboard Interrupt, shutting down!")
      sys.exit(1)
    except:
      print("Something strange happend with file \"" + str(filename) + "\".")
      return
    if not stat.S_ISREG(mode):
      if not stat.S_ISLNK(mode):
        return
      try:
        self.insertlink(filename, os.readlink(filename), mode, uid, gid, mtime)
      except FileNotFoundError:
        print("Could not find file \"" + str(filename) + "\", maybe it was deleted or it has unprintable characters?")
        return
      except PermissionError:
        print("Could not read file \"" + str(filename) + "\", permission denied.")
        return
      except KeyboardInterrupt:
        print("Caught Keyboard Interrupt, shutting down!")
        sys.exit(1)
      except:
        print("Something strange happend with file \"" + str(filename) + "\".")
        return
      return
    try:
      with open(filename, mode='rb') as f:
        allhash = hashlib.sha512()
        chunkhash = hashlib.sha512()
        read=1
        toread = self.config["chunksize"]
        firstchunk = None
        last = None
        while read>0:
          data = f.read(min(self.config["readsize"], toread))
          read = len(data)
          
          allhash.update(data)
          chunkhash.update(data)
          
          toread-=read
          if toread == 0:
            last = self.insertchunk(filename, chunkhash.hexdigest(), last, f.tell()-(self.config["chunksize"]-toread), (self.config["chunksize"]-toread))
            if firstchunk == None: firstchunk=last
            chunkhash = hashlib.sha512()
            toread = self.config["chunksize"]
            
        chunkhash.hexdigest()
        last = self.insertchunk(filename, chunkhash.hexdigest(), last, f.tell()-(self.config["chunksize"]-toread), (self.config["chunksize"]-toread))
        if firstchunk == None: firstchunk=last
        
        self.insertfile(filename, allhash.hexdigest(), firstchunk, mode, uid, gid, mtime)
      
    except FileNotFoundError:
      print("Could not find file \"" + str(filename) + "\", maybe it was deleted or it has unprintable characters?")
      return
    except PermissionError:
      print("Could not read file \"" + str(filename) + "\".")
      return
    except KeyboardInterrupt:
      print("Caught Keyboard Interrupt, shutting down!")
      sys.exit(1)
    except:
      print("Something strange happend with file \"" + str(filename) + "\".")
      return

  def insertfile(self, filename, allhash, firstchunk, mode, uid, gid, mtime):
    self.csr.execute("insert into files (hash, chunkchain) values (:hash, :chunkchain)",
                     {"hash": allhash, "chunkchain": firstchunk})
    fileid = self.csr.lastrowid
    self.csr.execute("insert into set2files (backupset, file, name, mode, uid, gid, mtime) values (:setid, :fileid, :name, :mode, :uid, :gid, :mtime)",
                     {"setid": self.setid, "fileid": fileid, "name": filename, "mode": mode, "uid": uid, "gid": gid, "mtime": mtime})
  
  def insertchunk(self, filename, chunkhash, lastchunk, offset, size):
    self.csr.execute("insert into chunks (hash) values (:hash)",
                     {"hash": chunkhash})
    chunkid = self.csr.lastrowid
    self.csr.execute("insert into chunkorigin (chunk, name, offset, size) values (:chunk, :name, :offset, :size)",
                     {"chunk": chunkid, "name": filename, "offset": offset, "size":size})
    self.csr.execute("insert into chunkchains (chunk, nextchunk) values (:chunkid, NULL)",
                     {"chunkid": chunkid})
    chainid = self.csr.lastrowid
    if lastchunk != None:
      self.csr.execute("update chunkchains set nextchunk=:newchunkid where chunk=:lastchunkid",
                       {"newchunkid": chainid, "lastchunkid": lastchunk})
    return chainid
  
  def insertlink(self, filename, dest, mode, uid, gid, mtime):
    self.csr.execute("insert into links (backupset, name, dest, mode, uid, gid, mtime) values (:setid, :name, :dest, :mode, :uid, :gid, :mtime)",
                     {"setid": self.setid, "name": filename, "dest": dest, "mode": mode, "uid": uid, "gid": gid, "mtime": mtime})
    
  def insertdir(self, dirname, mode, uid, gid, mtime):
    self.csr.execute("insert into dirs (backupset, name, mode, uid, gid, mtime) values (:setid, :name, :mode, :uid, :gid, :mtime)",
                     {"setid": self.setid, "name": dirname, "mode": mode, "uid": uid, "gid": gid, "mtime": mtime})
  
  def dbinit(self):
    self.csr.execute("create table if not exists files ( hash text, chunkchain integer )")
    self.csr.execute("create table if not exists dirs ( backupset integer, name text, mode integer, uid integer, gid integer, mtime integer )")
    self.csr.execute("create table if not exists links ( backupset integer, name text, dest text, mode integer, uid integer, gid integer, mtime integer )")
    self.csr.execute("create table if not exists chunks ( hash text )")
    self.csr.execute("create table if not exists chunkorigin ( chunk integer, name text, offset integer, size integer )")
    self.csr.execute("create table if not exists chunkchains ( chunk integer, nextchunk integer )")
    self.csr.execute("create table if not exists backupsets ( host text, time datetime default current_timestamp, configname text )")
    self.csr.execute("create table if not exists set2files ( backupset integer, file integer, name text, mode integer, uid integer, gid integer, mtime integer )")

    self.dbconn.commit()
  
  def newset(self):
    self.csr.execute("insert into backupsets (host, time, configname) values (:host, datetime(:time, 'unixepoch'), :configname) ",
                     {"host":self.config["host"], "time":int(time.time()), "configname":self.config["name"]})
    return self.csr.lastrowid
  
  def dbdump(self):
    dump = ''
    for line in self.dbconn.iterdump():
      dump += line + '\n'
    return dump

def getmeta(filename):
  statobj = os.lstat(filename)
  mode = statobj.st_mode
  uid = statobj.st_uid
  gid = statobj.st_gid
  mtime = statobj.st_mtime
  return (mode, uid, gid, mtime)

def sendmeta(data, server, port, compression=2):
  ### Connect here to the server
  socket = None
  
  if compression == None:
    socket.sendall(data)
  else:
    comp = lzma.LZMACompressor(check=lzma.CHECK_SHA256, preset=compression)
    socket.sendall(comp.compress(data))
    socket.sendall(comp.flush())
  
  ### Terminate the connection

if __name__ == "__main__":
  conn = sqlite3.connect(":memory:")
  bak = Backuper({"chunksize":256*1024, "readsize":4*1024, "host":"turing", "name":"test"}, conn)
  for root in sys.argv[1:-1]:
    bak.index(root)
  
  
  with open(sys.argv[-1], 'w') as f: 
    for line in conn.iterdump():
      f.write('%s\n' % line)
  
  """
  data = bak.dbdump()
  sendmeta(data, "fooserver", barport, 9)
  """







