#!/usr/bin/env python

"""Simple HTTP Server With Upload.

This module builds on BaseHTTPServer by implementing the standard GET
and HEAD requests in a fairly straightforward manner.

"""
# encoding: utf-8
#from __future__ import print_function


__version__ = "0.1"
__all__ = ["SimpleHTTPRequestHandler"]
__author__ = "bones7456"
__home_page__ = "http://li2z.cn/"

import os
import posixpath
import BaseHTTPServer
import cgi
import shutil
import mimetypes
import urllib
import re
import argparse
import json
from collections import namedtuple
import hashlib
from urlparse import urlparse, parse_qs
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
from poster.encode import multipart_encode
from poster.streaminghttp import register_openers
import urllib2
import sys
import cgi
from threading import Timer

register_openers()

def json2obj(data): return json.JSONDecoder().decode(data)

def read_files():
    if os.path.exists("files"):
        with open("files") as f:
            return json2obj(f.read())
    else:
        return []

def write_files(files):
  with open("files", "w") as f:
    f.write(json.JSONEncoder().encode(files))


our_files = read_files()
our_chunk_locations_ = {}
our_chunkservers = []
__data_dir__ = ""

def get_chunk_locations():
    return our_chunk_locations_

def set_chunk_locations(cl):
    global our_chunk_locations_
    our_chunk_locations_ = cl

def get_chunkserver(chunk_id):
    if id in our_chunk_locations_:
        return our_chunk_locations_[chunk_id]
    return None

class MasterRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_POST(self):
        ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
        if ctype == 'multipart/form-data':
            postvars = cgi.parse_multipart(self.rfile, pdict)
        elif ctype == 'application/x-www-form-urlencoded':
            length = int(self.headers.getheader('content-length'))
            postvars = cgi.parse_qs(self.rfile.read(length), keep_blank_values=1)
        if 'id' in postvars and 'chunks' in postvars:
            reporter = postvars['id'][0]
            chunks = postvars['chunks'][0]

            new_chunk_locations = {}
            for chunk_id, chunkserver in get_chunk_locations().iteritems():
                if chunkserver != reporter:
                    new_chunk_locations[chunk_id] = chunkserver
            for chunk in chunks.split():
                new_chunk_locations[chunk] = reporter

            set_chunk_locations(new_chunk_locations)
            
            global our_chunkservers
            if not reporter in our_chunkservers:
                our_chunkservers.append(reporter)
            self.send_response(200)
        else:
            self.send_response(400, 'Please specify chunkserver id and chunk list')

    def do_GET(self):
        parsed_path = urlparse(self.path)
        if parsed_path.path == "/new_chunk":
            if len(our_chunkservers) == 0:
                self.send_response(404, "No registered chunk servers. No one can write")
                return

            query_components = parse_qs(parsed_path.query)
            if not "f" in query_components:
                self.send_response(400, "Please specify 'f' argument")
                return
            filename = query_components["f"][0] 
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()

            chunkserver = hash(filename) % len(our_chunkservers)

            existing_chunks = []
            for file in our_files:
                if file["name"] == filename:
                    existing_chunks = file["chunks"]

            chunk_id = "%s_%d" % (hashlib.md5(filename).hexdigest(), len(existing_chunks))
            if len(existing_chunks) == 0:                
                our_files.append({"name": filename, "chunks": existing_chunks})
            existing_chunks.append(chunk_id)

            write_files(our_files)
            self.wfile.write("%s %s" % (our_chunkservers[chunkserver], chunk_id))

        if parsed_path.path == "/files":            
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(json.JSONEncoder().encode(our_files))
            return
        if parsed_path.path == "/chunk_locations":            
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()

            self.wfile.write(json.JSONEncoder().encode(
                [{"id": chunk_id, "chunkserver": chunkserver} for chunk_id, chunkserver in get_chunk_locations().iteritems()]))
            return            

def send_heartbeat():
    try:
        list = os.listdir(__data_dir__)
    except os.error as e:
        print "ERROR: can't list directory %s: %s" % (__data_dir__, str(e))
        return False
    list.sort(key=lambda a: a.lower())
    datagen, headers = multipart_encode({"chunks": "\n".join(list), "id": __chunkserver_url__})
    request = urllib2.Request("http://%s/heartbeat" % __master_url__, datagen, headers)
    response = urllib2.urlopen(request)
    if response.getcode() != 200:
        sys.stderr.write("Heartbeat failed: %s" % str(response))
        return False
    return True

class ChunkServerRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    """Simple HTTP request handler with GET/HEAD/POST commands.

    This serves files from the current directory and any of its
    subdirectories.  The MIME type for files is determined by
    calling the .guess_type() method. And can reveive file uploaded
    by client.

    The GET/HEAD/POST requests are identical except that the HEAD
    request omits the actual contents of the file.

    """

    server_version = "SimpleHTTPWithUpload/" + __version__

    def do_GET(self):
        """Serve a GET request."""
        f = self.send_head()
        if f:
            shutil.copyfileobj(f, self.wfile)
            f.close()

    def do_HEAD(self):
        """Serve a HEAD request."""
        f = self.send_head()
        if f:
            f.close()

    def do_POST(self):
        """Serve a POST request."""
        parsed_path = urlparse(self.path)
        if parsed_path.path != "/write":
            self.send_response(400)

        r, info = self.deal_post_data()
        if r:
            if send_heartbeat():
                self.send_response(200)
            else:
                self.send_response(500, "ERROR: failed to report new chunk to master. Pleae retry writing")
        else:
            self.send_response(500, info)
        self.end_headers()
        
    def deal_post_data(self):
        ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
        if ctype == 'multipart/form-data':
            postvars = cgi.parse_multipart(self.rfile, pdict)
        elif ctype == 'application/x-www-form-urlencoded':
            length = int(self.headers.getheader('content-length'))
            postvars = cgi.parse_qs(self.rfile.read(length), keep_blank_values=1)
        if "chunk_id" in postvars and "data" in postvars:
            path = self.translate_path(postvars["chunk_id"][0])
            try:
                out = open(path, 'wb')
                out.write(postvars["data"][0])
                out.close()
                return (True, "OK")
            except IOError as e:
                print e
                return (False, "Can't create file to write, do you have permission to write?")
                    

    def send_head(self):
        """Common code for GET and HEAD commands.

        This sends the response code and MIME headers.

        Return value is either a file object (which has to be copied
        to the outputfile by the caller unless the command was HEAD,
        and must be closed by the caller under all circumstances), or
        None, in which case the caller has nothing further to do.

        """
        parsed_path = urlparse(self.path)
        if parsed_path.path != "/read":
            self.send_response(400, "Unknown action: %s" % parsed_path.path)

        query_components = parse_qs(parsed_path.query)
        if "id" not in query_components:
            self.send_response(400, "id argument is expected")

        path = self.translate_path(query_components["id"][0])
        f = None
        if os.path.isdir(path):
            self.send_response(400, "Path is a directory")
            return None
        try:
            # Always read in binary mode. Opening files in text mode may cause
            # newline translations, making the actual size of the content
            # transmitted *less* than the content-length!
            f = open(path, 'rb')
        except IOError:
            self.send_error(404, "File not found")
            return None
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        fs = os.fstat(f.fileno())
        self.send_header("Content-Length", str(fs[6]))
        self.send_header("Last-Modified", self.date_time_string(fs.st_mtime))
        self.end_headers()
        return f


    def translate_path(self, path):
        """Translate a /-separated PATH to the local filename syntax.

        Components that mean special things to the local file system
        (e.g. drive or directory names) are ignored.  (XXX They should
        probably be diagnosed.)

        """
        # abandon query parameters
        path = path.split('?',1)[0]
        path = path.split('#',1)[0]
        path = posixpath.normpath(urllib.unquote(path))
        words = path.split('/')
        words = filter(None, words)
        path = os.getcwd() + "/" + __data_dir__
        for word in words:
            drive, word = os.path.splitdrive(word)
            head, word = os.path.split(word)
            if word in (os.curdir, os.pardir): continue
            path = os.path.join(path, word)
        return path

parser = argparse.ArgumentParser()
parser.add_argument("--role", required = False,  default = 'master')
parser.add_argument("--chunkserver", required = False, default = 'localhost')
parser.add_argument("--master", required = False)
parser.add_argument("--port", required = False, default = 8000)
parser.add_argument("--data", required = False, default = "")
args = parser.parse_args()

server_address = ('', int(args.port))
if args.role == 'master':
    httpd = BaseHTTPServer.HTTPServer(server_address, MasterRequestHandler)
elif args.role == 'chunkserver':
    if args.master is None:
        raise Exception("Please specify master address")
    __master_url__ = args.master
    __chunkserver_url__ = "%s:%d" % (args.chunkserver, int(args.port))
    httpd = BaseHTTPServer.HTTPServer(server_address, ChunkServerRequestHandler)
    __data_dir__ = args.data
    send_heartbeat()
    Timer(30, send_heartbeat, ()).start()

httpd.serve_forever()

