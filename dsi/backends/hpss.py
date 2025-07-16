import sqlite3
import re
import subprocess
from datetime import datetime
import textwrap
import os
import textwrap
import base64
import random

from hashlib import sha1
from dsi.backends.filesystem import Backend
from collections import OrderedDict

# HPSS backend class
class HPSS(Backend):
   def __init__(self, hpss_files):
        """
        Initializes an HPSS backend
        
        `hpss_files`: list with hpss file paths

        """
        self.hpss_info = OrderedDict()
        for hpss_file in hpss_files.keys():
           self.hpss_info[hpss_file] = {
              'local_path': hpss_files[hpss_file],
              'hpss_hash': None, 
           }
           stdout, stderr, _ = self.run_hsi("hashlist", [hpss_file])
           hpss_hash = self.parse_hpss_hash(stdout, stderr)
           self.hpss_info[hpss_file]['hpss_hash'] = hpss_hash
              
   def git_commit_sha(self):
      pass

   def create_hpss_hash(self, hpss_file) -> str:
      """
      Creates and HPSS hash
      """

      stdout, stderr, returncode = self.run_hsi("hashcreate", [hpss_file])
      if returncode != 0:
         print(stderr)
         return None
     
      hash = self.parse_hpss_hash(stdout, stderr)
      return hash

   def put(self, local_file, hpss_dest) -> bool:
      """
      Puts a local file on HPSS
      """

      cwd = os.getcwd()
      new_dir = None
      file_to_put = local_file
      if '/' in local_file:
        new_dir = '/'.join(local_file.split('/')[:-1])
        os.chdir(new_dir)
        file_to_put = local_file.split('/')[-1]

      stdout, stderr, returncode = self.run_hsi("put", [file_to_put])
      if new_dir is not None:
        os.chdir(cwd)

      if returncode == 0:
          hash = create_hpss_hash(file_to_put)
          return True
      
      return False

   def get(self, hpss_file, tmp_dir) -> bool:
      """
      Gets an HPSS file and puts it in the tmp_dir
      """
      cwd = os.getcwd()
      try:
         os.chdir(tmp_dir)
      except:
         print("Error changing to temp dir: %s" % tmp_dir)
         return False

      stdout, stderr, returncode = self.run_hsi("get", local_file)
      try:
        os.chdir(cwd)
      except:
         print("Error changing to dir: %s" % cwd)
        
      if returncode == 0:
         return True

      return False
   
   def parse_hpss_hash(self, stdout, stderr) -> str:
      """
      Parses the result of an HPSS hash command
      """

      output = stdout + stderr
      hash = None
      for line in output.splitlines():
         if " md5" not in line:
            continue

         line = line.strip()
         matches = re.search(r'(\S+)\s+(\S+)\s+(\S+).*', line)
         if not matches:
            continue

         if len(matches.groups()) == 3:
            hash = matches.group(1)
            break
            
      return hash
  
   # OLD NAME OF ingest_artifacts(). TO BE DEPRECATED IN FUTURE DSI RELEASE
   def put_artifacts(self, collection, isVerbose=False):
      return self.ingest_artifacts(collection, isVerbose)

   def ingest_artifacts(self, collection, isVerbose=False):
      for f in self.hpss_info.keys():
          self.put(self.hpss_info[f]['local_path'], f)

    # DEPRECATING IN FUTURE DSI RELEASE. USE query_artifacts()
   def get_artifacts(self, query, kwargs):
      pass

   def query_artifacts(self, query, kwargs):
      pass

    # DEPRECATING IN FUTURE DSI RELEASE. USE notebook()
   def inspect_artifacts(self, kwargs):
      pass

   def notebook(self, kwargs):
      pass

    # DEPRECATING IN FUTURE DSI RELEASE. USE process_artifacts()
   def read_to_artifacts(self, kwargs):
      pass

   def process_artifacts(self, kwargs):
      pass

   def find(self, query_object, kwargs):
      pass

   def find_table(self, query_object, kwargs):
      pass

   def find_column(self, query_object, kwargs):
      pass

   def find_cell(self, query_object, kwargs):
      pass

   def close(self):
      pass

   def run_hsi(self, subcmd, arg_list):
      """
      Runs hsi wth the supplied subcmd and arguments
      """

      command = ["hsi", subcmd]
      command += arg_list
      
      stdout = ""
      stderr = ""
      returncode = -1
      try:
         process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='latin-1')
         
         stdout, stderr = process.communicate()
         returncode = process.communicate()
      except FileNotFoundError as e:
         print("Error running hsi: %s" % e)
         
      return stdout, stderr, returncode

