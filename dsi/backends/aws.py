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
class AWS(Backend):
   def __init__(self, region, id, aws_files, role=None):
        """
        Initializes AWS backend
        
        'region': The aws region
        'id': The aws id to use
        `aws_files`: list of files with aws file paths
        'role': The desired role to use
        """
        self.aws_info = OrderedDict()
        self.aws_file_info = OrderedDict()
        self.aws_info['region'] = region
        self.aws_info['id'] = id
        self.aws_info['role'] = role
        self.aws_info['profile'] = None
        for aws_file in aws_files.keys():
           self.aws_file_info[aws_file] = {
              'local_path': aws_files[aws_file],
           }

        self.run_aws_saml()
        
   def git_commit_sha(self):
      pass


   def put(self, local_file, remote_file) -> bool:
      """
      Puts a local file/directory on AWS
      """

      result = self.aws_copy(local_file, remote_file)

      return result
  
   def get(self, remote_file, local_file) -> bool:

      result = self.aws_copy(remote_file, local_file)

      return result
  
   # OLD NAME OF ingest_artifacts(). TO BE DEPRECATED IN FUTURE DSI RELEASE
   def put_artifacts(self, collection, isVerbose=False):
      return self.ingest_artifacts(collection, isVerbose)

   def ingest_artifacts(self, collection, isVerbose=False):
      for f in self.aws_info.keys():
          self.put(self.aws_info[f]['local_path'], f)

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

   def run_aws_saml(self):
      command = ["aws-saml-cmd"]
      arg_list = ['-r', self.aws_info['region'], '-id', self.aws_info['id']]
      if self.aws_info['role'] is not None:
          arg_list += ['-ro', self.aws_info['role']]
          
      command += arg_list
      
      stdout = ""
      stderr = ""
      returncode = -1
      try:
         process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
         stdout, stderr = process.communicate()
         returncode = process.communicate()
      except FileNotFoundError as e:
         print("Error running aws-saml-cmd: %s" % e)

      print(stdout)
      if len(stdout) > 0:
          for line in stdout.splitlines():
              line = line.strip()

              if "Configured aws profile:" not in line:
                  continue

              tokens = line.split(': ')
              if len(tokens) != 2:
                  continue
              self.aws_info['profile'] = tokens[1]
#              print(self.aws_info)
              
      return stdout, stderr, returncode

   def run_aws(self, subcmd, arg_list):
      """
      Runs aws command wth the supplied subcmd and arguments
      """

      profile = self.aws_info['profile']
      command = ["aws", "--profile", profile, "s3", subcmd]
      command += arg_list

      stdout = ""
      stderr = ""
      returncode = -1
      try:
         process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
         
         stdout, stderr = process.communicate()
         returncode = process.communicate()
      except FileNotFoundError as e:
         print("Error running aws: %s" % e)
         
      return stdout, stderr, returncode

   def aws_sync(self, source_dir, dest_dir) -> bool:
      """
      Syncs a directory to or from aws
      source_dir: a local directory or an s3:// path
      dest_dir: a local directory or an s3:// path

      returns: True for success or False on failure
      """

      result = True
      stdout, stderr, returncode = self.run_aws('sync', [source_dir, dest_dir])
      print(stdout)
      if returncode:
          print(stderr)
          result = False

      return result

   def aws_copy(self, source_file, dest_file) -> bool:
      """
      Copies an aws remote file to a local directory
      source_file: a local file or an s3:// path
      dest_file: a local file or an s3:// path

      returns: True for success or False on failure
      """

      result = True
      stdout, stderr, returncode = self.run_aws('cp', [source_file, dest_file])
      print(stdout)
      if returncode:
          result = False
          print(stderr)

      return result
