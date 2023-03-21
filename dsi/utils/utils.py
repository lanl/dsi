"""
dsi.utils are sets of generic helper functions used throughout the dsi module.
"""

import os
import stat
import time
import datetime

def dircrawl(filepath):
  """
  Crawls the root 'filepath' directory and returns files

  `filepath`: source filepath to be crawled

  `return`: returns crawled file-list
  """
  file_list = []
  for root, dirs, files in os.walk(filepath):
    #if os.path.basename(filepath) != 'tmp': # Lets skip some files
    #    continue

    for f in files: # Appent root-level files
      file_list.append(os.path.join(root, f))
    for d in dirs: # Recursively dive into directories
      sub_list = dircrawl(os.path.join(root, d))
      for sf in sub_list:
        file_list.append(sf)
    
    return file_list

def epochToDate(time):
    """
    Converts epoch to date

    `time`: unix epoch time

    `return`: returns a human-readable string date
    """
    ymd = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time))
    return ymd

def posixToDate(time):
    """
    Converts posix stamp to date

    `time`: posix time value

    `return`: returns a human-readable string date
    """
    ymd = datetime.datetime.fromtimestamp(time, tz=datetime.timezone.utc)
    return ymd

def dateToPosix(yr,mth,day,hr,min):
    """
    Converts date into posix stamp
    """
    # assigned regular string date
    posixstamp = datetime.datetime(yr,mth,day,hr,min)
    return time.mktime(posixstamp.timetuple())

def isgroupreadable(filepath):
  """
  Returns group file permissions

  `filepath`: text filepath to file

  `return`: boolean of the file's group read state
  """
  st = os.stat(filepath)
  #print(st)
  print(st.st_mode)
  return bool(st.st_mode & stat.S_IRGRP)

def printpermissions(filepath):
  """
  Checks and prints os.stat file permissions

  `filepath`: text filepath to file

  `return`: none
  """
  st = os.stat(filepath)
  #owner
  print( bool(st.st_mode & stat.S_IRUSR) )
  print( bool(st.st_mode & stat.S_IWUSR) )
  print( bool(st.st_mode & stat.S_IXUSR) )
  #group
  print( bool(st.st_mode & stat.S_IRGRP) )
  print( bool(st.st_mode & stat.S_IWGRP) )
  print( bool(st.st_mode & stat.S_IXGRP) )
  #others
  print( bool(st.st_mode & stat.S_IROTH) )
  print( bool(st.st_mode & stat.S_IWOTH) )
  print( bool(st.st_mode & stat.S_IXOTH) )

def check_type(text):
  """
  Tests input text and returns a predicted compatible SQL Type

  `text`: text string

  `return`: string description of a SQL data type
  """
  try:
    value = int(text)
    return " INT"
  except ValueError:
      try:
          value = float(text)
          return " FLOAT"
      except ValueError:
          return " VARCHAR"