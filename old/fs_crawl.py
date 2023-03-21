import os
import stat
import sqlite3

# Crawls the root 'filepath' directory and returns files
def dircrawl(filepath):
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

# Returns file permissions
def isgroupreadable(filepath):
  st = os.stat(filepath)
  #print(st)
  print(st.st_mode)
  return bool(st.st_mode & stat.S_IRGRP)

def printpermissions(filepath):
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


#Main

filepath = "H:/src/cosmocompare-master/CosmoCompare/"
file_list = dircrawl(filepath)
#print("The complete set of files are ", file_list)

st_list = []
# Do a quick validation of group permissions
for file in file_list:
  isgroupreadable(file) # quick test
  st = os.stat(filepath)
  st_list.append(st)

print(st_list)

# create new db
con = sqlite3.connect('fs.db')

# write
cur = con.cursor()

#st_mode= # , st_ino= #, st_dev= #, st_nlink= #, st_uid= #, st_gid= #, st_size= #, st_atime= #, st_mtime= #, st_ctime= #
# Add simulation_table
cur.execute('''CREATE TABLE IF NOT EXISTS world
               (st_mode, st_ino, st_dev, st_nlink, st_uid, st_gid, st_size,st_atime,st_mtime,st_ctime )''')

# Add filesystem db data #INSERT OR IGNORE
for st in st_list:
  str_query = "INSERT INTO world VALUES (" + str(st.st_mode) +","+ str(st.st_ino) +","+ str(st.st_dev) +","+ str(st.st_nlink) +","+ str(st.st_uid) +","+ str(st.st_gid) +","+ str(st.st_size) +","+ str(st.st_atime) +","+ str(st.st_mtime) +","+ str(st.st_ctime) + ")"
  print(str_query)
  cur.execute(str_query)

con.commit()
con.close()

