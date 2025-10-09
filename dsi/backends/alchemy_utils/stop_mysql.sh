#!/bin/bash
# Launch mysql
path_to_db=$1
echo "Path to mysql: "$path_to_db

# stop
$path_to_db/bin/mysqladmin -u root -p --socket=$path_to_db/mysql.sock shutdown


# usage: source stop_mysql.sh <path_to_db_installation>
#  e.g.  source stop_mysql.sh /home/pascalgrosset/projects/alchemy_dsi/test/my_sql_db 