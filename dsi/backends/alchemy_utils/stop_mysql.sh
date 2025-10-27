#!/bin/bash
# Launch mysql
path_to_db=$1
echo "-PPPath to mysql: "$path_to_db

# stop
$path_to_db/bin/mysqladmin -u root --socket=$path_to_db/mysql.sock shutdown

echo "All done!"
# usage: source stop_mysql.sh <path_to_db_installation>
#  e.g.  source stop_mysql.sh /home/pascalgrosset/projects/alchemy_dsi/test/my_sql_db