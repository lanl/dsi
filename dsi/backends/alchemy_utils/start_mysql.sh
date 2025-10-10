#!/bin/bash
# Launch mysql
path_to_db=$1
path_to_data=$2
port=$3

path_to_db_installati=$path_to_data/$db_name

echo "Path to mysql: "$path_to_db
echo "Path to data: "$path_to_data
echo "Port to run on: "$port


# start 
$path_to_db/bin/mysqld --basedir=$path_to_db --datadir=$path_to_data --socket=$path_to_db/mysql.sock --pid-file=$path_to_db/mysql.pid --port=$port &

ps aux | grep mysqld


echo "All done!"
# usage: source start_mysql.sh <path_to_db_installation> <path_to_data>  <port>
#  e.g. source start_mysql.sh /home/pascalgrosset/projects/alchemy_dsi/test/my_sql_db /home/pascalgrosset/projects/alchemy_dsi/test/mysql_data 3310
