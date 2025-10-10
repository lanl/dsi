#!/bin/bash
path_to_db=$1
mysql_db_folder=$2
mysql_data_folder=$3

myql_version="mysql-8.4.5-linux-glibc2.28-x86_64"
path_to_sql_db=$path_to_db/$mysql_db_folder
path_to_sql_data=$path_to_db/$mysql_data_folder

# Script to setup mysql in user space
echo "Path to install in: "$path_to_db
echo "MySQL DB Folder: "$path_to_sql_db
echo "MySQL Data Folder: "$path_to_sql_data

echo "mysql: "$myql_version

# Get mysql
wget https://dev.mysql.com/get/Downloads/MySQL-8.0/$myql_version.tar.xz
#https://downloads.mysql.com/archives/community/


# Extract
echo "Decompressing ..."
tar -xJf $myql_version.tar.xz -C $path_to_db

# Rename folder:
mv $path_to_db/$myql_version $path_to_db/$mysql_db_folder

# create a data directory and register it
mkdir -p $path_to_sql_data
$path_to_sql_db/bin/mysqld --initialize-insecure --basedir=$path_to_sql_db --datadir=$path_to_sql_data


# Define output YAML file name
OUTPUT_FILE="sqlalchemy_dsi_config.yaml"

# Create YAML file
cat <<EOF > $OUTPUT_FILE
path_to_db_installation: $path_to_sql_db
path_to_data: $path_to_sql_data
EOF

echo "YAML file '$OUTPUT_FILE' created successfully."

echo "All done!"
# usage: source install_mysql.sh <path_to_db_installation> <folder_name_of_mysql_db> <data folder>
# e.g.   source install_mysql.sh /home/pascalgrosset/projects/alchemy_dsi/test my_sql_db mysql_data
