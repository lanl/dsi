#!/bin/bash
# Create a database and user for the data

path_to_db=$1
database=$2
user=$3
password=$4

echo "Path to mysql: "$path_to_db
echo "database to create: "$database
echo "user to create: "$user
echo "password for user: "$password


$path_to_db/bin/mysql -u root --socket=$path_to_db/mysql.sock -e "CREATE DATABASE $database";
$path_to_db/bin/mysql -u root --socket=$path_to_db/mysql.sock -e "SHOW DATABASES";

$path_to_db/bin/mysql -u root --socket=$path_to_db/mysql.sock -e "CREATE USER '$user'@'localhost' IDENTIFIED BY '$password'";
$path_to_db/bin/mysql -u root --socket=$path_to_db/mysql.sock -e "GRANT ALL PRIVILEGES ON $database.* TO '$user'@'localhost'"; # add permission
$path_to_db/bin/mysql -u root --socket=$path_to_db/mysql.sock -e "FLUSH PRIVILEGES"; 
$path_to_db/bin/mysql -u root --socket=$path_to_db/mysql.sock -e "SHOW GRANTS FOR '$user'@'localhost'";  # check permissions"
$path_to_db/bin/mysql -u root --socket=$path_to_db/mysql.sock -e "USE $database";  # check permissions"

echo "All done!"
# usage: source create_db_user.sh <path_to_db> <database> <user> <password>
#   e.g. source create_db_user.sh /home/pascalgrosset/projects/alchemy_dsi/test/my_sql_db sample1 dev_user1 dev_passwd
