#!/bin/bash
# https://gist.github.com/sheharyarn/0f04c1ba18462cddaaf5 
MONGO_DATABASE="mongdb"

MONGO_HOST="127.0.0.1"
MONGO_PORT="27017"
TIMESTAMP=`date +%F-%H%M`
MONGODUMP_PATH="/usr/bin/mongodump"
BACKUPS_DIR="/root/backups"
BACKUP_NAME="mongobackup-$TIMESTAMP"
 
# mongo admin --eval "printjson(db.fsyncLock())"
# $MONGODUMP_PATH -h $MONGO_HOST:$MONGO_PORT -d $MONGO_DATABASE
$MONGODUMP_PATH -d $MONGO_DATABASE
# mongo admin --eval "printjson(db.fsyncUnlock())"
 
mkdir -p $BACKUPS_DIR
mv dump $BACKUP_NAME
tar -zcvf $BACKUPS_DIR/$BACKUP_NAME.tgz $BACKUP_NAME
rm -rf $dock

# Make it executable:

# chmod +x mongobackup.sh
# Schedule a Cronjob:

# sudo su
# crontab -e
# Enter this in a new line:

# # Everyday at 1 a.m.
# 00 01 * * * /bin/bash /root/mongobackup.sh