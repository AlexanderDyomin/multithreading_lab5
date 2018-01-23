#!/usr/bin/env bash

sed -i s/my-hadoop-master/$HOSTNAME/ $HADOOP_CONF_DIR/core-site.xml
sed -i s/my-hadoop-master/$HOSTNAME/ $HADOOP_CONF_DIR/yarn-site.xml

start-dfs.sh && start-yarn.sh
start-master.sh && start-slaves.sh
hdfs dfs -mkdir /nasa
hdfs dfs -put Jul /nasa
echo "hadoop cluster is running"
