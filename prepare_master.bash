#!/bin/bash

master_name=$(docker ps --filter "name=my-hadoop-master" --format "{{.Names}}")
spark_home="/usr/local/spark"
hadoop_conf_dir="/usr/local/hadoop/etc/hadoop"

docker cp "init_master.bash" "$master_name:/root/"
docker cp "slaves" "$master_name:$spark_home/conf/"
docker cp "slaves" "$master_name:$hadoop_conf_dir/"
docker cp over_yarn.bash $master_name:/root/
docker cp standalone.bash $master_name:/root/
docker cp code/task2.py $master_name:/root/
docker cp code/task3.py $master_name:/root/
docker cp code/task4.py $master_name:/root/
