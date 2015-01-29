#!/bin/bash

# DATA_DIR should be set to the absolute path of data

if [ $# -ne 1 ]; then
  echo "$0 _data_dir_"
  exit 1
fi

export DATA_DIR="$1"

export SPARK_MEM=2G
ADD_JARS=./target/mvd-1.0-SNAPSHOT-jar-with-dependencies.jar spark-shell -i shell/shell_init.scala

