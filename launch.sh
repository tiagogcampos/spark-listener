#!/bin/bash

if [ -z $SPARK_HOME ] then
  SPARK_HOME=/Users/tiagocampos/Software/spark-3.3.2-bin-hadoop3/
fi

java -classpath ./target/spark-launcher-test-1.0-SNAPSHOT.jar:$SPARK_HOME/jars/spark-launcher_2.12-3.3.2.jar com.tiagocampos.Launcher ./main.py 2> error.log
