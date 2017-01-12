#!/bin/bash

now=`date "+%G-%m-%d %H:%M:%S"`
d=`date -d "-1 day" +%Y%m%d`
echo $d

statisticsPath=/server/app/test/spark_demo/lib
logPath=/server/logs/spark_project

export LIB_PATH=$statisticsPath/*.jar


jarpaths=`echo $LIB_PATH | sed -e 's/ /,/g'`

##echo ---------------------
##echo $jarpaths
spark-submit --name spark_demo_test \
--master yarn --class com.maming.spark.temple.examples.SparkPiTest \
--jars $jarpaths \
--verbose \
/server/app/test/spark_demo/job/spark_demo.jar $* 1>>$logPath/appout.log 2>>$logPath/apperr.log


echo ending
