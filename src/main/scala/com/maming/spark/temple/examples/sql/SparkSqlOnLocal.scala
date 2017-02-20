package com.maming.spark.temple.examples.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}


object SparkSqlOnLocal extends App {

  val conf = new SparkConf().setAppName("Arima_premium_predict").setMaster("local")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  /*0.获取数据*/
  val filepath1 = "E:\\mm\\document\\workspcaceYinker\\xai\\document\\samples.csv"//09388e8eadca4d32b5f24270b8a41298,62484.72,20161014
  val filepath2 = "E:\\mm\\document\\workspcaceYinker\\xai\\document\\user_redeem.csv" //00762413efd14fffb5cea863ef2941e7,2017-02-09,27000.0
  val premiumRawData = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema","true").load(filepath1).toDF("userid","premium_final","log_day") //存量数据
  val redeemData = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema","true").load(filepath2).toDF("userid","redeem_day","amount") //赎回数据

  print(premiumRawData)
  print(redeemData)

}
