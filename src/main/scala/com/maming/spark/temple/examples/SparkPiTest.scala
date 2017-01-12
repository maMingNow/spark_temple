package com.maming.spark.temple.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.math._

/**
 执行命令
spark-submit --name spark_demo_test \
--master yarn --class com.maming.spark.temple.examples.SparkPiTest \
--verbose \
/server/app/test/spark_demo/job/spark_demo-jar-with-dependencies.jar
 */
object SparkPiTest {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x*x + y*y < 1) 1 else 0
      }.reduce(_ + _)
    //计算N次圆的面积总和
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }

}
