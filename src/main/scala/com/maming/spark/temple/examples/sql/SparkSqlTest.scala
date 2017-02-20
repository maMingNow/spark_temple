package com.maming.spark.temple.examples.sql

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import scala.math._

/**
 执行命令
spark-submit --name spark_demo_test \
--master yarn --class com.maming.spark.temple.examples.sql.SparkSqlTest \
--verbose \
/server/app/test/spark_demo/job/spark_demo-jar-with-dependencies.jar

      sqlContext.sql("select * from fact_jlc.user_info limit 10").take(10).foreach({ row =>
      row
    })
 */
object SparkSqlTest {

  //读取hive表数据
  def test1(){

    val conf = new SparkConf().setAppName("Spark sql")
    val sc = new SparkContext(conf)
    //val sqlContext = new SQLContext(sc)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

  /*  sqlContext.sql("select * from fact_jlc.user_info limit 10").take(10).foreach{ row=>
      row.fieldIndex("")
    }*/
    sqlContext.sql("select * from fact_jlc.user_info limit 10").take(10).foreach(println(_))
    sc.stop()

  }

  case class Customer(name: String, gender: String, ctfId: String, birthday: String, address: String)

  //读取HDFS上数据
  /**
   * 数据内容
   * 张三,1,男,1,19850303,"哈尔滨市区"
   * 张四,2,男,2,19850303,"北京市区"
   * 麻五,3,女,3,19850603,"天津市区"
   * 康六,3,女,3,19851303,"武汉市区"
   * 康七,3,女,3,19851303---该值是无效的
   */
  def test2(){
    val conf = new SparkConf().setAppName("Spark sql")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val hdfsPath = "hdfs://namenode01.yinker.com:8020/log/spark/sql/demo1/*.txt";
    val customer = sc.textFile(hdfsPath).map(_.split(","))
      .filter( lineArr=>lineArr.length >= 6 )
      .map( lineArr=> Customer( lineArr(0), lineArr(2), lineArr(3), lineArr(4), lineArr(5) ) )

    val customerSchema = sqlContext.createDataFrame(customer)
    customerSchema.registerTempTable("customer")

    sqlContext.udf.register("constellation",  (x:String) => myfun(x))


    var result = sqlContext.sql("SELECT count(0) FROM customer")
    result.collect().foreach(println) //打印4

    result = sqlContext.sql("SELECT constellation(birthday), count(constellation(birthday)) FROM customer group by constellation(birthday)")
    result.collect().foreach(println) //打印[双子座,1]  [未知,1]  [双鱼座,2]

    sc.stop()
  }

  def toInt(s: String):Int = {
    try {
      s.toInt
    } catch {
      case e:Exception => 9999
    }
  }

  def myfun(birthday: String) : String = {
    var rt = "未知"
    if (birthday.length == 8) {
      val md = toInt(birthday.substring(4))
      if (md >= 120 & md <= 219)
        rt = "水瓶座"
      else if (md >= 220 & md <= 320)
        rt = "双鱼座"
      else if (md >= 321 & md <= 420)
        rt = "白羊座"
      else if (md >= 421 & md <= 521)
        rt = "金牛座"
      else if (md >= 522 & md <= 621)
        rt = "双子座"
      else if (md >= 622 & md <= 722)
        rt = "巨蟹座"
      else if (md >= 723 & md <= 823)
        rt = "狮子座"
      else if (md >= 824 & md <= 923)
        rt = "处女座"
      else if (md >= 924 & md <= 1023)
        rt = "天秤座"
      else if (md >= 1024 & md <= 1122)
        rt = "天蝎座"
      else if (md >= 1123 & md <= 1222)
        rt = "射手座"
      else if ((md >= 1223 & md <= 1231) | (md >= 101 & md <= 119))
        rt = "摩蝎座"
      else
        rt = "未知"
    }
    rt
  }

  //调用RDD的group by 进行分组,然后将日期间隔的数据填充满,并且排序数据
  def test3(){

    val conf = new SparkConf().setAppName("Spark sql")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val hdfsPath = "hdfs://namenode01.yinker.com:8020/log/spark/sql/demo1/*.txt";
    val customer = sc.textFile(hdfsPath).map(_.split(","))
      .filter( lineArr=>lineArr.length >= 6 )
      .map( lineArr=> Customer( lineArr(0), lineArr(2), lineArr(3), lineArr(4), lineArr(5) ) )
     .groupBy(customer=> customer.name).map(v=> v._2)

  }

  def main(args: Array[String]) {
    test2();
  }


}
