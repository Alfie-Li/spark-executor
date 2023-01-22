package com.test.helloworld

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 23-1-1.
  */
object SparkTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("testSpark")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new HiveContext(sc)

    sqlContext.sql("select id, name from test").show()
  }

}
