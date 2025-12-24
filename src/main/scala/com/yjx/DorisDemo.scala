package com.yjx

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author zhang
 * @time 9/12/2025 下午7:02
 * @description
 */
object DorisDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DorisDemo")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    val sql =
      """
        |CREATE TEMPORARY VIEW doris_spark
        |USING doris
        |OPTIONS(
        |'table.identifier'='scott.emp',
        |'fenodes'='node01:8030',
        |'user'='root',
        |'password'='123456'
        |)
        |""".stripMargin
        session.sql(sql)
    session.sql("SELECT * FROM doris_spark").show()
    session.close()
  }
}
