package com.yjx

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author zhang
 * @time 9/12/2025 下午7:08
 * @description
 */
object DorisWriteDemo {
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
    session.sql("INSERT INTO doris_spark VALUES (9000,'lisi','clear',7839,DATE '2000-01-01',2000,null,20)")
    session.close()
  }
}
