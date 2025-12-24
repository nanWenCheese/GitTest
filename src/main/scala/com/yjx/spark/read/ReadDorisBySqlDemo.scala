package com.yjx.spark.read

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zhang
 * @time 9/12/2025 上午10:44
 * @description
 */
object ReadDorisBySqlDemo {
  def main(args: Array[String]): Unit = {
    // 创建Spark配置对象，设置本地运行模式和应用名称
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReadDorisBySqlDemo")

    // 创建SparkSession对象，用于Spark SQL操作
    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 获取SparkContext对象
    val sc: SparkContext = session.sparkContext

    // 设置日志级别为ERROR，减少不必要的日志输出
    sc.setLogLevel("ERROR")

    // 定义创建临时视图的SQL语句，用于连接Doris数据库
    lazy val sql =
      """
        |CREATE TEMPORARY VIEW spark_doris
        |USING doris
        |OPTIONS(
        |"table.identifier"="scott.emp",
        |"fenodes"="node01:8030",
        |"user"="root",
        |"password"="123456"
        |)
        |""".stripMargin

    // 执行SQL创建临时视图
    session.sql(sql)

    // 查询临时视图中的所有数据并展示
    session.sql("SELECT * FROM spark_doris").show()

    // 关闭SparkSession，释放资源
    session.close()
  }
}
