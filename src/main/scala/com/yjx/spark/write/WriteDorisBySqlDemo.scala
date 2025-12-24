package com.yjx.spark.write

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author zhang
 * @time 9/12/2025 下午3:23
 * @description
 */
object WriteDorisBySqlDemo {
  def main(args: Array[String]): Unit = {
    // 创建Spark配置，设置本地运行模式和应用名称
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WriteDorisBySqlDemo")

    // 构建并获取SparkSession实例
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 设置日志级别为ERROR，减少不必要的日志输出
    session.sparkContext.setLogLevel("ERROR")

    // 定义创建临时视图的SQL语句，用于连接Doris数据库
    val sql =
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

    // 向Doris表中插入一条记录
    session.sql("INSERT INTO spark_doris VALUES(8000,'zhangSan','Clear',7839,DATE '2000-01-01',2000,null,20)")

    // 关闭SparkSession，释放资源
    session.close()
  }
}

