package com.yjx.spark.read

import org.apache.doris.spark.sparkContextFunctions
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zhang
 * @time 9/12/2025 下午2:49
 * @description
 */
object ReadDorisByRddDemo {
  def main(args: Array[String]): Unit = {
    // 创建Spark配置对象，设置运行模式为本地模式，应用名称为ReadDorisByRddDemo
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReadDorisByRddDemo")

    // 创建SparkSession对象，用于与Spark进行交互
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 获取SparkContext对象，用于创建RDD
    val context: SparkContext = session.sparkContext

    // 设置日志级别为ERROR，减少不必要的日志输出
    context.setLogLevel("ERROR")


    // 从Doris数据库中读取scott.emp表的数据，创建DorisSparkRDD并打印每条记录
    val dorisSparkRDD = context.dorisRDD(
      tableIdentifier = Some("scott.emp"),
      cfg = Some(Map(
        "doris.fenodes" -> "node01:8030",
        "doris.request.auth.user" -> "root",
        "doris.request.auth.password" -> "123456"
      ))
    ).foreach(println)

    // 关闭SparkSession，释放资源
    session.close()
  }
}
