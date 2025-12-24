package com.yjx.spark.read

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zhang
 * @time 9/12/2025 上午11:20
 * @description
 */
object ReadDorisByDataFrameDemo {

  def main(args: Array[String]): Unit = {
    // 创建Spark配置对象，设置运行模式为本地模式，应用名称为ReadDorisByDataFrameDemo
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReadDorisByDataFrameDemo")

    // 创建SparkSession对象，用于与Spark进行交互
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 获取SparkContext对象，用于执行Spark操作
    val context: SparkContext = sparkSession.sparkContext

    // 设置日志级别为ERROR，只显示错误信息
    context.setLogLevel("ERROR")

    // 通过DataFrame API从Doris数据库读取数据
    val dataFrame: DataFrame = sparkSession.read.format("doris")
      .option("doris.table.identifier", "scott.emp")  // 指定要读取的Doris表名
      .option("doris.fenodes", "node01:8030")         // 指定Doris前端节点地址
      .option("user", "root")                         // 数据库用户名
      .option("password", "123456")                   // 数据库密码
      .load()

    // 显示读取到的数据
    dataFrame.show()

    // 关闭SparkSession，释放资源
    sparkSession.close()
  }

}

