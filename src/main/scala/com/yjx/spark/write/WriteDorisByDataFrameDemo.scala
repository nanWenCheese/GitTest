package com.yjx.spark.write

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author zhang
 * @time 9/12/2025 下午4:02
 * @description 使用Spark DataFrame将数据写入Doris数据库的示例程序。
 */
object WriteDorisByDataFrameDemo {
  def main(args: Array[String]): Unit = {
    // 创建Spark配置对象，并设置运行模式和应用名称
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WriteDorisBySqlDemo")

    // 构建并获取SparkSession实例
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 设置日志级别为ERROR，减少控制台输出信息
    session.sparkContext.setLogLevel("ERROR")

    // 导入隐式转换，支持将Scala集合转为DataFrame
    import session.implicits._

    // 构造模拟员工数据并转换为DataFrame
    val mockDataDF = List(
      (8000, "zhangSan", "Clear", 7839, "2000-01-01", 2000, null, 20),
    ).toDF("empno","ename","job","mgr","hiredate","sal","comm","deptno")

    // 将DataFrame中的数据写入到Doris表中
    mockDataDF.write.format("doris")
      .option("doris.table.identifier", "scott.emp") // 指定要写入的Doris表名
      .option("doris.fenodes", "node01:8030")        // Doris FE节点地址
      .option("user", "root")                        // 登录用户名
      .option("password", "123456")                  // 登录密码
      .save                                          // 执行保存操作

//    val mockDataDF1 = List(
//      (9000, "lisi", "Clear", 20),
//    ).toDF("empno", "ename", "job", "deptno")

//    mockDataDF1.write.format("doris")
//      .option("doris.table.identifier", "scott.emp")
//      .option("doris.fenodes", "node01:8030")
//      .option("user", "root")
//      .option("password", "123456")
//      .option("doris.wirte.fields","empno,ename,job,deptno")
//      .save

    // 关闭SparkSession，释放资源
    session.stop
  }
}
