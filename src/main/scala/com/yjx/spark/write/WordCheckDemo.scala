package com.yjx.spark.write

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
object WordCheckDemo {
  def main(args: Array[String]): Unit = {
    // 建立连接
    val sc = new SparkContext("local[*]", "WordCheckDemo")
    // 模拟用户社交互动信息
    val words = List("我喜欢唱歌", "我喜欢跳舞", "我还会RAP", "顺便打打篮球", "每个技能都练了两年半")
    // 敏感词过滤，敏感词为："唱"，"跳"，"RAP"，"篮球"，"两年半"
    val regex = "(唱|跳|RAP|rap|篮球|两年半)"
    val broadcast: Broadcast[String] = sc.broadcast(regex)
    // 分发给 Executor 的多个 Task 去处理
    val makeRDD: RDD[String] = sc.makeRDD(words, 4)
    val result: RDD[String] = makeRDD.map(_.replaceAll(broadcast.value, "**"))
    result.foreach(println)
    // 关闭连接
    if (!sc.isStopped) sc.stop()
  }
}