package com.see.networkflow

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by mouzwang on 2019-10-26 08:30
  */
//输入用户行为样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val inputStream = env.readTextFile("/Users/mouzwang/idea-workspace/flink-project/HotItermsAnalysis/src/main/resources/UserBehavior.csv")
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
        .assignAscendingTimestamps(_.timestamp * 1000L)
    //统计PV:过滤出pv的数据
    inputStream.filter(_.behavior == "pv")
        .map(data => ("pv",1))
        .keyBy(_._1)
        .timeWindow(Time.hours(1))
//        .sum(1)
      .reduce((x,y) => ("Result",x._2 + y._2))
        .print()
    env.execute("PV")
  }
}
