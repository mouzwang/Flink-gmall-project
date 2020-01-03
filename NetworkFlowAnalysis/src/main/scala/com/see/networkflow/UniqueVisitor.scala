package com.see.networkflow

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * Created by mouzwang on 2019-10-27 14:51
  */
//输出样例类
case class UvCount(windowEnd: Long, count: Long)

object UniqueVisitor {
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

    val aggStream = inputStream.filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1)) //设置1小时滚动窗口
      .apply(new UVCountByALLWindow) //常规的window处理方法,当然可以用更强大的process
      .print()
    env.execute("unique visitor")
  }
}

//每一个窗口内的每个元素都会调用apply方法
class UVCountByALLWindow extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //用一个set来去重
    var uvSet = mutable.Set[Long]()
    //吧每个数据放入set
    val iterator = input.iterator
    while(iterator.hasNext){
      uvSet += iterator.next().itemId
    }
    //将结果输出到主流
    out.collect(UvCount(window.getEnd,uvSet.size))
  }
}
