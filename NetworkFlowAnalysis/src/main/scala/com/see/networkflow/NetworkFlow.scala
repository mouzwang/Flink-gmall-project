package com.see.networkflow

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 统计热门页面浏览数 : 每隔5秒，输出最近10分钟内访问量最多的前N个URL
  */
//输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

//聚合结果样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val dataStream = env.readTextFile("/Users/mouzwang/idea-workspace/flink-project/NetworkFlowAnalysis/src/main/resources/apache.log")
      .map(data => {
        val dataArray = data.split(" ")
        //转换时间戳
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val time = simpleDateFormat.parse(dataArray(3).trim).getTime //得到的是毫秒数
        ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, time, dataArray(5).trim, dataArray(6)
          .trim)
      })
      .assignTimestampsAndWatermarks(new
          BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.minutes(1)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = {
          t.eventTime
        }
      })

    val aggStream = dataStream.filter(_.method == "GET")
      .filter(data => {
        val pattern = "^((?!\\.(css|js|png|ico)$).)*$".r
        pattern.findFirstIn(data.url).nonEmpty
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      //再加上允许处理迟到数据的参数
      .allowedLateness(Time.minutes(1))
      .aggregate(new CountAgg(), new UrlCountResult())

    val processedStream = aggStream.keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))
    aggStream.print("agg")
    processedStream.print("process")
    env.execute("networkflow")
  }
}

class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// WindowFunction的输入为预聚合的输出,Key为前面keyby的字段
class UrlCountResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long],
                     out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

//自定义KeyedProcessFunction实现排序输出,还是将全部数据得到后排序
class TopNHotUrls(i: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  //因为有迟到数据会更新状态,所以就不能用ListState了,而是用Map来存比较好
  private var urlMapState: MapState[String, Long] = _

  override def open(parameters: Configuration): Unit = {
    urlMapState = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]
    ("urlMap-state", classOf[String], classOf[Long]))
  }

  override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount,
    String]#Context, collector: Collector[String]): Unit = {
    urlMapState.put(i.url, i.count)
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount,
    String]#OnTimerContext, out: Collector[String]): Unit = {
    val listBuffer: ListBuffer[(String, Long)] = ListBuffer()
    //用迭代器方式遍历就不需要隐式转换了
    val entries = urlMapState.entries().iterator()
    while (entries.hasNext) {
      val entry = entries.next()
      listBuffer += ((entry.getKey, entry.getValue))
    }
    //urlMapState需要清空,但是不能在这里清空,否则最后还是会内存泄露,那么如何实现呢?
    //可以再在上面processElement方法中定义一个定时器,选择在窗口关闭时间再加上处理延迟数据时间完毕的时候清空map状态

    //排序
    val sortedResult = listBuffer.sortBy(_._2)(Ordering.Long.reverse).take(i)
    //格式化输出
    val results = new StringBuffer()
    results.append("时间").append(new Timestamp(timestamp - 1)).append("\n")
    for (index <- sortedResult.indices) {
      val currentItem = listBuffer(index)
      results.append("NO").append(index + 1).append(":")
        .append(" URL=").append(currentItem._1)
        .append(" 点击量=").append(currentItem._2)
        .append("\n")
    }
    results.append("===============")
    Thread.sleep(1000L)
    out.collect(results.toString)
  }
}


