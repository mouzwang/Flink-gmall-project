package com.see.market_analysis

import java.lang
import java.sql.Timestamp

import org.apache.flink.api.common.functions.{AggregateFunction, RichFilterFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by mouzwang on 2019-10-26 09:19
  */
//数据源样例类
case class AdClickEvent(userId: Long, adId: Long, province: String, city: String, timestap: Long)

//聚合结果样例类
case class AdCountByProvince(windowEnd: String, province: String, count: Long)

//侧输出流的黑名单报警信息样例类
case class BlackListWarning(userId: Long, adId: Long, msg: String)

object AdClickByGeo {
  //定义侧输出流
  val blackListOutputTag = new OutputTag[BlackListWarning]("blacklist")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/AdClickLog.csv")
    val adLogStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        AdClickEvent(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray
        (4).toLong)
      })
      .assignAscendingTimestamps(_.timestap * 1000L)

    //过滤黑名单,将黑名单的用户信息输出到侧输出流,向后传递的流依然是主流
    //按每一个用户点击的每一个广告来分组,输出的还是输入源样例类
    val filterBlackListStream = adLogStream.keyBy(data => (data.userId, data.adId)).process(new FilterBlackListUser(100))

    //按省份分组,然后开窗,聚合每个省份的此广告的点击量
    val adCountStream = filterBlackListStream.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      //使用aggregate要自定义预聚合函数和窗口函数
      .aggregate(new CountAgg(), new AdCountResult())
      .print("aggregate by province")
    filterBlackListStream.getSideOutput(blackListOutputTag).print("black list")
    env.execute("ad click by province")
  }

  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long),
    AdClickEvent, AdClickEvent] {
    //定义一个状态,保存用户对这个广告的点击量
    lazy private val countState: ValueState[Long] = getRuntimeContext.getState(new
        ValueStateDescriptor[Long]("clickCount-state", classOf[Long]))
    //定义一个标识位,标记是否发送过黑名单信息
    lazy private val isSent: ValueState[Boolean] = getRuntimeContext.getState(new
        ValueStateDescriptor[Boolean]("isSent-state", classOf[Boolean]))
    //保存定时器触发的时间戳
    lazy private val resetTime = getRuntimeContext.getState(new ValueStateDescriptor[Long]
    ("resetTime-state", classOf[Long]))

    override def processElement(i: AdClickEvent, context: KeyedProcessFunction[(Long, Long),
      AdClickEvent, AdClickEvent]#Context, collector: Collector[AdClickEvent]): Unit = {
      //获取当前的count值
      val curCount = countState.value()
      // 如果是第一条数据,那么count为0,注册一个定时器,一天触发一次
      if (curCount == 0) {
        //获得processing time,通过系统的时间来触发每一天的定时器
        val ts = (context.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * 24 * 60 * 60 * 1000L
        context.timerService().registerProcessingTimeTimer(ts)
        resetTime.update(ts)
      }
      countState.update(curCount + 1)
      // 判断点击量是否超过阈值,如果超过输出黑名单信息到侧输出流
      if (curCount >= maxCount) {
        val isSentState = isSent.value()
        if (!isSentState) { //没有发送过黑名单信息,则输出
          context.output(blackListOutputTag, BlackListWarning(i.userId, i.adId, "click " +
            "over" + maxCount + "times today"))
          isSent.update(true)
        }
      } else {
        collector.collect(i)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long),
      AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      //如果当前定时器是重置状态定时器,那么清空状态
      if (timestamp == resetTime.value()) {
        isSent.clear()
        countState.clear()
      }
    }
  }

}

class CountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//窗口函数是一个窗口计算一次,input这个迭代器中只有一个元素,就是该窗口的所有元素个数
class AdCountResult() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long],
                     out: Collector[AdCountByProvince]): Unit = {
    val windowEnd = new Timestamp(window.getEnd).toString
    out.collect(AdCountByProvince(windowEnd, key, input.iterator.next()))
  }
}


