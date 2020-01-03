package com.see.hotitems_analysis


import java.sql.Timestamp
import java.util
import java.util._

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by mouzwang on 2019-10-24 14:24
  */
// 输入用户行为数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 中间聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long) extends Ordered[ItemViewCount] {
  override def compare(that: ItemViewCount): Int = {
    (that.count - this.count).toInt
  }
}

object HostItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop101:9092")
    properties.setProperty("group.id", "hotitems-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization" +
      ".StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
            val inputStream = env.addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))
      .map(data => {
      val dataArray = data.split(",")
      UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3),
        dataArray(4).toLong)
    })
      // 数据经过ETL了,已经是有序的了,直接使用如下方式
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 过滤pv数据并开窗然后统计每个窗口内的每个商品的pv数量
    val aggStream = inputStream.filter(_.behavior == "pv") //过滤出pv数据
      //      .keyBy("itemId")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5)) //开窗进行统计
      .aggregate(new CountAgg, new WindowCountResult) //聚合出当前商品在时间窗口内的统计数量
    //到这里仅仅是对每一个itemId操作完成了,想要直接排序是不对的,需要按窗口再keyby,然后排序
    val resultStream = aggStream.keyBy(_.windowEnd)
      .process(new SortAndTopNFunction(3))
    aggStream.print("agg")
    resultStream.print("process")
    env.execute("HostItems")
  }
}

//自定义的预聚合函数,来一条数据就加1
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1L

  override def getResult(acc: Long): Long = acc

  //分区间聚合的方法
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//举个例子:求平均值
class AvgAgg() extends AggregateFunction[Long, (Long, Int), Double] {
  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def add(in: Long, acc: (Long, Int)): (Long, Int) = {
    (acc._1 + in, acc._2 + 1)
  }

  override def getResult(acc: (Long, Int)): Double = acc._1.toDouble / acc._2

  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = {
    (acc._1 + acc1._1, acc._2 + acc1._2)
  }
}

// 如果keyby的字段使用的是字段名就必须使用这种方式,但是比较麻烦
// WindowFunction的输入就是累加器预聚合的结果,key为前面keyby的字段的类型(java.Tuple类型)
//class WindowCountResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
//  override def apply(key: Tuple, window: TimeWindow, input: lang.Iterable[Long],
//                     collector: Collector[ItemViewCount]): Unit = {
//    //取出itemId,必须先转换为Tuple1类型,通过f0来取出第一个也是唯一一个元素
//    val itemId = key.asInstanceOf[Tuple1[Long]].f0
//    val windowEnd = window.getEnd //得到窗口的结束时间
//    val count = input.iterator().next() //输入的累加器的结果为一个迭代器,取出count值
//    ItemViewCount(itemId,windowEnd,count)
//  }
// }

//自定义WindowFunction,它的输入就是前面的Aggregate的结果
class WindowCountResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long],
                     out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

//自定义ProcessFunction,实现对按窗口分组后的每个窗口内数据排序然后取TopK为keyby的字段类型
//这种实现方式是将一个窗口的全部数据拉取到一起后再排序取TopN
class SortAndTopNFunction(i: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  //定义一个列表状态，用于保存所有的商品个数统计值
  private var itemListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]
    ("item-liststate", classOf[ItemViewCount]))
  }

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long,
    ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    // 每来一条数据，就保存入list state，注册一个定时器
    itemListState.add(i)
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  //Flink中定时器的触发条件是必须watermark推进了之后才会触发
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount,
    String]#OnTimerContext, out: Collector[String]): Unit = {
    // 先将所有数据从状态中读出
    val listBuffer: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    val itemViewCounts = itemListState.get()
    for (item <- itemViewCounts) {
      listBuffer += item
    }
    itemListState.clear()
    // 按照点击量从大到小排序,取TopN
    val sortedItems = listBuffer.sortBy(_.count)(Ordering.Long.reverse).take(i)
    // 将信息格式化为String
    val sb = new StringBuffer()
    // 这个timestamp是触发定时器的毫秒级时间戳,而不是数据里面的时间戳,但是上面触发的条件为数据时间戳+1,所以这里-1
    sb.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
    //indices表示遍历索引
    for (index <- sortedItems.indices) {
      val oneItem = sortedItems(index)
      sb.append("NO").append(index + 1).append(":") //第几名
      sb.append("商品ID").append(oneItem.itemId)
        .append("点击量=").append(oneItem.count)
        .append("\n")
    }
    sb.append("==============")
    Thread.sleep(1000)
    out.collect(sb.toString)
  }
}

class SortAndTopNFunction2(n: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  //我这种方法没有吧treeset存到状态后端去,所以每次进来都是重新创建的??
  //感觉这种实现方式不能用treeset,还是要用ListState,然后每次进来的数据和上一次的进行比较,而不是像上面的实现方式那样吧所有数据先存起来,最后一起排序
  private var treeSet: mutable.TreeSet[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    treeSet = new mutable.TreeSet[ItemViewCount]()
  }

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long,
    ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    treeSet.add(i)
    treeSet = treeSet.take(n)
    for (elem <- treeSet) {
      print(elem)
      print("\n")
    }
  }

  //  override def close(): Unit = {
  //    for (elem <- treeSet) {
  //      print(elem)
  //      print("\n")
  //    }
  //  }
}

