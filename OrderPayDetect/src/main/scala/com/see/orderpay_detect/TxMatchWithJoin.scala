package com.see.orderpay_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Created by mouzwang on 2019-10-29 13:57
  */
object TxMatchWithJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 读取订单支付数据源，包装成样例类
    val orderResource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(orderResource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.txId != "")
      .keyBy(_.txId)
    // 2. 读取到账信息数据源
    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(receiptResource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.txId)

    // 3.使用window join,这种方式不要求两个是keyedStream,因为会调用算子来指定用哪个字段join
    val windowJoinStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream.join(receiptEventStream)
      .where(_.txId)
      .equalTo(_.txId)
      //window里就不能直接传简写的timeWindow了,可以传入以下三个:
      //Tumbling Window Join,Sliding Window Join,Session Window Join
      .window(TumblingEventTimeWindows.of(Time.seconds(15)))
      .apply((pay,receipt) => (pay,receipt))

    // 3.使用interval join,这种方式要求了必须两个流是keyedStream,否则无法确认哪个字段作为join条件
    val intervalJoinStream = orderEventStream.intervalJoin(receiptEventStream)
      .between(Time.seconds(-3),Time.seconds(5))
      .process(new MyJoinFunction())
    intervalJoinStream.print()
    env.execute("tx match with join ")
  }
}


class MyJoinFunction() extends ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
  //只有匹配的数据才会调用这个方法,所以没法将不匹配的数据输出到侧输出流
  override def processElement(in1: OrderEvent, in2: ReceiptEvent,
                              context: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    collector.collect((in1,in2))
  }
}
