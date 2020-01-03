package com.see.orderpay_detect

import com.see.orderpay_detect.OrderTimeout.getClass
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Created by mouzwang on 2019-10-28 10:14
  */
object OrderTimeoutWithoutCEP {
  //定义侧输出流
  private val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    //    val resource = getClass.getResource("/TestOrder.csv")
    //    val orderEventStream = env.readTextFile(resource.getPath)
    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.orderId)

    //使用process function
    val orderResultStream: DataStream[OrderResult] = orderEventStream.process(new OrderPayMatch())
    orderResultStream.print("order result")
    orderResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")
    env.execute("order timeout")
  }

  //自定义实现匹配和超时的处理函数,这里写成一个内部类
  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
    //定义一个标识位,用于判断pay事件是否来过
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new
        ValueStateDescriptor[Boolean]("isPayed-state",
          classOf[Boolean]))
    //定义一个状态,用来保存定时器时间戳,为了管理定时器的创建与删除,才需要这个时间戳状态
    lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("ts-state", classOf[Long]))

    override def processElement(i: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent,
      OrderResult]#Context, collector: Collector[OrderResult]): Unit = {
      //取出状态
      val isPayed = isPayedState.value()
      val timerTS = timerState.value()
      if (i.eventType == "create") {
        //如果进来的数据是create且已经pay过了,就直接输出成功的信息
        if (isPayed) {
          collector.collect(OrderResult(i.orderId, "payed successfully"))
          //清空状态和定时器
          context.timerService().deleteEventTimeTimer(timerTS)
          isPayedState.clear()
          timerState.clear()
        } else {
          //注册定时器,并将时间戳更新到状态中
          val ts = i.timestamp * 1000L + 900 * 1000L
          context.timerService().registerEventTimeTimer(ts)
          timerState.update(ts)
        }
      } else if (i.eventType == "pay") {
        //如果定时器有值,说明create之前到了
        if (timerTS > 0) {
          //判断时间,如果小于定时器的时间,则正常匹配
          if (i.timestamp * 1000L < timerTS) {
            collector.collect(OrderResult(i.orderId, "payed successfully"))
          } else {
            context.output(orderTimeoutOutputTag, OrderResult(i.orderId, "payed but already timeout"))
          }
          //最终匹配成功与否的结果都输出了,所以记得清空状态
          context.timerService().deleteEventTimeTimer(timerTS)
          isPayedState.clear()
          timerState.clear()
        } else { //定时器没有值,说明create还没到
          //create数据可能是乱序的,后面才来,也可能真的丢失了,所以也注册一个定时器等待一段时间
          //此定时器时间就设置为当前pay的时间,表示watermark要涨过这个时间,才会触发,即表示位于这个时间之前的数据都已经到了
          context.timerService().registerEventTimeTimer(i.timestamp * 1000L)
          isPayedState.update(true)
          timerState.update(i.timestamp * 1000L)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent,
      OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      //
      if (timerState.value() == timestamp) {
        //如果pay过,那么就说明create的逻辑判断中没有删除定时器,即create数据没来
        if (isPayedState.value()) {
          ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "already payed but not " +
            "found created log"))
        } else { //pay的状态为空,则说明超时时间内pay没来,即超时了
          ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "time out"))
        }
        isPayedState.clear()
        timerState.clear()
      }
    }
  }

}
