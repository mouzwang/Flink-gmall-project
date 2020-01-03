package com.see.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * 两个流的合并计算
  */
// 定义到账信息样例类
case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)

object TxMatch {
  //定义侧输出流标签
  private val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  private val unmatchedReceipt = new OutputTag[ReceiptEvent]("unmatchedReceipt")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    // 1.读取订单支付信息数据
    val resource = getClass.getResource("/OrderLog.csv")
    val orderStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.txId) //两个keyedStream要connect,那么keyBy的字段要对应
    // 2.读取到账信息数据
    val recieptResource = getClass.getResource("/ReceiptLog.csv")
    val receiptStream = env.readTextFile(recieptResource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.txId)
    // 3.连接两条流,进行处理,需要判断前面的数据是否有另一个流的数据,所以用状态编程
    val resultStream = orderStream.connect(receiptStream)
      .process(new TxPayMatch())


    env.execute("tx pay match")
  }

  //自定义CoProcessFunction内部类,方便使用前面定义的侧输出流,输入类型的顺序就是函数调用的顺序,输出类型这里就定义为元组了
  class TxPayMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
    //定义两个状态,分别保存订单和第三方支付的数据信息状态
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]
    ("pay-state", classOf[OrderEvent]))
    lazy val receiptState = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]
    ("receipt-state", classOf[ReceiptEvent]))

    override def processElement1(pay: OrderEvent, context: CoProcessFunction[OrderEvent,
      ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      //取出状态
      val receipt = receiptState.value()
      // 判断receipt是否到了
      if (receipt != null) {
        //如果到了,那么正常输出匹配
        collector.collect((pay, receipt))
        receiptState.clear()
      } else {
        //如果没到,就保存pay进状态,并注册定时器等待一下
        //为什么这个需求不需要判断是否超过定时器时间呢?因为没有像支付超时时间那样的规定,这里就定义一个5s咯
        payState.update(pay)
        context.timerService().registerEventTimeTimer(pay.timestamp * 1000L + 5000L)
      }
    }

    override def processElement2(receipt: ReceiptEvent, context: CoProcessFunction[OrderEvent,
      ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val pay = payState.value()
      //判断pay是否到了
      if (pay != null) {
        collector.collect((pay, receipt))
        payState.clear()
      } else {
        receiptState.update(receipt)
        context.timerService().registerEventTimeTimer(receipt.timestamp * 1000L + 3000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent,
      (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      //根据状态的值来判断触发的是哪一个的定时器
      if(payState.value() != null){
        //如果payState有状态,则说明对应的receipt还没到
        ctx.output(unmatchedPays,payState.value())
      }
      if(receiptState.value() != null){
        //如果receipt有状态,则说明对应的pay还没到
        ctx.output(unmatchedReceipt,receiptState.value())
      }
      payState.clear()
      receiptState.clear()
    }
  }
}
