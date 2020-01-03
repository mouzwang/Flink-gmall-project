package com.see.orderpay_detect

import java.util

import com.sun.xml.internal.rngom.binary.visitor.PatternFunction
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by mouzwang on 2019-10-28 09:23
  */
//定义输入源样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

//输出订单支付结果样例类
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
        val resource = getClass.getResource("/OrderLog.csv")
        val orderEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.orderId)
    // 定义pattern
    val pattern = Pattern.begin[OrderEvent]("create").where(_.eventType == "create")
      //使用宽松近邻,因为事件之间也可以有其他的操作
      .followedBy("pay").where(_.eventType == "pay").where(_.txId != "")
      .within(Time.minutes(15)) //这个是eventTime,这里是无法单独调整为processing time的

    // 将模式应用到流上
    val patternStream = CEP.pattern(orderEventStream, pattern)

    // 用select方法获取匹配到的事件序列并处理,将支付超时的信息输出到侧输出流
    // resultStream主流输出的是匹配成功的交易记录
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")
    val resultStream: DataStream[OrderResult] = patternStream.select(orderTimeoutOutputTag, new myPatternTimeoutFunction,
      new myPatternSelectFunction)

    resultStream.print("pay")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("time out")
    env.execute("OrderTimeout")
  }
}

class myPatternTimeoutFunction extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  //超时的事件就通过这个方法来处理
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    //这里必须拿"create",因为"pay"可能是没有的,没有时会空指针异常
    val timeoutOrderId = map.get("create").iterator().next().orderId
    OrderResult(timeoutOrderId, "time out")
  }
}

class myPatternSelectFunction extends PatternSelectFunction[OrderEvent, OrderResult] {
  //检测到定义好的模式序列时,就会调用这个方法来处理
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    //这里拿"create"和"pay"都行,因为匹配的时候orderId都存在
    val payedOrderId = map.get("pay").iterator().next().orderId
    OrderResult(payedOrderId, "payed successfully")
  }
}


