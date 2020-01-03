package com.see.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Created by mouzwang on 2019-10-26 10:50
  */
//输入数据源登录事件样例类
case class LoginEvent(userId: Long, ip: String, status: String, timestamp: Long)

//报警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val resource = getClass.getResource("/LoginLog.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time
        .seconds(3)) {
        override def extractTimestamp(t: LoginEvent): Long = {
          t.timestamp * 1000L
        }
      })
      .keyBy(_.userId)
      .process(new LoginFailWarning(2))

    dataStream.print()
    env.execute("Login Fail Warning")
  }
}

//先判断是成功还是失败,如果为失败则注册定时器(2s后触发),为成功就不计并且清空前面的状态
class LoginFailWarning(num: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  //定义一个List状态,保存连续登陆失败的事件
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new
      ListStateDescriptor[LoginEvent]("loginFailList", classOf[LoginEvent]))

  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent,
    Warning]#Context, collector: Collector[Warning]): Unit = {
    //判断是否是失败事件,如果是就添加到状态中并定义一个定时器

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent,
    Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    //判断状态列表中的失败事件的个数

    //清空状态
  }
}

class LoginFailWarning2(num: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  //定义一个List状态,保存连续登陆失败的事件
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new
      ListStateDescriptor[LoginEvent]("loginFailList", classOf[LoginEvent]))

  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent,
    Warning]#Context, collector: Collector[Warning]): Unit = {
    //按照事件的成功与失败来筛选
    if (i.status == "fail") {
      //定义迭代器获取状态
      val iter = loginFailListState.get().iterator()
      //如果已经有失败事件,那么才做处理,没有的话将当前事件就加进到状态中
      if (iter.hasNext) {
        //如果两次登录失败事件间隔小于2秒,则输出报警信息
        val firstFailEvent = iter.next()
        if ((firstFailEvent.timestamp - i.timestamp).abs < 2) {
          collector.collect(Warning(i.userId, firstFailEvent.timestamp, i.timestamp, "login fail " +
            "in 2 seconds"))
        }
        //清空原来的状态,然后再把本次的状态添加进去
        loginFailListState.clear()
        loginFailListState.add(i)
      } else {
        loginFailListState.add(i)
      }
    } else {
      //如果成功,清空状态
      loginFailListState.clear()
    }
  }
}
