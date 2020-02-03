//package com.see.loginfail_detect
//
//import java.util
//
//import com.see.loginfail_detect.LoginFail.getClass
//import org.apache.flink.cep.PatternSelectFunction
//import org.apache.flink.cep.scala.CEP
//import org.apache.flink.cep.scala.pattern.Pattern
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.windowing.time.Time
//
///**
//  * Created by mouzwang on 2019-10-26 15:11
//  */
//object LoginFailWithCEP {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)
//    val resource = getClass.getResource("/LoginLog.csv")
//    val dataStream = env.readTextFile(resource.getPath)
//      .map(data => {
//        val dataArray = data.split(",")
//        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
//      })
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time
//        .seconds(3)) {
//        override def extractTimestamp(t: LoginEvent): Long = {
//          t.timestamp * 1000L
//        }
//      })
//      .keyBy(_.userId)
//
//    //定义匹配模式pattern
//    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("start")
//      .where(_.status == "fail")  //定义第一个失败事件模式
//      .next("next").where(_.status == "fail") //严格紧邻,因为这两个事件模式之间不能有其他不匹配的事件
//      .within(Time.seconds(5)) //要求两个模式的间隔为5秒之内
//
//    //将匹配模式应用到数据流上
//    val patternStream = CEP.pattern(dataStream,loginFailPattern)
//    //从patternStream中检出符合规则的事件序列
//    val loginFailWarningStream = patternStream.select(new LoginFailDetect())
//    loginFailWarningStream.print()
//    env.execute("CEP")
//  }
//}
//
//class LoginFailDetect() extends PatternSelectFunction[LoginEvent,Warning]{
//  //检测到一个匹配的就会调用一次这个select方法
//  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
//    //先取出事件的时间戳,因为我前面没有使用循环模式,所以这个List中只有一个元素,这里就直接next()方法取出了
//    val firstFailEvent = map.get("start").iterator().next()
//    val secondFailEvent = map.get("next").iterator().next()
//    Warning(firstFailEvent.userId,firstFailEvent.timestamp,secondFailEvent.timestamp,"Login fail " +
//      "2 times")
//  }
//}
