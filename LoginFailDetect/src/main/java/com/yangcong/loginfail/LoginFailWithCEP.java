package com.yangcong.loginfail;

import com.yangcong.bigdata.bean.LoginEvent;
import com.yangcong.bigdata.bean.Warning;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by mouzwang on 2020-01-19 15:10
 */
public class LoginFailWithCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        //生成watermark的周期时间默认为200ms,这时时间不是watermark的时间,而是系统的处理时间
//        env.getConfig().setAutoWatermarkInterval(300L);
        InputStream resource = LoginFailWithCEP.class.getClassLoader().getResourceAsStream("config.properties");
        Properties prop = new Properties();
        prop.load(resource);
        prop.setProperty("bootstrap.servers", prop.getProperty("bootstrap.servers"));
        prop.setProperty("group.id", prop.getProperty("group.id"));
        prop.setProperty("key.deserializer", prop.getProperty("key.deserializer"));
        prop.setProperty("value.deserializer", prop.getProperty("value.deserializer"));
        prop.setProperty("auto.offset.reset", prop.getProperty("auto.offset.reset"));
        SingleOutputStreamOperator<LoginEvent> sourceStream =
                env.addSource(new FlinkKafkaConsumer011<String>("flinktest2",
                        new SimpleStringSchema(), prop))
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] dataArray = value.split(",");
                        LoginEvent event = new LoginEvent();
                        event.setUserId(Long.parseLong(dataArray[0]));
                        event.setIp(dataArray[1]);
                        event.setStatus(dataArray[2]);
                        event.setTimestamp(Long.parseLong(dataArray[3]));
                        return event;
                    }
                });
        sourceStream.print();
        //定义eventTime
        KeyedStream<LoginEvent, Long> keyedStream = sourceStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(LoginEvent element) {
                return element.getTimestamp() * 1000L;
            }
        })
                .keyBy(x -> x.getUserId());
        //定义模式序列
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getStatus());
                    }
                })
                .next("next")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getStatus());
                    }
                })
                .within(Time.minutes(3));
        //应用定义的模式序列
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);
        //从patternStream中检出符合规则的事件序列
        SingleOutputStreamOperator<Warning> selectedStream = patternStream.select(new PatternSelectFunction<LoginEvent, Warning>() {
            //检测到一个匹配的事件,就会调用这个方法
            @Override
            public Warning select(Map<String, List<LoginEvent>> map) throws Exception {
                Warning warn = new Warning();
                //先取出事件的时间戳,因为前面没有使用循环模式,所以这个List中只有一个元素
                LoginEvent start = map.get("start").iterator().next();
                LoginEvent next = map.get("next").iterator().next();
                warn.setUserId(start.getUserId());
                warn.setFirstFailTime(start.getTimestamp());
                warn.setLastFailTime(next.getTimestamp());
                warn.setWarningMsg("fail login in 2 seconds");
                return warn;
            }
        });
        selectedStream.print();
        env.execute();
    }
}
