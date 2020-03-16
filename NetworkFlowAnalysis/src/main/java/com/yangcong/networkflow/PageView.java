package com.yangcong.networkflow;

import com.yangcong.bigdata.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by mouzwang on 2020-02-04 15:57
 *
 * 209.85.238.199 - - 17/05/2015:10:05:15 +0000 GET /test.xml
 *
 * 662867,2244074,1575622,pv,1511658000  --UserBehavior
 */
public class PageView {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);
        DataStreamSource<String> fileStream = env.readTextFile("/Users/mouzwang/idea-workspace/flink-project/HotItermsAnalysis/src/main/resources/UserBehavior.csv");
        SingleOutputStreamOperator<UserBehavior> mapedStream = fileStream.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] dataArray = value.split(",");
                UserBehavior user = new UserBehavior();
                user.setUserId(Long.parseLong(dataArray[0]));
                user.setItermId(Long.parseLong(dataArray[1]));
                user.setCategoryId(Integer.parseInt(dataArray[2]));
                user.setBehavior(dataArray[3]);
                user.setTimestamp(Long.parseLong(dataArray[4]));
                return user;
            }
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        //统计pv:过滤出pv
        DataStreamSink<Tuple2<String, Long>> resultStream = mapedStream.filter(x -> "pv".equals(x.getBehavior()))
                .map(x -> new Tuple2<String, Long>("pv", 1L))
                .keyBy(x -> x.f0)
                .timeWindow(Time.hours(1))
                .reduce((x, y) -> new Tuple2<String, Long>("pv", x.f1 + y.f1))
                .print();
        env.execute("pv");
    }
}
