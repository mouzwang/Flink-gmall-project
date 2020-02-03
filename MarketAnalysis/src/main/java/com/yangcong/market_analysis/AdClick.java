package com.yangcong.market_analysis;

import com.yangcong.bigdata.bean.AdClickEvent;
import com.yangcong.bigdata.bean.AdCountByProvince;
import com.yangcong.bigdata.bean.BlackListWarning;
import com.yangcong.bigdata.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Properties;

/**
 * Created by mouzwang on 2020-01-30 21:32
 */
public class AdClick {
    //定义侧输出流
    public static final OutputTag<BlackListWarning> blackList = new OutputTag<BlackListWarning>(
            "blackList"){};

    public static void main(String[] args) throws Exception {
        AdClick adClick = new AdClick() {};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        //生成watermark的周期时间默认为200ms,这时时间不是watermark的时间,而是系统的处理时间
//        env.getConfig().setAutoWatermarkInterval(300L);
        InputStream resource = AppMarketingByChannel.class.getClassLoader().getResourceAsStream("config.properties");
        Properties prop = new Properties();
        prop.load(resource);
        prop.setProperty("bootstrap.servers", prop.getProperty("bootstrap.servers"));
        prop.setProperty("group.id", prop.getProperty("group.id"));
        prop.setProperty("key.deserializer", prop.getProperty("key.deserializer"));
        prop.setProperty("value.deserializer", prop.getProperty("value.deserializer"));
        prop.setProperty("auto.offset.reset", prop.getProperty("auto.offset.reset"));
        SingleOutputStreamOperator<AdClickEvent> sourceStream =
                env.addSource(new FlinkKafkaConsumer011<String>("flinktest3",
                        new SimpleStringSchema(), prop))
                        .map(new MapFunction<String, AdClickEvent>() {
                            @Override
                            public AdClickEvent map(String value) throws Exception {
                                String[] dataArray = value.split(",");
                                AdClickEvent event = new AdClickEvent();
                                event.setUserId(Long.parseLong(dataArray[0]));
                                event.setAdId(Long.parseLong(dataArray[1]));
                                event.setProvince(dataArray[2]);
                                event.setCity(dataArray[3]);
                                event.setTimestamp(Long.parseLong(dataArray[4]));
                                return event;
                            }
                        })
                        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<AdClickEvent>(Time.seconds(3)) {
                            @Override
                            public long extractTimestamp(AdClickEvent element) {
                                return element.getTimestamp() * 1000L;
                            }
                        });
        //按用户和广告id来分组,并进行黑名单过滤
        KeyedStream<AdClickEvent, Tuple2<Long, Long>> keyedStream = sourceStream.keyBy(new KeySelector<AdClickEvent, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> getKey(AdClickEvent adClickEvent) throws Exception {
                Tuple2<Long, Long> tuple2 = new Tuple2<>(adClickEvent.getUserId(),
                        adClickEvent.getAdId());
                return tuple2;
            }
        });
        //将点击超过阈值的输出到黑名单
        SingleOutputStreamOperator<AdClickEvent> filteredStream =
                keyedStream.process(new AdClick().new BlackListFilter());
        //按省份分组,并开窗,聚合出每个省份的此广告点击量
        DataStreamSink<AdCountByProvince> adCountStream =
                filteredStream.keyBy(new KeySelector<AdClickEvent, String>() {
                    @Override
                    public String getKey(AdClickEvent adClickEvent) throws Exception {
                        return adClickEvent.getProvince();
                    }
                })
                        .timeWindow(Time.hours(1), Time.seconds(5))
                        .aggregate(new AdClick().new AggFunction(), new AdClick().new AdCount())
                        .print("aggregate by province");
        //获取侧输出流
        DataStreamSink<BlackListWarning> blackListStream =
                filteredStream.getSideOutput(AdClick.blackList).print("blackList");
        //执行
        env.execute("blackList");
    }

    class BlackListFilter extends KeyedProcessFunction<Tuple2<Long, Long>, AdClickEvent, AdClickEvent> {
        //定义一个状态,保存用户对这个广告的点击量
        private transient ValueState<Long> countState =
                getRuntimeContext().getState(new ValueStateDescriptor<Long>("adClink", Long.class));
        //定义一个标识位来标记是否发送过黑名单信息
        private transient ValueState<Boolean> isSent =
                getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isSent", Boolean.class));
        //保存定时器触发的时间戳
        private transient ValueState<Long> reset =
                getRuntimeContext().getState(new ValueStateDescriptor<Long>("reset", Long.class));

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
            //获取当前count值
            Long curCount = countState.value();
            //如果是第一条数据,那么count就为0,则注册一个定时器,每天触发一次
            if (curCount == 0) {
                //这个触发器是一天触发一次,所以用processTime,先获取,然后注册
                long nextDay = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * 1000 * 60 * 60 * 24;
                ctx.timerService().registerProcessingTimeTimer(nextDay);
                reset.update(nextDay);
            }
            //更新count值
            countState.update(curCount + 1);
            //如果不为0,则判断count值是否超过5,如果超过,则输出黑名单信息到侧输出流
            if (curCount > 5) {
                Boolean sent = isSent.value();
                //判断今日是否发送了,如果没有,则发送
                if (!sent) {
                    BlackListWarning black = new BlackListWarning();
                    black.setUserId(value.getUserId());
                    black.setAdId(value.getAdId());
                    black.setMsg("click over " + countState.value() + " today");
                    ctx.output(AdClick.blackList, black);
                    isSent.update(true);
                }
            } else {
                out.collect(value);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            //如果当前定时器是重置状态定时器,则清空状态
            if (timestamp == reset.value()) {
                isSent.clear();
                countState.clear();
            }
        }
    }

    class AggFunction implements AggregateFunction<AdClickEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent adClickEvent, Long acc) {
            return acc + 1L;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc, Long acc1) {
            return acc + acc1;
        }
    }

    //窗口函数是一个窗口计算一次,input这个迭代器中只有一个元素,就是该窗口的所有元素个数
    class AdCount implements WindowFunction<Long, AdCountByProvince, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<AdCountByProvince> out) throws Exception {
            //input迭代器只有一个元素
            Long iter = input.iterator().next();
            String windowEnd = new Timestamp(window.getEnd()).toString();
            AdCountByProvince adCount = new AdCountByProvince();
            adCount.setProvince(s);
            adCount.setCount(iter);
            adCount.setWindowEnd(windowEnd);
        }
    }
}


