package com.yangcong.market_analysis;

import com.yangcong.bigdata.bean.LoginEvent;
import com.yangcong.bigdata.bean.MarketingUserBehavior;
import com.yangcong.bigdata.bean.MarketingViewCount;
import com.yangcong.bigdata.bean.MarketingViewTemp;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Properties;

/**
 * Created by mouzwang on 2020-01-28 17:58
 *
 * 统计每小时的不同渠道来源和用户行为的数量
 */
public class AppMarketingByChannel {
    public static void main(String[] args) throws IOException {
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
        SingleOutputStreamOperator<MarketingUserBehavior> sourceStream =
                env.addSource(new FlinkKafkaConsumer011<String>("flinktest3",
                        new SimpleStringSchema(), prop))
                        .map(new MapFunction<String, MarketingUserBehavior>() {
                            @Override
                            public MarketingUserBehavior map(String value) throws Exception {
                                String[] dataArray = value.split(",");
                                MarketingUserBehavior user = new MarketingUserBehavior();
                                user.setUserId(dataArray[0]);
                                user.setBehavior(dataArray[1]);
                                user.setChannel(dataArray[2]);
                                user.setTimestamp(Long.parseLong(dataArray[3]));
                                return user;
                            }
                        });
        sourceStream.print();
        sourceStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<MarketingUserBehavior>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(MarketingUserBehavior marketingUserBehavior) {
                return marketingUserBehavior.getTimestamp() * 1000L;
            }
        });
        //过滤出不活跃的(卸载)的行为,然后组成一个元组为(MarketingViewCount,1)
        SingleOutputStreamOperator<Tuple2<MarketingViewTemp, Long>> mapedStream = sourceStream.filter(new FilterFunction<MarketingUserBehavior>() {
            @Override
            public boolean filter(MarketingUserBehavior marketingUserBehavior) throws Exception {
                if ("UNINSTALL".equals(marketingUserBehavior.getBehavior())) {
                    return false;
                } else {
                    return true;
                }
            }
        })
                .map(new MapFunction<MarketingUserBehavior, Tuple2<MarketingViewTemp, Long>>() {
                    @Override
                    public Tuple2<MarketingViewTemp, Long> map(MarketingUserBehavior marketingUserBehavior) throws Exception {
                        MarketingViewTemp count = new MarketingViewTemp();
                        count.setChannel(marketingUserBehavior.getChannel());
                        count.setBehavior(marketingUserBehavior.getBehavior());
                        Tuple2<MarketingViewTemp, Long> tuple2 = new Tuple2<MarketingViewTemp, Long>(count, 1L);
                        return tuple2;
                    }
                });
        //按渠道和行为来分组,然后开窗并统计
        mapedStream.keyBy(new KeySelector<Tuple2<MarketingViewTemp,Long>, MarketingViewTemp>() {
            @Override
            public MarketingViewTemp getKey(Tuple2 tuple2) throws Exception {
                return (MarketingViewTemp) tuple2.f0;
            }
        })
                .timeWindow(Time.hours(1), Time.seconds(1))
                .process(new CountByChannel())
                .print();
    }
}

//ProcessWindowFunction可以获得一个包含窗口中所有元素的迭代器，并且拥有所有窗口函数的最大灵活性。但是这些是以性能和资源消耗为代价的，因为元素不能增量聚合，相反还需要在内部缓存，直到窗口做好准备处理
class CountByChannel extends ProcessWindowFunction<Tuple2<MarketingViewTemp,Long>,
        MarketingViewCount,
        MarketingViewTemp,
        TimeWindow> {
    @Override
    public void process(MarketingViewTemp marketingViewTemp, Context context, Iterable<Tuple2<MarketingViewTemp, Long>> elements, Collector<MarketingViewCount> out) throws Exception {
        long windowStart = context.window().getStart();
        String start = new Timestamp(windowStart).toString();
        String channel = marketingViewTemp.getChannel();
        String behavior = marketingViewTemp.getBehavior();
        long count = 0;
        for (Tuple2<MarketingViewTemp, Long> element : elements) {
            count++;
        }
        MarketingViewCount viewCount = new MarketingViewCount();
        viewCount.setBehavior(behavior);
        viewCount.setChannel(channel);
        viewCount.setWindowStart(start);
        viewCount.setCount(count);
        out.collect(viewCount);
    }
}
