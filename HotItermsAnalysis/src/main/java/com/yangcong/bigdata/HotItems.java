package com.yangcong.bigdata;

import com.yangcong.bigdata.bean.HotItemsItemViewCount;
import com.yangcong.bigdata.bean.HotItemsUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;


import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.*;

/**
 * Created by mouzwang on 2020-01-04 23:13
 * 每隔5分钟输出最近一小时内点击量最多的前N个商品
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        //生成watermark的周期时间默认为200ms,这时时间不是watermark的时间,而是系统的处理时间
//        env.getConfig().setAutoWatermarkInterval(300L);
        InputStream resource = HotItems.class.getClassLoader().getResourceAsStream("config.properties");
        Properties prop = new Properties();
        prop.load(resource);
        prop.setProperty("bootstrap.servers", prop.getProperty("bootstrap.servers"));
        prop.setProperty("group.id", prop.getProperty("group.id"));
        prop.setProperty("key.deserializer", prop.getProperty("key.deserializer"));
        prop.setProperty("value.deserializer", prop.getProperty("value.deserializer"));
        prop.setProperty("auto.offset.reset", prop.getProperty("auto.offset.reset"));
        SingleOutputStreamOperator<HotItemsUserBehavior> inputSource =
                env.addSource(new FlinkKafkaConsumer011<String>("flinktest1",
                        new SimpleStringSchema(),
                        prop))
                .map(new MapFunction<String, HotItemsUserBehavior>() {
                    @Override
                    public HotItemsUserBehavior map(String value) throws Exception {
                        HotItemsUserBehavior user = new HotItemsUserBehavior();
                        String[] dataArray = value.split(",");
                        user.setUserId(Long.parseLong(dataArray[0]));
                        user.setItemId(Long.parseLong(dataArray[1]));
                        user.setCategoryId(Integer.parseInt(dataArray[2]));
                        user.setBehaivor(dataArray[3]);
                        user.setTimestamp(Long.parseLong(dataArray[4]));
                        return user;
                    }
                })
                //第一个参数为延迟时间(maxOutOfOrderness),设置允许处理延迟数据的方法时allowedLateness
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<HotItemsUserBehavior>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(HotItemsUserBehavior hotItemsUserBehavior) {
                        return hotItemsUserBehavior.getTimestamp() * 1000L;
                    }
                });
        SingleOutputStreamOperator<HotItemsItemViewCount> aggStream = inputSource.filter(x -> "pv".equals(x.getBehaivor()))
                .keyBy(x -> x.getItemId())
                .timeWindow(Time.hours(1), Time.minutes(5))
                //聚合出当前商品在时间窗口内的统计数量
                .aggregate(new JavaCountAgg(), new JavaWindowCountResult());
        //到这里仅仅是按每隔itemId分组操作完成了,直接排序是不对的,需要先按窗口keyBy,再排序
        aggStream.keyBy(x -> x.getWindowEnd())
                .process(new SortTopNFunction());
        env.execute();
    }
}

class JavaCountAgg implements AggregateFunction<HotItemsUserBehavior, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(HotItemsUserBehavior value, Long accumulator) {
        return accumulator + 1L;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}

//自定义WindowFunction,它的输入就是前面agg的输出结果,key为前面keyBy的字段的类型
class JavaWindowCountResult implements WindowFunction<Long, HotItemsItemViewCount, Long, TimeWindow> {

    @Override
    public void apply(Long key, TimeWindow window, Iterable<Long> input, Collector<HotItemsItemViewCount> collector) throws Exception {
        HotItemsItemViewCount hotItemsItemViewCount = new HotItemsItemViewCount();
        hotItemsItemViewCount.setItemId(key);
        hotItemsItemViewCount.setWindowEnd(window.getEnd());
        hotItemsItemViewCount.setCount(input.iterator().next());
        collector.collect(hotItemsItemViewCount);
    }
}

//自定义ProcessFunction,实现对按窗口分组后的每个窗口内数据排序然后取TopK为keyby的字段类型
//这种实现方式是将一个窗口的全部数据拉取到一起后再排序取TopN
class SortTopNFunction extends KeyedProcessFunction<Long, HotItemsItemViewCount, String> {
    //定义一个状态,来保存所有的商品个数统计值
    private transient ListState<HotItemsItemViewCount> listState;

    @Override
    public void open(Configuration parameters) throws Exception {
        listState =
                getRuntimeContext().getListState(new ListStateDescriptor<HotItemsItemViewCount>(
                        "item-liststate", HotItemsItemViewCount.class));
    }

    @Override
    public void processElement(HotItemsItemViewCount value, Context ctx, Collector<String> out) throws Exception {
        //每来一条数据,就保存进listState,然后注册一个定时器
        listState.add(value);
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    //Flink中定时器的触发条件是必须watermark推进了之后才会触发
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        //先将数据从状态中读出
        ArrayList<HotItemsItemViewCount> list = new ArrayList<>();
        ArrayList<HotItemsItemViewCount> resultList = new ArrayList<>();
        Iterable<HotItemsItemViewCount> iter = listState.get();
        for (HotItemsItemViewCount item : iter) {
            list.add(item); //将状态后端的数据取出放到ArrayList
        }
        listState.clear(); //清除状态了
        //按照点击量从大到小排序,取TopN
        Collections.sort(list, new Comparator<HotItemsItemViewCount>() {
            @Override
            public int compare(HotItemsItemViewCount o1, HotItemsItemViewCount o2) {
                return (int) (o1.getCount() - o2.getCount());
            }
        });
        //取TopN
        Iterator<HotItemsItemViewCount> it = list.iterator();
        int flag = 0;
        while (it.hasNext()) {
            flag++;
            if(flag >3){
                break;
            }
            resultList.add(it.next());
        }
        //将装了TopN信息的list格式化为String
        StringBuffer sb = new StringBuffer();
        //这个timestamp是触发定时器的毫秒级时间戳,而不是数据里面的时间戳
        sb.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n");
        //将数据放到StringBuffer中去
        for (int i = 0; i < resultList.size(); i++) {
            HotItemsItemViewCount result = resultList.get(i);
            sb.append("NO").append(i + 1).append(":");
            sb.append("商品ID").append(result.getItemId());
            sb.append("点击量=").append(result.getCount()).append("\n");
        }
        Thread.sleep(1000);
        out.collect(sb.toString() );
    }
}
