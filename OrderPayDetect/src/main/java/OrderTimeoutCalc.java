
import com.yangcong.bigdata.bean.OrderEvent;
import com.yangcong.bigdata.bean.OrderResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by mouzwang on 2020-02-07 16:05
 *
 * 34729,pay,sd76f87d6,1558430844
 */
public class OrderTimeoutCalc {
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        //生成watermark的周期时间默认为200ms,这时时间不是watermark的时间,而是系统的处理时间
//        env.getConfig().setAutoWatermarkInterval(300L);
        InputStream resource = OrderTimeoutCalc.class.getClassLoader().getResourceAsStream("config.properties");
        Properties prop = new Properties();
        prop.load(resource);
        prop.setProperty("bootstrap.servers", prop.getProperty("bootstrap.servers"));
        prop.setProperty("group.id", prop.getProperty("group.id"));
        prop.setProperty("key.deserializer", prop.getProperty("key.deserializer"));
        prop.setProperty("value.deserializer", prop.getProperty("value.deserializer"));
        prop.setProperty("auto.offset.reset", prop.getProperty("auto.offset.reset"));
        SingleOutputStreamOperator<OrderEvent> sourceStream =
                env.addSource(new FlinkKafkaConsumer011<String>("flinktest5",
                        new SimpleStringSchema(), prop))
                        .map(new MapFunction<String, OrderEvent>() {
                            @Override
                            public OrderEvent map(String value) throws Exception {
                                String[] dataArray = value.split(",");
                                OrderEvent event = new OrderEvent();
                                event.setOrderId(Long.parseLong(dataArray[0]));
                                event.setEventType(dataArray[1]);
                                event.setTxId(dataArray[2]);
                                event.setTimestamp(Long.parseLong(dataArray[3]));
                                return event;
                            }
                        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        //按订单分组
        KeyedStream<OrderEvent, Long> keyedStream = sourceStream.keyBy(x -> x.getOrderId());
        //定义Pattern
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("start").where(new IterativeCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent, Context<OrderEvent> context) throws Exception {
                return "create".equals(orderEvent.getEventType());
            }
        })
                //使用宽松近邻,因为事件之间也可以有其他的操作
                .followedBy("pay").where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent, Context<OrderEvent> context) throws Exception {
                        return "pay".equals(orderEvent.getEventType()) && orderEvent.getTxId() != null;
                    }
                })
                .within(Time.minutes(15));//这里是EventTime,这里是无法单独调整为Process Time
        //将模式pattern应用到流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, pattern);
        //用select方法获取匹配到的事件序列并处理,将支付超时的信息输出到侧输出流,主流输出匹配成功的事件
        OutputTag<OrderResult> outputTag = new OutputTag<OrderResult>("outputTag"){};
        SingleOutputStreamOperator<OrderResult> resultStream = patternStream.select(outputTag, new MyPatternTimeoutFunction(), new MyPatternSelectFunction());
        resultStream.print();
        resultStream.getSideOutput(outputTag);
    }
}

class MyPatternTimeoutFunction implements PatternTimeoutFunction<OrderEvent, OrderResult> {

    //这个方法处理超时的事件
    @Override
    public OrderResult timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
        //这里必须拿create,因为超时时没有pay
        Long orderId = map.get("create").iterator().next().getOrderId();
        OrderResult result = new OrderResult();
        result.setOrderId(orderId);
        result.setResultMsg("payed timeout");
        return result;
    }
}

class MyPatternSelectFunction implements PatternSelectFunction<OrderEvent, OrderResult> {

    //这里匹配上了,拿create或者pay都行
    @Override
    public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
        Long orderId = map.get("pay").iterator().next().getOrderId();
        OrderResult result = new OrderResult();
        result.setOrderId(orderId);
        result.setResultMsg("payed successfully in 15 min");
        return result;
    }
}
