package com.wmy.flink.warehourse.app.dwm;

import com.alibaba.fastjson.JSON;
import com.wmy.flink.warehourse.bean.OrderWide;
import com.wmy.flink.warehourse.bean.PaymentInfo;
import com.wmy.flink.warehourse.bean.PaymentWide;
import com.wmy.flink.warehourse.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * ClassName:PaymentWideApp
 * Package:com.wmy.flink.warehourse.app.dwm
 *
 * @date:2021/7/23 7:18
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: 订单宽表
 * dwd dwm 也是可以做join，不是所有的指标都是从dwm层出指标
 *
 * kafka、zk、hbase、hdfs、phoenix、redis
 * ---> 业务数据
 * ods_base_db_m
 *
 * ---> 动态分流
 *  ---> Redis 旁路缓存
 *  ---> 线程池、Flink异步查询
 * dwd_order_info
 * dwd_order_detail
 * dwd_payment_info
 *
 * ---> 双流join
 * dwm_order_wide
 * dwm_payment_wide
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.1 设置检查点和状态后端
        // FileSystem#getUnguardedFileSystem(URI)
        //System.setProperty("HADOOP_USER_NAME", "root");
        //FileSystem.getUnguardedFileSystem(new URI("hdfs://yaxin01:9820/flink-realtime-warehourse/dwm_log/ck"));
        //env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(6000L);
        //env.setRestartStrategy(RestartStrategies.noRestart());

        // 2、获取kafka主题：dwd_payment_info dwm_order_wide
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        DataStreamSource<String> paymentKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId));
        DataStreamSource<String> orderWideKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));

        // 3、将数据转换为JavaBean并提取时间戳生成Watermark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentKafkaDS.map(jsonStr -> JSON.parseObject(jsonStr, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                long time = 0;
                                SimpleDateFormat simpleDateFormat;
                                DateTimeFormatter formator = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");;
                                try {
                                    simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                    time = simpleDateFormat.parse(element.getCreate_time()).getTime();

                                    LocalDateTime localDateTime = LocalDateTime.parse(element.getCreate_time(), formator);
                                    long time1 = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
                                    return time;
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException("时间格式错误：" + time); // 这里出现的错误，可以通过过滤的手段来进行完成的
                                }
                            }
                        }));

        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideKafkaDS.map(jsonStr -> JSON.parseObject(jsonStr, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                long time = 0;
                                SimpleDateFormat simpleDateFormat;
                                try {
                                    simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                    time = simpleDateFormat.parse(element.getCreate_time()).getTime();
                                    return time;
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException("时间格式错误：" + time); // 这里出现的错误，可以通过过滤的手段来进行完成的
                                }
                            }
                        }));

        // 4、按照OrderID分组
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedStream = paymentInfoDS.keyBy(PaymentInfo::getOrder_id);
        KeyedStream<OrderWide, Long> orderWideKeyedStream = orderWideDS.keyBy(OrderWide::getOrder_id);

        // 5、双流JOIN
        // join的顺序会影响时间，支付是尽量的往前找订单数据，如果数据join相反和时间也要相反
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedStream.intervalJoin(orderWideKeyedStream)
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });

        // 6、打印测试
        paymentWideDS.print();

        // 7、将数据写入kafka dwm_payment_wide
        // String，这个是sink的类型，所以必须要进行转换
        paymentWideDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        // 8、启动任务
        env.execute("PaymentWideApp >>>> ");
    }
}
