package com.wmy.flink.warehourse.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wmy.flink.warehourse.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * ClassName:LogBaseApp
 * Package:com.wmy.flink.warehourse.app.dwd
 *
 * @date:2021/7/13 16:22
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: 接收Kafka的数据，并进行转换
 * 1) 封装操作Kafka的工具类，并提供获取Kafka消费者的方法
 * 测试Kafka数据读取问题
 *  a）启动集群：Zookeeper\Kafka\HDFS
 *  b）启动Kafka生产者：kafka-console-producer.sh --broker-list yaxin01:9092 --topic ods_base_log
 *  c）测试数据：{"common":{"ar":"110000","uid":"7","os":"Android 11.0","ch":"xiaomi","is_new":"1","md":"Xiaomi 9","mid":"mid_41","vc":"v2.1.132","vc":"v2.1.134","ba":"Xiaomi"},"start":{"entry":"icon","loading_time":7815,"open_ad_id":8,"open_ad_ms":7872,"open_ad_skip_ms":6115},"ts":1592175234000}
 */
public class LogBaseApp {
    public static void main(String[] args) throws Exception {

        // 注意：会出现一个没有权限的错误
        System.setProperty("HADOOP_USER_NAME", "root");

        // 1、获取执行环境，设置并行度，设置状态后端（HDFS）、checkpoint，local模式是不能设置状态恢复的
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // kafka topic partitions nums

        // 1.1 设置状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://yaxin01:9820/flink-realtime-warehourse/dwd"));

        // 1.2 开启CheckPoint的
        //env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE); // 每隔多少时间开启checkpoint
        //env.getCheckpointConfig().setCheckpointTimeout(60000L); // 超时时间为一分钟

        // 1.3 最好是开启重启策略
        // 要不然会出现无限重启的情况
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(1)));
        //env.setRestartStrategy(RestartStrategies.noRestart());

        // 2、读取kafka_ods_base_log 主题数据
        // 2.1 封装一个消费Kafka的工具类
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource("ods_base_log", "ods_base_log");
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        // 3、将每行数据转换为JsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.map(JSON::parseObject);

        // 4、按照ID分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        // 5、使用状态做新老用户校验 ---> 使用复合函数
        SingleOutputStreamOperator<JSONObject> jsonWithNewFlagDS = keyedStream.map(new NewMidRichMapFunc());

        // 打印测试
        //jsonWithNewFlagDS.print();

        // 6、分流，使用ProcessFunction将ODS数据拆分成启动、曝光以及页面数据
        SingleOutputStreamOperator<String> pageDS = jsonWithNewFlagDS.process(new SpiltProcessFunc());

        // 7、将三个流的数据写入到对应的Kafka主题
        DataStream<String> startDS = pageDS.getSideOutput(new OutputTag<String>("start") {
        });
        DataStream<String> displayDS = pageDS.getSideOutput(new OutputTag<String>("display") {
        });

        // 打印测试
        pageDS.print("page >>>>>>> ");
        startDS.print("start >>>>>>> ");
        displayDS.print("display >>>>>>> ");

        pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        // 8、执行任务
        env.execute("read kafka source write kafka topic ... ");
    }

    /**
     * 做一个字段的校验
     */
    public static class NewMidRichMapFunc extends RichMapFunction<JSONObject,JSONObject> {
        // 声明状态，用于表示当前mid是否以及访问过
        private ValueState<String> firstVisitDateState;
        private SimpleDateFormat simpleDateFormat;
        @Override
        public void open(Configuration parameters) throws Exception {
            firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("new-mid", String.class));
            simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        }
        @Override
        public JSONObject map(JSONObject value) throws Exception {
            // 取出新用户标记
            String isNew = value.getJSONObject("common").getString("is_new");
            // 如果当前前端传输数据表示为新用户，则进行校验
            if ("1".equals(isNew)) {
                // 取出状态数据，并取出当前访问时间
                String firstDate = firstVisitDateState.value();
                Long ts = value.getLong("ts");

                // 判断状态数据是否为null
                if (firstDate != null) {
                    // 修复
                    value.getJSONObject("common").put("is_new", 0);
                } else {
                    // 更新数据
                    firstVisitDateState.update(simpleDateFormat.format(ts));
                }
            }
            return value;
        }
    }

    // 将数据分流到kafka中
    public static class SpiltProcessFunc extends ProcessFunction<JSONObject,String>{
        @Override
        public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
            // 提前"start字段"
            String startStr = value.getString("start");
            // 判断是否为启动数据
            if (startStr != null && startStr.length() > 0) {
                // 将启动日志输出到侧输出流
                ctx.output(new OutputTag<String>("start"){}, value.toString());
            } else {
                // 不是启动日志，继续判断是否是曝光数据
                JSONArray displays = value.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    // 曝光数据，是一个数组类型的，遍历写入侧输出流
                    for (int i = 0; i < displays.size(); i++) {
                        // 取出单条曝光数据
                        JSONObject displayJson = displays.getJSONObject(i);
                        // 添加页面ID
                        displayJson.put("page_id", value.getJSONObject("page").getString("page_id"));
                        // 输出到侧输出流中
                        ctx.output(new OutputTag<String>("display"){}, displayJson.toString());
                    }
                } else {
                    // 页面数据，将数据输出到主流，主流使用collect
                    out.collect(value.toString());
                }
            }
        }
    }
}
