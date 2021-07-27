package com.wmy.flink.warehourse.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wmy.flink.warehourse.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * ClassName:UserJumpDetailApp
 * Package:com.wmy.flink.warehourse.app.dwm
 *
 * @date:2021/7/19 9:18
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: 跳出率：需要通过page_log行为判断
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {

        // 1、创建流式环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://yaxin01:9820//flink-realtime-warehourse/dwm/ck"));
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(6000L);

        // 2、读取kafka dwd_page_log 主题数据创建流
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumDetailApp";
        String sinkTopic = "dwm_user_jump_detail";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        // 3.1 设置Watermark
        WatermarkStrategy<JSONObject> watermarkStrategy = WatermarkStrategy
                .<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                });

        // 3、将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObejctDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                    ctx.output(new OutputTag<String>("dirty") {
                    }, value);
                }
            }
        }).assignTimestampsAndWatermarks(watermarkStrategy);

        // 4、按照Mid进行分区
        KeyedStream<JSONObject, String> keyedStream = jsonObejctDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // 5、定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 取出上一条页面信息
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).followedBy("follow").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 取出上一条页面信息
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId != null && lastPageId.length() > 0;
            }
        }).within(Time.seconds(10));


        // 6、将模式序列作用在流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // 7、 提取事件和超时事件
        OutputTag<String> timeOutTag = new OutputTag<String>("timeOut") {
        };
        SingleOutputStreamOperator<Object> selectDS = patternStream.flatSelect(timeOutTag,
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                        out.collect(pattern.get("start").get(0).toJSONString());
                    }
                },
                new PatternFlatSelectFunction<JSONObject, Object>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<Object> out) throws Exception {
                        // 主流逻辑当中什么也不用写 。。。。
                    }
                });

        // 8、将数据写入到kafka中
        FlinkKafkaProducer<String> kafkaSink = MyKafkaUtil.getKafkaSink(sinkTopic);
        selectDS.getSideOutput(timeOutTag).addSink(kafkaSink);

        // 9、执行程序
        env.execute("UserJumpDetailApp >>>> ");
    }
}
