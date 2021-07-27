package com.wmy.flink.warehourse.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wmy.flink.warehourse.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
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
 * ClassName:DayAcitilyUserCountApp
 * Package:com.wmy.flink.warehourse.app.dwm
 *
 * @date:2021/7/17 17:31
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: 统计UV数量
 */
public class DayAcitilyUserCountApp {
    public static void main(String[] args) throws Exception {

        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://yaxin01:9820//flink-realtime-warehourse/dwm/ck"));
        //env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(6000L);

        // 2、读取Kafka dwd_page_log 主题数据创建流
        String groupId = "unqiue_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        // 3、将每行数据转换为JSON对象
        // 这个方法后面还要过滤
        //kafkaDS.map(new MapFunction<String, JSONObject>() {
        //    @Override
        //    public JSONObject map(String value) throws Exception {
        //        try {
        //            return JSONObject.parseObject(value);
        //        } catch (Exception e) {
        //            System.out.println("发现脏数据：" + value);
        //            e.printStackTrace();
        //            return null;
        //        }
        //    }
        //});

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(new OutputTag<String>("dirty") {
                    }, value);
                    e.printStackTrace();
                }
            }
        });

        // 4、按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        keyedStream.print("keyedStream >>>> ");

        // 5、过滤掉不是今天第一次访问的数据
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new UvRichFilterFunction());

        filterDS.print("filterDS >>>> ");
        // 6、写入DWM层kafka主题中
        filterDS.map(JSON::toString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        // 7、启动任务
        env.execute("DayAcitilyUserCountApp >>>> ");

        /**
         * 测试：
         *
         * 数据流：
         *  web/app ---> nginx ---> SpringBoot ---> kafka ---> FlinkApp(LogBaseApp(dwd)) ---> kafka
         *  FlinkApp(DayAcitilyUserCountApp(dwm)) ---> kafka
         * 服务：
         *  Nginx ---> Logger ---> ZK ---> kafka ---> LogBaseApp\DayAcitilyUserCountApp
         *  dwm_unique_visit(消费者)  ---> MockLog(行为数据)
         *
         *  自己本人测试：
         *  kafka生产者：ods_base_log
         *  kafka消费者：dwm_unique_visit/dwd_page_log
         *  开启程序：
         *  LogBaseApp
         *  DayAcitilyUserCountApp
         *
         *  测试数据：
         *
         *  {"common":{"ar":"310000","uid":"177","os":"Android 10.0","ch":"xiaomi","md":"Xiaomi 10 Pro ","mid":"mid_8","vc":"v2.1.134","ba":"Xiaomi"},"page":{"page_id":"good_list","item":"联想","during_time":19041,"item_type":"keyword","last_page_id":""},"ts":1592180030885}
         *
         * {"common":{"ar":"310000","uid":"177","os":"Android 10.0","ch":"xiaomi","md":"Xiaomi 10 Pro ","mid":"mid_9","vc":"v2.1.134","ba":"Xiaomi"},"page":{"page_id":"good_list","item":"联想","during_time":19041,"item_type":"keyword","last_page_id":""},"ts":1592183630}
         *
         *
         */

    }

    /**
     * 过滤掉不是今天第一次访问的数据
     */
    public static class UvRichFilterFunction extends RichFilterFunction<JSONObject> {
        // 声明状态
        private ValueState<String> firstVisitState;
        private SimpleDateFormat simpleDateFormat;

        @Override
        public void open(Configuration parameters) throws Exception {
            simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("visit-state", String.class);

            // 创建状态TTL配置项：这个可以不用写定时器
            StateTtlConfig build = StateTtlConfig.newBuilder(Time.days(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build();
            descriptor.enableTimeToLive(build);
            firstVisitState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public boolean filter(JSONObject value) throws Exception {
            // 取出上一次访问页面
            String lastPageID = value.getJSONObject("page").getString("last_page_id");

            // 判定是否存在上一个页面，没有下一跳
            if (lastPageID == null || lastPageID.length() <= 0) {
                // 取出状态数据
                String firstVisitDate = firstVisitState.value();
                Long ts = value.getLong("ts");
                String currentDate = simpleDateFormat.format(ts);
                if (firstVisitDate == null || firstVisitDate.equals(currentDate)) { // 第一条数据
                    firstVisitState.update(currentDate);
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
    }
}
