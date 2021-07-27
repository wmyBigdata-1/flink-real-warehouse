package com.wmy.flink.warehourse.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wmy.flink.warehourse.bean.TableProcess;
import com.wmy.flink.warehourse.func.DbSplitProcessFunction;
import com.wmy.flink.warehourse.func.DimSink;
import com.wmy.flink.warehourse.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * ClassName:DbBaseApp
 * Package:com.wmy.flink.warehourse.app.dwd
 *
 * @date:2021/7/14 10:07
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: 分析业务数据
 */
public class DbBaseApp {
    public static void main(String[] args) throws Exception {

        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1 checkpointn
        //env.setStateBackend(new FsStateBackend("hdfs://yaxin01:9820/flink-realtime-warehourse/dwd"));
        //env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(3)));
        //env.setRestartStrategy(RestartStrategies.noRestart());

        // 2、读取Kafka数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource("ods_base_db_m", "ods_base_db_m");
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        // 3、将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        // 4、过滤
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 获取一个data字段{"data":{"id":30,"user_name":"zhangsan","tel":"19845368388"}}
                String data = value.getString("data");
                return data != null && data.length() > 0;
            }
        });

        // 打印测试
        /**
         * 测试说明，由于我没有采集那些的数据，没有实时采集MySQL中的数据到kafka中，所以我是直接生产到kafka中
         *
         * 数据：
         * {"database":"gmall","xid":111,"data":{"id":30,"user_name":"zhangsan","tel":"19845368388"}}
         */
        filterDS.print();

        // 5、分流，ProcessFunction，如果使用状态的可以使用复合函数
        // 写一个工具类专门读MySQL中的数据
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };
        SingleOutputStreamOperator<JSONObject> kafkaJsonDS = filterDS.process(new DbSplitProcessFunction(hbaseTag));

        // 6、取出分流输出将数据写入kafka或者phonix
        DataStream<JSONObject> hbaseJsonDS = kafkaJsonDS.getSideOutput(hbaseTag);

        // 取出分流输出将数据写入hbase中
        hbaseJsonDS.print("hbase >>>> ");
        hbaseJsonDS.addSink(new DimSink());

        // 取出的分流输出将数据写入kafka中：在发送的时候指定主题，如果sink_table是一个null值，就会发送给那个主题
        kafkaJsonDS.print("kafka >>>> ");
        FlinkKafkaProducer<JSONObject> kafkaSinkBySchema = MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("开始序列化kafka数据！！！");
            }

            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(jsonObject.getString("sink_table"), jsonObject.getString("data").getBytes());
            }
        });
        kafkaJsonDS.addSink(kafkaSinkBySchema);

        // 7、执行任务
        env.execute("DbBaseApp >>>>>>> ");
    }
}
